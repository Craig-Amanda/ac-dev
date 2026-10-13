/**
 * Record tools: read one or many records under the app's read policy, follow
 * connections, aggregate, verify field shapes, and create/update/delete in batches.
 */
import fs from 'node:fs';
import path from 'node:path';

import { z } from 'zod';

import type { AppConfig } from '../config.js';
import {
    BATCH_CONCURRENCY,
    DEFAULT_API_BASE,
    HISTORY_BUDGET_MS,
} from '../config.js';
import type { KnackContext } from '../context.js';
import { type KnackApiResult, knackFetchJson } from '../http.js';
import { getCacheEntry } from '../lib/cache.js';
import { readAppTimeZone, timeZoneNote } from '../lib/date-field-defaults.js';
import {
    type DateReading,
    checkStoredDate,
    describeDateRefusal,
    readDateValue,
} from '../lib/date-values.js';
import { parseJsonObjectInput } from '../lib/field-payload.js';
import { describeWriteBlock } from '../lib/field-exclusion.js';
import {
    changedHistoryFields,
    describeHistoryOrigin,
    formulaFieldKeys,
    historyFieldKeys,
    historyUserMatches,
    historyValue,
    isBlankHistoryValue,
    sortHistoryNewestFirst,
} from '../lib/record-history.js';
import { getFieldShapeInfo } from '../lib/field-shapes.js';
import { getValuePreview, validateFieldShape } from '../lib/record-shapes.js';
import { asRecord, describeError, runWithConcurrency } from '../lib/util.js';
import {
    applyRecordReadPolicy,
    bucketDate,
    buildRecordSearchParams,
    getDefaultPermittedFieldKeys,
    getFilterFieldKeys,
    getNumericValue,
    getPermittedReadFields,
    getRecordMasks,
    getRecordsFromResponse,
    projectRecordFields,
    readPolicyApplies,
    validateReadQuery,
} from '../records.js';
import { type AnyToolDef, defineTool } from '../registry.js';
import type { HistoryPage, HistoryPageFetch } from '../types.js';
import { makeTextResponse } from '../response.js';

const RAW_FIELD_TIP =
    'Prefer field_xxx_raw for connections/dates — see knack_describe_field_shape.';

/**
 * Confirmed live (2026-09-23): Knack's `q=` free-text search only matches fields the
 * object has explicitly marked searchable in the Builder, and has been observed to come
 * back with the unfiltered first page and no error when nothing on the object qualifies
 * — there is nothing in the response that distinguishes "q genuinely matched everything"
 * from "q was silently ignored". `total_records` is the only signal available; a caller
 * suspicious of a q result should cross-check with an equivalent `filters` rule instead.
 */
const Q_UNRELIABLE_NOTE =
    "Knack's q= search only matches fields marked searchable on this object and can silently return the unfiltered result set when none do — nothing in the response distinguishes that from a genuine match. If total_records looks too high for this query, retry with filters instead.";

const filtersInput = z
    .union([z.string(), z.record(z.string(), z.unknown())])
    .optional()
    .describe('Knack filters object or JSON string.');

export const getRecord = defineTool({
    name: 'knack_get_record',
    description: 'Fetch one record by object key and record id.',
    access: 'read',
    input: {
        appKey: z.string().optional(),
        objectKey: z.string(),
        recordId: z.string(),
    },
    handler: async ({ appKey, objectKey, recordId }, ctx) => {
        const app = ctx.getApp(appKey);
        await getPermittedReadFields(ctx, app, objectKey, []);
        const result = await ctx.request(
            app,
            `/objects/${objectKey}/records/${recordId}`,
        );
        const safeResult = await applyRecordReadPolicy(
            ctx,
            app,
            objectKey,
            result,
        );
        return makeTextResponse({
            appKey: app.appKey,
            ...safeResult,
            ...(safeResult.ok ? { tip: RAW_FIELD_TIP } : {}),
        });
    },
});

/*
 * Measured on Powderpigs object_8 (2,680 entries, 12 October 2026): page 1 answers in
 * 7 s, page 2 in 22 s, page 3 in 50 s, a 100-row page 1 in 104 s, and pages 50 and 107
 * not within 120 s. Five rows at offset 50 still take 27 s and at offset 100 take
 * 100 s, so the cost is the offset, not the row count: a long history can only be read
 * from its newest end, a few pages per call, within HISTORY_BUDGET_MS.
 */
/** Below this much budget a page fetch is not started for this call alone. */
const HISTORY_MIN_FETCH_MS = 3_000;
/** How long a page fetch may run in the background before it is given up. */
const HISTORY_FETCH_CAP_MS = 10 * 60 * 1000;
/** Pages one call reads at most, whatever the budget allows. */
const HISTORY_MAX_PAGES_PER_CALL = 10;
/**
 * How long a fetched page is kept. Written entries do not change, so a page is good for
 * a whole investigation; the one hazard, a new save shifting every page by one, is
 * caught by comparing `total_records` on each live fetch.
 */
const HISTORY_CACHE_TTL_MS = 30 * 60 * 1000;
const HISTORY_RETRY_NOTE =
    'History reads only read, so calling again is safe; pages already read are cached for 30 minutes.';

type HistoryFetch =
    | { ok: true; page: HistoryPage; fromCache: boolean }
    /** The budget ran out; `pending` when the fetch carries on in the background. */
    | { ok: false; reason: 'budget'; pending: boolean }
    | { ok: false; reason: 'upstream'; status: number; body: unknown };

/** Whether a caught fetch failure is the abort from our own cap. */
function isAbort(error: unknown): boolean {
    const name = (error as { name?: unknown } | null)?.name;
    return name === 'AbortError' || name === 'TimeoutError';
}

/**
 * Start fetching one history page and, when it lands, cache it. The promise is kept in
 * `ctx.historyInFlight` until it settles, so a later call for the page joins it. A live
 * page whose `total_records` differs from the cached pages' means a save landed in
 * between and every page boundary moved, so the record's cached pages are dropped.
 */
function startHistoryFetch(
    ctx: KnackContext,
    app: AppConfig,
    objectKey: string,
    recordId: string,
    pageNumber: number,
): Promise<HistoryPageFetch> {
    const prefix = `${app.appKey}|${objectKey}|${recordId}|`;
    const key = `${prefix}${pageNumber}`;
    const fetching = (async (): Promise<HistoryPageFetch> => {
        let result: KnackApiResult;
        try {
            result = await ctx.requestWithRetry(
                app,
                `/applications/${app.appId}/objects/${encodeURIComponent(objectKey)}/record-history/${encodeURIComponent(recordId)}?page=${pageNumber}`,
                { signal: AbortSignal.timeout(HISTORY_FETCH_CAP_MS) },
            );
        } catch (error) {
            if (isAbort(error)) return { ok: false, aborted: true };
            throw error;
        }
        const body = asRecord(result.body);
        if (!result.ok || !body) {
            return { ok: false, status: result.status, body: result.body };
        }
        const page: HistoryPage = {
            entries: sortHistoryNewestFirst(
                (Array.isArray(body.records) ? body.records : [])
                    .map(asRecord)
                    .filter((entry): entry is Record<string, unknown> =>
                        Boolean(entry),
                    ),
            ),
            totalPages: Number(body.total_pages) || 1,
            totalRecords: Number(body.total_records) || 0,
        };
        for (const [cachedKey, entry] of ctx.historyPages) {
            if (
                cachedKey.startsWith(prefix) &&
                entry.value.totalRecords !== page.totalRecords
            )
                ctx.historyPages.delete(cachedKey);
        }
        const loadedAt = Date.now();
        ctx.historyPages.set(key, {
            value: page,
            source: 'runtime',
            loadedAt,
            expiresAt: loadedAt + HISTORY_CACHE_TTL_MS,
        });
        return { ok: true, page };
    })();
    ctx.historyInFlight.set(key, fetching);
    const forget = () => {
        if (ctx.historyInFlight.get(key) === fetching)
            ctx.historyInFlight.delete(key);
    };
    fetching.then(forget, forget);
    return fetching;
}

/**
 * One page of a record's history: from the cache, or from a fetch (already running or
 * started here) that answers before `deadline`. Past the deadline the fetch is left
 * running for the next call, and the caller is told so.
 */
async function fetchHistoryPage(
    ctx: KnackContext,
    app: AppConfig,
    objectKey: string,
    recordId: string,
    pageNumber: number,
    deadline: number,
): Promise<HistoryFetch> {
    const key = `${app.appKey}|${objectKey}|${recordId}|${pageNumber}`;
    const cached = getCacheEntry(ctx.historyPages, key);
    if (cached) return { ok: true, page: cached.value, fromCache: true };

    const remaining = deadline - Date.now();
    const running = ctx.historyInFlight.get(key);
    if (!running && remaining < HISTORY_MIN_FETCH_MS)
        return { ok: false, reason: 'budget', pending: false };
    const fetching =
        running ?? startHistoryFetch(ctx, app, objectKey, recordId, pageNumber);

    let timer: NodeJS.Timeout | undefined;
    const budget = new Promise<'budget'>((resolve) => {
        timer = setTimeout(() => resolve('budget'), Math.max(0, remaining));
    });
    const outcome = await Promise.race([fetching, budget]).finally(() =>
        clearTimeout(timer),
    );
    if (outcome === 'budget') {
        return {
            ok: false,
            reason: 'budget',
            pending: ctx.historyInFlight.get(key) === fetching,
        };
    }
    if (outcome.ok) return { ok: true, page: outcome.page, fromCache: false };
    if ('aborted' in outcome)
        return { ok: false, reason: 'budget', pending: false };
    return {
        ok: false,
        reason: 'upstream',
        status: outcome.status,
        body: outcome.body,
    };
}

/** An ISO timestamp input as epoch ms, or a thrown Error naming the parameter. */
function parseHistoryTime(
    label: string,
    value: string | undefined,
): number | null {
    if (value === undefined) return null;
    const ms = Date.parse(value);
    if (Number.isNaN(ms)) {
        throw new Error(
            `${label} must be an ISO 8601 date or timestamp, e.g. 2026-09-27 or 2026-09-27T22:00:00Z; got "${value}".`,
        );
    }
    return ms;
}

/**
 * A record's change history, walking the feed a page at a time until it has
 * `maxEntries` entries worth reporting, reaches the end (or the `from` bound), or runs
 * out of time. Pages are cached, and `nextPage` resumes a walk that stopped early.
 *
 * Formula fields and the daily "Record Update: Formula" saves are what fill a busy
 * record's history, so formula fields are left out of the comparison unless asked for;
 * `excludeFields` and `excludeUsers` drop whatever else is noise on the record.
 */
export const getRecordHistory = defineTool({
    name: 'knack_get_record_history',
    description:
        "A record's change history: who changed which fields, when and from where; walks pages within a time budget.",
    access: 'read',
    input: {
        appKey: z.string().optional(),
        objectKey: z.string(),
        recordId: z.string(),
        page: z
            .number()
            .int()
            .min(1)
            .optional()
            .describe('Page to start from; pass nextPage to resume'),
        order: z
            .enum(['desc', 'asc'])
            .default('desc')
            .describe('desc: newest first; asc: from the oldest page'),
        maxEntries: z
            .number()
            .int()
            .min(1)
            .max(200)
            .default(25)
            .describe('Stop reading pages once this many are gathered'),
        from: z.string().optional().describe('ISO date; no entries before'),
        to: z.string().optional().describe('ISO date; no entries after'),
        mode: z
            .enum(['changes', 'full'])
            .default('changes')
            .describe('changes: fields each save altered; full: every value'),
        fields: z
            .array(z.string())
            .optional()
            .describe('Only report these field keys'),
        excludeFields: z.array(z.string()).optional(),
        excludeUsers: z
            .array(z.string())
            .optional()
            .describe('Skip saves by users matching any (substring)'),
        includeFormulaFields: z
            .boolean()
            .default(false)
            .describe('Report formula fields too (noise by default)'),
        compact: z
            .boolean()
            .default(false)
            .describe('Entries carry only at, user and changes'),
    },
    handler: async (
        {
            appKey,
            objectKey,
            recordId,
            page: startPage,
            order,
            maxEntries,
            from,
            to,
            mode,
            fields,
            excludeFields,
            excludeUsers,
            includeFormulaFields,
            compact,
        },
        ctx,
    ) => {
        const startedAt = Date.now();
        const deadline = startedAt + HISTORY_BUDGET_MS;
        const app = ctx.getApp(appKey);
        const fromMs = parseHistoryTime('from', from);
        const toMs = parseHistoryTime('to', to);
        if (fromMs !== null && toMs !== null && fromMs > toMs) {
            throw new Error('from must not be later than to.');
        }
        const requested = (fields ?? []).map((key) => key.trim());
        const permitted = await getPermittedReadFields(
            ctx,
            app,
            objectKey,
            requested,
        );
        const exclusions = permitted.exclusions;
        // A field asked for by name is wanted even when it is a formula or excluded.
        const ignored = new Set(
            [
                ...(includeFormulaFields
                    ? []
                    : formulaFieldKeys(permitted.object.fields)),
                ...(excludeFields ?? []).map((key) => key.trim()),
            ].filter((key) => !requested.includes(key)),
        );
        const userPatterns = (excludeUsers ?? []).filter((p) => p.trim());

        const policed = readPolicyApplies(app, exclusions, objectKey);
        const defaultAllowed = policed
            ? new Set(
                  getDefaultPermittedFieldKeys(
                      app,
                      objectKey,
                      permitted.object,
                      exclusions,
                  ),
              )
            : null;
        const reportable = (entry: Record<string, unknown>) =>
            historyFieldKeys(entry).filter(
                (key) =>
                    (!defaultAllowed || defaultAllowed.has(key)) &&
                    !ignored.has(key) &&
                    (!requested.length || requested.includes(key)),
            );
        const masks = policed ? getRecordMasks(exclusions, objectKey) : {};
        const valueOf = (entry: Record<string, unknown>, key: string) =>
            historyValue(projectRecordFields(entry, [key], masks), key);
        const timeOf = (entry: Record<string, unknown>) =>
            typeof entry.timestamp === 'string'
                ? Date.parse(entry.timestamp)
                : NaN;

        let skippedNoChange = 0;
        let skippedByUser = 0;
        let skippedOutsideWindow = 0;
        /** An entry's reply shape minus its values, or null when it is filtered out. */
        const admit = (
            entry: Record<string, unknown>,
        ): Record<string, unknown> | null => {
            // An entry with no timestamp is the pre-history baseline, not a save.
            if (typeof entry.timestamp !== 'string') return null;
            const at = timeOf(entry);
            if (
                (fromMs !== null && at < fromMs) ||
                (toMs !== null && at > toMs)
            ) {
                skippedOutsideWindow += 1;
                return null;
            }
            if (historyUserMatches(entry.user, userPatterns)) {
                skippedByUser += 1;
                return null;
            }
            return compact
                ? { at: entry.timestamp, user: entry.user }
                : {
                      id: entry.id,
                      at: entry.timestamp,
                      operation: entry.operation,
                      user: entry.user,
                      via: describeHistoryOrigin(entry),
                      transactionId: entry.transaction_id,
                  };
        };
        /** One entry shaped for the reply, or null when it is not worth reporting. */
        const shape = (
            entry: Record<string, unknown>,
            older: Record<string, unknown> | undefined,
        ): Record<string, unknown> | null => {
            const base = admit(entry);
            if (!base) return null;
            const keys = reportable(entry);
            if (mode === 'full') {
                const values: Record<string, unknown> = {};
                for (const key of keys) {
                    if (!isBlankHistoryValue(historyValue(entry, key)))
                        values[key] = valueOf(entry, key);
                }
                return { ...base, values };
            }
            const changes: Record<string, { from?: unknown; to: unknown }> = {};
            for (const key of changedHistoryFields(entry, older, keys)) {
                changes[key] = {
                    ...(older && !isBlankHistoryValue(historyValue(older, key))
                        ? { from: valueOf(older, key) }
                        : {}),
                    to: valueOf(entry, key),
                };
            }
            if (!Object.keys(changes).length) {
                skippedNoChange += 1;
                return null;
            }
            return { ...base, changes };
        };

        const pages = new Map<number, HistoryPage>();
        /** Pages whose entries are in the reply, in the order they were shaped. */
        const pagesRead: number[] = [];
        let fromCache = 0;
        let totalPages = 0;
        let totalRecords = 0;
        // One object rather than two lets: `read` below assigns them from a closure,
        // which TypeScript's narrowing of a let would not see.
        const stop: {
            by:
                | 'maxEntries'
                | 'timeBudget'
                | 'pageCap'
                | 'upstreamError'
                | null;
            upstream: { status: number; body: unknown } | null;
            /** A page still being fetched in the background when the call ended. */
            pendingPage: number | null;
        } = { by: null, upstream: null, pendingPage: null };

        const read = async (
            pageNumber: number,
        ): Promise<HistoryPage | null> => {
            const known = pages.get(pageNumber);
            if (known) return known;
            const fetched = await fetchHistoryPage(
                ctx,
                app,
                objectKey,
                recordId,
                pageNumber,
                deadline,
            );
            if (!fetched.ok) {
                if (fetched.reason === 'upstream') {
                    stop.by = 'upstreamError';
                    stop.upstream = {
                        status: fetched.status,
                        body: fetched.body,
                    };
                } else {
                    stop.by = 'timeBudget';
                    if (fetched.pending) stop.pendingPage = pageNumber;
                }
                return null;
            }
            pages.set(pageNumber, fetched.page);
            if (fetched.fromCache) fromCache += 1;
            totalPages = fetched.page.totalPages;
            totalRecords = fetched.page.totalRecords;
            return fetched.page;
        };

        let uncompared = 0;
        // Entries of page N are compared with the first entry of page N+1, so a page is
        // shaped once its older neighbour is in hand (or it is the last page). When that
        // neighbour could not be read, the page's oldest entry is still listed, with
        // `changes: null`: dropping it could hide the one save being looked for.
        const shapePage = (
            pageNumber: number,
            olderNeighbour: Record<string, unknown> | undefined,
            neighbourKnown: boolean,
        ): Record<string, unknown>[] => {
            const entries = pages.get(pageNumber)!.entries;
            pagesRead.push(pageNumber);
            return entries.flatMap((entry, index) => {
                const last = index + 1 >= entries.length;
                if (last && !neighbourKnown && mode === 'changes') {
                    const base = admit(entry);
                    if (!base) return [];
                    uncompared += 1;
                    return [{ ...base, changes: null, uncompared: true }];
                }
                const shaped = shape(
                    entry,
                    last ? olderNeighbour : entries[index + 1],
                );
                return shaped ? [shaped] : [];
            });
        };

        let first = startPage ?? 1;
        let aborted = false;
        if (order === 'asc' && startPage === undefined) {
            // The oldest page is the last one, and only page 1 says how many there are.
            if (await read(1)) first = totalPages;
            else aborted = true;
        }

        const shaped: Record<string, unknown>[] = [];
        const step = order === 'asc' ? -1 : 1;
        let current = first;
        /** desc: the page read but not yet shaped, waiting for its older neighbour. */
        let unshaped: number | null = null;
        /** The from (desc) or to (asc) bound was reached: the walk is finished. */
        let bounded = false;
        let pagesThisCall = 0;
        while (
            !aborted &&
            current >= 1 &&
            (totalPages === 0 || current <= totalPages)
        ) {
            if (pagesThisCall >= HISTORY_MAX_PAGES_PER_CALL) {
                stop.by = 'pageCap';
                break;
            }
            const page = await read(current);
            if (!page) break;
            pagesThisCall += 1;
            if (order === 'asc') {
                // The older neighbour (page N+1) was read before page N, or is the end.
                const neighbour =
                    current < totalPages
                        ? (pages.get(current + 1) ?? (await read(current + 1)))
                        : undefined;
                // Pages arrive oldest first here, so each goes after the last.
                shaped.push(
                    ...shapePage(
                        current,
                        neighbour?.entries[0],
                        current === totalPages || Boolean(neighbour),
                    ).reverse(),
                );
                if (current < totalPages && !neighbour) break;
                if (
                    toMs !== null &&
                    page.entries.some((entry) => timeOf(entry) > toMs)
                ) {
                    bounded = true;
                    break;
                }
            } else {
                if (unshaped !== null)
                    shaped.push(...shapePage(unshaped, page.entries[0], true));
                unshaped = current;
                if (current === totalPages) {
                    shaped.push(...shapePage(current, undefined, true));
                    unshaped = null;
                }
                if (
                    fromMs !== null &&
                    page.entries.some((entry) => timeOf(entry) < fromMs)
                ) {
                    bounded = true;
                    break;
                }
            }
            if (shaped.length >= maxEntries) {
                stop.by = 'maxEntries';
                break;
            }
            current += step;
        }

        // A desc walk that stopped mid-history still owes its last page's entries, which
        // need the next page's first entry. Try for it in the time left; the walk's own
        // reason for stopping stands if this extra read is what runs out of time.
        // A page left for the resume instead is cached, so the resume costs nothing.
        let resumeAt: number | null = null;
        if (
            unshaped !== null &&
            (stop.by === 'maxEntries' || stop.by === 'pageCap')
        ) {
            // Enough was gathered without this page, so it waits for the resume rather
            // than cost another read for its neighbour.
            resumeAt = unshaped;
        } else if (unshaped !== null) {
            const oldest = pages.get(unshaped)!.entries.at(-1);
            if (bounded && oldest && timeOf(oldest) < fromMs!) {
                // The oldest entry is before `from`, so it is filtered out uncompared.
                shaped.push(...shapePage(unshaped, undefined, true));
            } else {
                const stoppedBy = stop.by;
                const next = await read(unshaped + 1);
                if (stoppedBy) stop.by = stoppedBy;
                shaped.push(
                    ...shapePage(unshaped, next?.entries[0], Boolean(next)),
                );
            }
        }

        const elapsedMs = Date.now() - startedAt;
        if (!pagesRead.length) {
            if (stop.upstream) {
                return makeTextResponse({
                    appKey: app.appKey,
                    ok: false,
                    ...stop.upstream,
                });
            }
            return makeTextResponse({
                appKey: app.appKey,
                ok: false,
                error: 'HISTORY_TIMEOUT',
                objectKey,
                recordId,
                page: first,
                elapsedMs,
                ...(stop.pendingPage !== null
                    ? { pendingPage: stop.pendingPage }
                    : {}),
                message: `Knack did not return history page ${first} within ${Math.round(HISTORY_BUDGET_MS / 1000)} s.${stop.pendingPage !== null ? ' The fetch carries on in the background: call again in a minute or so and it is picked up from the cache.' : ''} ${HISTORY_RETRY_NOTE} Deep pages of a long history take longer the deeper they are; read from the newest end where possible.`,
            });
        }

        const lastRead =
            order === 'asc' ? Math.min(...pagesRead) : Math.max(...pagesRead);
        const reachedEnd =
            order === 'asc' ? lastRead === 1 : lastRead === totalPages;
        // Finished when the walk reached the end or a bound with nothing pending; a
        // maxEntries stop on the last page is finished too.
        const complete =
            resumeAt === null &&
            stop.by !== 'timeBudget' &&
            stop.by !== 'upstreamError' &&
            (reachedEnd || bounded);
        const nextPage = complete
            ? null
            : (resumeAt ?? (order === 'asc' ? lastRead - 1 : lastRead + 1));
        const pendingNote =
            stop.pendingPage !== null
                ? ` Page ${stop.pendingPage} is still being fetched in the background: call again in a minute or so and it is picked up from the cache.`
                : '';

        return makeTextResponse({
            appKey: app.appKey,
            ok: true,
            objectKey,
            recordId,
            order,
            page: first,
            pagesRead,
            totalPages,
            totalRecords,
            mode,
            entries: shaped,
            entryCount: shaped.length,
            ...(skippedNoChange ? { skippedNoChange } : {}),
            ...(skippedByUser ? { skippedByUser } : {}),
            ...(skippedOutsideWindow ? { skippedOutsideWindow } : {}),
            ...(ignored.size ? { fieldsExcluded: ignored.size } : {}),
            complete,
            ...(nextPage !== null ? { nextPage } : {}),
            ...(stop.by ? { stoppedBy: stop.by } : {}),
            ...(stop.pendingPage !== null
                ? { pendingPage: stop.pendingPage }
                : {}),
            ...(stop.upstream ? { upstream: stop.upstream } : {}),
            ...(fromCache ? { pagesFromCache: fromCache } : {}),
            elapsedMs,
            ...(uncompared
                ? {
                      uncompared,
                      note: `The next page did not arrive in time, so the oldest entry on page ${lastRead} is listed with changes: null rather than compared with its predecessor. Re-read page ${lastRead} (cached) once the next page answers.${stop.by === 'timeBudget' ? ` Stopped at the ${Math.round(HISTORY_BUDGET_MS / 1000)} s budget.` : ''}${pendingNote} ${HISTORY_RETRY_NOTE}`,
                  }
                : stop.by === 'timeBudget'
                  ? {
                        note: `Stopped at the ${Math.round(HISTORY_BUDGET_MS / 1000)} s budget. Resume with page ${nextPage}.${pendingNote} ${HISTORY_RETRY_NOTE}`,
                    }
                  : {}),
            tip: `${order === 'asc' ? 'Oldest' : 'Newest'} first. Field keys only: resolve names with knack_get_field.`,
        });
    },
});

export const findRecords = defineTool({
    name: 'knack_find_records',
    description:
        'Search records with filters, paging and sorting; optionally with the object schema.',
    access: 'read',
    input: {
        appKey: z.string().optional(),
        objectKey: z.string(),
        page: z.number().int().min(1).default(1),
        rowsPerPage: z.number().int().min(1).max(1000).default(25),
        q: z
            .string()
            .optional()
            .describe(
                'Free text search (q=); only matches fields marked searchable on this object in the Builder — prefer filters for a reliable exact match',
            ),
        filters: filtersInput,
        sortField: z
            .string()
            .optional()
            .describe('Field key to sort by, e.g. field_66'),
        sortOrder: z.enum(['asc', 'desc']).optional(),
        fields: z
            .array(z.string())
            .optional()
            .describe(
                'Project each record down to only these field keys (plus id and any _raw counterpart) — trims a wide object down to what you actually need',
            ),
        includeSchema: z
            .boolean()
            .optional()
            .describe('Also return the object field schema.'),
    },
    handler: async (
        {
            appKey,
            objectKey,
            page,
            rowsPerPage,
            q,
            filters,
            sortField,
            sortOrder,
            fields,
            includeSchema,
        },
        ctx,
    ) => {
        const app = ctx.getApp(appKey);
        const maxRecords = await validateReadQuery(ctx, app, objectKey, {
            filters,
            q,
            sortField,
        });
        const params = buildRecordSearchParams({
            page,
            rowsPerPage: Math.min(rowsPerPage, maxRecords),
            q,
            filters,
            sortField,
            sortOrder,
        });

        const result = await ctx.request(
            app,
            `/objects/${objectKey}/records?${params.toString()}`,
        );
        const safeResult = await applyRecordReadPolicy(
            ctx,
            app,
            objectKey,
            result,
            fields,
        );

        const base = {
            appKey: app.appKey,
            ...safeResult,
            ...(q ? { qUsed: q, qNote: Q_UNRELIABLE_NOTE } : {}),
            ...(safeResult.ok ? { tip: RAW_FIELD_TIP } : {}),
        };
        if (!includeSchema) return makeTextResponse(base);

        const schemaResult = await ctx.getSchema(app);
        const object =
            schemaResult.schema?.objects?.find(
                (entry) => entry.key === objectKey,
            ) || null;

        return makeTextResponse({
            ...base,
            // The records fetch can succeed while the schema half fails to resolve the
            // object (a stale cache, a bad objectKey) — ok reflects both halves, as it
            // did before the two tools this one replaces were merged, so a caller that
            // only checks ok does not miss a schema failure sitting under it.
            ok: base.ok && Boolean(object),
            objectKey,
            objectName: object?.name || null,
            schemaSource: schemaResult.source,
            schemaAvailable: Boolean(object),
            schemaMessage: object
                ? null
                : schemaResult.schema?.objects?.length
                  ? `Object not found in schema: ${objectKey}`
                  : 'No schema available from runtime API or schema.json.',
            schema: object
                ? {
                      key: object.key,
                      name: object.name,
                      fieldCount: (object.fields || []).length,
                      fields: (object.fields || []).map((field) => ({
                          key: field.key,
                          name: field.name,
                          type: field.type,
                          required: field.required,
                          description: field.description,
                      })),
                  }
                : null,
        });
    },
});

export const getRelatedRecords = defineTool({
    name: 'knack_get_related_records',
    description:
        'Fetch approved fields from records connected to a record, forward or in reverse.',
    access: 'read',
    input: {
        appKey: z.string().optional(),
        sourceObjectKey: z.string(),
        sourceRecordId: z.string(),
        direction: z.enum(['forward', 'reverse']),
        connectionFieldKey: z
            .string()
            .describe(
                'On the source object (forward) or related object (reverse).',
            ),
        relatedObjectKey: z
            .string()
            .optional()
            .describe('Required for reverse lookups.'),
        fieldKeys: z
            .array(z.string())
            .min(1)
            .max(50)
            .describe('Approved fields to return.'),
        limit: z.number().int().min(1).max(100).default(25),
        sortField: z.string().optional(),
        sortOrder: z.enum(['asc', 'desc']).optional(),
    },
    handler: async (
        {
            appKey,
            sourceObjectKey,
            sourceRecordId,
            direction,
            connectionFieldKey,
            relatedObjectKey,
            fieldKeys,
            limit,
            sortField,
            sortOrder,
        },
        ctx,
    ) => {
        const app = ctx.getApp(appKey);
        const sourceSchema = await ctx.getSchema(app);
        const sourceObject = sourceSchema.schema?.objects?.find(
            (entry) => entry.key === sourceObjectKey,
        );
        if (!sourceObject) {
            throw new Error(
                `Object ${sourceObjectKey} was not found in the available schema.`,
            );
        }

        const effectiveLimit = Math.min(
            limit,
            (await getPermittedReadFields(ctx, app, sourceObjectKey, []))
                .maxRecords,
        );
        let targetObjectKey = relatedObjectKey;
        let records: Record<string, unknown>[] = [];
        const skippedRecordIds: string[] = [];

        if (direction === 'forward') {
            const connection = (sourceObject.fields || []).find(
                (field) => field.key === connectionFieldKey,
            );
            if (!connection?.connectedObject) {
                throw new Error(
                    `${connectionFieldKey} is not a recognised connection field on ${sourceObjectKey}.`,
                );
            }
            targetObjectKey = connection.connectedObject;

            await getPermittedReadFields(ctx, app, sourceObjectKey, [
                connectionFieldKey,
            ]);
            const target = await getPermittedReadFields(
                ctx,
                app,
                targetObjectKey,
                fieldKeys,
            );

            const sourceResult = await ctx.request(
                app,
                `/objects/${sourceObjectKey}/records/${sourceRecordId}`,
            );
            // getRecordsFromResponse treats any non-list body as a single record,
            // including an error body — without this check, a failed fetch here reads
            // as a source record with no connection value, silently returning zero
            // related records instead of reporting why.
            if (!sourceResult.ok) {
                return makeTextResponse({
                    ok: false,
                    appKey: app.appKey,
                    objectKey: sourceObjectKey,
                    recordId: sourceRecordId,
                    status: sourceResult.status,
                    body: sourceResult.body,
                });
            }
            const sourceRecord = getRecordsFromResponse(sourceResult)[0];
            const connectionValue =
                sourceRecord?.[`${connectionFieldKey}_raw`] ??
                sourceRecord?.[connectionFieldKey];
            const relatedIds = (
                Array.isArray(connectionValue) ? connectionValue : []
            )
                .map((entry) => asRecord(entry)?.id)
                .filter((id): id is string => typeof id === 'string')
                .slice(0, effectiveLimit);

            // Up to effectiveLimit (max 100) independent single-record fetches, so
            // fetched with the same concurrency budget batch mutations use rather than
            // one at a time.
            const fetches = await runWithConcurrency(
                relatedIds,
                BATCH_CONCURRENCY,
                async (recordId) => ({
                    recordId,
                    result: await ctx.request(
                        app,
                        `/objects/${targetObjectKey}/records/${recordId}`,
                    ),
                }),
            );
            for (const { recordId, result } of fetches) {
                // Same hazard per related record: an error body from one deleted or
                // unreadable connected record must not become a blank fake record
                // indistinguishable from a real one with every field empty.
                if (!result.ok) {
                    skippedRecordIds.push(recordId);
                    continue;
                }
                const record = getRecordsFromResponse(result)[0];
                if (record)
                    records.push(
                        projectRecordFields(
                            record,
                            target.fields,
                            getRecordMasks(target.exclusions, targetObjectKey),
                        ),
                    );
            }
        } else {
            if (!targetObjectKey) {
                throw new Error(
                    'relatedObjectKey is required for reverse related-record lookups.',
                );
            }
            const target = await getPermittedReadFields(
                ctx,
                app,
                targetObjectKey,
                fieldKeys,
            );
            const targetField = (target.object.fields || []).find(
                (field) => field.key === connectionFieldKey,
            );
            if (
                !targetField ||
                targetField.connectedObject !== sourceObjectKey
            ) {
                throw new Error(
                    `${connectionFieldKey} must be a connection from ${targetObjectKey} to ${sourceObjectKey}.`,
                );
            }

            await getPermittedReadFields(ctx, app, targetObjectKey, [
                connectionFieldKey,
            ]);
            if (sortField)
                await getPermittedReadFields(ctx, app, targetObjectKey, [
                    sortField,
                ]);

            const params = buildRecordSearchParams({
                page: 1,
                rowsPerPage: effectiveLimit,
                filters: {
                    match: 'and',
                    rules: [
                        {
                            field: connectionFieldKey,
                            operator: 'is',
                            value: sourceRecordId,
                        },
                    ],
                },
                sortField,
                sortOrder,
            });
            const result = await ctx.request(
                app,
                `/objects/${targetObjectKey}/records?${params.toString()}`,
            );
            // Same hazard as the forward direction's single-record fetches: an error
            // body here is not a list, so getRecordsFromResponse would read it as one
            // fake record with every requested field blank.
            if (!result.ok) {
                return makeTextResponse({
                    ok: false,
                    appKey: app.appKey,
                    objectKey: targetObjectKey,
                    status: result.status,
                    body: result.body,
                });
            }
            records = getRecordsFromResponse(result)
                .slice(0, effectiveLimit)
                .map((record) =>
                    projectRecordFields(
                        record,
                        target.fields,
                        getRecordMasks(target.exclusions, target.object.key),
                    ),
                );
        }

        return makeTextResponse({
            ok: true,
            appKey: app.appKey,
            source: { objectKey: sourceObjectKey, recordId: sourceRecordId },
            direction,
            relatedObjectKey: targetObjectKey,
            returned: records.length,
            limit: effectiveLimit,
            records,
            ...(skippedRecordIds.length ? { skippedRecordIds } : {}),
        });
    },
});

export const countRecords = defineTool({
    name: 'knack_count_records',
    description:
        'Count the records in one or more objects, optionally filtered, without reading them.',
    access: 'read',
    input: {
        appKey: z.string().optional(),
        objects: z
            .array(z.object({ objectKey: z.string(), filters: filtersInput }))
            .min(1)
            .max(25)
            .describe('Each object with its own optional filters.'),
    },
    handler: async ({ appKey, objects }, ctx) => {
        const app = ctx.getApp(appKey);
        // One row is enough: Knack reports the match count in total_records whatever
        // the page size, so nothing is paged. A policy refusal or a Knack error on one
        // object is reported against that object and the others still count.
        const counts = await runWithConcurrency(
            objects,
            BATCH_CONCURRENCY,
            async ({ objectKey, filters }) => {
                try {
                    // Knack ignores a filter it cannot read and returns every record,
                    // which as a count would be a plausible wrong number. So a string
                    // must be a JSON object, or this object reports the error.
                    const parsed =
                        typeof filters === 'string'
                            ? parseJsonObjectInput(filters, 'filters')
                            : undefined;
                    if (parsed?.errors.length) {
                        throw new Error(parsed.errors.join(' '));
                    }
                    const checked = parsed?.payload ?? filters;
                    // Filtering by a field the policy hides would reveal its values
                    // through the count, so the filter is checked like any other read.
                    await validateReadQuery(ctx, app, objectKey, {
                        filters: checked,
                    });
                    const params = buildRecordSearchParams({
                        page: 1,
                        rowsPerPage: 1,
                        filters: checked,
                    });
                    const result = await ctx.requestWithRetry(
                        app,
                        `/objects/${objectKey}/records?${params.toString()}`,
                    );
                    if (!result.ok) {
                        return {
                            objectKey,
                            error: 'Knack refused the count.',
                            ...(result.status ? { status: result.status } : {}),
                            ...(result.body !== undefined
                                ? { body: result.body }
                                : {}),
                        };
                    }
                    const total = asRecord(result.body)?.total_records;
                    if (typeof total !== 'number') {
                        return {
                            objectKey,
                            error: 'Knack returned no total_records for this object.',
                            status: result.status,
                        };
                    }
                    return { objectKey, count: total };
                } catch (error) {
                    return { objectKey, error: describeError(error) };
                }
            },
        );
        return makeTextResponse({
            ok: counts.every((entry) => !('error' in entry)),
            appKey: app.appKey,
            counts,
        });
    },
});

export const aggregateRecords = defineTool({
    name: 'knack_aggregate_records',
    description:
        'Count, sum, average, min or max approved records with filters and grouping; returns aggregates only.',
    access: 'read',
    input: {
        appKey: z.string().optional(),
        objectKey: z.string(),
        filters: filtersInput,
        groupByFieldKeys: z.array(z.string()).max(3).default([]),
        dateBucket: z
            .object({
                fieldKey: z.string(),
                granularity: z.enum(['day', 'month', 'year']),
            })
            .optional(),
        metrics: z
            .array(
                z.object({
                    type: z.enum(['count', 'sum', 'avg', 'min', 'max']),
                    fieldKey: z.string().optional(),
                }),
            )
            .min(1)
            .max(10)
            .default([{ type: 'count' }]),
        maxRecords: z
            .number()
            .int()
            .min(1)
            .max(10000)
            .default(1000)
            .describe('Maximum records to scan; a capped result is reported.'),
    },
    handler: async (
        {
            appKey,
            objectKey,
            filters,
            groupByFieldKeys,
            dateBucket,
            metrics,
            maxRecords,
        },
        ctx,
    ) => {
        const app = ctx.getApp(appKey);
        const requestedFields = [
            ...groupByFieldKeys,
            ...(dateBucket ? [dateBucket.fieldKey] : []),
            ...metrics.flatMap((metric) =>
                metric.fieldKey ? [metric.fieldKey] : [],
            ),
        ];
        for (const metric of metrics) {
            if (metric.type !== 'count' && !metric.fieldKey) {
                throw new Error(`A ${metric.type} metric requires fieldKey.`);
            }
        }
        const { fields, exclusions } = await getPermittedReadFields(
            ctx,
            app,
            objectKey,
            requestedFields,
        );
        // A group-by on a connection whose linked object's display field is redacted
        // would bucket by that display value; projecting each record first masks it.
        // Only then: the permitted fields already exclude everything else a mask covers,
        // so copying every scanned record would change nothing.
        const masks = getRecordMasks(exclusions, objectKey);
        const needsMasking = fields.some(
            (key) =>
                masks.masked?.has(key) || masks.maskedConnections?.has(key),
        );
        const policyMaximum = await validateReadQuery(ctx, app, objectKey, {
            filters,
        });
        const scanLimit = Math.min(maxRecords, policyMaximum);
        // Fixed across every page. Knack computes a page's offset as
        // (page-1)*rows_per_page, so shrinking rows_per_page as the scan budget ran
        // low — the previous behaviour — misaligned that offset from what had already
        // been scanned: page 2 at a smaller page size re-read the tail of page 1
        // instead of continuing where it left off, double-counting some records and
        // never reaching others.
        const pageSize = Math.min(1000, scanLimit);

        const groups = new Map<string, Record<string, unknown>>();
        // Per group and metric: the running numbers an average, min or max is finished
        // from once the scan ends. Kept apart from the group so they never reach the reply.
        const running = new Map<
            string,
            Map<string, { sum: number; n: number; min: number; max: number }>
        >();
        let scanned = 0;
        let hasMore = true;

        while (hasMore && scanned < scanLimit) {
            const page = Math.floor(scanned / pageSize) + 1;
            const params = buildRecordSearchParams({
                page,
                rowsPerPage: pageSize,
                filters,
            });
            const result = await ctx.request(
                app,
                `/objects/${objectKey}/records?${params.toString()}`,
            );
            if (!result.ok) {
                return makeTextResponse({
                    ok: false,
                    appKey: app.appKey,
                    objectKey,
                    status: result.status,
                    body: result.body,
                });
            }

            const fetchedRecords = getRecordsFromResponse(result);
            // A full-size page can still overshoot scanLimit when the limit is not a
            // multiple of pageSize; only the ones within budget are counted.
            const inBudget = fetchedRecords.slice(0, scanLimit - scanned);
            const records = needsMasking
                ? inBudget.map((record) =>
                      projectRecordFields(record, fields, masks),
                  )
                : inBudget;
            for (const record of records) {
                const dimensions: Record<string, unknown> = {};
                for (const fieldKey of groupByFieldKeys) {
                    dimensions[fieldKey] = record[fieldKey] ?? null;
                }
                if (dateBucket) {
                    dimensions[dateBucket.fieldKey] =
                        bucketDate(
                            record[dateBucket.fieldKey],
                            dateBucket.granularity,
                        ) || 'Unknown';
                }

                const key = JSON.stringify(dimensions);
                const group = groups.get(key) || { dimensions, metrics: {} };
                const values = group.metrics as Record<string, number>;
                const groupRunning = running.get(key) || new Map();
                for (const metric of metrics) {
                    if (metric.type === 'count') {
                        values.count = (values.count || 0) + 1;
                        continue;
                    }
                    const numeric = getNumericValue(record[metric.fieldKey!]);
                    if (numeric === null) continue;
                    const metricKey = `${metric.type}:${metric.fieldKey}`;
                    const acc = groupRunning.get(metricKey) || {
                        sum: 0,
                        n: 0,
                        min: numeric,
                        max: numeric,
                    };
                    acc.sum += numeric;
                    acc.n += 1;
                    acc.min = Math.min(acc.min, numeric);
                    acc.max = Math.max(acc.max, numeric);
                    groupRunning.set(metricKey, acc);
                }
                running.set(key, groupRunning);
                groups.set(key, group);
            }

            scanned += records.length;
            // Whether Knack has more matching records beyond this page, judged on what
            // it actually returned before the scanLimit trim — trimming makes this
            // page's own count look partial even when Knack itself had more to give.
            hasMore = fetchedRecords.length === pageSize;
        }

        // A metric stays absent from a group with no numeric values for it, as sum
        // always has: an average of nothing is not zero.
        for (const [key, group] of groups) {
            const values = group.metrics as Record<string, number>;
            for (const [metricKey, acc] of running.get(key) || []) {
                const type = metricKey.slice(0, metricKey.indexOf(':'));
                values[metricKey] =
                    type === 'sum'
                        ? acc.sum
                        : type === 'avg'
                          ? acc.sum / acc.n
                          : type === 'min'
                            ? acc.min
                            : acc.max;
            }
        }

        const capped = scanned >= scanLimit && hasMore;
        return makeTextResponse({
            ok: true,
            appKey: app.appKey,
            objectKey,
            scanned,
            capped,
            scanLimit,
            fields,
            groups: [...groups.values()],
            ...(capped
                ? {
                      warning: `Only the first ${scanned} matching record(s) were scanned (scanLimit: ${scanLimit}); more records exist. These aggregates are PARTIAL, not the true total — raise maxRecords or narrow filters before treating them as final.`,
                  }
                : {}),
        });
    },
});

export const verifyRecordFieldShapes = defineTool({
    name: 'knack_verify_record_field_shapes',
    description:
        "Compare a live record's formatted and raw values against the documented field shapes.",
    access: 'diagnostic',
    input: {
        appKey: z.string().optional(),
        objectKey: z.string(),
        recordId: z.string(),
        includeBlankFields: z
            .boolean()
            .optional()
            .describe(
                'Include fields whose formatted and raw values are blank.',
            ),
    },
    handler: async (
        { appKey, objectKey, recordId, includeBlankFields = false },
        ctx,
    ) => {
        const app = ctx.getApp(appKey);
        // A diagnostic gate controls who can call this tool at all; it says nothing
        // about which objects and fields the app's own dataAccess policy approves, and
        // this tool echoes formatted and raw values, so it enforces that policy the
        // same as every record-read tool does.
        if (
            app.dataAccess?.allowedObjectKeys &&
            !app.dataAccess.allowedObjectKeys.includes(objectKey)
        ) {
            throw new Error(
                `Read access to ${objectKey} is not allowed by this app's dataAccess policy.`,
            );
        }
        const [schemaResult, recordResult] = await Promise.all([
            ctx.getSchema(app),
            ctx.request(app, `/objects/${objectKey}/records/${recordId}`),
        ]);

        const obj =
            schemaResult.schema?.objects?.find(
                (entry) => entry.key === objectKey,
            ) || null;
        const record = asRecord(recordResult.body);

        if (!recordResult.ok || !record) {
            return makeTextResponse({
                ok: false,
                appKey: app.appKey,
                objectKey,
                recordId,
                message: 'Unable to fetch the requested record.',
                recordResponse: recordResult,
            });
        }
        if (!obj) {
            return makeTextResponse({
                ok: false,
                appKey: app.appKey,
                objectKey,
                recordId,
                schemaSource: schemaResult.source,
                message:
                    'Object was not found in the available schema, so field types could not be verified.',
            });
        }

        // Field-level policy: redacted fields, and fields outside an allowedFieldKeys
        // list, never appear in the preview. Write-only fields and masked connections
        // are left out too: their shape cannot be checked without showing the value.
        const exclusions = await ctx.getFieldExclusions(app);
        const masks = getRecordMasks(exclusions, objectKey);
        const permittedFieldKeys = readPolicyApplies(app, exclusions, objectKey)
            ? new Set(
                  getDefaultPermittedFieldKeys(
                      app,
                      objectKey,
                      obj,
                      exclusions,
                  ).filter(
                      (key) =>
                          !masks.masked?.has(key) &&
                          !masks.maskedConnections?.has(key),
                  ),
              )
            : null;
        const checkableFields = permittedFieldKeys
            ? (obj.fields || []).filter((field) =>
                  permittedFieldKeys.has(field.key),
              )
            : obj.fields || [];

        const results = checkableFields.map((field) => {
            const formatted = record[field.key];
            const raw = record[`${field.key}_raw`];
            const validation = validateFieldShape(
                field.type || '',
                formatted,
                raw,
            );
            const shapeInfo = field.type ? getFieldShapeInfo(field.type) : null;
            return {
                fieldKey: field.key,
                fieldName: field.name || null,
                fieldType: field.type || null,
                status: validation.status,
                observedFormattedShape: validation.observedFormattedShape,
                observedRawShape: validation.observedRawShape,
                formattedPreview: getValuePreview(formatted),
                rawPreview: getValuePreview(raw),
                expectedSummary: shapeInfo?.summary || null,
                findings: validation.findings,
            };
        });

        const filteredResults = includeBlankFields
            ? results
            : results.filter((entry) => entry.status !== 'skipped');

        const summary = {
            checkedFieldCount: filteredResults.length,
            matchCount: filteredResults.filter(
                (entry) => entry.status === 'match',
            ).length,
            mismatchCount: filteredResults.filter(
                (entry) => entry.status === 'mismatch',
            ).length,
            skippedCount: results.filter((entry) => entry.status === 'skipped')
                .length,
            unknownCount: filteredResults.filter(
                (entry) => entry.status === 'unknown',
            ).length,
        };

        return makeTextResponse({
            ok: true,
            appKey: app.appKey,
            objectKey,
            objectName: obj.name || null,
            recordId,
            schemaSource: schemaResult.source,
            includeBlankFields,
            summary,
            results: filteredResults,
        });
    },
});

/** Per-item outcome of a batch mutation; `error` replaces status/body when the call threw. */
type BatchItemResult = {
    index?: number;
    recordId?: string;
    ok: boolean;
    status?: number;
    body?: unknown;
    error?: string;
};

/**
 * Run one Knack request per item, up to BATCH_CONCURRENCY at a time, catching a thrown
 * error into the same per-item result shape a failed request would have gotten instead
 * of letting it abort the rest of the batch.
 *
 * @param describe For one item: the request to send, and the identity fields
 *   (index and/or recordId) its result should carry.
 */
async function runRecordBatch<T>(
    ctx: KnackContext,
    app: AppConfig,
    items: T[],
    describe: (
        item: T,
    ) => { apiPath: string; init: RequestInit } & Pick<
        BatchItemResult,
        'index' | 'recordId'
    >,
): Promise<{
    results: BatchItemResult[];
    successCount: number;
    failureCount: number;
}> {
    // One request per item. Refuse up front when the daily allowance that Knack last
    // reported cannot cover the batch, rather than fail partway through it.
    const shortfall = ctx.usage.budgetShortfall(
        app.appKey,
        items.length,
        Date.now(),
    );
    if (shortfall) throw new Error(shortfall);

    const results = await runWithConcurrency(
        items,
        BATCH_CONCURRENCY,
        async (item): Promise<BatchItemResult> => {
            const { apiPath, init, ...identity } = describe(item);
            try {
                const result = await ctx.requestWithRetry(app, apiPath, init);
                return {
                    ...identity,
                    ok: result.ok,
                    status: result.status,
                    body: result.body,
                };
            } catch (error) {
                return { ...identity, ok: false, error: describeError(error) };
            }
        },
    );
    const successCount = results.filter((r) => r.ok).length;
    return {
        results,
        successCount,
        failureCount: results.length - successCount,
    };
}

/**
 * The read policy as it applies to a write on one object: payload keys naming a no-data
 * field without `_mcp_allowwrite` are refused, and Knack's echoed record is cut down
 * before it is returned, so a no-data value does not come straight back in the response.
 */
async function getRecordWritePolicy(
    ctx: KnackContext,
    app: AppConfig,
    objectKey: string,
) {
    const exclusions = await ctx.getFieldExclusions(app);
    const { schema } = await ctx.getSchema(app);
    const object = schema?.objects?.find((entry) => entry.key === objectKey);
    const allowedObjectKeys = app.dataAccess?.allowedObjectKeys;
    const echoFields =
        allowedObjectKeys && !allowedObjectKeys.includes(objectKey)
            ? []
            : getDefaultPermittedFieldKeys(app, objectKey, object, exclusions);
    const policyApplies = readPolicyApplies(app, exclusions, objectKey);
    const baseKey = (key: string) => key.replace(/_raw$/, '');
    const dateFields = new Map(
        (object?.fields || [])
            .filter((field) => field.type === 'date_time')
            .map((field) => [field.key, field]),
    );
    return {
        /**
         * Read each date field of one payload once, in the field's own order: what was
         * understood, one error per date that order cannot hold (nothing is sent), and the
         * payload with each ISO date rewritten in that order.
         */
        prepareDates: (
            payload: Record<string, unknown> | null,
            label: string,
        ) => {
            const readings: DateReading[] = [];
            const errors: string[] = [];
            let prepared = payload;
            for (const [key, value] of Object.entries(payload || {})) {
                const field = dateFields.get(baseKey(key));
                if (!field) continue;
                const reading = readDateValue(field, value);
                readings.push(reading);
                for (const problem of reading.problems) {
                    errors.push(
                        `${label}: ${describeDateRefusal(reading, problem)}`,
                    );
                }
                if (reading.converted !== undefined) {
                    prepared = { ...prepared, [key]: reading.converted };
                }
            }
            return { readings, errors, payload: prepared };
        },
        /** One error per payload key naming a field the model may not write. */
        refuseWriteBlocked: (
            payload: Record<string, unknown> | null,
            label: string,
        ) =>
            Object.keys(payload || {})
                .filter((key) => exclusions.writeBlocked.has(baseKey(key)))
                .map(
                    (key) =>
                        `${label}: ${describeWriteBlock(exclusions, baseKey(key))}.`,
                ),
        projectEcho: (result: BatchItemResult): BatchItemResult =>
            policyApplies && result.ok && result.body !== undefined
                ? {
                      ...result,
                      body: projectRecordFields(
                          result.body,
                          echoFields,
                          getRecordMasks(exclusions, objectKey),
                      ),
                  }
                : result,
    };
}

const AMBIGUOUS_DATE_WARNING =
    'Both parts are 12 or less, so the other order is also a real date. Check it is the one meant.';

/** The date fields of one record's payload, as read before writing. */
type DateChecks = { record: number; readings: DateReading[] };

/** The dry-run view of the date fields in each payload: what was understood of each. */
async function describeDates(
    ctx: KnackContext,
    app: AppConfig,
    checks: DateChecks[],
) {
    const dated = checks.flatMap(({ record, readings }) =>
        readings.map((reading) => ({ record, reading })),
    );
    if (!dated.length) return {};
    const timeZone = readAppTimeZone(await ctx.getRuntimeMetadata(app));
    return {
        dateFields: dated.map(({ record, reading }) => ({
            record,
            field: reading.field,
            ...(reading.fieldName ? { fieldName: reading.fieldName } : {}),
            input: reading.input,
            format: reading.format,
            understood: reading.understood,
            ...(reading.converted !== undefined
                ? { sent: reading.converted }
                : {}),
            ...(reading.ambiguous
                ? { ambiguous: true, warning: AMBIGUOUS_DATE_WARNING }
                : {}),
            ...(reading.notes.length ? { notes: reading.notes } : {}),
        })),
        dateTimeZone: timeZoneNote(timeZone),
    };
}

/**
 * Warnings worth showing after a real write: an ambiguous date or a note on one (from
 * `inputs`, what was read before sending), and a stored day that differs from the one
 * meant (from `written`, each record's dates against Knack's response).
 */
function dateWarnings(
    inputs: DateChecks[],
    written: DateChecks[],
    results: BatchItemResult[],
): { dateWarnings?: string[] } {
    const warnings = inputs.flatMap(({ record, readings }) =>
        readings.flatMap((reading) => [
            ...(reading.ambiguous
                ? [
                      `records[${record}]: ${reading.field} read ${JSON.stringify(reading.input)} as ${reading.understood} (${reading.format}); ${AMBIGUOUS_DATE_WARNING}`,
                  ]
                : []),
            ...reading.notes.map(
                (note) => `records[${record}]: ${reading.field}: ${note}`,
            ),
        ]),
    );
    const bodies = new Map(
        results.flatMap((result) =>
            result.ok && result.index !== undefined
                ? [[result.index, asRecord(result.body)] as const]
                : [],
        ),
    );
    for (const { record, readings } of written) {
        for (const reading of readings) {
            const warning = checkStoredDate(
                reading,
                bodies.get(record)?.[`${reading.field}_raw`],
            );
            if (warning) warnings.push(`records[${record}]: ${warning}`);
        }
    }
    return warnings.length ? { dateWarnings: warnings } : {};
}

/** Most records one update or delete by filter may touch. */
const MAX_FILTER_MATCHES = 1000;

const maxMatchesInput = z
    .number()
    .int()
    .min(1)
    .max(MAX_FILTER_MATCHES)
    .default(100)
    .describe(
        'With filters: refuse, changing nothing, if more records than this match',
    );

/**
 * The ids of every record matching `filters`, for an update or delete by filter.
 *
 * The filter goes through the same read policy a find does, so it cannot be used to
 * probe a redacted or write-only field. A match larger than `limit` is refused with its
 * total rather than acted on in part, and a filter naming no field is refused outright:
 * a bulk change by filter never runs over a whole table.
 */
async function collectMatchingRecordIds(
    ctx: KnackContext,
    app: AppConfig,
    objectKey: string,
    filters: string | Record<string, unknown>,
    limit: number,
): Promise<
    | { ok: true; ids: string[] }
    | { ok: false; message: string; extra: Record<string, unknown> }
> {
    if (!getFilterFieldKeys(filters).length) {
        return {
            ok: false,
            message:
                'filters must name at least one field. A bulk change by filter never runs over a whole table; pass recordIds for that. Nothing was changed.',
            extra: {},
        };
    }
    const cap = Math.min(
        limit,
        await validateReadQuery(ctx, app, objectKey, { filters }),
    );
    const tooMany = (count: number) => ({
        ok: false as const,
        message: `The filter matches ${count} records, more than the limit of ${cap}. Nothing was changed. Narrow the filter, or raise maxMatches (up to ${MAX_FILTER_MATCHES}) after checking the count with the user.`,
        extra: {},
    });

    // One row more than the cap is enough to tell "too many" from "all of them", so a
    // big match is refused without downloading a thousand records first.
    const rowsPerPage = Math.min(cap + 1, MAX_FILTER_MATCHES);
    const ids: string[] = [];
    for (let page = 1; ; page += 1) {
        const params = buildRecordSearchParams({
            page,
            rowsPerPage,
            filters,
        });
        const result = await ctx.request(
            app,
            `/objects/${objectKey}/records?${params.toString()}`,
        );
        if (!result.ok) {
            return {
                ok: false,
                message:
                    'Knack refused the query that finds the matching records. Nothing was changed.',
                extra: {
                    ...(result.status ? { status: result.status } : {}),
                    ...(result.body !== undefined ? { body: result.body } : {}),
                },
            };
        }
        const total = asRecord(result.body)?.total_records;
        if (typeof total === 'number' && total > cap) return tooMany(total);
        const records = getRecordsFromResponse(result);
        for (const record of records) {
            if (typeof record.id === 'string') ids.push(record.id);
        }
        if (ids.length > cap) return tooMany(ids.length);
        if (
            records.length < rowsPerPage ||
            (typeof total === 'number' && ids.length >= total)
        ) {
            return { ok: true, ids };
        }
    }
}

/**
 * A record's field values, as the caller naturally writes them (an object) or as the
 * JSON string the legacy tools demanded. The string-only schema failed MCP input
 * validation before the handler ran, so a caller sending the obvious shape got a
 * schema error and never reached the permission checks — measured 6 September, when it
 * also spoiled two rows of the permission matrix.
 */
const RECORD_PAYLOAD = z.union([z.string(), z.record(z.string(), z.unknown())]);

function parseRecordPayload(
    value: string | Record<string, unknown>,
    label: string,
): ReturnType<typeof parseJsonObjectInput> {
    if (typeof value === 'string') return parseJsonObjectInput(value, label);
    return { payload: value, errors: [] };
}

export const createRecords = defineTool({
    name: 'knack_create_records',
    description:
        'Create one or more records in an object, one request each with per-record results.',
    access: 'write',
    input: {
        appKey: z.string().optional(),
        objectKey: z.string(),
        records: z
            .array(RECORD_PAYLOAD)
            .min(1)
            .max(100)
            .describe(
                'One entry per record: an object of field_key: value pairs, or that object as a JSON string.',
            ),
        dryRun: z.boolean().optional().default(false),
    },
    handler: async ({ appKey, objectKey, records, dryRun }, ctx) => {
        const app = ctx.getApp(appKey);

        const parsedRecords = records.map((raw, index) => ({
            index,
            ...parseRecordPayload(raw, `records[${index}]`),
        }));
        const writePolicy = await getRecordWritePolicy(ctx, app, objectKey);
        const dateChecks: DateChecks[] = [];
        for (const entry of parsedRecords) {
            const dates = writePolicy.prepareDates(
                entry.payload,
                `records[${entry.index}]`,
            );
            entry.errors.push(
                ...writePolicy.refuseWriteBlocked(
                    entry.payload,
                    `records[${entry.index}]`,
                ),
                ...dates.errors,
            );
            entry.payload = dates.payload;
            dateChecks.push({ record: entry.index, readings: dates.readings });
        }
        const invalid = parsedRecords.filter((entry) => entry.errors.length);
        if (invalid.length) {
            return makeTextResponse({
                ok: false,
                appKey: app.appKey,
                objectKey,
                action: 'batch_create_records_preflight',
                errors: invalid.flatMap((entry) => entry.errors),
            });
        }

        if (dryRun) {
            return makeTextResponse({
                ok: true,
                appKey: app.appKey,
                objectKey,
                action: 'batch_create_records_dry_run',
                dryRun: true,
                wouldCreateCount: parsedRecords.length,
                wouldCreate: parsedRecords.map((entry) => entry.payload),
                ...(await describeDates(ctx, app, dateChecks)),
            });
        }

        const { results, successCount, failureCount } = await runRecordBatch(
            ctx,
            app,
            parsedRecords,
            (entry) => ({
                index: entry.index,
                apiPath: `/objects/${objectKey}/records`,
                init: { method: 'POST', body: JSON.stringify(entry.payload) },
            }),
        );

        return makeTextResponse({
            ok: failureCount === 0,
            appKey: app.appKey,
            objectKey,
            action: 'batch_create_records',
            requestedCount: records.length,
            successCount,
            failureCount,
            results: results.map(writePolicy.projectEcho),
            ...dateWarnings(dateChecks, dateChecks, results),
            note: `Records were created with up to ${BATCH_CONCURRENCY} requests in flight at once, retrying individual requests on a 429 with backoff (not on 5xx — a lost/delayed 5xx response after a create that actually succeeded would otherwise risk creating a duplicate record). Check each entry in results for its own ok/status rather than assuming the whole batch succeeded.`,
        });
    },
});

export const updateRecords = defineTool({
    name: 'knack_update_records',
    description:
        'Update records by id, or every record matching a filter (previews unless confirm is true); one request each with per-record results.',
    access: 'write',
    input: {
        appKey: z.string().optional(),
        objectKey: z.string(),
        records: z
            .array(
                z.object({
                    recordId: z.string(),
                    data: RECORD_PAYLOAD.describe(
                        'field_key: value pairs, as an object or a JSON string',
                    ),
                }),
            )
            .min(1)
            .max(100)
            .optional()
            .describe('Records by id; or use where instead'),
        where: z
            .object({
                filters: z.union([
                    z.string(),
                    z.record(z.string(), z.unknown()),
                ]),
                data: RECORD_PAYLOAD,
            })
            .optional()
            .describe(
                'Set the same data on every record matching filters; previews unless confirm is true',
            ),
        maxMatches: maxMatchesInput,
        confirm: z
            .boolean()
            .optional()
            .default(false)
            .describe('With where: true to apply after checking the preview'),
        dryRun: z.boolean().optional().default(false),
    },
    handler: async (
        { appKey, objectKey, records, where, maxMatches, confirm, dryRun },
        ctx,
    ) => {
        const app = ctx.getApp(appKey);
        const refuse = (message: string, extra: Record<string, unknown> = {}) =>
            makeTextResponse({
                ok: false,
                appKey: app.appKey,
                objectKey,
                action: 'batch_update_records_preflight',
                errors: [message],
                ...extra,
            });

        if (Boolean(records) === Boolean(where)) {
            return refuse('Pass exactly one of records or where.');
        }

        let parsedRecords: Array<{
            index: number;
            recordId: string;
            payload: Record<string, unknown> | null;
            errors: string[];
        }>;
        const writePolicy = await getRecordWritePolicy(ctx, app, objectKey);
        // Dates as read before writing. A `where` update shares one payload, so its dates
        // are read once (shown for record 0) and checked against every record written.
        let dateChecks: DateChecks[] = [];
        let writtenChecks: DateChecks[] = [];
        if (where) {
            const parsedData = parseRecordPayload(where.data, 'where.data');
            if (parsedData.errors.length) return refuse(parsedData.errors[0]);
            // One shared payload: checked once, before the query that finds the matches.
            const dates = writePolicy.prepareDates(
                parsedData.payload,
                'where.data',
            );
            const writeBlockErrors = [
                ...writePolicy.refuseWriteBlocked(
                    parsedData.payload,
                    'where.data',
                ),
                ...dates.errors,
            ];
            if (writeBlockErrors.length) {
                return makeTextResponse({
                    ok: false,
                    appKey: app.appKey,
                    objectKey,
                    action: 'batch_update_records_preflight',
                    errors: writeBlockErrors,
                });
            }
            const matched = await collectMatchingRecordIds(
                ctx,
                app,
                objectKey,
                where.filters,
                maxMatches,
            );
            if (!matched.ok) return refuse(matched.message, matched.extra);
            parsedRecords = matched.ids.map((recordId, index) => ({
                index,
                recordId,
                payload: dates.payload,
                errors: [],
            }));
            dateChecks = [{ record: 0, readings: dates.readings }];
            writtenChecks = parsedRecords.map((entry) => ({
                record: entry.index,
                readings: dates.readings,
            }));
        } else {
            parsedRecords = records!.map((record, index) => ({
                index,
                recordId: record.recordId,
                ...parseRecordPayload(record.data, `records[${index}].data`),
            }));
        }
        if (!where) {
            for (const entry of parsedRecords) {
                const dates = writePolicy.prepareDates(
                    entry.payload,
                    `records[${entry.index}].data`,
                );
                entry.errors.push(
                    ...writePolicy.refuseWriteBlocked(
                        entry.payload,
                        `records[${entry.index}].data`,
                    ),
                    ...dates.errors,
                );
                entry.payload = dates.payload;
                dateChecks.push({
                    record: entry.index,
                    readings: dates.readings,
                });
            }
            writtenChecks = dateChecks;
        }
        const invalid = parsedRecords.filter((entry) => entry.errors.length);
        if (invalid.length) {
            return makeTextResponse({
                ok: false,
                appKey: app.appKey,
                objectKey,
                action: 'batch_update_records_preflight',
                errors: invalid.flatMap((entry) => entry.errors),
            });
        }

        if (where && (dryRun || !confirm)) {
            return makeTextResponse({
                ok: false,
                appKey: app.appKey,
                objectKey,
                action: 'batch_update_records_preview',
                message: parsedRecords.length
                    ? `This would set the same data on ${parsedRecords.length} matching record(s) in ${objectKey}. Pass confirm: true only after checking the count and data with the user.`
                    : 'No records match the filter. Nothing would change.',
                matchCount: parsedRecords.length,
                matchedRecordIds: parsedRecords.map((entry) => entry.recordId),
                wouldSet: parsedRecords[0]?.payload ?? null,
                ...(await describeDates(ctx, app, dateChecks)),
            });
        }
        if (where && !parsedRecords.length) {
            return makeTextResponse({
                ok: true,
                appKey: app.appKey,
                objectKey,
                action: 'batch_update_records',
                requestedCount: 0,
                message: 'No records match the filter. Nothing was changed.',
            });
        }

        if (dryRun) {
            return makeTextResponse({
                ok: true,
                appKey: app.appKey,
                objectKey,
                action: 'batch_update_records_dry_run',
                dryRun: true,
                wouldUpdateCount: parsedRecords.length,
                wouldUpdate: parsedRecords.map((entry) => ({
                    recordId: entry.recordId,
                    data: entry.payload,
                })),
                ...(await describeDates(ctx, app, dateChecks)),
            });
        }

        const { results, successCount, failureCount } = await runRecordBatch(
            ctx,
            app,
            parsedRecords,
            (entry) => ({
                index: entry.index,
                recordId: entry.recordId,
                apiPath: `/objects/${objectKey}/records/${entry.recordId}`,
                init: { method: 'PUT', body: JSON.stringify(entry.payload) },
            }),
        );

        return makeTextResponse({
            ok: failureCount === 0,
            appKey: app.appKey,
            objectKey,
            action: 'batch_update_records',
            requestedCount: parsedRecords.length,
            successCount,
            failureCount,
            results: results.map(writePolicy.projectEcho),
            ...dateWarnings(dateChecks, writtenChecks, results),
            note: `Records were updated with up to ${BATCH_CONCURRENCY} requests in flight at once, retrying individual requests on a 429/5xx with backoff. Check each entry in results for its own ok/status rather than assuming the whole batch succeeded.`,
        });
    },
});

export const deleteRecords = defineTool({
    name: 'knack_delete_records',
    description:
        'Delete records by id, or every record matching a filter; previews unless confirm is true.',
    access: 'delete',
    input: {
        appKey: z.string().optional(),
        objectKey: z.string(),
        recordIds: z
            .array(z.string())
            .min(1)
            .max(100)
            .optional()
            .describe('Records by id; or use filters instead'),
        filters: z
            .union([z.string(), z.record(z.string(), z.unknown())])
            .optional()
            .describe('Delete every record matching these Knack filters'),
        maxMatches: maxMatchesInput,
        confirm: z
            .boolean()
            .optional()
            .default(false)
            .describe(
                'Must be true to delete; otherwise a preview is returned.',
            ),
    },
    handler: async (
        {
            appKey,
            objectKey,
            recordIds: givenIds,
            filters,
            maxMatches,
            confirm,
        },
        ctx,
    ) => {
        const app = ctx.getApp(appKey);
        const refuse = (message: string, extra: Record<string, unknown> = {}) =>
            makeTextResponse({
                ok: false,
                appKey: app.appKey,
                objectKey,
                action: 'batch_delete_records_preflight',
                message,
                ...extra,
            });

        if (Boolean(givenIds) === (filters !== undefined)) {
            return refuse('Pass exactly one of recordIds or filters.');
        }

        let recordIds = givenIds ?? [];
        if (filters !== undefined) {
            const matched = await collectMatchingRecordIds(
                ctx,
                app,
                objectKey,
                filters,
                maxMatches,
            );
            if (!matched.ok) return refuse(matched.message, matched.extra);
            recordIds = matched.ids;
            if (!recordIds.length) {
                return makeTextResponse({
                    ok: true,
                    appKey: app.appKey,
                    objectKey,
                    action: 'batch_delete_records',
                    requestedCount: 0,
                    message:
                        'No records match the filter. Nothing was deleted.',
                });
            }
        }

        if (!confirm) {
            return makeTextResponse({
                ok: false,
                appKey: app.appKey,
                objectKey,
                action: 'batch_delete_records_preflight',
                message: `This would permanently delete ${recordIds.length} record(s) from ${objectKey}. This cannot be undone. Pass confirm: true only after explicitly confirming this with the user.`,
                wouldDeleteCount: recordIds.length,
                wouldDeleteRecordIds: recordIds,
            });
        }

        const { results, successCount, failureCount } = await runRecordBatch(
            ctx,
            app,
            recordIds,
            (recordId) => ({
                recordId,
                apiPath: `/objects/${objectKey}/records/${recordId}`,
                init: { method: 'DELETE' },
            }),
        );

        return makeTextResponse({
            ok: failureCount === 0,
            appKey: app.appKey,
            objectKey,
            action: 'batch_delete_records',
            requestedCount: recordIds.length,
            successCount,
            failureCount,
            results,
            note: `Records were deleted with up to ${BATCH_CONCURRENCY} requests in flight at once, retrying individual requests on a 429/5xx with backoff. Check each entry in results for its own ok/status — a partial failure means some records were deleted and others were not.`,
        });
    },
});

export const uploadAsset = defineTool({
    name: 'knack_upload_asset',
    description:
        'Upload a local file to Knack as a file or image asset and return its asset id.',
    access: 'write',
    input: {
        appKey: z.string().optional(),
        filePath: z.string().describe('Absolute path to the local file'),
        assetType: z.enum(['file', 'image']).default('file'),
    },
    handler: async ({ appKey, filePath, assetType }, ctx) => {
        const app = ctx.getApp(appKey);
        const apiKey = ctx.getApiKey(app.appKey);

        if (!fs.existsSync(filePath)) {
            return makeTextResponse({
                ok: false,
                status: 0,
                body: { error: 'file_not_found', filePath },
            });
        }
        const stat = fs.statSync(filePath);
        if (!stat.isFile()) {
            return makeTextResponse({
                ok: false,
                status: 0,
                body: { error: 'not_a_file', filePath },
            });
        }

        const buffer = fs.readFileSync(filePath);
        const fileName = path.basename(filePath);
        const blob = new Blob([new Uint8Array(buffer)]);
        const form = new FormData();
        form.append('files', blob, fileName);

        const url = `${app.apiBase || DEFAULT_API_BASE}/applications/${encodeURIComponent(
            app.appId,
        )}/assets/${assetType}/upload`;
        // Not ctx.request (a multipart body sets its own content type), but paced and
        // recorded the same way.
        const result = await ctx.trackApiCall(app, () =>
            knackFetchJson(url, {
                method: 'POST',
                headers: {
                    'X-Knack-Application-Id': app.appId,
                    'X-Knack-REST-API-Key': apiKey,
                },
                body: form,
            }),
        );
        return makeTextResponse({
            appKey: app.appKey,
            action: 'upload_asset',
            filePath,
            fileName,
            sizeBytes: stat.size,
            assetType,
            ...result,
        });
    },
});

export const recordTools: AnyToolDef[] = [
    getRecord,
    getRecordHistory,
    findRecords,
    getRelatedRecords,
    countRecords,
    aggregateRecords,
    verifyRecordFieldShapes,
    createRecords,
    updateRecords,
    deleteRecords,
    uploadAsset,
];
