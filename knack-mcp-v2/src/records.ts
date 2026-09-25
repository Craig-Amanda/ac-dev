/**
 * Record reads under an app's optional dataAccess policy and its `_mcp_*` field
 * exclusions (lib/field-exclusion.ts), and small record helpers.
 */
import type { AppConfig } from './config.js';
import type { KnackContext } from './context.js';
import type { KnackApiResult } from './http.js';
import {
    type FieldExclusions,
    REDACTED_VALUE,
    describeExclusion,
} from './lib/field-exclusion.js';
import { asRecord } from './lib/util.js';
import type { CachedObject } from './types.js';

/**
 * The field keys a policy allows to be returned by default, when nothing specific was
 * requested — as opposed to a caller-requested list, which getPermittedReadFields
 * validates and throws on. An allowedFieldKeys entry that has since been redacted, or
 * that names a field no longer in the schema, is silently excluded here: it describes
 * what the policy currently permits, not a request to be rejected. A write-only field
 * stays in the list, because projectRecordFields returns it as "[redacted]".
 */
export function getDefaultPermittedFieldKeys(
    app: AppConfig,
    objectKey: string,
    object: CachedObject | null | undefined,
    exclusions?: FieldExclusions,
): string[] {
    const knownFieldKeys = new Set(
        (object?.fields || []).map((field) => field.key),
    );
    const readBlocked =
        exclusions?.readBlocked ?? new Set(app.dataAccess?.redactedFieldKeys);
    const masked = exclusions?.masked ?? new Set<string>();
    const policyFields = app.dataAccess?.allowedFieldKeys?.[objectKey];
    return (policyFields ?? [...knownFieldKeys]).filter(
        (key) =>
            knownFieldKeys.has(key) &&
            (!readBlocked.has(key) || masked.has(key)),
    );
}

/** What projectRecordFields masks on one object's records. */
export type RecordMasks = {
    masked?: Set<string>;
    maskedConnections?: Set<string>;
};

export function getRecordMasks(
    exclusions: FieldExclusions,
    objectKey: string,
): RecordMasks {
    return {
        masked: exclusions.masked,
        maskedConnections: exclusions.maskedConnections.get(objectKey),
    };
}

export function buildRecordSearchParams({
    page,
    rowsPerPage,
    q,
    filters,
    sortField,
    sortOrder,
}: {
    page: number;
    rowsPerPage: number;
    q?: string;
    filters?: string | Record<string, unknown>;
    sortField?: string;
    sortOrder?: 'asc' | 'desc';
}): URLSearchParams {
    const params = new URLSearchParams();
    params.set('page', String(page));
    params.set('rows_per_page', String(rowsPerPage));
    if (q) params.set('q', q);
    const trimmedSortField = sortField?.trim();
    if (sortField !== undefined && !trimmedSortField) {
        throw new Error('sortField cannot be empty.');
    }
    if (sortOrder !== undefined && !trimmedSortField) {
        throw new Error('sortOrder requires sortField.');
    }
    if (trimmedSortField) {
        params.set('sort_field', trimmedSortField);
        params.set('sort_order', sortOrder === 'desc' ? 'desc' : 'asc');
    }
    if (filters !== undefined) {
        if (typeof filters === 'string') {
            const trimmed = filters.trim();
            if (!trimmed) throw new Error('filters string cannot be empty.');
            if (trimmed.startsWith('{') || trimmed.startsWith('['))
                JSON.parse(trimmed);
            params.set('filters', trimmed);
        } else {
            params.set('filters', JSON.stringify(filters));
        }
    }
    return params;
}

/**
 * Resolve and enforce the app's read policy before exposing record data.
 * @returns The object's schema entry, the permitted requested fields and the row cap.
 */
export async function getPermittedReadFields(
    ctx: KnackContext,
    app: AppConfig,
    objectKey: string,
    requestedFieldKeys: string[],
) {
    const policy = app.dataAccess;
    if (
        policy?.allowedObjectKeys &&
        !policy.allowedObjectKeys.includes(objectKey)
    ) {
        throw new Error(
            `Read access to ${objectKey} is not allowed by this app's dataAccess policy.`,
        );
    }

    const { schema } = await ctx.getSchema(app);
    const object = schema?.objects?.find((entry) => entry.key === objectKey);
    if (!object)
        throw new Error(
            `Object ${objectKey} was not found in the available schema.`,
        );

    const knownFields = new Set(
        (object.fields || []).map((field) => field.key),
    );
    const policyFields = policy?.allowedFieldKeys?.[objectKey];
    const exclusions = await ctx.getFieldExclusions(app);
    const fields = requestedFieldKeys.map((key) => key.trim()).filter(Boolean);

    for (const fieldKey of fields) {
        if (!knownFields.has(fieldKey)) {
            throw new Error(
                `Field ${fieldKey} does not belong to ${objectKey}.`,
            );
        }
        if (policyFields && !policyFields.includes(fieldKey)) {
            throw new Error(
                `Field ${fieldKey} is not allowed by this app's dataAccess policy.`,
            );
        }
        if (exclusions.readBlocked.has(fieldKey)) {
            throw new Error(
                exclusions.reasons.get(fieldKey) ===
                    'dataAccess.redactedFieldKeys'
                    ? `Field ${fieldKey} is redacted by this app's dataAccess policy.`
                    : `Field ${describeExclusion(exclusions, fieldKey)}: its value cannot be read, filtered, sorted or aggregated through MCP.`,
            );
        }
    }

    return {
        object,
        fields,
        maxRecords: policy?.maxRecordsPerQuery || 1000,
        exclusions,
    };
}

/** Field keys a Knack filter tree touches. */
export function getFilterFieldKeys(
    filters: string | Record<string, unknown> | undefined,
): string[] {
    if (filters === undefined) return [];
    const parsed = typeof filters === 'string' ? JSON.parse(filters) : filters;
    const fields = new Set<string>();
    const visit = (value: unknown): void => {
        if (Array.isArray(value)) {
            value.forEach(visit);
            return;
        }
        const record = asRecord(value);
        if (!record) return;
        if (typeof record.field === 'string') fields.add(record.field);
        Object.values(record).forEach(visit);
    };
    visit(parsed);
    return [...fields];
}

/**
 * Whether the read policy governs `objectKey`: always under a dataAccess block, and
 * otherwise only on an object carrying a field-exclusion keyword.
 */
export function readPolicyApplies(
    app: AppConfig,
    exclusions: FieldExclusions,
    objectKey: string,
): boolean {
    return Boolean(app.dataAccess || exclusions.objects.has(objectKey));
}

/**
 * Validate everything that can reveal data through a query (filters, sort, free text).
 * @returns The maximum records permitted for the app.
 */
export async function validateReadQuery(
    ctx: KnackContext,
    app: AppConfig,
    objectKey: string,
    options: {
        filters?: string | Record<string, unknown>;
        q?: string;
        sortField?: string;
    },
): Promise<number> {
    const exclusions = await ctx.getFieldExclusions(app);
    if (!readPolicyApplies(app, exclusions, objectKey)) {
        return (await getPermittedReadFields(ctx, app, objectKey, []))
            .maxRecords;
    }
    const requested = [
        ...getFilterFieldKeys(options.filters),
        ...(options.sortField ? [options.sortField] : []),
    ];
    const { maxRecords } = await getPermittedReadFields(
        ctx,
        app,
        objectKey,
        requested,
    );
    if (options.q?.trim()) {
        throw new Error(
            'Free-text search is disabled for apps with a dataAccess policy, and on objects with redacted fields, because it can search fields whose values are not readable. Use approved structured filters instead.',
        );
    }
    return maxRecords;
}

/**
 * A record reduced to its id and the approved fields (formatted and `_raw`). A masked
 * field reads as "[redacted]"; a masked connection keeps its linked record ids but not
 * their display values.
 */
export function projectRecordFields(
    value: unknown,
    fieldKeys: string[],
    masks: RecordMasks = {},
): Record<string, unknown> {
    const record = asRecord(value) || {};
    const projected: Record<string, unknown> = {
        id: record.id || record._id || null,
    };
    for (const fieldKey of fieldKeys) {
        const rawKey = `${fieldKey}_raw`;
        if (masks.masked?.has(fieldKey)) {
            projected[fieldKey] = REDACTED_VALUE;
            if (rawKey in record) projected[rawKey] = REDACTED_VALUE;
        } else if (masks.maskedConnections?.has(fieldKey)) {
            projected[fieldKey] = REDACTED_VALUE;
            if (rawKey in record)
                projected[rawKey] = maskConnectionIdentifiers(record[rawKey]);
        } else {
            projected[fieldKey] = record[fieldKey] ?? null;
            if (rawKey in record) projected[rawKey] = record[rawKey];
        }
    }
    return projected;
}

/** `[{id, identifier}]` with each identifier masked; any other shape is masked whole. */
function maskConnectionIdentifiers(value: unknown): unknown {
    if (!Array.isArray(value)) return REDACTED_VALUE;
    return value.map((entry) => {
        const link = asRecord(entry);
        return link ? { ...link, identifier: REDACTED_VALUE } : REDACTED_VALUE;
    });
}

/** Project every record in a list response, or a single-record response, down to `fieldKeys`. */
export function projectResultFields(
    result: KnackApiResult,
    fieldKeys: string[],
    masks: RecordMasks = {},
): KnackApiResult {
    const body = asRecord(result?.body);
    if (!body) return result;
    if (Array.isArray(body.records)) {
        return {
            ...result,
            body: {
                ...body,
                records: body.records.map((record) =>
                    projectRecordFields(record, fieldKeys, masks),
                ),
            },
        };
    }
    return {
        ...result,
        body: projectRecordFields(body, fieldKeys, masks),
    };
}

/**
 * Apply the app's read policy to a record or record-list response, optionally narrowed
 * further to a caller-requested field list. `narrowFields` can only narrow what a caller
 * receives, never widen it: it is intersected with the policy's permitted fields when a
 * policy applies, and used on its own when there is none. A policy applies when the app
 * has a dataAccess block or the object has an excluded field.
 */
export async function applyRecordReadPolicy(
    ctx: KnackContext,
    app: AppConfig,
    objectKey: string,
    result: KnackApiResult,
    narrowFields?: string[],
): Promise<KnackApiResult> {
    const exclusions = await ctx.getFieldExclusions(app);
    if (!readPolicyApplies(app, exclusions, objectKey)) {
        return narrowFields?.length
            ? projectResultFields(result, narrowFields)
            : result;
    }

    const { object } = await getPermittedReadFields(ctx, app, objectKey, []);
    const fields = getDefaultPermittedFieldKeys(
        app,
        objectKey,
        object,
        exclusions,
    );
    const effectiveFields = narrowFields?.length
        ? fields.filter((field) => narrowFields.includes(field))
        : fields;

    return projectResultFields(
        result,
        effectiveFields,
        getRecordMasks(exclusions, objectKey),
    );
}

/** Records from a list or single-record response. */
export function getRecordsFromResponse(
    result: unknown,
): Record<string, unknown>[] {
    const body = asRecord(asRecord(result)?.body);
    const records = body?.records;
    if (Array.isArray(records)) {
        return records
            .map(asRecord)
            .filter((record): record is Record<string, unknown> =>
                Boolean(record),
            );
    }
    return body ? [body] : [];
}

/** A Knack numeric or formatted currency value as a number, or null. */
export function getNumericValue(value: unknown): number | null {
    if (typeof value === 'number' && Number.isFinite(value)) return value;
    if (typeof value !== 'string') return null;
    // A blank value is no number, not zero: Number('') is 0, which would pull an
    // average or a minimum down to 0 for every empty field.
    if (!/\d/.test(value)) return null;
    const parsed = Number(value.replace(/[^0-9.-]/g, ''));
    return Number.isFinite(parsed) ? parsed : null;
}

/** A stable date bucket from Knack's display and raw date shapes. */
export function bucketDate(
    value: unknown,
    granularity: 'day' | 'month' | 'year',
): string | null {
    const raw = asRecord(value);
    const text =
        typeof value === 'string'
            ? value
            : typeof raw?.iso === 'string'
              ? raw.iso
              : typeof raw?.date === 'string'
                ? raw.date
                : null;
    const isoMatch = text?.match(/(\d{4})[-/](\d{1,2})(?:[-/](\d{1,2}))?/);
    const ukMatch = text?.match(/(\d{1,2})[-/](\d{1,2})[-/](\d{4})/);
    const year = isoMatch?.[1] || ukMatch?.[3];
    const monthValue = isoMatch?.[2] || ukMatch?.[2];
    const dayValue = isoMatch?.[3] || ukMatch?.[1];
    if (!year || !monthValue) return null;
    const month = monthValue.padStart(2, '0');
    const day = dayValue?.padStart(2, '0');
    if (granularity === 'year') return year;
    if (granularity === 'month') return `${year}-${month}`;
    return day ? `${year}-${month}-${day}` : null;
}
