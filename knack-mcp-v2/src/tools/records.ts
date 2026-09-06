/**
 * Record tools: read one or many records under the app's read policy, follow
 * connections, aggregate, verify field shapes, and create/update/delete in batches.
 */
import fs from 'node:fs';
import path from 'node:path';

import { z } from 'zod';

import type { AppConfig } from '../config.js';
import { BATCH_CONCURRENCY, DEFAULT_API_BASE } from '../config.js';
import type { KnackContext } from '../context.js';
import { knackFetchJson } from '../http.js';
import { parseJsonObjectInput } from '../lib/field-payload.js';
import { getFieldShapeInfo } from '../lib/field-shapes.js';
import { getValuePreview, validateFieldShape } from '../lib/record-shapes.js';
import { asRecord, describeError, runWithConcurrency } from '../lib/util.js';
import {
    applyRecordReadPolicy,
    bucketDate,
    buildRecordSearchParams,
    getDefaultPermittedFieldKeys,
    getNumericValue,
    getPermittedReadFields,
    getRecordsFromResponse,
    projectRecordFields,
    validateReadQuery,
} from '../records.js';
import { type AnyToolDef, defineTool } from '../registry.js';
import { makeTextResponse } from '../response.js';

const RAW_FIELD_TIP =
    'Prefer field_xxx_raw for connections/dates — see knack_describe_field_shape.';

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
        q: z.string().optional().describe('Free text search (q=)'),
        filters: filtersInput,
        sortField: z
            .string()
            .optional()
            .describe('Field key to sort by, e.g. field_66'),
        sortOrder: z.enum(['asc', 'desc']).optional(),
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
        );

        const base = {
            appKey: app.appKey,
            ...safeResult,
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
            app.dataAccess?.maxRecordsPerQuery || 1000,
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
                    records.push(projectRecordFields(record, target.fields));
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
                .map((record) => projectRecordFields(record, target.fields));
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

export const aggregateRecords = defineTool({
    name: 'knack_aggregate_records',
    description:
        'Count or sum approved records with filters and grouping; returns aggregates only.',
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
                    type: z.enum(['count', 'sum']),
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
            if (metric.type === 'sum' && !metric.fieldKey) {
                throw new Error('A sum metric requires fieldKey.');
            }
        }
        const { fields } = await getPermittedReadFields(
            ctx,
            app,
            objectKey,
            requestedFields,
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
            const records = fetchedRecords.slice(0, scanLimit - scanned);
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
                for (const metric of metrics) {
                    const metricKey =
                        metric.type === 'count'
                            ? 'count'
                            : `sum:${metric.fieldKey}`;
                    if (metric.type === 'count') {
                        values[metricKey] = (values[metricKey] || 0) + 1;
                    } else {
                        const numeric = getNumericValue(
                            record[metric.fieldKey!],
                        );
                        if (numeric !== null) {
                            values[metricKey] =
                                (values[metricKey] || 0) + numeric;
                        }
                    }
                }
                groups.set(key, group);
            }

            scanned += records.length;
            // Whether Knack has more matching records beyond this page, judged on what
            // it actually returned before the scanLimit trim — trimming makes this
            // page's own count look partial even when Knack itself had more to give.
            hasMore = fetchedRecords.length === pageSize;
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
                      warning: `Only the first ${scanned} matching record(s) were scanned (scanLimit: ${scanLimit}); more records exist. These counts/sums are PARTIAL, not the true total — raise maxRecords or narrow filters before treating them as final.`,
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
        // list, never appear in the preview — the same set applyRecordReadPolicy
        // would return for a plain record read on this object.
        const permittedFieldKeys = app.dataAccess
            ? new Set(getDefaultPermittedFieldKeys(app, objectKey, obj))
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
            results,
            note: `Records were created with up to ${BATCH_CONCURRENCY} requests in flight at once, retrying individual requests on a 429 with backoff (not on 5xx — a lost/delayed 5xx response after a create that actually succeeded would otherwise risk creating a duplicate record). Check each entry in results for its own ok/status rather than assuming the whole batch succeeded.`,
        });
    },
});

export const updateRecords = defineTool({
    name: 'knack_update_records',
    description:
        'Update one or more records in an object, one request each with per-record results.',
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
            .max(100),
        dryRun: z.boolean().optional().default(false),
    },
    handler: async ({ appKey, objectKey, records, dryRun }, ctx) => {
        const app = ctx.getApp(appKey);

        const parsedRecords = records.map((record, index) => ({
            index,
            recordId: record.recordId,
            ...parseRecordPayload(record.data, `records[${index}].data`),
        }));
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
            requestedCount: records.length,
            successCount,
            failureCount,
            results,
            note: `Records were updated with up to ${BATCH_CONCURRENCY} requests in flight at once, retrying individual requests on a 429/5xx with backoff. Check each entry in results for its own ok/status rather than assuming the whole batch succeeded.`,
        });
    },
});

export const deleteRecords = defineTool({
    name: 'knack_delete_records',
    description:
        'Delete one or more records from an object; previews unless confirm is true.',
    access: 'delete',
    input: {
        appKey: z.string().optional(),
        objectKey: z.string(),
        recordIds: z.array(z.string()).min(1).max(100),
        confirm: z
            .boolean()
            .optional()
            .default(false)
            .describe(
                'Must be true to delete; otherwise a preview is returned.',
            ),
    },
    handler: async ({ appKey, objectKey, recordIds, confirm }, ctx) => {
        const app = ctx.getApp(appKey);

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
        const result = await knackFetchJson(url, {
            method: 'POST',
            headers: {
                'X-Knack-Application-Id': app.appId,
                'X-Knack-REST-API-Key': apiKey,
            },
            body: form,
        });
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
    findRecords,
    getRelatedRecords,
    aggregateRecords,
    verifyRecordFieldShapes,
    createRecords,
    updateRecords,
    deleteRecords,
    uploadAsset,
];
