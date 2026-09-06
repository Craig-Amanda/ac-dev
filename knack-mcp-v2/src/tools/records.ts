/**
 * Record tools: read one or many records under the app's read policy, follow
 * connections, aggregate, verify field shapes, and create/update/delete in batches.
 */
import fs from 'node:fs';
import path from 'node:path';

import { z } from 'zod';

import { BATCH_CONCURRENCY, DEFAULT_API_BASE } from '../config.js';
import { knackFetchJson } from '../http.js';
import { parseJsonObjectInput } from '../lib/field-payload.js';
import { getFieldShapeInfo } from '../lib/field-shapes.js';
import { getValuePreview, validateFieldShape } from '../lib/record-shapes.js';
import { asRecord, runWithConcurrency } from '../lib/util.js';
import {
    applyRecordReadPolicy,
    bucketDate,
    buildRecordSearchParams,
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

            for (const recordId of relatedIds) {
                const result = await ctx.request(
                    app,
                    `/objects/${targetObjectKey}/records/${recordId}`,
                );
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

        const groups = new Map<string, Record<string, unknown>>();
        let scanned = 0;
        let page = 1;
        let hasMore = true;

        while (hasMore && scanned < scanLimit) {
            const rowsPerPage = Math.min(1000, scanLimit - scanned);
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
                return makeTextResponse({
                    ok: false,
                    appKey: app.appKey,
                    objectKey,
                    status: result.status,
                    body: result.body,
                });
            }

            const records = getRecordsFromResponse(result);
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
            hasMore = records.length === rowsPerPage;
            page += 1;
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

        const results = (obj.fields || []).map((field) => {
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

const describeError = (error: unknown): string =>
    error instanceof Error ? error.message : String(error);

export const createRecords = defineTool({
    name: 'knack_create_records',
    description:
        'Create one or more records in an object, one request each with per-record results.',
    access: 'write',
    input: {
        appKey: z.string().optional(),
        objectKey: z.string(),
        records: z
            .array(z.string())
            .min(1)
            .max(100)
            .describe(
                'JSON strings of field_key: value pairs, one per record.',
            ),
        dryRun: z.boolean().optional().default(false),
    },
    handler: async ({ appKey, objectKey, records, dryRun }, ctx) => {
        const app = ctx.getApp(appKey);

        const parsedRecords = records.map((raw, index) => ({
            index,
            ...parseJsonObjectInput(raw, `records[${index}]`),
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

        const itemResults = await runWithConcurrency(
            parsedRecords,
            BATCH_CONCURRENCY,
            async (entry): Promise<BatchItemResult> => {
                try {
                    const result = await ctx.requestWithRetry(
                        app,
                        `/objects/${objectKey}/records`,
                        { method: 'POST', body: JSON.stringify(entry.payload) },
                    );
                    return {
                        index: entry.index,
                        ok: result.ok,
                        status: result.status,
                        body: result.body,
                    };
                } catch (error) {
                    return {
                        index: entry.index,
                        ok: false,
                        error: describeError(error),
                    };
                }
            },
        );

        const successCount = itemResults.filter((r) => r.ok).length;
        const failureCount = itemResults.length - successCount;

        return makeTextResponse({
            ok: failureCount === 0,
            appKey: app.appKey,
            objectKey,
            action: 'batch_create_records',
            requestedCount: records.length,
            successCount,
            failureCount,
            results: itemResults,
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
                    data: z
                        .string()
                        .describe('JSON string of field_key: value pairs'),
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
            ...parseJsonObjectInput(record.data, `records[${index}].data`),
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

        const itemResults = await runWithConcurrency(
            parsedRecords,
            BATCH_CONCURRENCY,
            async (entry): Promise<BatchItemResult> => {
                try {
                    const result = await ctx.requestWithRetry(
                        app,
                        `/objects/${objectKey}/records/${entry.recordId}`,
                        { method: 'PUT', body: JSON.stringify(entry.payload) },
                    );
                    return {
                        index: entry.index,
                        recordId: entry.recordId,
                        ok: result.ok,
                        status: result.status,
                        body: result.body,
                    };
                } catch (error) {
                    return {
                        index: entry.index,
                        recordId: entry.recordId,
                        ok: false,
                        error: describeError(error),
                    };
                }
            },
        );

        const successCount = itemResults.filter((r) => r.ok).length;
        const failureCount = itemResults.length - successCount;

        return makeTextResponse({
            ok: failureCount === 0,
            appKey: app.appKey,
            objectKey,
            action: 'batch_update_records',
            requestedCount: records.length,
            successCount,
            failureCount,
            results: itemResults,
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

        const itemResults = await runWithConcurrency(
            recordIds,
            BATCH_CONCURRENCY,
            async (recordId): Promise<BatchItemResult> => {
                try {
                    const result = await ctx.requestWithRetry(
                        app,
                        `/objects/${objectKey}/records/${recordId}`,
                        { method: 'DELETE' },
                    );
                    return {
                        recordId,
                        ok: result.ok,
                        status: result.status,
                        body: result.body,
                    };
                } catch (error) {
                    return { recordId, ok: false, error: describeError(error) };
                }
            },
        );

        const successCount = itemResults.filter((r) => r.ok).length;
        const failureCount = itemResults.length - successCount;

        return makeTextResponse({
            ok: failureCount === 0,
            appKey: app.appKey,
            objectKey,
            action: 'batch_delete_records',
            requestedCount: recordIds.length,
            successCount,
            failureCount,
            results: itemResults,
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
