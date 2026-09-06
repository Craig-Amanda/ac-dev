/**
 * Schema tools: the cached object/field model, alias resolution and mapping checks.
 *
 * Everything here reads the runtime metadata (or its on-disk fallback) through the
 * context caches; only knack_get_field and the diagnostic `raw` modes hit the REST API.
 */
import { z } from 'zod';

import { assertDiagnosticAccess } from '../access.js';
import type { AppConfig } from '../config.js';
import { makeFieldBuilderUrl } from '../lib/builder-urls.js';
import { resolveAliasToFieldKey } from '../lib/field-map.js';
import {
    KNACK_CONDITIONAL_RULES_SHAPE,
    KNACK_FIELD_SHAPES,
    getFieldShapeInfo,
} from '../lib/field-shapes.js';
import { getObjectAtPath } from '../lib/metadata.js';
import { asRecord } from '../lib/util.js';
import { type AnyToolDef, defineTool } from '../registry.js';
import { getInlineDetail, makeTextResponse } from '../response.js';
import type {
    CacheSource,
    CachedFieldMap,
    CachedObject,
    CachedSchema,
    RuntimeMetadata,
} from '../types.js';

const NO_SCHEMA_MESSAGE =
    'No schema available from runtime API or schema.json.';

const FIELD_MAP_FILE_NOTE =
    'Resolved from the on-disk fieldMap.json cache. If a field was created, renamed, or deleted recently, this map may be stale until knack_refresh_cache is run with warm: true, persistFiles: true.';

/** The projected field list shared by the summary and fields views of an object. */
function describeObjectFields(
    app: AppConfig,
    obj: CachedObject,
    runtimeMetadata: RuntimeMetadata | null,
) {
    return (obj.fields || []).map((field) => ({
        key: field.key,
        name: field.name,
        type: field.type,
        required: field.required,
        description: field.description,
        builderUrl: makeFieldBuilderUrl(
            app,
            { objectKey: obj.key, fieldKey: field.key },
            runtimeMetadata,
        ),
    }));
}

/** Every field key the schema knows about, across all objects. */
function collectValidFieldKeys(schema: CachedSchema): Set<string> {
    return new Set(
        (schema.objects || [])
            .flatMap((obj) => (obj.fields || []).map((field) => field.key))
            .filter((key): key is string => Boolean(key)),
    );
}

type MappingProblem = { mappingKey: string; input: string; reason: string };

/**
 * Resolve each mapping value (field key or alias) against the schema, grouping the
 * successes by resolved field so duplicates can be reported.
 */
function resolveMappingObject(
    mappingObject: Record<string, string>,
    fieldMap: CachedFieldMap,
    validFieldKeys: Set<string>,
): {
    resolvedMapping: Record<string, string>;
    invalid: MappingProblem[];
    usageByField: Map<string, string[]>;
} {
    const resolvedMapping: Record<string, string> = {};
    const invalid: MappingProblem[] = [];
    const usageByField = new Map<string, string[]>();

    for (const [mappingKey, value] of Object.entries(mappingObject)) {
        const directFieldKey = /^field_\d+$/i.test(value) ? value : null;
        const resolvedFieldKey =
            directFieldKey || resolveAliasToFieldKey(fieldMap, value) || null;

        if (!resolvedFieldKey) {
            invalid.push({
                mappingKey,
                input: value,
                reason: 'Not a field key and alias was not found in fieldMap.',
            });
            continue;
        }

        if (!validFieldKeys.has(resolvedFieldKey)) {
            invalid.push({
                mappingKey,
                input: value,
                reason: `Resolved to ${resolvedFieldKey}, but that field does not exist in schema.`,
            });
            continue;
        }

        resolvedMapping[mappingKey] = resolvedFieldKey;
        usageByField.set(resolvedFieldKey, [
            ...(usageByField.get(resolvedFieldKey) || []),
            mappingKey,
        ]);
    }

    return { resolvedMapping, invalid, usageByField };
}

export const listObjects = defineTool({
    name: 'knack_list_objects',
    description:
        'List every object in the app schema with its key, name and field count.',
    access: 'read',
    input: {
        appKey: z.string().optional(),
    },
    handler: async ({ appKey }, ctx) => {
        const app = ctx.getApp(appKey);
        const { schema, source } = await ctx.getSchema(app);

        if (!schema?.objects?.length) {
            return makeTextResponse({
                ok: false,
                appKey: app.appKey,
                message: NO_SCHEMA_MESSAGE,
            });
        }

        return makeTextResponse({
            ok: true,
            appKey: app.appKey,
            source,
            objectCount: schema.objects.length,
            objects: schema.objects.map((obj) => ({
                key: obj.key,
                name: obj.name,
                fieldCount: (obj.fields || []).length,
            })),
        });
    },
});

export const getObject = defineTool({
    name: 'knack_get_object',
    description:
        'Return one object: its fields (default), a summary, a type breakdown, or the raw API/metadata payload.',
    access: 'read',
    input: {
        appKey: z.string().optional(),
        objectKey: z.string(),
        detail: z
            .enum(['summary', 'fields', 'types', 'raw', 'rawMetadata'])
            .default('fields')
            .describe('raw/rawMetadata need allowDiagnostics on the app'),
    },
    handler: async ({ appKey, objectKey, detail }, ctx) => {
        const app = ctx.getApp(appKey);

        if (detail === 'raw' || detail === 'rawMetadata') {
            assertDiagnosticAccess(app, ctx.options);
        }

        if (detail === 'raw') {
            const result = await ctx.request(app, `/objects/${objectKey}`);
            const bodyDetail = getInlineDetail(result.body);
            return makeTextResponse({
                appKey: app.appKey,
                objectKey,
                action: 'get_raw_object',
                ok: result.ok,
                status: result.status,
                bodyIncluded: bodyDetail.included,
                bodySizeBytes: bodyDetail.sizeBytes,
                body: bodyDetail.value,
                bodySummary: bodyDetail.summary,
            });
        }

        if (detail === 'rawMetadata') {
            const runtimeMetadata = await ctx.getRuntimeMetadata(app);
            if (!runtimeMetadata) {
                return makeTextResponse({
                    ok: false,
                    appKey: app.appKey,
                    message:
                        'No runtime metadata available from Knack application metadata endpoint.',
                });
            }

            const directObjects = getObjectAtPath(runtimeMetadata, 'objects');
            const nestedObjects = getObjectAtPath(
                runtimeMetadata,
                'application',
                'objects',
            );
            const objectsRaw = Array.isArray(directObjects)
                ? directObjects
                : Array.isArray(nestedObjects)
                  ? nestedObjects
                  : null;

            if (!objectsRaw) {
                return makeTextResponse({
                    ok: false,
                    appKey: app.appKey,
                    message:
                        'Runtime metadata did not contain an objects array.',
                });
            }

            const rawObject = objectsRaw.find((entry) => {
                const obj = asRecord(entry);
                return obj && obj.key === objectKey;
            });

            if (!rawObject) {
                return makeTextResponse({
                    ok: false,
                    appKey: app.appKey,
                    objectKey,
                    message: `Object not found in runtime metadata: ${objectKey}`,
                    availableObjectKeys: objectsRaw
                        .map((entry) => {
                            const obj = asRecord(entry);
                            return typeof obj?.key === 'string'
                                ? obj.key
                                : null;
                        })
                        .filter((key): key is string => Boolean(key)),
                });
            }

            const rawObjectDetail = getInlineDetail(rawObject);
            return makeTextResponse({
                ok: true,
                appKey: app.appKey,
                source: 'runtime',
                objectKey,
                rawObjectIncluded: rawObjectDetail.included,
                rawObjectSizeBytes: rawObjectDetail.sizeBytes,
                rawObject: rawObjectDetail.value,
                rawObjectSummary: rawObjectDetail.summary,
            });
        }

        const { schema, source } = await ctx.getSchema(app);
        if (!schema?.objects?.length) {
            return makeTextResponse({
                ok: false,
                appKey: app.appKey,
                message: NO_SCHEMA_MESSAGE,
            });
        }

        const obj = schema.objects.find((entry) => entry.key === objectKey);
        if (!obj) {
            return makeTextResponse({
                ok: false,
                appKey: app.appKey,
                ...(detail === 'types' ? { schemaSource: source } : { source }),
                message: `Object not found in schema: ${objectKey}`,
                availableObjectKeys: schema.objects.map((entry) => entry.key),
            });
        }

        if (detail === 'types') {
            const fields = (obj.fields || []).map((field) => ({
                fieldKey: field.key,
                fieldName: field.name,
                fieldType: field.type || null,
            }));

            const typeCounts = new Map<string, number>();
            fields.forEach((field) => {
                const typeKey = field.fieldType || 'unknown';
                typeCounts.set(typeKey, (typeCounts.get(typeKey) || 0) + 1);
            });

            const typeSummary = [...typeCounts.entries()]
                .map(([fieldType, count]) => ({ fieldType, count }))
                .sort(
                    (a, b) =>
                        b.count - a.count ||
                        a.fieldType.localeCompare(b.fieldType),
                );

            return makeTextResponse({
                ok: true,
                appKey: app.appKey,
                schemaSource: source,
                objectKey: obj.key,
                objectName: obj.name,
                fieldCount: fields.length,
                typeSummary,
                fields,
            });
        }

        const runtimeMetadata = await ctx.getRuntimeMetadata(app);
        const fields = describeObjectFields(app, obj, runtimeMetadata);

        if (detail === 'summary') {
            return makeTextResponse({
                ok: true,
                appKey: app.appKey,
                source,
                object: {
                    key: obj.key,
                    name: obj.name,
                    fieldCount: (obj.fields || []).length,
                    fields,
                },
            });
        }

        return makeTextResponse({
            ok: true,
            appKey: app.appKey,
            source,
            objectKey: obj.key,
            objectName: obj.name,
            fields,
        });
    },
});

export const getField = defineTool({
    name: 'knack_get_field',
    description:
        'Return the complete raw definition of one field (format, rules, relationship) from the Knack API.',
    access: 'read',
    input: {
        appKey: z.string().optional(),
        objectKey: z.string(),
        fieldKey: z.string(),
    },
    handler: async ({ appKey, objectKey, fieldKey }, ctx) => {
        const app = ctx.getApp(appKey);
        const result = await ctx.request(app, `/objects/${objectKey}`);

        if (!result.ok) {
            return makeTextResponse({
                ok: false,
                appKey: app.appKey,
                objectKey,
                fieldKey,
                action: 'get_field',
                status: result.status,
                message: `Could not fetch object ${objectKey} from the Knack API.`,
            });
        }

        const rawFields = asRecord(asRecord(result.body)?.object)?.fields;
        const fields = (Array.isArray(rawFields) ? rawFields : [])
            .map((entry) => asRecord(entry))
            .filter((entry): entry is Record<string, unknown> =>
                Boolean(entry),
            );
        const field = fields.find((entry) => entry.key === fieldKey);
        if (!field) {
            return makeTextResponse({
                ok: false,
                appKey: app.appKey,
                objectKey,
                fieldKey,
                action: 'get_field',
                message: `Field ${fieldKey} not found on ${objectKey}.`,
                availableFieldKeys: fields
                    .map((entry) =>
                        typeof entry.key === 'string' ? entry.key : null,
                    )
                    .filter((key): key is string => Boolean(key)),
            });
        }

        return makeTextResponse({
            ok: true,
            appKey: app.appKey,
            objectKey,
            fieldKey,
            action: 'get_field',
            field,
        });
    },
});

export const resolve = defineTool({
    name: 'knack_resolve',
    description:
        'Resolve a field key or fieldMap alias to its field key, name, type, object and Builder URL.',
    access: 'read',
    input: {
        appKey: z.string().optional(),
        identifier: z
            .string()
            .describe('Field key (field_12) or alias (object_2.name)'),
        objectKey: z
            .string()
            .optional()
            .describe('Restrict the lookup to one object'),
    },
    handler: async ({ appKey, identifier, objectKey }, ctx) => {
        const app = ctx.getApp(appKey);

        const schemaResult = await ctx.getSchema(app);
        const schema = schemaResult.schema;
        if (!schema?.objects?.length) {
            return makeTextResponse({
                ok: false,
                appKey: app.appKey,
                message: NO_SCHEMA_MESSAGE,
            });
        }

        const trimmed = identifier.trim();
        if (!trimmed) {
            return makeTextResponse({
                ok: false,
                appKey: app.appKey,
                message: 'identifier cannot be empty.',
            });
        }

        let resolvedFieldKey: string | null;
        let resolvedBy: 'fieldKey' | 'alias';
        let fieldMapSource: CacheSource | null = null;

        if (/^field_\d+$/i.test(trimmed)) {
            resolvedFieldKey = trimmed;
            resolvedBy = 'fieldKey';
        } else {
            const fieldMapResult = await ctx.getFieldMap(app);
            fieldMapSource = fieldMapResult.source;
            const fieldMap = fieldMapResult.fieldMap;

            if (!fieldMap) {
                return makeTextResponse({
                    ok: false,
                    appKey: app.appKey,
                    schemaSource: schemaResult.source,
                    message:
                        'No field map available from runtime API or fieldMap.json; cannot resolve alias identifier.',
                });
            }

            resolvedFieldKey = resolveAliasToFieldKey(fieldMap, trimmed);
            if (!resolvedFieldKey) {
                return makeTextResponse({
                    ok: false,
                    appKey: app.appKey,
                    schemaSource: schemaResult.source,
                    fieldMapSource,
                    identifier: trimmed,
                    message: 'Identifier not found as alias or field key.',
                    availableAliases: Object.keys(fieldMap),
                });
            }
            resolvedBy = 'alias';
        }

        const runtimeMetadata = await ctx.getRuntimeMetadata(app);
        const matches: Array<{
            objectKey: string;
            objectName?: string;
            fieldKey: string;
            fieldName?: string;
            fieldType?: string;
            builderUrl: string | null;
        }> = [];

        for (const obj of schema.objects) {
            if (objectKey && obj.key !== objectKey) continue;
            for (const field of obj.fields || []) {
                if (field.key !== resolvedFieldKey) continue;
                matches.push({
                    objectKey: obj.key,
                    objectName: obj.name,
                    fieldKey: field.key,
                    fieldName: field.name,
                    fieldType: field.type,
                    builderUrl: makeFieldBuilderUrl(
                        app,
                        { objectKey: obj.key, fieldKey: field.key },
                        runtimeMetadata,
                    ),
                });
            }
        }

        if (!matches.length) {
            return makeTextResponse({
                ok: false,
                appKey: app.appKey,
                schemaSource: schemaResult.source,
                fieldMapSource,
                resolvedFieldKey,
                message: objectKey
                    ? `Resolved field not found in schema for object ${objectKey}: ${resolvedFieldKey}`
                    : `Resolved field not found in schema: ${resolvedFieldKey}`,
            });
        }

        return makeTextResponse({
            ok: true,
            appKey: app.appKey,
            schemaSource: schemaResult.source,
            fieldMapSource,
            identifier: trimmed,
            resolvedBy,
            resolvedFieldKey,
            matchCount: matches.length,
            matches,
            primary: matches[0],
            ...(resolvedBy === 'alias' && fieldMapSource === 'file'
                ? { note: FIELD_MAP_FILE_NOTE }
                : {}),
        });
    },
});

export const getObjectConnections = defineTool({
    name: 'knack_get_object_connections',
    description:
        'List the connection fields of an object and the objects they link to.',
    access: 'read',
    input: {
        appKey: z.string().optional(),
        objectKey: z.string(),
    },
    handler: async ({ appKey, objectKey }, ctx) => {
        const app = ctx.getApp(appKey);
        const { schema, source } = await ctx.getSchema(app);

        if (!schema?.objects?.length) {
            return makeTextResponse({
                ok: false,
                appKey: app.appKey,
                message: NO_SCHEMA_MESSAGE,
            });
        }

        const obj = schema.objects.find((entry) => entry.key === objectKey);
        if (!obj) {
            return makeTextResponse({
                ok: false,
                appKey: app.appKey,
                source,
                message: `Object not found in schema: ${objectKey}`,
                availableObjectKeys: schema.objects.map((entry) => entry.key),
            });
        }

        const connectionFields = (obj.fields || [])
            .filter((field) => field.type === 'connection')
            .map((field) => {
                const connectedObjectKey = field.connectedObject || null;
                const connectedObject = connectedObjectKey
                    ? schema.objects?.find(
                          (o) => o.key === connectedObjectKey,
                      ) || null
                    : null;
                return {
                    fieldKey: field.key,
                    fieldName: field.name,
                    connectedObjectKey,
                    connectedObjectName: connectedObject?.name || null,
                };
            });

        return makeTextResponse({
            ok: true,
            appKey: app.appKey,
            source,
            objectKey: obj.key,
            objectName: obj.name,
            connectionCount: connectionFields.length,
            connections: connectionFields,
            note: connectionFields.some((c) => !c.connectedObjectKey)
                ? 'Some connection targets are unknown. Run knack_refresh_cache with warm:true to load fresh runtime metadata which includes relationship details.'
                : null,
        });
    },
});

export const describeFieldShape = defineTool({
    name: 'knack_describe_field_shape',
    description:
        'Describe a field type: its record value shapes, the create/update definition shape and rule shape.',
    access: 'read',
    input: {
        fieldType: z
            .string()
            .describe('e.g. connection, date_time, multiple_choice'),
    },
    handler: async ({ fieldType }) => {
        const info = getFieldShapeInfo(fieldType);

        if (!info) {
            const knownTypes = Object.keys(KNACK_FIELD_SHAPES).sort();
            return makeTextResponse({
                ok: false,
                fieldType,
                message: `Unknown field type: ${fieldType}. See knownTypes for the full list.`,
                knownTypes,
            });
        }

        return makeTextResponse({
            ok: true,
            fieldType,
            summary: info.summary,
            valueShape: {
                formattedShape: info.formattedShape,
                rawShape: info.rawShape,
                notes: info.notes || null,
                tip: 'Knack returns both field_xxx (formatted) and field_xxx_raw (raw) for every field. Prefer raw values when you need machine-readable data (numbers, IDs, arrays).',
            },
            definitionShape: info.definitionShape
                ? {
                      format: info.definitionShape,
                      notes: info.definitionNotes || null,
                      tip: 'This is the format/relationship payload for knack_create_field or knack_update_field — not what a record value looks like. Use knack_get_field on a working example field of this type to see a live comparison.',
                  }
                : {
                      format: null,
                      notes: `No verified definition example is recorded yet for "${fieldType}". Use knack_get_field on a working example field of this type on your app to read one instead of guessing.`,
                  },
            conditionalRules: KNACK_CONDITIONAL_RULES_SHAPE,
        });
    },
});

export const validateFieldMapping = defineTool({
    name: 'knack_validate_field_mapping',
    description:
        'Validate a mapping of names to field keys or aliases, reporting unresolved and duplicate entries.',
    access: 'read',
    input: {
        appKey: z.string().optional(),
        mappingObject: z.record(z.string(), z.string()),
    },
    handler: async ({ appKey, mappingObject }, ctx) => {
        const app = ctx.getApp(appKey);
        const schemaResult = await ctx.getSchema(app);
        const fieldMapResult = await ctx.getFieldMap(app);
        const schema = schemaResult.schema;
        const fieldMap = fieldMapResult.fieldMap || {};

        if (!schema?.objects?.length) {
            return makeTextResponse({
                ok: false,
                appKey: app.appKey,
                message: NO_SCHEMA_MESSAGE,
            });
        }

        const { resolvedMapping, invalid, usageByField } = resolveMappingObject(
            mappingObject,
            fieldMap,
            collectValidFieldKeys(schema),
        );

        const duplicateResolvedFields = [...usageByField.entries()]
            .filter(([, mappingKeys]) => mappingKeys.length > 1)
            .map(([fieldKey, mappingKeys]) => ({ fieldKey, mappingKeys }));

        return makeTextResponse({
            ok: invalid.length === 0,
            appKey: app.appKey,
            schemaSource: schemaResult.source,
            fieldMapSource: fieldMapResult.source,
            totalMappings: Object.keys(mappingObject).length,
            validMappings: Object.keys(resolvedMapping).length,
            invalidMappings: invalid,
            duplicateResolvedFields,
            resolvedMapping,
        });
    },
});

export const generateSnapshotStructure = defineTool({
    name: 'knack_generate_snapshot_structure',
    description:
        'Return empty snapshot templates for an object keyed by field key and by field name.',
    access: 'read',
    input: {
        appKey: z.string().optional(),
        objectKey: z.string(),
    },
    handler: async ({ appKey, objectKey }, ctx) => {
        const app = ctx.getApp(appKey);
        const { schema, source } = await ctx.getSchema(app);

        if (!schema?.objects?.length) {
            return makeTextResponse({
                ok: false,
                appKey: app.appKey,
                message: NO_SCHEMA_MESSAGE,
            });
        }

        const obj = schema.objects.find((entry) => entry.key === objectKey);
        if (!obj) {
            return makeTextResponse({
                ok: false,
                appKey: app.appKey,
                source,
                message: `Object not found in schema: ${objectKey}`,
            });
        }

        const snapshotByFieldKey: Record<string, null> = {};
        const snapshotByFieldName: Record<string, null> = {};

        for (const field of obj.fields || []) {
            snapshotByFieldKey[field.key] = null;
            if (field.name) {
                snapshotByFieldName[field.name] = null;
            }
        }

        return makeTextResponse({
            ok: true,
            appKey: app.appKey,
            source,
            objectKey: obj.key,
            objectName: obj.name,
            fieldCount: (obj.fields || []).length,
            snapshotByFieldKey,
            snapshotByFieldName,
        });
    },
});

export const checkDuplicateFieldUsage = defineTool({
    name: 'knack_check_duplicate_field_usage',
    description:
        'Report fields referenced by more than one fieldMap alias, and by more than one key of a mapping.',
    access: 'read',
    input: {
        appKey: z.string().optional(),
        mappingObject: z.record(z.string(), z.string()).optional(),
    },
    handler: async ({ appKey, mappingObject }, ctx) => {
        const app = ctx.getApp(appKey);
        const schemaResult = await ctx.getSchema(app);
        const fieldMapResult = await ctx.getFieldMap(app);
        const schema = schemaResult.schema;
        const fieldMap = fieldMapResult.fieldMap || {};

        if (!schema?.objects?.length) {
            return makeTextResponse({
                ok: false,
                appKey: app.appKey,
                message: NO_SCHEMA_MESSAGE,
            });
        }

        const validFieldKeys = collectValidFieldKeys(schema);

        const aliasUsageByField = new Map<string, string[]>();
        for (const [alias, entry] of Object.entries(fieldMap)) {
            const fieldKey = entry.fieldKey;
            if (!validFieldKeys.has(fieldKey)) continue;
            aliasUsageByField.set(fieldKey, [
                ...(aliasUsageByField.get(fieldKey) || []),
                alias,
            ]);
        }

        const fieldMapDuplicates = [...aliasUsageByField.entries()]
            .filter(([, aliases]) => aliases.length > 1)
            .map(([fieldKey, aliases]) => ({ fieldKey, aliases }));

        let mappingDuplicates: Array<{
            fieldKey: string;
            mappingKeys: string[];
        }> = [];
        let mappingInvalidEntries: MappingProblem[] = [];

        if (mappingObject) {
            const { invalid, usageByField } = resolveMappingObject(
                mappingObject,
                fieldMap,
                validFieldKeys,
            );
            mappingInvalidEntries = invalid;
            mappingDuplicates = [...usageByField.entries()]
                .filter(([, mappingKeys]) => mappingKeys.length > 1)
                .map(([fieldKey, mappingKeys]) => ({ fieldKey, mappingKeys }));
        }

        return makeTextResponse({
            ok: true,
            appKey: app.appKey,
            schemaSource: schemaResult.source,
            fieldMapSource: fieldMapResult.source,
            fieldMapDuplicateCount: fieldMapDuplicates.length,
            fieldMapDuplicates,
            mappingProvided: Boolean(mappingObject),
            mappingDuplicateCount: mappingDuplicates.length,
            mappingDuplicates,
            mappingInvalidEntries,
        });
    },
});

export const schemaTools: AnyToolDef[] = [
    listObjects,
    getObject,
    getField,
    resolve,
    getObjectConnections,
    describeFieldShape,
    validateFieldMapping,
    generateSnapshotStructure,
    checkDuplicateFieldUsage,
];
