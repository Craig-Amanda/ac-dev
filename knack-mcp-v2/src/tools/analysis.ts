/**
 * Read-only analysis over cached metadata: targeted context bundles, data-model
 * overviews, field-reference lookups, KTL keyword and email searches, and seed CSVs.
 */
import { z } from 'zod';

import { BATCH_CONCURRENCY, type AppConfig } from '../config.js';
import type { KnackContext } from '../context.js';
import { buildAppOverview, buildDataModelAnalysis } from '../lib/analysis.js';
import {
    makeFieldBuilderUrl,
    makeSceneBuilderUrl,
    makeViewBuilderUrl,
} from '../lib/builder-urls.js';
import {
    FIELD_ALIAS_OBJECT_FIELD_KEY_PATTERN,
    FIELD_KEY_PATTERN,
} from '../lib/field-payload.js';
import {
    collectEmailNodes,
    extractKtlKeywordsFromText,
    truncateText,
} from '../lib/field-references.js';
import {
    getViewFieldSettings,
    getViewObjectFields,
    parseRuntimeViewContextMap,
} from '../lib/metadata.js';
import { extractConnectionDisplayValues } from '../lib/record-shapes.js';
import { runWithConcurrency } from '../lib/util.js';
import {
    type ExternalConnectionLookup,
    generateSeedCsvWorkbook,
    getExternalSeedConnectionTargets,
} from '../lib/seed-csv.js';
import { type AnyToolDef, defineTool } from '../registry.js';
import { getInlineDetail, makeTextResponse } from '../response.js';
import type { CachedObject, FieldReference } from '../types.js';

export const getContextBundle = defineTool({
    name: 'knack_get_context_bundle',
    description:
        'Fetch selected object schemas, resolved field aliases and view context in one call.',
    access: 'read',
    input: {
        appKey: z.string().optional(),
        objectKeys: z.array(z.string()).min(1).max(20).optional(),
        fieldAliases: z
            .array(z.string())
            .min(1)
            .max(100)
            .optional()
            .describe(
                '"object_key.field_key" or fieldMap alias "object_key.name"',
            ),
        viewKeys: z.array(z.string()).min(1).max(20).optional(),
        includeViewAttributes: z.boolean().default(false),
    },
    handler: async (
        { appKey, objectKeys, fieldAliases, viewKeys, includeViewAttributes },
        ctx,
    ) => {
        const requestedObjectKeys = [...new Set(objectKeys || [])];
        const requestedAliases = [...new Set(fieldAliases || [])];
        const requestedViewKeys = [...new Set(viewKeys || [])];

        if (
            !requestedObjectKeys.length &&
            !requestedAliases.length &&
            !requestedViewKeys.length
        ) {
            return makeTextResponse({
                ok: false,
                message:
                    'Provide at least one objectKey, fieldAlias, or viewKey. This tool intentionally does not return an unbounded app dump.',
            });
        }

        const app = ctx.getApp(appKey);

        const hasQualifiedFieldKeyAlias = requestedAliases.some((alias) =>
            FIELD_ALIAS_OBJECT_FIELD_KEY_PATTERN.test(alias),
        );

        const [schemaResult, fieldMapResult, viewMapResult, runtimeMetadata] =
            await Promise.all([
                requestedObjectKeys.length ||
                requestedViewKeys.length ||
                hasQualifiedFieldKeyAlias
                    ? ctx.getSchema(app)
                    : Promise.resolve(null),
                requestedAliases.length
                    ? ctx.getFieldMap(app)
                    : Promise.resolve(null),
                requestedViewKeys.length
                    ? ctx.getViewMap(app)
                    : Promise.resolve(null),
                requestedObjectKeys.length || requestedViewKeys.length
                    ? ctx.getRuntimeMetadata(app)
                    : Promise.resolve(null),
            ]);

        const viewContextMap = parseRuntimeViewContextMap(runtimeMetadata);
        const schemaObjects = schemaResult?.schema?.objects || [];
        const objectByKey = new Map(
            schemaObjects.map((object) => [object.key, object]),
        );
        const fieldMap = fieldMapResult?.fieldMap || {};
        const viewMap = viewMapResult?.viewMap || {};

        const objects = requestedObjectKeys.map((objectKey) => {
            const object = objectByKey.get(objectKey);
            return object
                ? {
                      found: true,
                      key: object.key,
                      name: object.name,
                      fields: (object.fields || []).map((field) => ({
                          key: field.key,
                          name: field.name,
                          type: field.type,
                          required: field.required,
                          description: field.description,
                          connectedObject: field.connectedObject,
                          builderUrl: makeFieldBuilderUrl(
                              app,
                              { objectKey: object.key, fieldKey: field.key },
                              runtimeMetadata,
                          ),
                      })),
                  }
                : { found: false, key: objectKey };
        });

        const aliases = requestedAliases.map((alias) => {
            const qualifiedKeyMatch = alias.match(
                FIELD_ALIAS_OBJECT_FIELD_KEY_PATTERN,
            );
            if (qualifiedKeyMatch) {
                const [, objectKey, fieldKey] = qualifiedKeyMatch;
                const object = objectByKey.get(objectKey);
                const field = object?.fields?.find(
                    (entry) => entry.key === fieldKey,
                );
                if (field) {
                    return {
                        found: true,
                        alias,
                        fieldKey: field.key,
                        fieldType: field.type || null,
                    };
                }
                return {
                    found: false,
                    alias,
                    message: object
                        ? `${fieldKey} was not found on ${objectKey}.`
                        : `${objectKey} was not found in the cached schema for this app. Confirm the object key is correct, or run knack_cache with appKey set to this app plus refresh: true if it was added or renamed recently — the schema is loaded in full regardless of which objectKeys were requested.`,
                };
            }

            const entry = fieldMap[alias];
            if (entry) {
                return {
                    found: true,
                    alias,
                    fieldKey: entry.fieldKey,
                    fieldType: entry.fieldType || null,
                };
            }
            return {
                found: false,
                alias,
                message:
                    'Alias not found. fieldAliases accepts either a direct "object_key.field_key" reference (e.g. object_2.field_123) or a fieldMap alias in "object_key.normalised_field_name" form (e.g. object_2.name) — a bare field name or field key without the object_key prefix will not resolve.',
            };
        });

        const views = requestedViewKeys.map((viewKey) => {
            const attributes = viewMap[viewKey];
            const context = viewContextMap[viewKey] || {};
            const viewName =
                typeof attributes?.name === 'string'
                    ? attributes.name
                    : undefined;
            const viewType =
                typeof attributes?.type === 'string'
                    ? attributes.type
                    : undefined;
            const attributesDetail =
                includeViewAttributes && attributes
                    ? getInlineDetail(attributes)
                    : null;
            const fieldSettings = attributes
                ? getViewFieldSettings(
                      attributes,
                      getViewObjectFields(attributes, schemaResult?.schema),
                  )
                : null;

            return {
                found: Boolean(attributes || context.sceneKey),
                viewKey,
                ...context,
                viewName,
                viewType,
                builderUrl: makeViewBuilderUrl(
                    app,
                    { sceneKey: context.sceneKey, viewKey, viewType },
                    runtimeMetadata,
                ),
                attributesIncluded: attributesDetail?.included || false,
                attributes: attributesDetail?.value,
                attributesSummary: attributesDetail?.summary,
                attributesSizeBytes: attributesDetail?.sizeBytes,
                fieldSettings,
            };
        });

        return makeTextResponse({
            ok: true,
            appKey: app.appKey,
            requested: {
                objectKeys: requestedObjectKeys,
                fieldAliases: requestedAliases,
                viewKeys: requestedViewKeys,
                includeViewAttributes,
            },
            sources: {
                schema: schemaResult?.source || null,
                fieldMap: fieldMapResult?.source || null,
                viewMap: viewMapResult?.source || null,
                viewContext: runtimeMetadata ? 'runtime' : null,
            },
            objects,
            aliases,
            views,
        });
    },
});

export const getAppOverview = defineTool({
    name: 'knack_get_app_overview',
    description:
        'Summarise every object with field counts, field types and connection relationships.',
    access: 'read',
    input: {
        appKey: z.string().optional(),
        includeFieldDetails: z.boolean().default(false),
    },
    handler: async ({ appKey, includeFieldDetails }, ctx) => {
        const app = ctx.getApp(appKey);
        const { schema, source } = await ctx.getSchema(app);

        if (!schema?.objects?.length) {
            return makeTextResponse({
                ok: false,
                appKey: app.appKey,
                message: 'No schema available from runtime API or schema.json.',
            });
        }

        const overview = buildAppOverview(schema, includeFieldDetails);

        return makeTextResponse({
            ok: true,
            appKey: app.appKey,
            source,
            objectCount: overview.objectCount,
            totalFields: overview.totalFields,
            relationshipCount: overview.relationshipCount,
            objects: overview.objects,
            relationships: overview.relationships,
        });
    },
});

export const analyzeDataModel = defineTool({
    name: 'knack_analyze_data_model',
    description:
        'Return data-model design feedback: field distribution, isolated objects, connection density.',
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
                message:
                    'No schema available. Run knack_cache with appKey set to this app plus refresh: true and warm: true, or ensure schema.json is present. Without the appKey it refreshes every configured app.',
            });
        }

        const analysis = buildDataModelAnalysis(schema);

        return makeTextResponse({
            ok: true,
            appKey: app.appKey,
            source,
            ...analysis,
        });
    },
});

export const appDeepDive = defineTool({
    name: 'knack_app_deep_dive',
    description:
        'One-call onboarding snapshot: data model, design observations and UI structure summary.',
    access: 'read',
    input: {
        appKey: z.string().optional(),
        includeFieldDetails: z.boolean().default(false),
        includeScenes: z
            .boolean()
            .default(false)
            .describe('List each scene under ui.scenes'),
        maxRelationshipsListed: z.number().int().min(0).max(2000).default(200),
    },
    handler: async (
        { appKey, includeFieldDetails, includeScenes, maxRelationshipsListed },
        ctx,
    ) => {
        const app = ctx.getApp(appKey);

        const { schema, source } = await ctx.getSchema(app);
        if (!schema?.objects?.length) {
            return makeTextResponse({
                ok: false,
                appKey: app.appKey,
                message:
                    'No schema available from runtime API or schema.json. Run knack_cache with appKey set to this app plus refresh: true and warm: true, or ensure schema.json is present.',
            });
        }

        const overview = buildAppOverview(schema, includeFieldDetails);
        const analysis = buildDataModelAnalysis(schema);

        const relationshipsTruncated =
            overview.relationships.length > maxRelationshipsListed;
        const relationships = overview.relationships.slice(
            0,
            maxRelationshipsListed,
        );

        const scenes = await ctx.getScenes(app);
        const viewTypeCounts = new Map<string, number>();
        let totalViewCount = 0;
        for (const scene of scenes) {
            for (const view of scene.views) {
                totalViewCount += 1;
                const vType = view.viewType || 'unknown';
                viewTypeCounts.set(vType, (viewTypeCounts.get(vType) || 0) + 1);
            }
        }
        const viewTypeSummary = [...viewTypeCounts.entries()]
            .map(([type, count]) => ({ type, count }))
            .sort((a, b) => b.count - a.count);

        const ui: Record<string, unknown> = scenes.length
            ? {
                  available: true,
                  sceneCount: scenes.length,
                  totalViewCount,
                  viewTypeSummary,
              }
            : {
                  available: false,
                  message:
                      'No scene/view metadata cached yet. Run knack_cache with appKey set to this app plus refresh: true and warm: true to include UI structure here.',
              };

        if (includeScenes && scenes.length) {
            ui.scenes = scenes.map((scene) => ({
                sceneKey: scene.sceneKey,
                sceneName: scene.sceneName,
                sceneSlug: scene.sceneSlug,
                viewCount: scene.views.length,
            }));
        }

        return makeTextResponse({
            ok: true,
            appKey: app.appKey,
            source,
            dataModel: {
                objectCount: overview.objectCount,
                totalFields: overview.totalFields,
                relationshipCount: overview.relationshipCount,
                relationshipsTruncated,
                objects: overview.objects,
                relationships,
                analysisSummary: analysis.summary,
                fieldTypeDistribution: analysis.fieldTypeDistribution,
                isolatedObjects: analysis.isolatedObjects,
                highFieldCountObjects: analysis.highFieldCountObjects,
                lowFieldCountObjects: analysis.lowFieldCountObjects,
                observations: analysis.observations,
            },
            ui,
            nextSteps: [
                'knack_get_app_overview / knack_analyze_data_model for the full data-model detail behind this summary.',
                'knack_list_scenes / knack_list_views to drill into specific pages once you know what you are looking for.',
                'knack_get_object_connections on a specific object to trace its relationships in isolation.',
            ],
        });
    },
});

/** Every class the reference index assigns; see classifyFieldReference. */
const FIELD_REFERENCE_CLASSIFICATIONS = [
    'schema',
    'schemaMetadata',
    'fieldDefinition',
    'fieldMap',
    'fieldAlias',
    'viewMap',
    'view',
    'rule',
    'record',
    'viewRecordRule',
    'propertyKey',
] as const;

type ViewReferenceGroup = {
    viewKey: string;
    viewName?: string;
    viewType?: string;
    sceneKey?: string;
    sceneName?: string;
    sceneSlug?: string;
    matchedPaths: string[];
    matches: FieldReference[];
};

export const listFieldReferences = defineTool({
    name: 'knack_list_field_references',
    description:
        'List cached schema, alias and view references to one field, optionally grouped by view.',
    access: 'read',
    input: {
        appKey: z.string().optional(),
        fieldKey: z.string().regex(FIELD_KEY_PATTERN),
        classification: z
            .enum(FIELD_REFERENCE_CLASSIFICATIONS)
            .optional()
            .describe('Keep only references carrying this class'),
        groupByView: z
            .boolean()
            .default(false)
            .describe('Group view references per view'),
        maxResults: z.number().int().min(1).max(10000).default(200),
    },
    handler: async (
        { appKey, fieldKey, classification, groupByView, maxResults },
        ctx,
    ) => {
        const app = ctx.getApp(appKey);
        const normalisedFieldKey = fieldKey.toLowerCase();

        const fieldReferenceResult = await ctx.getFieldReferenceIndex(app);
        const allReferences =
            fieldReferenceResult.index?.[normalisedFieldKey] || [];
        const runtimeMetadata = await ctx.getRuntimeMetadata(app);
        const fieldOwner = await ctx.findFieldOwner(app, normalisedFieldKey);

        const fieldBuilderUrl = makeFieldBuilderUrl(
            app,
            { objectKey: fieldOwner?.objectKey, fieldKey: normalisedFieldKey },
            runtimeMetadata,
        );

        if (groupByView) {
            // The legacy knack_find_views_with_record_rule_field shape: the same index,
            // narrowed to references inside a view and grouped per view.
            const allViewRefs = allReferences.filter(
                (reference) =>
                    reference.viewKey &&
                    (!classification ||
                        reference.classification.includes(classification)),
            );
            // Counted before slicing, so a truncated result still reports its true
            // total rather than reporting back its own cap.
            const viewRefs = allViewRefs.slice(0, maxResults);

            const viewsByKey = new Map<string, ViewReferenceGroup>();
            for (const reference of viewRefs) {
                if (!reference.viewKey) continue;
                const existing = viewsByKey.get(reference.viewKey) || {
                    viewKey: reference.viewKey,
                    viewName: reference.viewName,
                    viewType: reference.viewType,
                    sceneKey: reference.sceneKey,
                    sceneName: reference.sceneName,
                    sceneSlug: reference.sceneSlug,
                    matchedPaths: [],
                    matches: [],
                };
                existing.matchedPaths.push(reference.path);
                existing.matches.push(reference);
                viewsByKey.set(reference.viewKey, existing);
            }

            const results = [...viewsByKey.values()].map((entry) => ({
                ...entry,
                matchedPaths: [...new Set(entry.matchedPaths)].sort(
                    (left, right) => left.localeCompare(right),
                ),
                matchCount: entry.matches.length,
                builderUrls: {
                    scene: makeSceneBuilderUrl(
                        app,
                        entry.sceneKey,
                        runtimeMetadata,
                    ),
                    view: makeViewBuilderUrl(
                        app,
                        {
                            sceneKey: entry.sceneKey,
                            viewKey: entry.viewKey,
                            viewType: entry.viewType,
                        },
                        runtimeMetadata,
                    ),
                },
            }));

            return makeTextResponse({
                ok: true,
                appKey: app.appKey,
                source: fieldReferenceResult.source,
                fieldKey: normalisedFieldKey,
                builderUrls: { field: fieldBuilderUrl },
                totalMatches: allViewRefs.length,
                returnedMatches: viewRefs.length,
                totalViews: results.length,
                results,
            });
        }

        const filtered = classification
            ? allReferences.filter((reference) =>
                  reference.classification.includes(classification),
              )
            : allReferences;
        const references = filtered.slice(0, maxResults);

        const countsBySource = new Map<string, number>();
        const countsByClassification = new Map<string, number>();

        for (const reference of references) {
            countsBySource.set(
                reference.sourceType,
                (countsBySource.get(reference.sourceType) || 0) + 1,
            );
            for (const entry of reference.classification) {
                countsByClassification.set(
                    entry,
                    (countsByClassification.get(entry) || 0) + 1,
                );
            }
        }

        return makeTextResponse({
            ok: true,
            appKey: app.appKey,
            source: fieldReferenceResult.source,
            fieldKey: normalisedFieldKey,
            ...(classification ? { classification } : {}),
            builderUrls: { field: fieldBuilderUrl },
            totalReferences: filtered.length,
            returnedReferences: references.length,
            countsBySource: [...countsBySource.entries()]
                .map(([sourceType, count]) => ({ sourceType, count }))
                .sort(
                    (left, right) =>
                        right.count - left.count ||
                        left.sourceType.localeCompare(right.sourceType),
                ),
            countsByClassification: [...countsByClassification.entries()]
                .map(([entry, count]) => ({ classification: entry, count }))
                .sort(
                    (left, right) =>
                        right.count - left.count ||
                        left.classification.localeCompare(right.classification),
                ),
            references: references.map((reference) => ({
                ...reference,
                builderUrls: {
                    scene: makeSceneBuilderUrl(
                        app,
                        reference.sceneKey,
                        runtimeMetadata,
                    ),
                    view: makeViewBuilderUrl(
                        app,
                        {
                            sceneKey: reference.sceneKey,
                            viewKey: reference.viewKey,
                            viewType: reference.viewType,
                        },
                        runtimeMetadata,
                    ),
                    field: makeFieldBuilderUrl(
                        app,
                        {
                            objectKey: reference.objectKey,
                            fieldKey: reference.fieldKey,
                        },
                        runtimeMetadata,
                    ),
                },
            })),
        });
    },
});

export const searchKtlKeywords = defineTool({
    name: 'knack_search_ktl_keywords',
    description:
        'Find KTL-style underscore keywords in view titles and descriptions.',
    access: 'read',
    input: {
        appKey: z.string().optional(),
        keyword: z.string().optional().describe('e.g. _sth'),
        maxResults: z.number().int().min(1).max(5000).default(100),
    },
    handler: async ({ appKey, keyword, maxResults }, ctx) => {
        const app = ctx.getApp(appKey);

        const { viewMap, source } = await ctx.getViewMap(app);
        if (!viewMap) {
            return makeTextResponse({
                ok: false,
                appKey: app.appKey,
                message:
                    'No view map available from runtime API or viewMap.json.',
            });
        }

        const keywordFilter = keyword ? keyword.trim().toLowerCase() : null;
        const viewContextMap = await ctx.getViewContextMap(app);
        const matches: Array<Record<string, unknown>> = [];
        const keywordCounts = new Map<string, number>();

        for (const [viewKey, viewAttrs] of Object.entries(viewMap)) {
            const title =
                typeof viewAttrs.title === 'string' ? viewAttrs.title : '';
            const description =
                typeof viewAttrs.description === 'string'
                    ? viewAttrs.description
                    : '';
            const viewName =
                typeof viewAttrs.name === 'string' ? viewAttrs.name : undefined;
            const viewType =
                typeof viewAttrs.type === 'string' ? viewAttrs.type : undefined;

            const titleHits = extractKtlKeywordsFromText(title).map(
                (entry) => ({
                    ...entry,
                    source: 'title',
                }),
            );
            const descriptionHits = extractKtlKeywordsFromText(description).map(
                (entry) => ({
                    ...entry,
                    source: 'description',
                }),
            );
            const allHits = [...titleHits, ...descriptionHits];
            if (!allHits.length) continue;

            const filteredHits = keywordFilter
                ? allHits.filter(
                      (hit) =>
                          hit.keyword.toLowerCase() === keywordFilter ||
                          hit.keyword.toLowerCase().includes(keywordFilter),
                  )
                : allHits;

            if (!filteredHits.length) continue;

            const uniqueKeywords = [
                ...new Set(filteredHits.map((hit) => hit.keyword)),
            ];
            uniqueKeywords.forEach((kw) =>
                keywordCounts.set(kw, (keywordCounts.get(kw) || 0) + 1),
            );

            const sceneContext = viewContextMap[viewKey] || {};
            matches.push({
                viewKey,
                viewName,
                viewType,
                sceneKey: sceneContext.sceneKey,
                sceneName: sceneContext.sceneName,
                sceneSlug: sceneContext.sceneSlug,
                matchedKeywords: uniqueKeywords,
                hitCount: filteredHits.length,
                snippets: filteredHits.slice(0, 20),
            });

            if (matches.length >= maxResults) break;
        }

        const topKeywords = [...keywordCounts.entries()]
            .map(([kw, count]) => ({ keyword: kw, viewCount: count }))
            .sort((a, b) => b.viewCount - a.viewCount)
            .slice(0, 200);

        return makeTextResponse({
            ok: true,
            appKey: app.appKey,
            source,
            keywordFilter: keyword || null,
            totalMatches: matches.length,
            topKeywords,
            results: matches,
        });
    },
});

export const searchEmails = defineTool({
    name: 'knack_search_emails',
    description:
        'Find email rules and actions in views, with recipients, subject and message context.',
    access: 'read',
    input: {
        appKey: z.string().optional(),
        query: z
            .string()
            .optional()
            .describe('Text filter on to/cc/bcc/subject/message/path'),
        includeMessage: z.boolean().default(false),
        maxResults: z.number().int().min(1).max(5000).default(100),
    },
    handler: async ({ appKey, query, includeMessage, maxResults }, ctx) => {
        const app = ctx.getApp(appKey);

        const { viewMap, source } = await ctx.getViewMap(app);
        if (!viewMap) {
            return makeTextResponse({
                ok: false,
                appKey: app.appKey,
                message:
                    'No view map available from runtime API or viewMap.json.',
            });
        }

        const viewContextMap = await ctx.getViewContextMap(app);
        const filter = query ? query.trim().toLowerCase() : null;
        const matches: Array<Record<string, unknown>> = [];

        for (const [viewKey, viewAttrs] of Object.entries(viewMap)) {
            const sceneContext = viewContextMap[viewKey] || {};
            const emailNodes = collectEmailNodes(viewAttrs, ['$']);
            if (!emailNodes.length) continue;

            for (const node of emailNodes) {
                const searchable = [
                    node.path,
                    node.to,
                    node.cc,
                    node.bcc,
                    node.subject,
                    node.message,
                    node.action,
                ]
                    .filter((part): part is string => Boolean(part))
                    .join(' || ')
                    .toLowerCase();

                if (filter && !searchable.includes(filter)) continue;

                matches.push({
                    viewKey,
                    viewName:
                        typeof viewAttrs.name === 'string'
                            ? viewAttrs.name
                            : undefined,
                    viewType:
                        typeof viewAttrs.type === 'string'
                            ? viewAttrs.type
                            : undefined,
                    sceneKey: sceneContext.sceneKey,
                    sceneName: sceneContext.sceneName,
                    sceneSlug: sceneContext.sceneSlug,
                    path: node.path,
                    action: node.action,
                    to: node.to,
                    cc: node.cc,
                    bcc: node.bcc,
                    subject: truncateText(node.subject, 2000),
                    message: includeMessage
                        ? truncateText(node.message, 4000)
                        : undefined,
                });

                if (matches.length >= maxResults) break;
            }

            if (matches.length >= maxResults) break;
        }

        return makeTextResponse({
            ok: true,
            appKey: app.appKey,
            source,
            query: query || null,
            includeMessage,
            totalMatches: matches.length,
            results: matches,
        });
    },
});

type ExternalSeedFetch = {
    objectKey: string;
    objectName?: string;
    apiPath: string;
    fetchedValues: number;
    ok: boolean;
    message?: string;
};

/**
 * First-page display values from each external parent object, one authenticated call
 * per target. Only reached after the caller confirmed the API-call estimate.
 */
async function fetchExternalSeedConnectionLookups(
    ctx: KnackContext,
    app: AppConfig,
    targets: CachedObject[],
    rowsPerObject: number,
): Promise<{
    lookups: Record<string, ExternalConnectionLookup>;
    fetches: ExternalSeedFetch[];
}> {
    ctx.getApiKey(app.appKey);

    // Independent per-target reads, run with the same concurrency budget batch
    // mutations use rather than one at a time — the caller has already consented to
    // the request count via the confirmed apiCallEstimate.
    const perTarget = await runWithConcurrency(
        targets,
        BATCH_CONCURRENCY,
        async (target) => {
            const params = new URLSearchParams();
            params.set('page', '1');
            params.set('rows_per_page', String(Math.max(rowsPerObject, 2)));
            const apiPath = `/objects/${target.key}/records?${params.toString()}`;
            const result = await ctx.request(app, apiPath);
            const values = result.ok
                ? extractConnectionDisplayValues(result.body, target.identifier)
                : [];

            const fetch: ExternalSeedFetch = {
                objectKey: target.key,
                objectName: target.name,
                apiPath,
                fetchedValues: values.length,
                ok: result.ok,
                message: result.ok
                    ? values.length
                        ? undefined
                        : 'No display values were returned from the first page of records.'
                    : `Request failed with status ${result.status}.`,
            };
            return { target, values, fetch };
        },
    );

    const lookups: Record<string, ExternalConnectionLookup> = {};
    for (const { target, values } of perTarget) {
        if (!values.length) continue;
        lookups[target.key] = {
            objectKey: target.key,
            objectName: target.name,
            values,
            source: 'api',
            lookupField: target.identifier ?? 'identifier',
        };
    }

    return { lookups, fetches: perTarget.map((entry) => entry.fetch) };
}

export const generateSeedCsvs = defineTool({
    name: 'knack_generate_seed_csvs',
    description:
        'Generate import-ready seed CSV content per object, with matching connection values.',
    access: 'read',
    input: {
        appKey: z.string().optional(),
        objectKeys: z.array(z.string()).optional(),
        rowsPerObject: z.number().int().min(2).max(10).default(4),
        useExistingConnectionValues: z
            .boolean()
            .default(false)
            .describe(
                'Fetch display values from parent objects outside objectKeys',
            ),
        confirmExistingConnectionValueFetch: z
            .boolean()
            .default(false)
            .describe('Required before any API-key-backed parent lookups run'),
    },
    handler: async (
        {
            appKey,
            objectKeys,
            rowsPerObject,
            useExistingConnectionValues,
            confirmExistingConnectionValueFetch,
        },
        ctx,
    ) => {
        const app = ctx.getApp(appKey);
        const effectiveRowsPerObject = Math.max(rowsPerObject, 2);
        const { schema, source } = await ctx.getSchema(app);

        if (!schema?.objects?.length) {
            return makeTextResponse({
                ok: false,
                appKey: app.appKey,
                message: 'No schema available from runtime API or schema.json.',
            });
        }

        const candidateExternalTargets = useExistingConnectionValues
            ? getExternalSeedConnectionTargets(schema, objectKeys)
            : [];
        // A parent object outside objectKeys is read here purely to borrow its display
        // values, so the read-access half of dataAccess applies to it exactly as it
        // would to a direct read of that object — an app that restricted this object
        // is not opting into every object it merely connects to.
        const allowedObjectKeys = app.dataAccess?.allowedObjectKeys;
        const externalTargets = allowedObjectKeys
            ? candidateExternalTargets.filter((target) =>
                  allowedObjectKeys.includes(target.key),
              )
            : candidateExternalTargets;
        const policyBlockedTargets = allowedObjectKeys
            ? candidateExternalTargets.filter(
                  (target) => !allowedObjectKeys.includes(target.key),
              )
            : [];
        const apiCallEstimate = {
            requiresApiKey:
                useExistingConnectionValues && externalTargets.length > 0,
            estimatedCalls: externalTargets.length,
            basis: useExistingConnectionValues
                ? `One authenticated records-list request per connected parent object not included in objectKeys, limited to the first page with up to ${effectiveRowsPerObject} rows.`
                : 'No authenticated API calls requested.',
            targets: externalTargets.map((target) => ({
                objectKey: target.key,
                objectName: target.name,
                plannedApiPath: `/objects/${target.key}/records?page=1&rows_per_page=${effectiveRowsPerObject}`,
            })),
        };

        if (
            apiCallEstimate.requiresApiKey &&
            !confirmExistingConnectionValueFetch
        ) {
            return makeTextResponse({
                ok: false,
                appKey: app.appKey,
                source,
                confirmationRequired: true,
                message:
                    'Authenticated API fetches for existing parent connection values were requested. Review the estimated call count and re-run with confirmExistingConnectionValueFetch:true to proceed.',
                apiCallEstimate,
            });
        }

        const externalLookupResult = apiCallEstimate.requiresApiKey
            ? await fetchExternalSeedConnectionLookups(
                  ctx,
                  app,
                  externalTargets,
                  effectiveRowsPerObject,
              )
            : { lookups: {}, fetches: [] };

        const workbook = generateSeedCsvWorkbook(schema, {
            objectKeys,
            rowsPerObject: effectiveRowsPerObject,
            externalConnectionLookups: externalLookupResult.lookups,
        });

        return makeTextResponse({
            ok: true,
            appKey: app.appKey,
            source,
            objectCount: workbook.objects.length,
            importOrder: workbook.importOrder,
            objects: workbook.objects,
            apiCallEstimate,
            externalConnectionFetches: externalLookupResult.fetches,
            ...(policyBlockedTargets.length
                ? {
                      policyBlockedConnectionTargets: policyBlockedTargets.map(
                          (target) => ({
                              objectKey: target.key,
                              objectName: target.name,
                          }),
                      ),
                  }
                : {}),
            note: apiCallEstimate.requiresApiKey
                ? 'Connection values use generated unique keys for included parent objects and API-fetched existing display values for connected parent objects outside objectKeys.'
                : 'Connection values reference each object’s suggested unique import key. Import parent/lookup objects before child objects that connect to them.',
        });
    },
});

export const analysisTools: AnyToolDef[] = [
    getContextBundle,
    getAppOverview,
    analyzeDataModel,
    appDeepDive,
    listFieldReferences,
    searchKtlKeywords,
    searchEmails,
    generateSeedCsvs,
];
