/**
 * Read-side view tools: scenes, views, one view's context/fields/attributes, the repoint
 * plan, the create-payload template builder and the manual snapshot.
 *
 * Every note string and response field here was measured against a live app (see
 * knack-mcp/TESTED.md); the shape of the code changed in the port, the behaviour did not.
 */
import { z } from 'zod';

import { assertDiagnosticAccess } from '../access.js';
import type { AppConfig } from '../config.js';
import type { KnackContext } from '../context.js';
import {
    makeSceneBuilderUrl,
    makeViewBuilderUrl,
} from '../lib/builder-urls.js';
import { FIELD_KEY_PATTERN } from '../lib/field-payload.js';
import {
    findRawViewInMetadata,
    getViewFieldSettings,
    getViewObjectFields,
} from '../lib/metadata.js';
import { asRecord, cloneJsonValue, parseJsonInput } from '../lib/util.js';
import { planViewRepoint } from '../lib/view-references.js';
import {
    KNACK_VIEW_SOURCE_SHAPE,
    type ViewSourceFilters,
    type ViewSourceSort,
    buildNoDataText,
    buildStarterPageGroups,
    buildViewSource,
    buildViewTemplatePayload,
    describeLayoutKeyGap,
    getSceneViewKeys,
    resolveTemplateFields,
    viewTypeCarriesNoDataText,
} from '../lib/view-templates.js';
import { type AnyToolDef, defineTool } from '../registry.js';
import { getInlineDetail, makeTextResponse } from '../response.js';
import { writeMutationSnapshot } from '../view-mutation.js';

const NO_VIEW_MAP_MESSAGE =
    'No view map available from runtime API or viewMap.json.';
const NO_SCENES_MESSAGE =
    'No scene data available. Run knack_refresh_cache with warm: true to load runtime metadata.';

const TEMPLATE_VIEW_TYPES = [
    'grid',
    'table',
    'form',
    'details',
    'list',
] as const;

export const listScenes = defineTool({
    name: 'knack_list_scenes',
    description:
        "List the app's scenes (pages) with key, name, slug and view count.",
    access: 'read',
    input: {
        appKey: z.string().optional(),
        includeViews: z.boolean().default(false),
        includeBuilderUrls: z.boolean().default(false),
    },
    handler: async ({ appKey, includeViews, includeBuilderUrls }, ctx) => {
        const app = ctx.getApp(appKey);
        const scenes = await ctx.getScenes(app);

        if (!scenes.length) {
            return makeTextResponse({
                ok: false,
                appKey: app.appKey,
                message: NO_SCENES_MESSAGE,
            });
        }

        const runtimeMetadata = await ctx.getRuntimeMetadata(app);
        const sceneSummaries = scenes.map((scene) => {
            const summary: Record<string, unknown> = {
                sceneKey: scene.sceneKey,
                sceneName: scene.sceneName,
                sceneSlug: scene.sceneSlug,
                viewCount: scene.views.length,
            };
            if (includeBuilderUrls) {
                summary.builderUrl = makeSceneBuilderUrl(
                    app,
                    scene.sceneKey,
                    runtimeMetadata,
                );
            }
            if (includeViews) {
                summary.views = scene.views;
            }
            return summary;
        });

        return makeTextResponse({
            ok: true,
            appKey: app.appKey,
            sceneCount: scenes.length,
            totalViewCount: scenes.reduce(
                (sum, scene) => sum + scene.views.length,
                0,
            ),
            scenes: sceneSummaries,
        });
    },
});

export const listViews = defineTool({
    name: 'knack_list_views',
    description:
        'List views with scene context and type, optionally filtered by scene or type.',
    access: 'read',
    input: {
        appKey: z.string().optional(),
        sceneKey: z.string().optional(),
        viewType: z
            .string()
            .optional()
            .describe('e.g. form, table, menu, rich_text'),
        maxResults: z.number().int().min(1).max(5000).default(100),
        includeBuilderUrls: z.boolean().default(false),
    },
    handler: async (
        { appKey, sceneKey, viewType, maxResults, includeBuilderUrls },
        ctx,
    ) => {
        const app = ctx.getApp(appKey);
        const scenes = await ctx.getScenes(app);

        if (!scenes.length) {
            return makeTextResponse({
                ok: false,
                appKey: app.appKey,
                message: NO_SCENES_MESSAGE,
            });
        }

        const runtimeMetadata = await ctx.getRuntimeMetadata(app);
        const normSceneKey = sceneKey?.toLowerCase();
        const normViewType = viewType?.toLowerCase();
        const results: Array<Record<string, unknown>> = [];
        const viewTypeCounts = new Map<string, number>();

        for (const scene of scenes) {
            if (normSceneKey && scene.sceneKey.toLowerCase() !== normSceneKey)
                continue;

            for (const view of scene.views) {
                const vType = view.viewType || 'unknown';
                viewTypeCounts.set(vType, (viewTypeCounts.get(vType) || 0) + 1);

                if (normViewType && vType.toLowerCase() !== normViewType)
                    continue;

                if (results.length < maxResults) {
                    results.push({
                        viewKey: view.viewKey,
                        viewName: view.viewName,
                        viewType: view.viewType,
                        sceneKey: scene.sceneKey,
                        sceneName: scene.sceneName,
                        sceneSlug: scene.sceneSlug,
                        ...(includeBuilderUrls
                            ? {
                                  builderUrl: makeViewBuilderUrl(
                                      app,
                                      {
                                          sceneKey: scene.sceneKey,
                                          viewKey: view.viewKey,
                                          viewType: view.viewType,
                                      },
                                      runtimeMetadata,
                                  ),
                              }
                            : {}),
                    });
                }
            }
        }

        const viewTypeSummary = [...viewTypeCounts.entries()]
            .map(([type, count]) => ({ type, count }))
            .sort((a, b) => b.count - a.count);

        return makeTextResponse({
            ok: true,
            appKey: app.appKey,
            filters: { sceneKey: sceneKey || null, viewType: viewType || null },
            totalViews: results.length,
            viewTypeSummary,
            views: results,
        });
    },
});

/**
 * One tool for the three legacy per-view reads. `detail` picks which legacy response
 * comes back, unchanged: `context` (knack_get_view_context), `fields`
 * (knack_list_view_fields) and `attributes` (knack_get_view_attributes, diagnostic).
 */
export const getView = defineTool({
    name: 'knack_get_view',
    description:
        "Return one view's scene context, configured field settings or raw attributes.",
    access: 'read',
    input: {
        appKey: z.string().optional(),
        viewKey: z.string(),
        detail: z.enum(['context', 'fields', 'attributes']).default('context'),
        includeRaw: z
            .boolean()
            .default(false)
            .describe('With detail attributes: add the full raw view JSON'),
    },
    handler: async ({ appKey, viewKey, detail, includeRaw }, ctx) => {
        const app = ctx.getApp(appKey);

        if (detail === 'context') {
            const contextMap = await ctx.getViewContextMap(app);
            const context = contextMap[viewKey];

            if (!context) {
                return makeTextResponse({
                    ok: false,
                    appKey: app.appKey,
                    message: `View context not found for view key: ${viewKey}`,
                    availableViewKeyCount: Object.keys(contextMap).length,
                });
            }

            const viewMapResult = await ctx.getViewMap(app);
            const viewType =
                typeof viewMapResult.viewMap?.[viewKey]?.type === 'string'
                    ? (viewMapResult.viewMap[viewKey].type as string)
                    : undefined;

            const builderUrls = await ctx.getBuilderLinks(app, {
                sceneKey: context.sceneKey,
                viewKey,
                viewType,
            });

            return makeTextResponse({
                ok: true,
                appKey: app.appKey,
                viewKey,
                context,
                builderUrls,
            });
        }

        // The raw-attributes mode keeps the legacy diagnostic gate: the tool as a whole is
        // read-level, this branch alone needs allowDiagnostics.
        if (detail === 'attributes') {
            assertDiagnosticAccess(app, ctx.options);
        }

        const { viewMap, source } = await ctx.getViewMap(app);
        if (!viewMap) {
            return makeTextResponse({
                ok: false,
                appKey: app.appKey,
                message: NO_VIEW_MAP_MESSAGE,
            });
        }

        const attributes = viewMap[viewKey];
        if (!attributes) {
            const allViewKeys = Object.keys(viewMap);
            return makeTextResponse({
                ok: false,
                appKey: app.appKey,
                source,
                message: `View not found in viewMap.json: ${viewKey}`,
                availableViewKeyCount: allViewKeys.length,
                availableViewKeySample: allViewKeys.slice(0, 200),
            });
        }

        const schemaResult = await ctx.getSchema(app);
        const fieldSettings = getViewFieldSettings(
            attributes,
            getViewObjectFields(attributes, schemaResult.schema),
        );

        if (detail === 'fields') {
            return makeTextResponse({
                ok: true,
                appKey: app.appKey,
                source,
                schemaSource: schemaResult.source,
                viewKey,
                viewName:
                    typeof attributes.name === 'string'
                        ? attributes.name
                        : null,
                viewType:
                    typeof attributes.type === 'string'
                        ? attributes.type
                        : null,
                fieldSettings,
            });
        }

        const viewContextMap = await ctx.getViewContextMap(app);
        const context = viewContextMap[viewKey] || {};
        const builderUrls = await ctx.getBuilderLinks(app, {
            sceneKey: context.sceneKey,
            viewKey,
            viewType:
                typeof attributes.type === 'string'
                    ? attributes.type
                    : undefined,
        });

        if (!includeRaw) {
            return makeTextResponse({
                ok: true,
                appKey: app.appKey,
                source,
                schemaSource: schemaResult.source,
                viewKey,
                fieldSettings,
                builderUrls,
                note: 'Pass includeRaw: true for the full raw view JSON (layout, pageGroups, rules) — fieldSettings above already covers per-field key/type/label/rules/defaults.',
            });
        }

        const attributeDetail = getInlineDetail(attributes);

        return makeTextResponse({
            ok: true,
            appKey: app.appKey,
            source,
            schemaSource: schemaResult.source,
            viewKey,
            attributesIncluded: attributeDetail.included,
            attributesSizeBytes: attributeDetail.sizeBytes,
            attributes: attributeDetail.value,
            attributeSummary: attributeDetail.summary,
            fieldSettings,
            builderUrls,
        });
    },
});

export const planViewRepointTool = defineTool({
    name: 'knack_plan_view_repoint',
    description:
        "List a view's references split into rescope vs retarget edits; changes nothing.",
    access: 'read',
    input: {
        appKey: z.string().optional(),
        viewKey: z.string(),
        includeScopedFields: z
            .boolean()
            .default(false)
            .describe(
                'Also list every field named in filters, rules, sorts and values',
            ),
    },
    handler: async ({ appKey, viewKey, includeScopedFields }, ctx) => {
        const app = ctx.getApp(appKey);
        const { viewMap, source } = await ctx.getViewMap(app);

        if (!viewMap) {
            return makeTextResponse({
                ok: false,
                appKey: app.appKey,
                message: NO_VIEW_MAP_MESSAGE,
            });
        }

        const attributes = asRecord(viewMap[viewKey]);
        if (!attributes) {
            return makeTextResponse({
                ok: false,
                appKey: app.appKey,
                viewKey,
                source,
                message: `View not found in view metadata: ${viewKey}`,
            });
        }

        const plan = planViewRepoint(attributes);
        const sourceObject = asRecord(attributes.source)?.object;

        const notes = [
            'Two different edits get called "repointing", and they invalidate different things. Read the two lists below accordingly.',
            `RESCOPE (change connection_key / parent_source / authenticated_user): ${plan.scopeConnections.length} reference(s), field(s) ${plan.distinctScopeKeys.join(', ') || 'none'}. Change these and nothing else — a rescope leaves display connections, fields, filters and sorts valid, because the object the view lists has not changed.`,
            `RETARGET (change source.object, currently ${typeof sourceObject === 'string' ? sourceObject : 'unknown'}): everything below is then suspect, because every field, display connection, filter, sort and rule names a field on the old object.`,
        ];

        if (plan.displayConnections.length > 0) {
            notes.push(
                `${plan.displayConnections.length} DISPLAY connection(s), field(s) ${plan.distinctDisplayKeys.join(', ') || 'none'}: these read a shown value from a connected record, out from this view's own object. Measured on 2026-09-04 — a builder rescope that added connection_key, relationship_type, authenticated_user and parent_source left every one of them untouched, and they were already set while the source had no connection at all. So do NOT rewrite them for a rescope. Revisit them only on a retarget.`,
            );
        }

        if (plan.navigation.length > 0) {
            notes.push(
                `${plan.navigation.length} navigation reference(s), listed separately: the cascade guard's concern rather than a repoint's. Note that copying a view appears to duplicate the child pages it owns and point the copy at the duplicates — see P1/P5 in TESTING.md — so a copy's links may not name the same pages the original's did.`,
            );
        }

        const embedded = plan.other.filter((reference) =>
            reference.path.endsWith('(embedded)'),
        );
        if (embedded.length > 0) {
            notes.push(
                `${embedded.length} key(s) are embedded in prose (a description's KTL directives). Nothing else in this server reads those, so a copy carries them verbatim and they keep naming the original's fields and views.`,
            );
        }

        return makeTextResponse({
            ok: true,
            appKey: app.appKey,
            viewKey,
            source,
            viewType:
                typeof attributes.type === 'string' ? attributes.type : null,
            sourceObject:
                typeof sourceObject === 'string' ? sourceObject : null,
            scopeConnections: plan.scopeConnections,
            distinctScopeKeys: plan.distinctScopeKeys,
            displayConnections: plan.displayConnections,
            distinctDisplayKeys: plan.distinctDisplayKeys,
            navigation: plan.navigation,
            other: plan.other,
            scopedFieldCount: plan.scopedFields.length,
            scopedFields: includeScopedFields ? plan.scopedFields : undefined,
            notes,
        });
    },
});

/**
 * One tool for the two legacy template builders. With `fromViewKey` it clones an
 * existing view (legacy knack_get_view_payload_template_from_view: same type or a
 * details/list conversion, identifiers stripped, pageGroups rebuilt); without it, it
 * builds a starter payload from `viewType` and the object schema (legacy
 * knack_get_view_payload_template). `viewType` is the built type in one mode and the
 * clone's target type in the other.
 */
const templateInput = {
    appKey: z.string().optional(),
    viewType: z
        .enum(TEMPLATE_VIEW_TYPES)
        .optional()
        .describe('grid saves as table; with fromViewKey, the clone type'),
    fromViewKey: z
        .string()
        .optional()
        .describe('Clone this view instead of building one'),
    objectKey: z
        .string()
        .optional()
        .describe('Source object; required when building'),
    sceneKey: z
        .string()
        .optional()
        .describe('Target page; derives existingViewKeys'),
    name: z.string().optional(),
    title: z.string().optional(),
    fieldKeys: z.array(z.string()).optional(),
    maxFields: z.number().int().min(1).max(100).default(12),
    existingViewKeys: z
        .array(z.string())
        .optional()
        .describe(
            'Views already on the page, in order; the new view is appended',
        ),
    connectionKey: z
        .string()
        .optional()
        .describe(
            'Scope to records through this field; needs relationshipType',
        ),
    relationshipType: z
        .enum(['foreign', 'local'])
        .optional()
        .describe(
            'foreign: connection field on this object; local: on the other',
        ),
    authenticatedUser: z
        .boolean()
        .optional()
        .describe('Scope to the logged-in account'),
    parentSourceObject: z
        .string()
        .optional()
        .describe(
            'Object of the page record context; pair with parentSourceConnection',
        ),
    parentSourceConnection: z.string().optional(),
    filters: z
        .string()
        .optional()
        .describe(
            'JSON { match: all|any, rules: [{field, operator, value}], groups }',
        ),
    sort: z
        .string()
        .optional()
        .describe('JSON [{ "field": "field_1", "order": "asc" }]'),
    columnConnections: z
        .string()
        .optional()
        .describe(
            'JSON { "field_10": "field_3" }: column key to connection field',
        ),
    noDataText: z
        .string()
        .optional()
        .describe('Empty-state line for table/list views'),
    includeSourceGuidance: z
        .boolean()
        .default(false)
        .describe('Include the measured source-shape guidance'),
};

type TemplateArgs = z.infer<z.ZodObject<typeof templateInput>>;

export const getViewPayloadTemplate = defineTool({
    name: 'knack_get_view_payload_template',
    description:
        'Build a starter create-view payload from a view type or by cloning a view.',
    // Read-only: it never sends a request to Knack, only builds a payload a caller
    // could later post through knack_create_view. Gating it behind allowViewMutation
    // would withhold the one tool that helps a caller build a valid payload for that
    // gated tool.
    access: 'read',
    input: templateInput,
    handler: async (args, ctx) => {
        const app = ctx.getApp(args.appKey);

        if (args.fromViewKey) {
            return buildTemplateFromView(app, args, ctx);
        }
        return buildTemplateFromType(app, args, ctx);
    },
});

async function buildTemplateFromType(
    app: AppConfig,
    args: TemplateArgs,
    ctx: KnackContext,
) {
    const {
        viewType,
        objectKey,
        sceneKey,
        name,
        title,
        fieldKeys = [],
        maxFields,
        existingViewKeys = [],
        connectionKey,
        relationshipType,
        authenticatedUser,
        parentSourceObject,
        parentSourceConnection,
        filters,
        sort,
        columnConnections,
        noDataText,
        includeSourceGuidance,
    } = args;

    if (!viewType) {
        throw new Error('viewType is required unless fromViewKey is given.');
    }

    const canonicalType = viewType === 'grid' ? 'table' : viewType;
    const displayName =
        name ||
        (viewType === 'grid'
            ? 'Grid'
            : canonicalType[0].toUpperCase() + canonicalType.slice(1));
    const resolvedTitle = title ?? displayName;
    const notes: string[] = [];
    let layoutViewKeys = existingViewKeys;

    const parsedSort = sort
        ? parseJsonInput<ViewSourceSort[]>('sort', sort)
        : undefined;

    let parsedColumnConnections: Record<string, string> | undefined;
    if (columnConnections) {
        const raw = parseJsonInput<unknown>(
            'columnConnections',
            columnConnections,
        );

        // parseJsonInput casts rather than checks, so nothing above this line
        // establishes the shape. Left unvalidated, an array was reported as a set of
        // unmatched entries and `{field_2: 42}` was emitted as `connection: {key: 42}`.
        if (!raw || typeof raw !== 'object' || Array.isArray(raw)) {
            throw new Error(
                'columnConnections must be a JSON object mapping a field key to a connection field key, e.g. { "field_10": "field_3" }.',
            );
        }

        for (const [fieldKey, connection] of Object.entries(raw)) {
            // A field key, not merely a non-empty string. All 241 `connection.key`
            // values in the export are `field_N`, so a label like "Contact" is not a
            // shape Knack stores.
            if (
                typeof connection !== 'string' ||
                !FIELD_KEY_PATTERN.test(connection)
            ) {
                throw new Error(
                    `columnConnections["${fieldKey}"] must be a connection field key like "field_3", not ${JSON.stringify(connection)}. All 241 stored connection.key values measured are field keys.`,
                );
            }

            if (!FIELD_KEY_PATTERN.test(fieldKey)) {
                throw new Error(
                    `columnConnections key "${fieldKey}" must be a field key like "field_10" — it names the column whose value is read through the connection.`,
                );
            }
        }

        parsedColumnConnections = raw as Record<string, string>;
    }

    if (!objectKey) {
        throw new Error(
            'objectKey is required for common record-backed view templates.',
        );
    }

    // The schema is loaded whether or not fields have to be derived: passing fieldKeys
    // used to skip it, so every column header fell back to the raw field key and a
    // sceneKey given without existingViewKeys never derived them (measured 2026-09-03).
    const { schema, source } = await ctx.getSchema(app);
    const schemaSource = source;
    const sourceObject = schema?.objects?.find(
        (object) => object.key === objectKey,
    );
    const allObjectFields = sourceObject?.fields || [];
    const sourceObjectName = sourceObject?.name ?? null;

    if (sceneKey) {
        const scenes = await ctx.getScenes(app);
        const sceneViewKeys = getSceneViewKeys(scenes, sceneKey);

        if (layoutViewKeys.length === 0) {
            layoutViewKeys = sceneViewKeys;
            if (layoutViewKeys.length > 0) {
                notes.push(
                    `Derived ${layoutViewKeys.length} existing view key(s) from scene ${sceneKey}.`,
                );
            }
        } else {
            // The scene is read even when keys were passed, purely to catch a list
            // that has gone stale between two creates.
            const gap = describeLayoutKeyGap(layoutViewKeys, sceneViewKeys);
            if (gap) notes.push(gap);
        }
    }

    const resolved = resolveTemplateFields({
        fieldKeys,
        allObjectFields,
        objectKey,
        canonicalType,
        maxFields,
    });
    const { derivedFromSchema } = resolved;
    notes.push(...resolved.notes);

    const fieldDescriptors = parsedColumnConnections
        ? resolved.fieldDescriptors.map((descriptor) => {
              const connection = parsedColumnConnections[descriptor.key];
              return connection
                  ? { ...descriptor, connectionKey: connection }
                  : descriptor;
          })
        : resolved.fieldDescriptors;

    if (parsedColumnConnections && canonicalType === 'form') {
        // A form input reaches a connected record through `source.connections[]`, an
        // entirely different shape from a column's `connection: { key }`.
        throw new Error(
            "columnConnections does not apply to a form template. A form input filters a connection through source.connections[], which is a different shape from a column's connection: { key } — build the form and set its input sources explicitly.",
        );
    }

    if (parsedColumnConnections) {
        const matched = fieldDescriptors.filter(
            (descriptor) => descriptor.connectionKey,
        );
        const unmatched = Object.keys(parsedColumnConnections).filter(
            (fieldKey) =>
                !fieldDescriptors.some(
                    (descriptor) => descriptor.key === fieldKey,
                ),
        );

        notes.push(
            `${matched.length} of ${fieldDescriptors.length} column(s) will reach through a connection, emitted as connection: { key } beside the column's own field. This is a DISPLAY path out of the view's own object and is independent of the source's scoping: a rescope leaves it correct, and only changing source.object invalidates it.`,
        );

        if (unmatched.length > 0) {
            notes.push(
                `columnConnections named ${unmatched.length} field(s) that are not among this template's columns (${unmatched.join(', ')}); those entries did nothing. A connection on a column the view does not show is silently inert.`,
            );
        }
    }

    const pageGroups = buildStarterPageGroups(layoutViewKeys);

    let parsedFilters: ViewSourceFilters | undefined;
    if (filters !== undefined) {
        try {
            parsedFilters = JSON.parse(filters) as ViewSourceFilters;
        } catch (error) {
            throw new Error(
                `filters is not valid JSON: ${error instanceof Error ? error.message : String(error)}`,
                { cause: error },
            );
        }
    }

    if (
        (parentSourceObject && !parentSourceConnection) ||
        (parentSourceConnection && !parentSourceObject)
    ) {
        throw new Error(
            'parentSourceObject and parentSourceConnection must be passed together — a half-specified hop cannot be resolved.',
        );
    }

    // Every branch below shares one source, so a connected or filtered source is
    // available on each view type rather than only on tables.
    const viewSource = buildViewSource({
        objectKey,
        connectionKey,
        relationshipType,
        authenticatedUser,
        parentSource:
            parentSourceObject && parentSourceConnection
                ? {
                      object: parentSourceObject,
                      connection: parentSourceConnection,
                  }
                : undefined,
        filters: parsedFilters,
        sort: parsedSort,
    });

    if (connectionKey) {
        notes.push(
            `Source is scoped through ${connectionKey} with relationship_type "${relationshipType}". That value follows which object owns the connection field, so recompute it rather than copying it when repointing this view at a different connection.`,
        );
    }

    if (parsedFilters?.groups?.length) {
        notes.push(
            `Filter carries ${parsedFilters.groups.length} group(s). With match "${parsedFilters.match ?? 'all'}", each group combines internally as ${(parsedFilters.match ?? 'all') === 'all' ? 'OR' : 'AND'} — the inverse of the top-level match.`,
        );
    }

    // Resolved for every type, applied only to the two that carry the key.
    const resolvedNoDataText = noDataText ?? buildNoDataText(sourceObjectName);

    if (viewTypeCarriesNoDataText(canonicalType)) {
        notes.push(
            `no_data_text set to "${resolvedNoDataText}". Left unset, Knack stores an empty string and the view falls back to its stock empty-state line; all 223 views carrying the key in a 738-view export held a non-empty value. Knack does not template this string, so it cannot vary per record set — pass noDataText to override.`,
        );
    } else if (noDataText !== undefined) {
        notes.push(
            `noDataText was ignored: a ${canonicalType} view does not carry no_data_text. Measured across a 738-view export, the key appears only on table and list views.`,
        );
    }

    const payload = buildViewTemplatePayload({
        canonicalType,
        displayName,
        resolvedTitle,
        viewSource,
        fieldDescriptors,
        pageGroups,
        noDataText: resolvedNoDataText,
    });

    if (canonicalType === 'table') {
        notes.push('Knack stores grid views as type `table`.');
    } else if (canonicalType === 'form') {
        notes.push(
            'Review the generated inputs and rules before creating the form, especially when the object includes connection or special field types.',
        );
    }

    const payloadDetail = getInlineDetail(payload);

    return makeTextResponse({
        ok: true,
        action: 'view_payload_template',
        appKey: app.appKey,
        objectKey,
        sceneKey: sceneKey || null,
        requestedViewType: viewType,
        canonicalViewType: canonicalType,
        derivedFromSchema,
        schemaSource,
        layoutDerivedFromScene:
            layoutViewKeys.length > 0 && existingViewKeys.length === 0,
        existingViewKeysUsed: layoutViewKeys,
        fieldKeysUsed: fieldDescriptors.map((field) => field.key),
        // Static and long: sent only on request, since it was the same 7.5 KB on
        // every call whether or not the caller needed it.
        ...(includeSourceGuidance
            ? { viewSourceShape: KNACK_VIEW_SOURCE_SHAPE }
            : {
                  viewSourceShapeNote:
                      'Pass includeSourceGuidance: true for the measured source patterns, criteria semantics and repoint notes.',
              }),
        payloadIncluded: payloadDetail.included,
        payloadSizeBytes: payloadDetail.sizeBytes,
        payload: payloadDetail.value,
        payloadSummary: payloadDetail.summary,
        notes,
    });
}

async function buildTemplateFromView(
    app: AppConfig,
    args: TemplateArgs,
    ctx: KnackContext,
) {
    const sourceViewKey = args.fromViewKey as string;
    const {
        viewType: targetViewType,
        sceneKey,
        name,
        title,
        existingViewKeys,
        noDataText,
    } = args;

    const { viewMap, source } = await ctx.getViewMap(app);
    if (!viewMap) {
        return makeTextResponse({
            ok: false,
            appKey: app.appKey,
            message: NO_VIEW_MAP_MESSAGE,
        });
    }

    const sourceAttributes = asRecord(viewMap[sourceViewKey]);
    if (!sourceAttributes) {
        return makeTextResponse({
            ok: false,
            appKey: app.appKey,
            sourceViewKey,
            source,
            message: `View not found in view metadata: ${sourceViewKey}`,
        });
    }

    const sourceViewType =
        typeof sourceAttributes.type === 'string'
            ? sourceAttributes.type
            : null;
    const canonicalSourceViewType =
        sourceViewType === 'grid' ? 'table' : sourceViewType;
    const canonicalTargetViewType =
        targetViewType === 'grid' ? 'table' : targetViewType;

    const isDetailsListConversion =
        (canonicalSourceViewType === 'details' &&
            canonicalTargetViewType === 'list') ||
        (canonicalSourceViewType === 'list' &&
            canonicalTargetViewType === 'details');

    if (
        canonicalTargetViewType &&
        canonicalTargetViewType !== canonicalSourceViewType &&
        !isDetailsListConversion
    ) {
        return makeTextResponse({
            ok: false,
            appKey: app.appKey,
            sourceViewKey,
            sourceViewType,
            requestedTargetViewType: targetViewType || null,
            message:
                'This helper only supports details/list conversion. Other view types require a type-specific payload rather than a cloned layout.',
        });
    }

    const payload = cloneJsonValue(sourceAttributes) as Record<string, unknown>;
    delete payload._id;
    delete payload.key;

    if (canonicalTargetViewType) {
        payload.type = canonicalTargetViewType;
    }

    const sourceName =
        typeof sourceAttributes.name === 'string'
            ? sourceAttributes.name
            : sourceViewKey;
    payload.name = name || `${sourceName} Copy`;
    if (title !== undefined) {
        payload.title = title;
    }

    const viewContextMap = await ctx.getViewContextMap(app);
    const context = viewContextMap[sourceViewKey] || {};
    const scenes = await ctx.getScenes(app);
    const sourceSceneKey = context.sceneKey;
    const derivedSceneKey = sceneKey || sourceSceneKey;
    const sceneViews = getSceneViewKeys(scenes, derivedSceneKey);
    const layoutViewKeys =
        existingViewKeys && existingViewKeys.length > 0
            ? existingViewKeys
            : sceneViews;

    if (layoutViewKeys.length > 0) {
        payload.pageGroups = buildStarterPageGroups(layoutViewKeys);
    }

    // A details view carries no `no_data_text`, so converting one to a list would
    // otherwise produce a list with no empty-state line — the one case where cloning
    // silently loses a setting the target type expects.
    const effectiveTargetType =
        canonicalTargetViewType || canonicalSourceViewType || '';
    const noDataTextNotes: string[] = [];

    if (viewTypeCarriesNoDataText(effectiveTargetType)) {
        const clonedNoDataText =
            typeof payload.no_data_text === 'string'
                ? payload.no_data_text.trim()
                : '';

        if (noDataText !== undefined) {
            payload.no_data_text = noDataText;
            noDataTextNotes.push(
                `no_data_text was overridden to "${noDataText}".`,
            );
        } else if (!clonedNoDataText) {
            const sourceObjectKey = asRecord(payload.source)?.object;
            let objectName: string | null = null;

            if (typeof sourceObjectKey === 'string') {
                const { schema } = await ctx.getSchema(app);
                objectName =
                    schema?.objects?.find(
                        (object) => object.key === sourceObjectKey,
                    )?.name ?? null;
            }

            payload.no_data_text = buildNoDataText(objectName);
            noDataTextNotes.push(
                `The source view carried no no_data_text, so one was derived for the ${effectiveTargetType} target: "${String(payload.no_data_text)}". Pass noDataText to set it explicitly.`,
            );
        }
    } else if (noDataText !== undefined) {
        noDataTextNotes.push(
            `noDataText was ignored: a ${effectiveTargetType || 'clone of this'} view does not carry no_data_text.`,
        );
    }

    const payloadDetail = getInlineDetail(payload);

    return makeTextResponse({
        ok: true,
        action: 'view_payload_template_from_view',
        appKey: app.appKey,
        source,
        sourceViewKey,
        sourceViewType,
        requestedTargetViewType: targetViewType || null,
        targetViewType:
            canonicalTargetViewType ||
            (typeof sourceAttributes.type === 'string'
                ? sourceAttributes.type
                : null),
        sourceSceneKey: sourceSceneKey || null,
        targetSceneKey: derivedSceneKey || null,
        existingViewKeysUsed: layoutViewKeys,
        payloadIncluded: payloadDetail.included,
        payloadSizeBytes: payloadDetail.sizeBytes,
        payload: payloadDetail.value,
        payloadSummary: payloadDetail.summary,
        notes: [
            'The payload was cloned from existing view metadata with key/_id removed.',
            canonicalTargetViewType
                ? `The cloned view type was changed to ${canonicalTargetViewType}; configured columns, including static elements, were preserved.`
                : 'The cloned view type was preserved from the source view.',
            layoutViewKeys.length > 0
                ? `pageGroups were rebuilt using ${layoutViewKeys.length} existing view key(s).`
                : 'No pageGroups were derived automatically. Supply existingViewKeys if the target page layout matters.',
            ...noDataTextNotes,
        ],
    });
}

export const snapshotApp = defineTool({
    name: 'knack_snapshot_app',
    description:
        'Write a timestamped restore point: scene tree, schema pointer and optionally one view.',
    // Read-only from Knack's point of view: it only reads metadata and writes to the
    // local app folder. Gating it behind allowViewMutation would take away the backup
    // this tool exists for on exactly the apps where a manual builder change is riskiest
    // — an app with no view-mutation tools enabled at all.
    access: 'read',
    input: {
        appKey: z.string().optional(),
        sceneKey: z.string().optional(),
        viewKey: z
            .string()
            .optional()
            .describe('View to capture in full; requires sceneKey'),
    },
    handler: async ({ appKey, sceneKey, viewKey }, ctx) => {
        const app = ctx.getApp(appKey);

        if (viewKey && !sceneKey) {
            return makeTextResponse({
                ok: false,
                appKey: app.appKey,
                action: 'snapshot_app',
                error: 'INVALID_INPUT',
                message:
                    'viewKey requires sceneKey — a view can only be read through the scene that owns it. Supply both to capture the view, or neither to snapshot scenes and schema only. Refusing rather than writing a snapshot with no view in it and reporting success.',
            });
        }

        let view: unknown;
        if (sceneKey && viewKey) {
            // Read from runtime metadata, exactly as the guard's preflight does: Knack
            // serves no read handler on /scenes/<scene>/views/<view>. Uncached for the
            // same reason the preflight is — a restore point describing the app as it
            // stood up to five minutes ago is worse than an obvious failure.
            ctx.caches.runtimeMetadata.delete(app.appKey);
            const metadata = await ctx.getRuntimeMetadata(app);
            if (!metadata) {
                return makeTextResponse({
                    ok: false,
                    appKey: app.appKey,
                    action: 'snapshot_app',
                    error: 'COULD_NOT_VERIFY_VIEW',
                    message: `Runtime metadata could not be fetched from Knack, so ${viewKey} could not be read and the snapshot would be incomplete. Retry, or omit viewKey to snapshot scenes and schema only.`,
                });
            }

            const found = findRawViewInMetadata(metadata, sceneKey, viewKey);
            if (!found) {
                return makeTextResponse({
                    ok: false,
                    appKey: app.appKey,
                    action: 'snapshot_app',
                    error: 'COULD_NOT_VERIFY_VIEW',
                    message: `${viewKey} was not found in ${sceneKey} in this app's metadata, so the snapshot would be incomplete. Check both keys, or omit viewKey to snapshot scenes and schema only.`,
                });
            }
            view = found;
        }

        const result = await writeMutationSnapshot(ctx, app, {
            action: 'manual',
            sceneKey,
            viewKey,
            view,
        });

        if (!result.ok) {
            return makeTextResponse({
                ok: false,
                appKey: app.appKey,
                action: 'snapshot_app',
                error: 'SNAPSHOT_FAILED',
                message: `Could not write the snapshot: ${result.error}. Check KNACK_APPS_DIR and the app folder are writable.`,
            });
        }

        return makeTextResponse({
            ok: true,
            appKey: app.appKey,
            action: 'snapshot_app',
            snapshotPath: result.path,
            viewIncluded: Boolean(view),
        });
    },
});

export const viewTools: AnyToolDef[] = [
    listScenes,
    listViews,
    getView,
    planViewRepointTool,
    getViewPayloadTemplate,
    snapshotApp,
];
