/**
 * Session context tools: which apps exist, which one is active, and the state of the
 * per-app metadata caches.
 */
import { z } from 'zod';

import { CACHE_TTL_MS } from '../config.js';
import type { KnackContext, MetadataFileName } from '../context.js';
import {
    describeServerBuild,
    describePersistOutcome,
    summariseServerBuild,
} from '../lib/build-identity.js';
import { getCacheEntry } from '../lib/cache.js';
import { debugLog } from '../lib/log.js';
import { asRecord, describeError } from '../lib/util.js';
import { type AnyToolDef, defineTool } from '../registry.js';
import { describeAppListForHumans, makeTextResponse } from '../response.js';
import type { CacheEntry } from '../types.js';
import {
    describeCascadeBehaviour,
    getHumanConfirmationStatus,
} from '../view-mutation.js';

export const listApps = defineTool({
    name: 'knack_list_apps',
    description:
        'List every Knack app in the KnackApps folder, re-scanning it first.',
    access: 'read',
    input: {},
    handler: async (_args, ctx) => {
        const freshApps = ctx.rescanApps();
        const humanConfirmation = getHumanConfirmationStatus(ctx);
        debugLog('human_confirmation_status', humanConfirmation);
        // Reported once rather than per app: this depends only on the connected
        // client, and no app.json setting can change it.
        const cascadeDeleteBehaviour = describeCascadeBehaviour(
            humanConfirmation.available,
        );
        // Reported so a caller can tell a stale server from a current one
        // without inferring it from which keys are missing.
        const serverBuild = describeServerBuild(ctx.options.readOnly === true);
        const humanSummary = describeAppListForHumans({
            knackAppsDir: ctx.knackAppsDir,
            activeAppKey: ctx.state.activeAppKey,
            apps: freshApps,
            enforcedReadOnly: ctx.options.readOnly === true,
            humanConfirmation,
            cascadeDeleteBehaviour,
            buildSummary: summariseServerBuild(serverBuild),
        });
        return makeTextResponse(
            {
                ok: true,
                serverBuild,
                knackAppsDir: ctx.knackAppsDir,
                activeAppKey: ctx.state.activeAppKey,
                humanConfirmation,
                cascadeDeleteBehaviour,
                apps: freshApps.map((a) => ({
                    appKey: a.appKey,
                    appName: a.appName,
                    appId: a.appId,
                    readonly: a.readonly !== false,
                    allowViewMutation: a.allowViewMutation === true,
                    allowDelete: a.allowDelete === true,
                    allowDiagnostics: a.allowDiagnostics === true,
                    notes: a.notes,
                })),
            },
            humanSummary,
        );
    },
});

export const setContext = defineTool({
    name: 'knack_set_context',
    description:
        'Select the active app by appKey or infer it from a file or folder path.',
    access: 'read',
    input: {
        appKey: z.string().optional(),
        contextPath: z
            .string()
            .optional()
            .describe('File path (preferred) or folder path'),
    },
    handler: async ({ appKey, contextPath }, ctx) => {
        const availableApps = () => ctx.apps.map((a) => a.appKey);

        if (appKey) {
            const app = ctx.findApp(appKey);
            if (!app) {
                return makeTextResponse({
                    ok: false,
                    message: `Unknown appKey: ${appKey}`,
                    availableApps: availableApps(),
                });
            }

            ctx.state.activeAppKey = app.appKey;
            if (contextPath) ctx.state.lastContextPath = contextPath;

            return makeTextResponse({
                ok: true,
                activeAppKey: ctx.state.activeAppKey,
                contextPath: contextPath || null,
                inferenceMode: 'explicit-appkey',
            });
        }

        if (!contextPath) {
            return makeTextResponse({
                ok: false,
                message: 'Provide either appKey or contextPath.',
                availableApps: availableApps(),
            });
        }

        const inferred = ctx.inferAppKeyFromPath(contextPath);

        if (!inferred.appKey) {
            return makeTextResponse({
                ok: false,
                message: 'Could not infer appKey from the given contextPath.',
                contextPath,
                hint: 'Use a file inside KnackApps/<AppKey>/..., a path/basename containing the app name, or pass appKey directly.',
                candidateAppKeys: inferred.candidateAppKeys,
                availableApps: availableApps(),
            });
        }

        ctx.state.activeAppKey = inferred.appKey;
        ctx.state.lastContextPath = contextPath;

        return makeTextResponse({
            ok: true,
            activeAppKey: ctx.state.activeAppKey,
            contextPath,
            inferenceMode: inferred.inferenceMode,
        });
    },
});

function describeCacheEntry(
    entry: CacheEntry<unknown> | null,
    withSource: boolean,
) {
    if (!entry) return { cached: false };
    return {
        cached: true,
        ...(withSource ? { source: entry.source } : {}),
        loadedAt: new Date(entry.loadedAt).toISOString(),
        expiresAt: new Date(entry.expiresAt).toISOString(),
        expiresInMs: Math.max(0, entry.expiresAt - Date.now()),
    };
}

/** The legacy knack_cache_status report for one app. */
function buildCacheStatus(ctx: KnackContext, appKey: string | undefined) {
    const app = ctx.getApp(appKey);
    // Typed against MetadataFileName so an addition to one list without the other is a
    // compile error rather than a silently-omitted status entry.
    const fileNames: MetadataFileName[] = [
        'schema.json',
        'fieldMap.json',
        'viewMap.json',
        'fieldReferenceIndex.json',
    ];
    const [schemaPath, fieldMapPath, viewMapPath, fieldReferenceIndexPath] =
        fileNames.map((name) => ctx.resolveMetadataFilePath(app, name));

    return {
        ok: true,
        appKey: app.appKey,
        activeAppKey: ctx.state.activeAppKey,
        lastContextPath: ctx.state.lastContextPath,
        cacheTtlMs: CACHE_TTL_MS,
        files: {
            schemaPath,
            schemaExists: ctx.metadataFileExists(app, 'schema.json'),
            schemaPathCandidates: ctx.metadataFilePaths(app, 'schema.json'),
            fieldMapPath,
            fieldMapExists: ctx.metadataFileExists(app, 'fieldMap.json'),
            fieldMapPathCandidates: ctx.metadataFilePaths(app, 'fieldMap.json'),
            viewMapPath,
            viewMapExists: ctx.metadataFileExists(app, 'viewMap.json'),
            viewMapPathCandidates: ctx.metadataFilePaths(app, 'viewMap.json'),
            fieldReferenceIndexPath,
            fieldReferenceIndexExists: ctx.metadataFileExists(
                app,
                'fieldReferenceIndex.json',
            ),
            fieldReferenceIndexPathCandidates: ctx.metadataFilePaths(
                app,
                'fieldReferenceIndex.json',
            ),
        },
        cache: {
            schema: describeCacheEntry(
                getCacheEntry(ctx.caches.schema, app.appKey),
                true,
            ),
            fieldMap: describeCacheEntry(
                getCacheEntry(ctx.caches.fieldMap, app.appKey),
                true,
            ),
            viewMap: describeCacheEntry(
                getCacheEntry(ctx.caches.viewMap, app.appKey),
                true,
            ),
            runtimeMetadata: describeCacheEntry(
                getCacheEntry(ctx.caches.runtimeMetadata, app.appKey),
                false,
            ),
            fieldReferences: describeCacheEntry(
                getCacheEntry(ctx.caches.fieldReference, app.appKey),
                true,
            ),
        },
    };
}

/** The legacy knack_refresh_cache behaviour: clear, optionally warm, optionally persist. */
async function refreshCaches(
    ctx: KnackContext,
    args: { appKey?: string; warm: boolean; persistFiles: boolean },
) {
    const { appKey, warm, persistFiles } = args;
    const targetApps = appKey ? [ctx.getApp(appKey)] : ctx.apps;

    const getSizes = () => ({
        runtimeMetadata: ctx.caches.runtimeMetadata.size,
        schema: ctx.caches.schema.size,
        fieldMap: ctx.caches.fieldMap.size,
        viewMap: ctx.caches.viewMap.size,
        fieldReferences: ctx.caches.fieldReference.size,
    });

    const beforeSizes = getSizes();
    ctx.invalidate(appKey || undefined);

    const warmed: Array<Record<string, unknown>> = [];
    if (warm) {
        for (const app of targetApps) {
            try {
                const metadata = await ctx.getRuntimeMetadata(app);
                const schemaResult = await ctx.getSchema(app);
                const fieldMapResult = await ctx.getFieldMap(app);
                const viewMapResult = await ctx.getViewMap(app);
                const fieldReferenceResult =
                    await ctx.getFieldReferenceIndex(app);

                const persisted: Record<string, unknown> = {
                    enabled: persistFiles,
                };

                if (persistFiles) {
                    if (
                        schemaResult.source === 'runtime' &&
                        schemaResult.schema
                    ) {
                        persisted.schema = ctx.writeMetadataJson(
                            app,
                            'schema.json',
                            schemaResult.schema,
                        );
                    }
                    if (
                        fieldMapResult.source === 'runtime' &&
                        fieldMapResult.fieldMap
                    ) {
                        persisted.fieldMap = ctx.writeMetadataJson(
                            app,
                            'fieldMap.json',
                            fieldMapResult.fieldMap,
                        );
                    }
                    if (
                        viewMapResult.source === 'runtime' &&
                        viewMapResult.viewMap
                    ) {
                        persisted.viewMap = ctx.writeMetadataJson(
                            app,
                            'viewMap.json',
                            viewMapResult.viewMap,
                        );
                    }
                    if (fieldReferenceResult.index) {
                        persisted.fieldReferenceIndex = ctx.writeMetadataJson(
                            app,
                            'fieldReferenceIndex.json',
                            fieldReferenceResult.index,
                        );
                    }
                }

                // File names rather than four full paths per app: the paths
                // were the same directory each time and doubled the report.
                const written = Object.entries(persisted)
                    .filter(([name]) => name !== 'enabled')
                    .map(([name, result]) => ({
                        name,
                        result: asRecord(result),
                    }));
                const failed = written.filter(
                    (entry) => entry.result?.ok === false,
                );
                warmed.push({
                    appKey: app.appKey,
                    ok: true,
                    runtimeMetadataLoaded: Boolean(metadata),
                    sources: {
                        schema: schemaResult.source,
                        fieldMap: fieldMapResult.source,
                        viewMap: viewMapResult.source,
                        fieldReferences: fieldReferenceResult.source,
                    },
                    ...(persistFiles
                        ? {
                              persisted: written
                                  .filter((entry) => entry.result?.ok === true)
                                  .map((entry) => entry.name),
                              ...(failed.length > 0
                                  ? {
                                        persistFailed: failed.map((entry) => ({
                                            name: entry.name,
                                            error: entry.result?.error ?? null,
                                        })),
                                    }
                                  : {}),
                          }
                        : {}),
                });
            } catch (error) {
                warmed.push({
                    appKey: app.appKey,
                    ok: false,
                    error: describeError(error),
                });
            }
        }
    }

    const persistSkipped = describePersistOutcome(warm, persistFiles);

    return {
        ok: true,
        target: appKey || 'all',
        warm,
        persistFiles,
        ...(persistSkipped ? { persistSkipped } : {}),
        appCount: targetApps.length,
        beforeSizes,
        afterSizes: getSizes(),
        warmed,
    };
}

export const cache = defineTool({
    name: 'knack_cache',
    description:
        'Report metadata cache and local file status, or clear, re-warm and persist the caches.',
    access: 'read',
    input: {
        appKey: z.string().optional(),
        refresh: z
            .boolean()
            .default(false)
            .describe('Clear caches instead of reporting'),
        warm: z
            .boolean()
            .default(false)
            .describe('With refresh: reload metadata immediately'),
        persistFiles: z
            .boolean()
            .default(true)
            .describe('With refresh and warm: write metadata files'),
    },
    handler: async ({ appKey, refresh, warm, persistFiles }, ctx) => {
        if (!refresh) {
            return makeTextResponse(buildCacheStatus(ctx, appKey));
        }
        return makeTextResponse(
            await refreshCaches(ctx, { appKey, warm, persistFiles }),
        );
    },
});

export const contextTools: AnyToolDef[] = [listApps, setContext, cache];
