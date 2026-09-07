/**
 * View mutation tools. Every one of them runs through runViewMutationTool, which owns the
 * guard: fresh metadata, the cascade check, the human confirmation, the snapshot and the
 * response shape. Nothing here re-implements any of that; each tool only names the
 * action and performs the Knack request the guard lets through.
 */
import { z } from 'zod';

import { findRawViewInMetadata, parseRuntimeScenes } from '../lib/metadata.js';
import { parseJsonInput } from '../lib/util.js';
import {
    planSharedPageCopy,
    resolveViewAttributes,
    verifySharedPageCopy,
} from '../lib/view-safety.js';
import {
    buildStarterPageGroups,
    describeLayoutKeyGap,
    getSceneViewKeys,
} from '../lib/view-templates.js';
import { type AnyToolDef, defineTool } from '../registry.js';
import { makeTextResponse } from '../response.js';
import { runViewMutationTool } from '../view-mutation.js';

export const createView = defineTool({
    name: 'knack_create_view',
    description: 'Create a view on a scene from a full view definition.',
    access: 'view',
    input: {
        appKey: z.string().optional(),
        sceneKey: z.string(),
        payload: z
            .string()
            .describe('Full view definition as JSON, with pageGroups'),
    },
    handler: async ({ appKey, sceneKey, payload }, ctx) => {
        const app = ctx.getApp(appKey);
        // Resolved before the guard runs any I/O: a missing key must refuse here, not
        // after a human has already been prompted or a snapshot written.
        ctx.getApiKey(app.appKey);

        return makeTextResponse(
            await runViewMutationTool(
                ctx,
                app,
                { action: 'create_view', sceneKey, updates: payload },
                () =>
                    ctx.request(app, `/scenes/${sceneKey}/views`, {
                        method: 'POST',
                        body: payload,
                    }),
            ),
        );
    },
});

export const updateViewOrder = defineTool({
    name: 'knack_update_view_order',
    description:
        'Update the order and page-group layout of the views on a scene.',
    access: 'view',
    input: {
        appKey: z.string().optional(),
        sceneKey: z.string(),
        order: z
            .union([z.string(), z.array(z.string())])
            .describe(
                'View keys in the desired order, as an array or its JSON',
            ),
        pageGroups: z
            .union([z.string(), z.array(z.unknown())])
            .optional()
            .describe(
                'Page groups layout, as an array or its JSON. Omitted: one full-width row per view, in order',
            ),
    },
    handler: async ({ appKey, sceneKey, order, pageGroups }, ctx) => {
        const app = ctx.getApp(appKey);
        ctx.getApiKey(app.appKey);

        // Both inputs used to be JSON strings only, and `pageGroups` was required. A
        // caller sending the array itself, or leaving the layout alone, failed MCP input
        // validation before this handler ran — a schema error that looked like a
        // refusal and was not one (6 September). Knack's sort route needs both, so a
        // missing layout is derived from the order: one row per view.
        const orderKeys =
            typeof order === 'string'
                ? parseJsonInput<unknown[]>('order', order)
                : order;
        // Checked after parsing, whichever form arrived: an empty list, or an entry
        // that is not a view key, would otherwise reach Knack as a sort request naming
        // no views, with a derived layout just as empty.
        if (
            !Array.isArray(orderKeys) ||
            orderKeys.length === 0 ||
            orderKeys.some(
                (key) => typeof key !== 'string' || key.trim() === '',
            )
        ) {
            throw new Error(
                'order must be a non-empty array of view keys (as an array or its JSON).',
            );
        }
        const layout =
            pageGroups === undefined
                ? orderKeys.map((viewKey) => ({
                      columns: [{ keys: [viewKey], width: 100 }],
                  }))
                : typeof pageGroups === 'string'
                  ? parseJsonInput<unknown[]>('pageGroups', pageGroups)
                  : pageGroups;
        const body = JSON.stringify({ order: orderKeys, pageGroups: layout });

        return makeTextResponse(
            await runViewMutationTool(
                ctx,
                app,
                {
                    action: 'update_view_order',
                    sceneKey,
                    // Passed so the payload gets the same depth and links inspection as
                    // any other caller-supplied JSON, rather than reaching the API
                    // unexamined.
                    updates: body,
                },
                () =>
                    ctx.request(app, `/scenes/${sceneKey}/views/sort`, {
                        method: 'POST',
                        body,
                    }),
            ),
        );
    },
});

export const updateView = defineTool({
    name: 'knack_update_view',
    description:
        'Update a view: the changes are merged into its live definition and sent whole.',
    access: 'view',
    input: {
        appKey: z.string().optional(),
        sceneKey: z.string(),
        viewKey: z.string(),
        updates: z
            .string()
            .optional()
            .describe(
                'JSON of the top-level properties to replace; omit if only using keywordEdits',
            ),
        keywordEdits: z
            .string()
            .optional()
            .describe(
                'JSON: {"title"?: {"_keyword": "value or null"}, "description"?: {...}} — adds each keyword at the end of the trailing KTL keyword cluster if new, or updates it in place (siblings untouched) if it already exists',
            ),
        confirmRemoveKtlKeywords: z
            .boolean()
            .default(false)
            .describe(
                'Allow a title/description change to drop an existing KTL keyword token',
            ),
        confirmDestructive: z
            .boolean()
            .optional()
            .describe('Removed; any value is refused'),
    },
    handler: async (
        {
            appKey,
            sceneKey,
            viewKey,
            updates,
            keywordEdits,
            confirmRemoveKtlKeywords,
            confirmDestructive,
        },
        ctx,
    ) => {
        const app = ctx.getApp(appKey);
        ctx.getApiKey(app.appKey);

        return makeTextResponse(
            await runViewMutationTool(
                ctx,
                app,
                {
                    action: 'update_view',
                    sceneKey,
                    viewKey,
                    updates: updates ?? '{}',
                    keywordEdits,
                    confirmRemoveKtlKeywords,
                    confirmDestructive,
                },
                async ({ outgoingBody }) => {
                    // The guard merged this from the live definition and the caller's
                    // patch, and every decision it made — which pages die, whether a
                    // human had to agree — was made against this exact object.
                    // Rebuilding it here would put two reasoners on one payload.
                    const completeBody = outgoingBody;

                    return ctx.request(
                        app,
                        `/scenes/${sceneKey}/views/${viewKey}`,
                        {
                            method: 'PUT',
                            body: completeBody
                                ? JSON.stringify(completeBody)
                                : updates,
                        },
                    );
                },
            ),
        );
    },
});

/**
 * One tool for the two legacy copies. `sharePages: false` is Knack's own copyview
 * endpoint (legacy knack_copy_view), which duplicates a table's owned child pages.
 * `sharePages: true` creates the copy from the source's definition (legacy
 * knack_copy_view_sharing_pages) so its link columns keep pointing at the original
 * pages, and checks Knack's response for exactly that.
 */
export const copyView = defineTool({
    name: 'knack_copy_view',
    description:
        "Copy a view to another scene, via Knack's copy or sharing its child pages.",
    access: 'view',
    input: {
        appKey: z.string().optional(),
        viewKey: z.string(),
        targetSceneKey: z.string(),
        sourceSceneKey: z
            .string()
            .optional()
            .describe(
                'Scene owning the view; derived only when sharePages is true',
            ),
        sharePages: z
            .boolean()
            .default(false)
            .describe(
                'Create from the source definition so link columns keep their pages',
            ),
        completeViewSchema: z
            .boolean()
            .default(false)
            .describe('Knack copyView flag; plain copy only'),
        name: z
            .string()
            .optional()
            .describe('sharePages only; defaults to "<name> Copy"'),
        title: z
            .string()
            .optional()
            .describe('sharePages only; source title kept if omitted'),
        existingViewKeys: z
            .array(z.string())
            .optional()
            .describe(
                'sharePages only: views already on the target page, in order',
            ),
    },
    handler: async (args, ctx) => {
        const app = ctx.getApp(args.appKey);
        ctx.getApiKey(app.appKey);
        const { viewKey, targetSceneKey, sourceSceneKey } = args;

        if (!args.sharePages) {
            if (!sourceSceneKey) {
                throw new Error(
                    'sourceSceneKey is required when sharePages is false.',
                );
            }

            return makeTextResponse({
                // `sceneKey` is what the guard reports, but this tool has always named
                // its two scenes explicitly. Keep both so a caller written against the
                // old response shape still finds sourceSceneKey.
                sourceSceneKey,
                targetSceneKey,
                ...(await runViewMutationTool(
                    ctx,
                    app,
                    { action: 'copy_view', sceneKey: sourceSceneKey, viewKey },
                    () =>
                        ctx.request(app, `/scenes/${sourceSceneKey}/copyview`, {
                            method: 'POST',
                            body: JSON.stringify({
                                action: 'copy',
                                target_scene_key: targetSceneKey,
                                view_key: viewKey,
                                completeViewSchema: args.completeViewSchema,
                            }),
                        }),
                )),
            });
        }

        const sourceViewKey = viewKey;
        const { name, title, existingViewKeys } = args;

        // Read the source fresh. The payload posted is its stored definition, and a
        // definition up to five minutes old is not the one being copied.
        ctx.caches.runtimeMetadata.delete(app.appKey);
        const metadata = await ctx.getRuntimeMetadata(app);
        if (!metadata) {
            return makeTextResponse({
                ok: false,
                appKey: app.appKey,
                action: 'copy_view_sharing_pages',
                sourceViewKey,
                error: 'COULD_NOT_VERIFY_VIEW',
                message:
                    'Runtime metadata could not be fetched from Knack, so the source view could not be read. Nothing was sent.',
            });
        }

        const scenes = parseRuntimeScenes(metadata);
        const resolvedSourceSceneKey =
            sourceSceneKey ??
            scenes.find((scene) =>
                scene.views.some((view) => view.viewKey === sourceViewKey),
            )?.sceneKey;
        const rawView = resolvedSourceSceneKey
            ? findRawViewInMetadata(
                  metadata,
                  resolvedSourceSceneKey,
                  sourceViewKey,
              )
            : null;
        const sourceAttributes = rawView
            ? resolveViewAttributes(rawView)
            : null;
        if (!resolvedSourceSceneKey || !sourceAttributes) {
            return makeTextResponse({
                ok: false,
                appKey: app.appKey,
                action: 'copy_view_sharing_pages',
                sourceViewKey,
                error: 'VIEW_NOT_FOUND',
                message: `${sourceViewKey} was not found${sourceSceneKey ? ` in ${sourceSceneKey}` : ''} in this app's metadata. Nothing was sent.`,
            });
        }

        const plan = planSharedPageCopy(sourceAttributes, { name, title });
        if (!plan.ok) {
            return makeTextResponse({
                ok: false,
                appKey: app.appKey,
                action: 'copy_view_sharing_pages',
                sourceSceneKey: resolvedSourceSceneKey,
                sourceViewKey,
                error: plan.code,
                message: plan.message,
            });
        }

        const sceneViewKeys = getSceneViewKeys(scenes, targetSceneKey);
        const layoutViewKeys =
            existingViewKeys && existingViewKeys.length > 0
                ? existingViewKeys
                : sceneViewKeys;
        const layoutWarning = existingViewKeys
            ? describeLayoutKeyGap(existingViewKeys, sceneViewKeys)
            : null;
        const payload = JSON.stringify({
            ...plan.payload,
            pageGroups: buildStarterPageGroups(layoutViewKeys),
        });

        const sharedPages = plan.linkedPageRefs.map((ref) => {
            const scene = scenes.find(
                (candidate) =>
                    candidate.sceneKey === ref || candidate.sceneSlug === ref,
            );
            return {
                ref,
                sceneKey: scene?.sceneKey ?? null,
                sceneName: scene?.sceneName ?? null,
            };
        });

        const outcome = await runViewMutationTool(
            ctx,
            app,
            {
                action: 'create_view',
                sceneKey: targetSceneKey,
                updates: payload,
            },
            () =>
                ctx.request(app, `/scenes/${targetSceneKey}/views`, {
                    method: 'POST',
                    body: payload,
                }),
            // Already fetched fresh above to resolve the source view — passing it on
            // avoids re-fetching the whole application payload a second time.
            { metadata },
        );

        // Knack's answer is the only account of whether the pages were shared.
        const verification =
            outcome.ok === true
                ? verifySharedPageCopy(plan.linkedPageRefs, outcome.body)
                : null;

        return makeTextResponse({
            sourceSceneKey: resolvedSourceSceneKey,
            sourceViewKey,
            targetSceneKey,
            sharedPages,
            ...(layoutWarning ? { layoutWarning } : {}),
            ...outcome,
            action: 'copy_view_sharing_pages',
            performedAs: 'create_view',
            ...(verification
                ? {
                      sharedPagesVerified: verification.verified,
                      ...(verification.problems.length > 0
                          ? {
                                sharedPagesProblems: verification.problems,
                                warning:
                                    'The copy did not come back as a shared-page copy. Read the pages above back before relying on it.',
                            }
                          : {}),
                  }
                : {}),
        });
    },
});

export const moveView = defineTool({
    name: 'knack_move_view',
    description:
        'Move a view to another scene; child pages reached only through it are at risk.',
    access: 'view',
    input: {
        appKey: z.string().optional(),
        sourceSceneKey: z.string(),
        targetSceneKey: z.string(),
        viewKey: z.string(),
        completeViewSchema: z
            .boolean()
            .default(false)
            .describe('Knack moveView flag'),
    },
    handler: async (
        { appKey, sourceSceneKey, targetSceneKey, viewKey, completeViewSchema },
        ctx,
    ) => {
        const app = ctx.getApp(appKey);
        ctx.getApiKey(app.appKey);

        return makeTextResponse({
            // `sceneKey` is what the guard reports, but this tool has always named its
            // two scenes explicitly. Keep both so a caller written against the old
            // response shape still finds sourceSceneKey.
            sourceSceneKey,
            targetSceneKey,
            ...(await runViewMutationTool(
                ctx,
                app,
                { action: 'move_view', sceneKey: sourceSceneKey, viewKey },
                () =>
                    ctx.request(app, `/scenes/${sourceSceneKey}/copyview`, {
                        method: 'POST',
                        body: JSON.stringify({
                            action: 'move',
                            target_scene_key: targetSceneKey,
                            view_key: viewKey,
                            completeViewSchema,
                        }),
                    }),
                undefined,
                // So the prompt can say who reaches the replacement pages under the
                // target, not only who reaches the pages being destroyed.
                { targetSceneKey },
            )),
        });
    },
});

export const deleteView = defineTool({
    name: 'knack_delete_view',
    description:
        'Delete a view; child pages reached only through it are destroyed with it.',
    access: 'view-delete',
    input: {
        appKey: z.string().optional(),
        sceneKey: z.string(),
        viewKey: z.string(),
    },
    handler: async ({ appKey, sceneKey, viewKey }, ctx) => {
        const app = ctx.getApp(appKey);
        ctx.getApiKey(app.appKey);

        return makeTextResponse(
            await runViewMutationTool(
                ctx,
                app,
                { action: 'delete_view', sceneKey, viewKey },
                () =>
                    ctx.request(app, `/scenes/${sceneKey}/views/${viewKey}`, {
                        method: 'DELETE',
                    }),
            ),
        );
    },
});

export const viewMutationTools: AnyToolDef[] = [
    createView,
    updateViewOrder,
    updateView,
    copyView,
    moveView,
    deleteView,
];
