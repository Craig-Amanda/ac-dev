/**
 * Wiring between the view tools and the pure safety guard in lib/view-safety.ts.
 *
 * Fresh metadata is read once per mutation and shared by the preflight, the scene tree,
 * the referrer graph and the snapshot, so all four describe the same instant. A cascade
 * that would destroy pages is put to a human through MCP elicitation; the calling model
 * cannot answer it. Behaviour here is what TESTED.md records; change it with evidence.
 */
import path from 'node:path';

import type { AppConfig } from './config.js';
import type { KnackContext } from './context.js';
import type { KnackApiResult } from './http.js';
import { makeSceneBuilderUrl } from './lib/builder-urls.js';
import { VIEW_CACHE_STALE_NOTE } from './lib/field-payload.js';
import { debugLog } from './lib/log.js';
import {
    collectSceneViewLinks,
    findRawViewInMetadata,
    parseRuntimeScenes,
} from './lib/metadata.js';
import { writeJsonFile } from './lib/util.js';
import {
    type PageDeletionConfirmation,
    type SceneNode,
    type ViewMutationAction,
    type ViewMutationDeps,
    type ViewMutationRequest,
    readChangedScenes,
    runGuardedViewMutation,
    sanitiseFileNameComponent,
} from './lib/view-safety.js';
import { compactKnackChanges } from './response.js';
import type { SceneInfo } from './types.js';

/** How long to wait for a human to answer a cascade-delete prompt. */
export const CASCADE_CONFIRMATION_TIMEOUT_MS = 300_000;

let snapshotSequence = 1;

export type SceneTreeResult =
    { ok: true; scenes: SceneInfo[] } | { ok: false; reason: string };

function sceneTreeFromMetadata(
    metadata: Record<string, unknown> | null,
): SceneTreeResult {
    if (!metadata) {
        return {
            ok: false,
            reason: 'runtime metadata could not be fetched from Knack',
        };
    }
    const scenes = parseRuntimeScenes(metadata);
    if (scenes.length === 0) {
        return {
            ok: false,
            reason: 'the runtime metadata contained no scenes, which cannot be right for an app being mutated',
        };
    }
    return { ok: true, scenes };
}

/**
 * Re-read the scene tree, bypassing the cache. A stale or empty answer under-reports
 * what a delete destroys, so failure is reported as failure rather than as "no pages".
 */
export async function getFreshSceneTree(
    ctx: KnackContext,
    app: AppConfig,
): Promise<SceneTreeResult> {
    ctx.caches.runtimeMetadata.delete(app.appKey);
    return sceneTreeFromMetadata(await ctx.getRuntimeMetadata(app));
}

/**
 * Write a timestamped restore point: the full scene tree (routes, slugs, parents), the
 * target view's complete definition, and a pointer to the app's schema.json.
 */
export async function writeMutationSnapshot(
    ctx: KnackContext,
    app: AppConfig,
    params: {
        action: ViewMutationAction | 'manual';
        sceneKey?: string;
        viewKey?: string;
        view?: unknown;
        /** A tree the caller already fetched, so it is not fetched twice. */
        sceneTree?: SceneTreeResult;
    },
): Promise<{ ok: true; path: string } | { ok: false; error: string }> {
    try {
        const takenAt = new Date().toISOString();
        // Milliseconds plus a per-process counter: two mutations of one view inside a
        // second must not share a filename, since writeJsonFile overwrites.
        const stamp = takenAt.replaceAll(':', '-').replace('.', '-');
        const subject = sanitiseFileNameComponent(
            params.viewKey || params.sceneKey || 'app',
        );
        const fileName = `${stamp}-${params.action}-${subject}-${snapshotSequence++}.json`;

        const sceneTree =
            params.sceneTree ?? (await getFreshSceneTree(ctx, app));
        if (!sceneTree.ok) {
            return {
                ok: false,
                error: `the app scene tree could not be read (${sceneTree.reason}), so the snapshot would contain no pages to restore from`,
            };
        }

        const targetPath = path.join(
            app.appFolder,
            'schema',
            'snapshots',
            fileName,
        );
        const writeResult = writeJsonFile(targetPath, {
            snapshotVersion: 2,
            takenAt,
            appKey: app.appKey,
            appId: app.appId,
            action: params.action,
            sceneKey: params.sceneKey ?? null,
            viewKey: params.viewKey ?? null,
            scenes: sceneTree.scenes,
            view: params.view ?? null,
            schemaPath: path.join(app.appFolder, 'schema', 'schema.json'),
        });
        if (!writeResult.ok) return { ok: false, error: writeResult.error };

        debugLog('mutation_snapshot', {
            appKey: app.appKey,
            action: params.action,
            path: targetPath,
            scenes: sceneTree.scenes.length,
        });
        return { ok: true, path: targetPath };
    } catch (error) {
        return {
            ok: false,
            error: error instanceof Error ? error.message : String(error),
        };
    }
}

/**
 * The injected I/O the guard runs on. The preflight reads the view from runtime
 * metadata: Knack serves no per-view route to a REST key.
 */
export async function makeViewMutationDeps(
    ctx: KnackContext,
    app: AppConfig,
): Promise<ViewMutationDeps> {
    // The five-minute cache is wrong immediately before a destructive mutation.
    ctx.caches.runtimeMetadata.delete(app.appKey);
    const runtimeMetadata = await ctx.getRuntimeMetadata(app);
    const sceneTree = sceneTreeFromMetadata(runtimeMetadata);

    return {
        fetchView: async (sceneKey, viewKey) => {
            if (!runtimeMetadata) {
                return {
                    ok: false,
                    status: 502,
                    body: {
                        error: 'runtime metadata could not be fetched from Knack, so the view could not be verified',
                    },
                };
            }
            const view = findRawViewInMetadata(
                runtimeMetadata,
                sceneKey,
                viewKey,
            );
            if (!view) {
                return {
                    ok: false,
                    status: 404,
                    body: {
                        error: `${viewKey} was not found in ${sceneKey} in this app's metadata`,
                    },
                };
            }
            return { ok: true, status: 200, body: view };
        },
        listScenes: async () => {
            if (!sceneTree.ok) return sceneTree;
            // The link graph the referrer count runs on, from the same payload as the
            // view being mutated. Omitted entirely when that read failed: `views: []`
            // would claim nothing links to a page, on invented evidence.
            const linksByScene = runtimeMetadata
                ? collectSceneViewLinks(runtimeMetadata)
                : null;
            return {
                ok: true as const,
                scenes: sceneTree.scenes.map((scene): SceneNode => ({
                    sceneKey: scene.sceneKey,
                    sceneName: scene.sceneName,
                    sceneSlug: scene.sceneSlug,
                    parentRef: scene.parentRef,
                    ...(linksByScene
                        ? { views: linksByScene.get(scene.sceneKey) ?? [] }
                        : {}),
                })),
            };
        },
        writeSnapshot: async (input) =>
            writeMutationSnapshot(ctx, app, { ...input, sceneTree }),
        builderUrlForScene: (sceneKey) =>
            makeSceneBuilderUrl(app, sceneKey, runtimeMetadata),
        confirmPageDeletion: (input) =>
            askHumanToConfirmPageDeletion(ctx, app, input),
    };
}

/** What a cascade delete would do, given whether this client can prompt a person. */
export function describeCascadeBehaviour(humanConfirmationAvailable: boolean): {
    mode: string;
    summary: string;
} {
    return humanConfirmationAvailable
        ? {
              mode: 'prompts-human',
              summary:
                  'A mutation that would delete child pages is put to the user for confirmation. The calling model cannot answer it.',
          }
        : {
              mode: 'refuses',
              summary:
                  'No human can be prompted, so a mutation that would delete child pages is refused outright. There is no override — make the change in the Knack builder.',
          };
}

/** Whether the connected client can put a confirmation prompt in front of a human. */
export function getHumanConfirmationStatus(ctx: KnackContext) {
    const available = ctx.clientCanPromptHuman();
    return {
        available,
        client: ctx.describeClient(),
        message: available
            ? 'This client can prompt a human, so a mutation that would delete child pages is put to the user directly. The calling model cannot answer that prompt.'
            : 'This client did not advertise the elicitation capability, so no human can be prompted. Any mutation that would delete child pages is refused, with no override. Make such changes in the Knack builder.',
    };
}

type ConfirmationInput = Parameters<
    NonNullable<ViewMutationDeps['confirmPageDeletion']>
>[0];

/**
 * Ask the person operating the client to confirm a cascade delete, via elicitation.
 * Any failure is `supported: false`, never an acceptance.
 */
export async function askHumanToConfirmPageDeletion(
    ctx: KnackContext,
    app: AppConfig,
    input: ConfirmationInput,
): Promise<PageDeletionConfirmation> {
    if (!ctx.server || !ctx.clientCanPromptHuman()) {
        return {
            supported: false,
            reason: 'the client did not advertise the elicitation capability',
        };
    }

    const pageList = input.childPages
        .map(
            (page) =>
                `  - ${page.sceneKey}${page.sceneName ? ` (${page.sceneName})` : ''}${
                    page.depth > 0 ? ' — child of a page above' : ''
                }`,
        )
        .join('\n');

    // With only unreadable links the count is zero and the list blank, so that case
    // gets its own wording rather than "delete 0 page(s)" above nothing.
    const named = input.childPages.length;
    const target = input.viewKey ?? input.sceneKey;
    const headline = named
        ? `Knack will permanently delete ${named} page(s) if this ${input.action} goes ahead on ${target} in "${app.appKey}".\n\nPages that would be destroyed:\n${pageList}`
        : `This ${input.action} on ${target} in "${app.appKey}" removes ${input.unresolvedLinkCount} link(s) whose target page this server could not identify.\n\nNo page can be named, so none can be listed — but a link that cannot be read is not a link to nothing, and accepting this may destroy pages that do not appear anywhere in this prompt.`;

    const externalNote = input.externalPages?.length
        ? `\n\nAlso losing their link, but NOT being deleted (these pages live elsewhere in the app):\n${input.externalPages
              .map(
                  (page) =>
                      `  - ${page.sceneKey ?? '?'}${page.sceneName ? ` (${page.sceneName})` : ''}`,
              )
              .join('\n')}`
        : '';

    const transferredNote = input.transferredPages?.length
        ? `\n\nAlso losing their link here, but NOT being deleted — another view still links to each of these, so Knack moves the page under that view instead:\n${input.transferredPages
              .map(
                  (page) =>
                      `  - ${page.sceneKey ?? '?'}${page.sceneName ? ` (${page.sceneName})` : ''} → now reached from ${
                          page.otherReferrers
                              .map((entry) => entry.viewKey)
                              .join(', ') || 'another view'
                      }`,
              )
              .join('\n')}`
        : '';

    const unresolvedNote =
        input.unresolvedLinkCount > 0
            ? `\n\nWARNING: ${input.unresolvedLinkCount} further link(s) point at pages this server could not identify, so they are not listed above. More pages than shown may be destroyed.`
            : '';

    try {
        const result = await ctx.server.server.elicitInput(
            {
                message: `${headline}\n${named ? `\n${unresolvedNote}\n` : ''}\nThis cannot be undone from here. A snapshot is written first, but rebuilding from it is manual.${externalNote}${transferredNote}`,
                requestedSchema: {
                    type: 'object',
                    properties: {
                        confirm: {
                            type: 'boolean',
                            title: named
                                ? `Delete these ${named} page(s)`
                                : 'Proceed, and accept that unnamed pages may be destroyed',
                            description:
                                'Leave unticked to cancel. Nothing is sent to Knack unless this is ticked.',
                        },
                    },
                    required: ['confirm'],
                },
            },
            { timeout: CASCADE_CONFIRMATION_TIMEOUT_MS },
        );

        if (result.action !== 'accept') {
            return { supported: true, accepted: false, outcome: result.action };
        }
        const confirmed = result.content?.confirm === true;
        return {
            supported: true,
            accepted: confirmed,
            outcome: confirmed ? 'accept' : 'decline',
        };
    } catch (error) {
        debugLog('elicitation_failed', {
            appKey: app.appKey,
            error: error instanceof Error ? error.message : String(error),
        });
        return {
            supported: false,
            reason: `the elicitation request failed: ${error instanceof Error ? error.message : String(error)}`,
        };
    }
}

/** The scenes Knack says it deleted, or null when the response carries none. */
export function readDeletedScenes(result: KnackApiResult): string[] | null {
    const keys = readChangedScenes(result.body, 'deletes').map(
        (scene) => scene.sceneKey,
    );
    return keys.length > 0 ? keys : null;
}

/**
 * Run a view mutation through the guard and shape the tool payload.
 *
 * Every view tool goes through here, so the rules hold whichever tool is used. The
 * response reports both what this server predicted and what Knack says happened, under
 * different names, and says explicitly when they disagree.
 */
export async function runViewMutationTool(
    ctx: KnackContext,
    app: AppConfig,
    request: ViewMutationRequest,
    perform: (context: {
        outgoingBody: Record<string, unknown> | null;
        currentAttributes: Record<string, unknown> | null;
    }) => Promise<KnackApiResult>,
): Promise<Record<string, unknown>> {
    const deps = await makeViewMutationDeps(ctx, app);
    const identity = {
        appKey: app.appKey,
        sceneKey: request.sceneKey,
        ...(request.viewKey ? { viewKey: request.viewKey } : {}),
        action: request.action,
    };

    const outcome = await runGuardedViewMutation(deps, request, perform);
    if (!outcome.ok) {
        debugLog('view_mutation_blocked', { ...identity, error: outcome.code });
        return {
            ok: false,
            ...identity,
            error: outcome.code,
            message: outcome.message,
            ...(outcome.details ?? {}),
        };
    }

    const reportedDeletes = readDeletedScenes(outcome.result);
    const reportedCreates = readChangedScenes(outcome.result.body, 'inserts');
    // Reconciled one to one, by name and (where both carry one) parent, so two
    // requested pages of one name need two created entries.
    const unmatched = [...reportedCreates];
    const requestedButNotCreated = outcome.requestedPages
        .filter((spec) => {
            const index = unmatched.findIndex(
                (page) =>
                    page.sceneName === spec.name &&
                    (spec.parentRef === null ||
                        page.parentRef === null ||
                        page.parentRef === spec.parentRef),
            );
            if (index === -1) return true;
            unmatched.splice(index, 1);
            return false;
        })
        .map((spec) => spec.name);

    return {
        ...identity,
        ...(outcome.snapshotPath ? { snapshotPath: outcome.snapshotPath } : {}),
        ...(outcome.acknowledgedPages.length > 0
            ? { pagesExpectedToBeDeleted: outcome.acknowledgedPages }
            : {}),
        ...(outcome.requestedPages.length > 0
            ? {
                  pagesRequested: outcome.requestedPages.map(
                      (spec) => spec.name,
                  ),
              }
            : {}),
        ...(reportedCreates.length > 0
            ? { pagesCreated: reportedCreates }
            : {}),
        ...(requestedButNotCreated.length > 0
            ? { pagesRequestedButNotCreated: requestedButNotCreated }
            : {}),
        ...(outcome.externalPages.length > 0
            ? {
                  linksRemovedPagesKept: outcome.externalPages.map((page) => ({
                      sceneKey: page.sceneKey,
                      sceneName: page.sceneName,
                      sceneSlug: page.sceneSlug,
                      parentSceneKey: page.parentSceneKey,
                  })),
              }
            : {}),
        ...(outcome.transferredPages.length > 0
            ? {
                  pagesMovedToAnotherLink: outcome.transferredPages.map(
                      (page) => ({
                          sceneKey: page.sceneKey,
                          sceneName: page.sceneName,
                          sceneSlug: page.sceneSlug,
                          previousParentSceneKey: page.parentSceneKey,
                          nowReachedFrom: page.otherReferrers,
                      }),
                  ),
              }
            : {}),
        ...(reportedDeletes
            ? { pagesKnackReportsDeleted: reportedDeletes }
            : {}),
        ...outcome.result,
        ...compactKnackChanges(outcome.result.body),
        ...(outcome.result.ok ? { cacheNote: VIEW_CACHE_STALE_NOTE } : {}),
    };
}
