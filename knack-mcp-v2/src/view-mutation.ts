/**
 * Wiring between the view tools and the pure safety guard in lib/view-safety.ts.
 *
 * Fresh metadata is read once per mutation and shared by the preflight, the scene tree,
 * the referrer graph and the snapshot, so all four describe the same instant. A cascade
 * that would destroy pages is put to a human through MCP elicitation; the calling model
 * cannot answer it. Behaviour here is what TESTED.md records; change it with evidence.
 */
import path from 'node:path';

import { ErrorCode } from '@modelcontextprotocol/sdk/types.js';

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
import { describeError, writeJsonFile } from './lib/util.js';
import {
    type PageDeletionConfirmation,
    type SceneNode,
    type ViewMutationAction,
    type ViewMutationDeps,
    type ViewMutationRequest,
    collectLinkTargets,
    readChangedScenes,
    runGuardedViewMutation,
    sanitiseFileNameComponent,
} from './lib/view-safety.js';
import { compactKnackChanges } from './response.js';
import type { RuntimeMetadata, SceneInfo } from './types.js';

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
        return { ok: false, error: describeError(error) };
    }
}

/**
 * The injected I/O the guard runs on. The preflight reads the view from runtime
 * metadata: Knack serves no per-view route to a REST key.
 */
export async function makeViewMutationDeps(
    ctx: KnackContext,
    app: AppConfig,
    /**
     * A metadata read the caller already did, moments earlier in the same handler, with
     * no intervening write — e.g. copyView's sharePages plan, which reads metadata to
     * resolve the source view before ever calling this. Passing it here avoids a second
     * full application-payload fetch for the "fresh read before a mutation" this
     * function's own cache-delete exists to guarantee; omit it to fetch fresh, as every
     * other caller does.
     */
    prefetchedMetadata?: { metadata: RuntimeMetadata | null },
): Promise<ViewMutationDeps> {
    const runtimeMetadata = prefetchedMetadata
        ? prefetchedMetadata.metadata
        : await (async () => {
              // The five-minute cache is wrong immediately before a destructive mutation.
              ctx.caches.runtimeMetadata.delete(app.appKey);
              return ctx.getRuntimeMetadata(app);
          })();
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
 * Whether a rejected elicitation was the request timing out rather than failing.
 *
 * The SDK cancels an overdue request with an `McpError` carrying
 * `ErrorCode.RequestTimeout`, so the code is the contract — matching on the message
 * text would break the first time the SDK rewords it. Anything else is a real failure
 * and stays one.
 *
 * @param error Whatever `elicitInput` rejected with.
 * @returns True only for the SDK's request-timeout error.
 */
function isRequestTimeout(error: unknown): boolean {
    return (
        typeof error === 'object' &&
        error !== null &&
        'code' in error &&
        (error as { code?: unknown }).code === ErrorCode.RequestTimeout
    );
}

/**
 * Ask the person operating the client to confirm a cascade delete, via elicitation.
 *
 * Never returns an acceptance for anything but a ticked box. A failure is
 * `supported: false`; an unanswered prompt is `outcome: 'timeout'`, which is a
 * refusal too but a different one — see the catch below.
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

    // A move reads as a re-parent, and it is not one. Measured live on 6 September:
    // an accepted move_view deleted the owned child page and Knack made a new page
    // under the target, with a new key. Both halves matter to whoever is deciding —
    // without the first they may think the page travels; without the second they may
    // not realise every reference to the old key is about to point at nothing. What
    // the replacement page carries was not measured, so this does not say.
    //
    //     Gated on the action alone, not on whether pages could be named. A move
    //     prompted only by unreadable links is the case where this matters most: the
    //     headline already says pages may die that it cannot list, and "move" is
    //     exactly what would make someone read that as survivable. Raised in review.
    const moveNote =
        input.action !== 'move_view'
            ? ''
            : named
              ? `\n\nThis is a move, not a re-parent: the page(s) above are destroyed rather than carried across. Knack makes a replacement page under the target, under a NEW key — so anything elsewhere in the app still pointing at the old key will be left pointing at nothing.`
              : `\n\nThis is a move, not a re-parent: any page owned through those unreadable links is destroyed rather than carried across, and Knack makes its replacement under a NEW key. None of them can be listed here, so nothing below tells you which references are about to point at nothing.`;

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
                message: `${headline}\n${named ? `\n${unresolvedNote}\n` : ''}\nThis cannot be undone from here. A snapshot is written first, but rebuilding from it is manual.${moveNote}${externalNote}${transferredNote}`,
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
        const timedOut = isRequestTimeout(error);
        debugLog('elicitation_failed', {
            appKey: app.appKey,
            timedOut,
            error: describeError(error),
        });
        // A timeout is the one failure here that says nothing about the client. The
        // prompt was delivered and rendered; a human simply did not answer it inside
        // CASCADE_CONFIRMATION_TIMEOUT_MS. Reporting that as `supported: false` put it
        // in the same bucket as a client with no elicitation capability at all, so the
        // refusal read "this MCP client cannot prompt a human" — false, and it sent the
        // operator to the builder when the fix was to answer the prompt still on their
        // screen. Measured live on 6 September; the outcome union already had a slot
        // for it that nothing produced.
        if (timedOut) {
            return { supported: true, accepted: false, outcome: 'timeout' };
        }
        return {
            supported: false,
            reason: `the elicitation request failed: ${describeError(error)}`,
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
 * The view a successful create or copy made, for its post-mutation snapshot.
 *
 * A create's response carries the view under `view`, key included. Knack's own copy
 * returns the *source* scene and names the new view only in `changes.inserts.views`,
 * so that one is read back from fresh metadata; `view` is null when it is not there
 * yet, and the caller says so rather than pretending.
 */
async function readCreatedView(
    ctx: KnackContext,
    app: AppConfig,
    request: ViewMutationRequest,
    result: KnackApiResult,
): Promise<{
    viewKey: string;
    sceneKey: string;
    view: Record<string, unknown> | null;
} | null> {
    const body = asPlainRecord(result.body);
    const responseView = asPlainRecord(body?.view);
    const responseViewKey =
        typeof responseView?.key === 'string' ? responseView.key : null;
    if (responseView && responseViewKey) {
        return {
            viewKey: responseViewKey,
            sceneKey: request.sceneKey,
            view: responseView,
        };
    }

    // `changes.inserts.views` entries come in three shapes: a bare key, `{ key }`, or
    // `{ view: {...} }` wrapping the whole inserted view (the same three
    // compactKnackChanges unwraps). Measured 6 September: Knack's copyview answered
    // with the wrapped shape, and a first cut that read only the first two filed no
    // snapshot for a live copy.
    const inserts = asPlainRecord(asPlainRecord(body?.changes)?.inserts);
    const insertedViews = Array.isArray(inserts?.views) ? inserts.views : [];
    let insertedKey: string | null = null;
    let insertedView: Record<string, unknown> | null = null;
    for (const entry of insertedViews) {
        if (typeof entry === 'string') {
            if (entry.trim()) insertedKey = entry.trim();
        } else {
            const item = asPlainRecord(entry);
            const wrapped = asPlainRecord(item?.view);
            const key =
                typeof wrapped?.key === 'string'
                    ? wrapped.key
                    : typeof item?.key === 'string'
                      ? item.key
                      : '';
            if (key.trim()) {
                insertedKey = key.trim();
                insertedView = wrapped && wrapped.key ? wrapped : null;
            }
        }
        if (insertedKey) break;
    }
    if (!insertedKey) return null;
    if (insertedView) {
        return {
            viewKey: insertedKey,
            sceneKey: request.sceneKey,
            view: insertedView,
        };
    }

    ctx.caches.runtimeMetadata.delete(app.appKey);
    const metadata = await ctx.getRuntimeMetadata(app);
    const owningScene = metadata
        ? parseRuntimeScenes(metadata).find((scene) =>
              scene.views.some((view) => view.viewKey === insertedKey),
          )
        : undefined;
    const rawView =
        metadata && owningScene
            ? findRawViewInMetadata(metadata, owningScene.sceneKey, insertedKey)
            : null;

    return {
        viewKey: insertedKey,
        sceneKey: owningScene?.sceneKey ?? request.sceneKey,
        view: rawView,
    };
}

/**
 * Page links in a sent body whose target is neither a scene key nor a slug in the tree.
 * Menu entries pointing outside the app (a `url`) are not links to a page and are not
 * counted. Page specifications are objects with nothing to resolve, so the collector
 * never yields a ref for them.
 */
async function findDanglingLinks(
    deps: ViewMutationDeps,
    body: Record<string, unknown> | null,
): Promise<Array<{ ref: string; sourcePaths: string[] }>> {
    if (!body) return [];
    const targets = collectLinkTargets(body);
    if (targets.childSceneRefs.length === 0) return [];

    const tree = await deps.listScenes();
    if (!tree.ok) return [];
    const known = new Set<string>();
    for (const scene of tree.scenes) {
        known.add(scene.sceneKey);
        if (scene.sceneSlug) known.add(scene.sceneSlug);
    }

    const links = [
        ...targets.linkColumns,
        ...targets.menuLinks.filter(
            (link) => link.linkType !== 'url' && !link.hasUrl,
        ),
    ];
    return targets.childSceneRefs
        .filter((ref) => !known.has(ref))
        .map((ref) => ({
            ref,
            sourcePaths: links
                .filter((link) => link.childSceneRef === ref)
                .map((link) => link.sourcePath),
        }));
}

function asPlainRecord(value: unknown): Record<string, unknown> | null {
    return value !== null && typeof value === 'object' && !Array.isArray(value)
        ? (value as Record<string, unknown>)
        : null;
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
    /** See makeViewMutationDeps — forwarded as-is; omit to fetch fresh. */
    prefetchedMetadata?: { metadata: RuntimeMetadata | null },
): Promise<Record<string, unknown>> {
    const deps = await makeViewMutationDeps(ctx, app, prefetchedMetadata);
    const identity = {
        appKey: app.appKey,
        sceneKey: request.sceneKey,
        ...(request.viewKey ? { viewKey: request.viewKey } : {}),
        action: request.action,
    };

    // The body the guard put on the wire, kept so the links it carries can be checked
    // against the scene tree after the fact.
    let outgoingBody: Record<string, unknown> | null = null;
    const outcome = await runGuardedViewMutation(deps, request, (context) => {
        outgoingBody = context.outgoingBody;
        return perform(context);
    });
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

    // A create or a copy destroys nothing, so the guard writes no snapshot before it.
    // But the view it makes then exists nowhere on disk — the recovery drill of 6
    // September found a copied table that had been deleted in the builder could not be
    // rebuilt, because the app-level snapshot carries only page and view keys. So the
    // made view is snapshotted after the fact. The write already happened, so a
    // failure here is reported, not turned into a refusal.
    let snapshotPath = outcome.snapshotPath;
    let snapshotNote: string | undefined;
    if (
        outcome.result.ok &&
        (request.action === 'create_view' || request.action === 'copy_view')
    ) {
        const created = await readCreatedView(
            ctx,
            app,
            request,
            outcome.result,
        );
        if (created) {
            const snapshot = await writeMutationSnapshot(ctx, app, {
                action: request.action,
                sceneKey: created.sceneKey,
                viewKey: created.viewKey,
                view: created.view,
            });
            if (snapshot.ok) {
                snapshotPath = snapshot.path;
                if (!created.view) {
                    snapshotNote = `${created.viewKey} was created, but could not be read back from Knack's metadata, so the snapshot holds the page tree and not the view's definition.`;
                }
            } else {
                snapshotNote = `The view was created, but its snapshot could not be written: ${snapshot.error}.`;
            }
        } else {
            snapshotNote =
                'The response named no created view, so no snapshot of it was written.';
        }
    }

    // Links in the sent body that name no page. The guard only asks about links a
    // mutation *removes*, because that is what destroys a page; a link it *adds* to a
    // slug no page has destroys nothing, so nothing asked — and on 6 September such a
    // link was stored without a word (A3's precondition). Knack keeps it and it opens
    // nothing, which is how the two dangling links on the 4 September menu came to be.
    const danglingLinks = outcome.result.ok
        ? await findDanglingLinks(deps, outgoingBody)
        : [];

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
        // Reported on every mutation, including — especially — the quiet ones. A
        // caller cannot otherwise tell a write a person approved from one that needed
        // no approval, and a reader working backwards through a transcript cannot
        // either. That ambiguity is what put the blame for a silent `ok` on two loud
        // refusals in the 4 September report.
        humanConfirmation: outcome.humanConfirmation,
        ...(snapshotPath ? { snapshotPath } : {}),
        ...(snapshotNote ? { snapshotNote } : {}),
        ...(danglingLinks.length > 0
            ? {
                  danglingLinks,
                  warning: `${danglingLinks.length} link(s) in the sent body point at a page this server could not find (${danglingLinks
                      .map((link) => link.ref)
                      .join(
                          ', ',
                      )}). Knack stored each one and it opens nothing. Check the slug against knack_list_scenes, or create the page with a page specification instead.`,
              }
            : {}),
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
