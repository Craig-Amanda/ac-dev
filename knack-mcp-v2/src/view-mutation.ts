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
    collectLayoutViewKeys,
    collectSceneViewLinks,
    findRawSceneInMetadata,
    findRawViewInMetadata,
    parseRuntimeScenes,
} from './lib/metadata.js';
import {
    type AudienceChange,
    type PageAccess,
    type ProfileNameIndex,
    buildProfileNameIndex,
    compareAudience,
    describeAudience,
    resolvePageAccess,
} from './lib/page-access.js';
import { describeError, writeJsonFile } from './lib/util.js';
import {
    type ClassifiedLinkTarget,
    type PageDeletionConfirmation,
    type SceneNode,
    type ViewMutationAction,
    type ViewMutationDeps,
    type ViewMutationRequest,
    collectLinkTargets,
    readChangedScenes,
    type ReportedScene,
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
        // Version 3 (7 September): scenes carry their access fields — `sceneType`,
        // `authenticated`, and on a login view `allowedProfiles` /
        // `limitProfileAccess` — and `profiles` maps each profile key to the user
        // object defining it. Before this a page rebuilt from a snapshot came back
        // without its access control, and nothing in the file said what it had been.
        // The cache is warm here (every caller has just read the tree from it), so
        // this is a lookup, not a second fetch.
        const profiles = [
            ...buildProfileNameIndex(
                await ctx.getRuntimeMetadata(app),
            ).values(),
        ];
        const writeResult = writeJsonFile(targetPath, {
            snapshotVersion: 3,
            takenAt,
            appKey: app.appKey,
            appId: app.appId,
            action: params.action,
            sceneKey: params.sceneKey ?? null,
            viewKey: params.viewKey ?? null,
            scenes: sceneTree.scenes,
            profiles,
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
    /**
     * What the prompt needs to say who can reach a page before and after. A move
     * names its target scene here; the guard's own request shape does not carry it.
     */
    audience?: { targetSceneKey?: string },
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
            askHumanToConfirmPageDeletion(ctx, app, input, {
                scenes: sceneTree.ok ? sceneTree.scenes : null,
                profileNames: buildProfileNameIndex(runtimeMetadata),
                targetSceneKey: audience?.targetSceneKey,
            }),
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
 * Where a surviving page is expected to end up, for the prompt.
 *
 * Three live transfers (TESTING.md Tier 6) put it on whichever surviving referrer comes
 * first in the app's own page order, the third run a pre-registered prediction across a
 * pair where page order and key order disagree. The referrer list arrives in that order,
 * so the first entry is the expectation.
 *
 * Named rather than listed because "reached from one of these three" leaves the person
 * deciding to go and find the page afterwards. Hedged rather than promised because the
 * rule rests on an order a builder edit can change, and a prompt that overstates its own
 * certainty is the defect this file has already been fixed for twice.
 *
 * @param referrers Views still linking to the page, in the app's page order.
 * @returns A clause naming the expected destination, and any alternatives.
 */
function describeTransferDestination(
    referrers: Array<{ sceneKey: string; viewKey: string }>,
): string {
    if (referrers.length === 0) return 'now reached from another view';
    if (referrers.length === 1) {
        return `now reached from ${referrers[0].viewKey}, which becomes its parent`;
    }
    const others = referrers
        .slice(1)
        .map((entry) => entry.viewKey)
        .join(', ');
    return `expected to land under ${referrers[0].viewKey} (first in this app's page order; measured, not guaranteed), with ${others} still linking to it`;
}

/** The tree and the role names the prompt resolves audiences against. */
export type AudienceContext = {
    /** Null when the tree could not be read; the prompt then asks rather than answers. */
    scenes: SceneInfo[] | null;
    profileNames: ProfileNameIndex;
    /** The scene a moved view lands on, so the replacement pages' audience is known. */
    targetSceneKey?: string;
};

type AudienceLine = {
    text: string;
    change: ReturnType<typeof compareAudience>;
};

/**
 * The same audience comparison as the prompt, as data rather than prose.
 *
 * `describeAudienceConsequence` only runs while a confirmation is being built, and a
 * confirmation is only built when something is destroyed — `destroysNothing` is
 * computed from the doomed pages and unresolved links alone, and `transferredPages`
 * is not in that condition. So a mutation that merely re-parents a page changed who
 * could reach it and said nothing, which is how the Noah's Place client pages left a
 * five-role login for a Developer-only one without a word (TESTING.md Tier 8).
 *
 * This runs on every mutation and goes in the response, so the change is reported
 * whether or not anyone was asked to approve it. It reports rather than blocks: a
 * re-parent is frequently legitimate, and refusing every one of them on a client that
 * cannot prompt would make ordinary edits impossible.
 *
 * Deliberately kept beside `describeAudienceConsequence` rather than folded into it —
 * the prompt's wording is pinned by its own tests. The incident suite's "agrees with
 * the prompt about whether anything changed" asserts the two never diverge on that.
 *
 * @param input The action, the pages it dooms (by key), and the pages it transfers.
 * @param audience Scene tree, profile names and a move's target scene.
 * @returns One row per page changing parent; empty when none does or none can be read.
 */
export function summariseAudienceChanges(
    input: {
        action: string;
        sceneKey: string;
        /** Doomed page keys — `acknowledgedPages` from the guard's outcome. */
        childPageKeys: string[];
        transferredPages: ClassifiedLinkTarget[];
    },
    audience: AudienceContext | undefined,
): Array<{
    sceneKey: string;
    change: AudienceChange;
    before: string;
    after: string;
    destinationSceneKey: string | null;
}> {
    const isMove = input.action === 'move_view';
    const transferred = input.transferredPages ?? [];
    if (!isMove && transferred.length === 0) return [];
    if (!audience?.scenes) return [];
    const { scenes, profileNames } = audience;

    const row = (
        sceneKey: string,
        beforeKey: string,
        afterKey: string | null,
    ) => {
        const before = resolvePageAccess(beforeKey, scenes);
        const after = afterKey ? resolvePageAccess(afterKey, scenes) : null;
        return {
            sceneKey,
            change: after
                ? compareAudience(before, after)
                : ('unknown' as AudienceChange),
            before: describeAudience(before, profileNames),
            after: after
                ? describeAudience(after, profileNames)
                : 'not known here',
            destinationSceneKey: afterKey,
        };
    };

    const rows: ReturnType<typeof row>[] = [];

    if (isMove && audience.targetSceneKey) {
        for (const key of input.childPageKeys) {
            rows.push(row(key, key, audience.targetSceneKey));
        }
    }

    for (const page of transferred) {
        if (!page.sceneKey) continue;
        rows.push(
            row(
                page.sceneKey,
                page.sceneKey,
                page.otherReferrers[0]?.sceneKey ?? null,
            ),
        );
    }

    return rows;
}

/**
 * Who can reach each re-parented page now, and who will be able to afterwards.
 *
 * Raised by the operator, and it is the consequence with the widest blast radius: in
 * Knack a page's login and permitted roles follow its parentage, so a page that changes
 * parent can change who can reach it — a transfer that looks like a tidy-up can quietly
 * take a page away from the people who used it.
 *
 * Until 7 September this asked ("CHECK THE AUDIENCE") rather than answered, because
 * nothing read the permissions. T22 measured where they live — on the login view of a
 * `type: "authentication"` ancestor, and nowhere on the pages beneath — so this now
 * resolves both sides and says which it is. It still asks, in the old words, wherever
 * it cannot resolve one side: a move whose target is not known here, a tree that could
 * not be read, a parent that matches no page. An unmeasured claim in a safety prompt is
 * the defect this file has been fixed for twice already, and "unchanged" is the one
 * answer that lets a prompt go quiet, so unknown is never rounded to it.
 *
 * @param input The guard's confirmation input.
 * @param audience The tree and names to resolve against; undefined asks, as before.
 * @returns A paragraph for the prompt, or '' when no page changes parent.
 */
export function describeAudienceConsequence(
    input: ConfirmationInput,
    audience: AudienceContext | undefined,
): string {
    const isMove = input.action === 'move_view';
    const transferred = input.transferredPages ?? [];
    if (!isMove && transferred.length === 0) return '';

    const ask = `\n\nCHECK THE AUDIENCE: a page's login and permitted roles follow its parent, so any page changing parent here may become reachable by a different set of users. Page permissions could not be resolved for this prompt — verify in the builder before accepting.`;
    if (!audience?.scenes) return ask;
    const { scenes, profileNames } = audience;

    const line = (
        label: string,
        before: PageAccess,
        after: PageAccess,
        destination: string,
    ): AudienceLine => {
        const change = compareAudience(before, after);
        const verdict =
            change === 'same'
                ? 'unchanged'
                : change === 'changed'
                  ? 'CHANGES'
                  : 'UNKNOWN — verify in the builder';
        return {
            change,
            text: `  - ${label}: now ${describeAudience(before, profileNames)}; ${destination} ${describeAudience(after, profileNames)} → ${verdict}`,
        };
    };

    const lines: AudienceLine[] = [];

    if (isMove) {
        if (!audience.targetSceneKey) return ask;
        const after = resolvePageAccess(audience.targetSceneKey, scenes);
        const destination = `its replacement under ${audience.targetSceneKey}:`;
        if (input.childPages.length > 0) {
            for (const page of input.childPages) {
                lines.push(
                    line(
                        page.sceneKey,
                        resolvePageAccess(page.sceneKey, scenes),
                        after,
                        destination,
                    ),
                );
            }
        } else {
            // Nothing could be named, so the pages owned through the unreadable links
            // are described through the page they hang off: they share its audience.
            lines.push(
                line(
                    `pages owned through ${input.sceneKey}'s unreadable links`,
                    resolvePageAccess(input.sceneKey, scenes),
                    after,
                    destination,
                ),
            );
        }
    }

    for (const page of transferred) {
        if (!page.sceneKey) continue;
        const before = resolvePageAccess(page.sceneKey, scenes);
        const first = page.otherReferrers[0];
        if (!first) {
            lines.push({
                change: 'unknown',
                text: `  - ${page.sceneKey}: now ${describeAudience(before, profileNames)}; where it lands is not known here → UNKNOWN — verify in the builder`,
            });
            continue;
        }
        lines.push(
            line(
                page.sceneKey,
                before,
                resolvePageAccess(first.sceneKey, scenes),
                `under ${first.sceneKey} (expected destination):`,
            ),
        );
    }

    if (lines.length === 0) return ask;
    const anyChanged = lines.some((entry) => entry.change === 'changed');
    const anyUnknown = lines.some((entry) => entry.change === 'unknown');
    const headline = anyChanged
        ? 'AUDIENCE CHANGES: a page below becomes reachable by a different set of users. A page whose login and roles follow its parent goes with its new parent, not with the people who used it.'
        : anyUnknown
          ? 'CHECK THE AUDIENCE: who can reach at least one page below could not be resolved on one side, so it may change without this prompt being able to say so.'
          : 'Audience unchanged: every page changing parent here stays reachable by the same users.';
    return `\n\n${headline}\n${lines.map((entry) => entry.text).join('\n')}`;
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
    audience?: AudienceContext,
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
                      `  - ${page.sceneKey ?? '?'}${page.sceneName ? ` (${page.sceneName})` : ''} → ${describeTransferDestination(page.otherReferrers)}`,
              )
              .join('\n')}`
        : '';

    const audienceNote = describeAudienceConsequence(input, audience);

    const unresolvedNote =
        input.unresolvedLinkCount > 0
            ? `\n\nWARNING: ${input.unresolvedLinkCount} further link(s) point at pages this server could not identify, so they are not listed above. More pages than shown may be destroyed.`
            : '';

    try {
        const result = await ctx.server.server.elicitInput(
            {
                message: `${headline}\n${named ? `\n${unresolvedNote}\n` : ''}\nThis cannot be undone from here. A snapshot is written first, but rebuilding from it is manual.${moveNote}${externalNote}${transferredNote}${audienceNote}`,
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
/**
 * Put a moved view into its new page's layout, when that page has one.
 *
 * A move adds the view to the target page's `views` and leaves its `groups` alone —
 * measured 10 September, both directions (TESTING.md Tier 8). On a page with an
 * explicit layout that means the view exists, opens by its builder URL, and renders
 * on neither the front end nor the back end. That invisible view is what sent someone
 * looking for a way to inspect a page and, finding none, reaching for a live move as
 * a probe. It is the first link in the incident chain, so it is repaired here rather
 * than reported.
 *
 * The repair is additive and was measured before being written: appending one
 * full-width row to the stored `groups` made the moved view render and left the rest
 * of the layout exactly as it was. It starts from the **stored** array, never from
 * `layoutViewKeys`, so a multi-column row survives.
 *
 * Three cases do nothing, and each is a real state rather than a failure:
 * an empty layout (Knack then renders every view, so there is nothing to fix), a
 * layout that already names the view, and a page or metadata that cannot be read —
 * where guessing a layout would be far worse than leaving one alone.
 *
 * The move has already happened when this runs, so a failure here is reported, never
 * turned into a refusal.
 */
export async function ensureMovedViewIsRendered(
    ctx: KnackContext,
    app: AppConfig,
    targetSceneKey: string,
    viewKey: string,
): Promise<Record<string, unknown>> {
    ctx.caches.runtimeMetadata.delete(app.appKey);
    const metadata = await ctx.getRuntimeMetadata(app);
    const scene = metadata
        ? findRawSceneInMetadata(metadata, targetSceneKey)
        : null;
    if (!scene) {
        return {
            layoutRepair: 'unknown',
            layoutNote: `${targetSceneKey} could not be read back after the move, so whether ${viewKey} is in its layout is unknown. If it does not appear on the page, add it with knack_update_view_order.`,
        };
    }

    const storedGroups = Array.isArray(scene.groups) ? scene.groups : [];
    if (storedGroups.length === 0) {
        // No explicit layout: Knack renders every view on the page, so the moved view
        // is already visible and writing a layout here would only arm the hazard for
        // the next move onto this page.
        return { layoutRepair: 'not-needed' };
    }

    if (collectLayoutViewKeys(storedGroups).includes(viewKey)) {
        return { layoutRepair: 'not-needed' };
    }

    const order = Array.isArray(scene.views)
        ? scene.views
              .map((view) => asPlainRecord(view)?.key)
              .filter((key): key is string => typeof key === 'string')
        : [];
    if (!order.includes(viewKey)) {
        return {
            layoutRepair: 'unknown',
            layoutNote: `${viewKey} was not found on ${targetSceneKey} when reading it back, so its layout was left alone.`,
        };
    }

    const pageGroups = [
        ...storedGroups,
        { columns: [{ keys: [viewKey], width: 100 }] },
    ];
    const written = await ctx.request(
        app,
        `/scenes/${targetSceneKey}/views/sort`,
        {
            // POST, not PUT. Measured 10 September: this endpoint answers PUT with a
            // 400, and knack_update_view_order — the one caller that had ever written
            // a layout for real — had always used POST. Both repairs here were written
            // against fake contexts that accept any method, and the move repair's PUT
            // was never executed live because every move tested was refused first. So
            // the wrong verb sat in two places, unexercised, with tests passing.
            method: 'POST',
            body: JSON.stringify({ order, pageGroups }),
        },
    );

    if (!written.ok) {
        return {
            layoutRepair: 'failed',
            layoutNote: `The move succeeded, but ${viewKey} could not be added to ${targetSceneKey}'s layout (status ${written.status}). Until it is, the view exists on the page and renders nowhere. Add it with knack_update_view_order.`,
        };
    }

    return {
        layoutRepair: 'added',
        layoutNote: `${viewKey} was appended to ${targetSceneKey}'s layout as a new full-width row — a move does not do this, and without it the view would exist on the page and render nowhere. The rest of the layout is unchanged. Knack's front end caches app metadata, so a page open in a browser may need a reload before it appears.`,
    };
}

/**
 * Which of a view's linked pages a copy will duplicate, and which it will share.
 *
 * Measured 10 September on one table with two link columns pointing at **sibling child
 * pages of the same parent**, differing only in the `remote` flag: the owned link
 * produced a new page under the copy's target page and the copy was repointed at it,
 * while the remote link was shared, with no page created and both views pointing at the
 * same page.
 *
 * So `remote` governs a copy as well as a move. Reported rather than acted on: a copy
 * duplicating the pages a view owns is Knack working as intended and usually what the
 * caller wants. What was missing was any way to know which links would do which before
 * looking at the result.
 *
 * @param attributes The source view's live definition.
 * @returns One row per linked page, or an empty array when the view links to none.
 */
export function summariseCopyLinkOwnership(
    attributes: Record<string, unknown> | null,
    /** Pages the copy's own response reported creating — `readChangedScenes(body, 'inserts')`. */
    createdPages: ReportedScene[],
): Array<{
    header: string | null;
    childSceneRef: string;
    owned: boolean;
    onCopy: 'duplicated' | 'shared';
}> {
    const { linkColumns } = collectLinkTargets(attributes);
    // Read from the response, not predicted from the flag. `remote` answers "does this
    // view claim the page", which is the right input for the cascade guard and the
    // wrong one for this question: measured 11 September, a plain copy duplicates a
    // table's `type: "link"` page and *shares* a details or list view's
    // `type: "scene_link"` page, with the flag absent in every case. Predicting from
    // ownership therefore told a caller its copy was independent when the two views
    // had just been left pointing at one page.
    const knackCreatedAPage = createdPages.length > 0;
    const rows: Array<{
        header: string | null;
        childSceneRef: string;
        owned: boolean;
        onCopy: 'duplicated' | 'shared';
    }> = [];
    for (const column of linkColumns) {
        if (!column.childSceneRef) continue;
        // Absent counts as owned: Knack treats a missing flag the same as false.
        const owned = column.remote !== true;
        rows.push({
            header: column.header,
            childSceneRef: column.childSceneRef,
            owned,
            // A renounced link has nothing to duplicate, so it is shared whatever the
            // response says. An owned one is only duplicated if a page actually appeared.
            onCopy: owned && knackCreatedAPage ? 'duplicated' : 'shared',
        });
    }
    return rows;
}

/**
 * The view keys a mutation's own response says it created.
 *
 * Knack reports these under `body.changes.inserts.views`. Read from the response rather
 * than inferred, because a copy of a view owning child pages creates a view per
 * duplicated page as well, and their number is not knowable in advance.
 *
 * @param outcome A tool outcome as assembled by runViewMutationTool.
 * @returns The created view keys, in the order Knack listed them; empty when the
 *     response carries none, including on a refusal.
 */
export function insertedViewKeysFromOutcome(
    outcome: Record<string, unknown>,
): string[] {
    const body = asPlainRecord(outcome.body);
    const inserts = asPlainRecord(asPlainRecord(body?.changes)?.inserts);
    const views = inserts?.views;
    if (!Array.isArray(views)) return [];
    return views.filter(
        (key): key is string => typeof key === 'string' && !!key,
    );
}

/**
 * The layout a page should have once a copied view has been put into it exactly once.
 *
 * Knack's `copyview` endpoint appends the new view's key to **every** existing row of
 * the target page's layout. Measured 10 September on a link-free `rich_text` view copied
 * onto a page with a five-row layout: the key landed in all five rows, so the one view
 * renders five times. This server sends no layout on a plain copy - the request body is
 * only `{action, target_scene_key, view_key, completeViewSchema}` - so the injection is
 * the endpoint's, not ours, and the caller cannot avoid it by asking differently.
 *
 * It only bites a page that already has an explicit layout. A page with `groups: []`
 * renders every view it holds, Knack writes nothing, and there is nothing to repair -
 * which is why the first copy measured looked clean and the second did not.
 *
 * @param storedGroups The target page's `groups` as Knack returned it after the copy,
 *     with `viewKey` already injected into each row.
 * @param viewKey The newly created copy.
 * @returns The corrected `groups` to write back, or `null` to decline the repair and
 *     leave the layout exactly as Knack left it.
 */
export function buildRepairedCopyLayout(
    storedGroups: unknown[],
    viewKey: string,
): unknown[] | null {
    // Strip every occurrence, in every column of every row.
    //
    // Adding the key to each row is the *only* change the endpoint makes to the layout,
    // so removing all of them reconstructs the page's pre-copy layout exactly. That is
    // what makes the second step defensible rather than a preference: this is not
    // rearranging someone's page, it is undoing an injection and then doing what a move
    // does.
    //
    // Deliberately not "keep the first occurrence and drop the rest". Knack put the key
    // in every row, so the first one carries no intent — it is an artefact of iteration
    // order, not a position anybody chose. Keeping it would dress an arbitrary pick up
    // as a decision.
    //
    // Unrecognised shapes are passed through untouched rather than normalised. A layout
    // this function does not fully understand is one it must not rewrite.
    const stripped = storedGroups.map((row) => {
        const rowRecord = asPlainRecord(row);
        if (!rowRecord || !Array.isArray(rowRecord.columns)) return row;
        return {
            ...rowRecord,
            columns: rowRecord.columns.map((column) => {
                const columnRecord = asPlainRecord(column);
                if (!columnRecord || !Array.isArray(columnRecord.keys)) {
                    return column;
                }
                return {
                    ...columnRecord,
                    keys: columnRecord.keys.filter((key) => key !== viewKey),
                };
            }),
        };
    });

    // A row left with no keys is kept, not dropped. Every row Knack injected into
    // already held something, so a row can only empty out if it was already empty
    // before the copy — and dropping it would then delete a row someone arranged, to
    // fix a problem they did not cause. Knack tolerates empty rows regardless: a move
    // measured earlier in this session left one behind and the page rendered fine.

    // If an occurrence survived the strip, it is inside a shape the walk above did not
    // handle. Appending now would leave the view rendering twice, which is the very bug
    // this repair exists to remove, so decline and let the caller report it by hand.
    if (collectLayoutViewKeys(stripped).includes(viewKey)) return null;

    // One full-width row at the end — byte-identical to what ensureMovedViewIsRendered
    // appends, which was itself matched against the builder performing a move. A copied
    // view arriving at the foot of the page is the same answer to the same question.
    return [...stripped, { columns: [{ keys: [viewKey], width: 100 }] }];
}

/**
 * Put a copied view into its new page's layout exactly once.
 *
 * The counterpart to ensureMovedViewIsRendered, and the opposite failure: a move writes
 * no layout at all, so the view renders nowhere; a copy writes too much, so it renders
 * once per row. Both leave the page misrendered, and neither is something the caller
 * asked for.
 *
 * @param ctx Server context, used to re-read metadata and write the corrected sort.
 * @param app The app being changed.
 * @param targetSceneKey The page the view was copied onto.
 * @param insertedViewKeys Every view key the copy created. A copy of a view that owns
 *     child pages creates one view per duplicated page too, so which of them is the
 *     copy itself is settled by reading the target page rather than by position.
 * @returns Fields describing the repair, to be merged into the tool response.
 */
export async function ensureCopiedViewRendersOnce(
    ctx: KnackContext,
    app: AppConfig,
    targetSceneKey: string,
    insertedViewKeys: string[],
): Promise<Record<string, unknown>> {
    if (insertedViewKeys.length === 0) {
        // Nothing to attribute the layout change to. Saying so beats guessing.
        return {
            layoutRepair: 'unknown',
            layoutNote: `The copy reported no new view key, so ${targetSceneKey}'s layout was left alone. Check whether the copied view renders more than once and use knack_update_view_order if it does.`,
        };
    }

    ctx.caches.runtimeMetadata.delete(app.appKey);
    const metadata = await ctx.getRuntimeMetadata(app);
    const scene = metadata
        ? findRawSceneInMetadata(metadata, targetSceneKey)
        : null;
    if (!scene) {
        return {
            layoutRepair: 'unknown',
            layoutNote: `${targetSceneKey} could not be read back after the copy, so how many times the new view appears in its layout is unknown. Check the page and use knack_update_view_order if it renders more than once.`,
        };
    }

    const onTargetPage = new Set(
        (Array.isArray(scene.views) ? scene.views : [])
            .map((view) => asPlainRecord(view)?.key)
            .filter((key): key is string => typeof key === 'string'),
    );
    const viewKey = insertedViewKeys.find((key) => onTargetPage.has(key));
    if (!viewKey) {
        return {
            layoutRepair: 'unknown',
            layoutNote: `None of the views the copy created (${insertedViewKeys.join(', ')}) was found on ${targetSceneKey} when reading it back, so its layout was left alone.`,
        };
    }

    const storedGroups = Array.isArray(scene.groups) ? scene.groups : [];
    if (storedGroups.length === 0) {
        // No explicit layout, so Knack had nothing to inject into and every view on the
        // page renders once. Writing a layout here would only arm the hazard.
        return { layoutRepair: 'not-needed' };
    }

    const occurrences = collectLayoutViewKeys(storedGroups).filter(
        (key) => key === viewKey,
    ).length;
    if (occurrences <= 1) {
        return { layoutRepair: 'not-needed' };
    }

    const pageGroups = buildRepairedCopyLayout(storedGroups, viewKey);
    if (!pageGroups) {
        return {
            layoutRepair: 'unknown',
            layoutNote: `${viewKey} appears ${occurrences} times in ${targetSceneKey}'s layout, because Knack's copy endpoint adds it to every row. The layout was left as Knack wrote it. Fix it with knack_update_view_order.`,
        };
    }

    const order = Array.isArray(scene.views)
        ? scene.views
              .map((view) => asPlainRecord(view)?.key)
              .filter((key): key is string => typeof key === 'string')
        : [];
    if (!order.includes(viewKey)) {
        return {
            layoutRepair: 'unknown',
            layoutNote: `${viewKey} was not found on ${targetSceneKey} when reading it back, so its layout was left alone.`,
        };
    }

    const written = await ctx.request(
        app,
        `/scenes/${targetSceneKey}/views/sort`,
        {
            // POST, not PUT. Measured 10 September: this endpoint answers PUT with a
            // 400, and knack_update_view_order — the one caller that had ever written
            // a layout for real — had always used POST. Both repairs here were written
            // against fake contexts that accept any method, and the move repair's PUT
            // was never executed live because every move tested was refused first. So
            // the wrong verb sat in two places, unexercised, with tests passing.
            method: 'POST',
            body: JSON.stringify({ order, pageGroups }),
        },
    );

    if (!written.ok) {
        return {
            layoutRepair: 'failed',
            layoutNote: `The copy succeeded, but ${targetSceneKey}'s layout could not be corrected (status ${written.status}). Until it is, ${viewKey} renders ${occurrences} times on the page. Fix it with knack_update_view_order.`,
        };
    }

    return {
        layoutRepair: 'deduplicated',
        layoutNote: `Knack's copy endpoint had put ${viewKey} into all ${occurrences} rows of ${targetSceneKey}'s layout, so it would have rendered ${occurrences} times. The layout was rewritten to show it once. Knack's front end caches app metadata, so a page open in a browser may need a reload.`,
    };
}

/** One row of `summariseAudienceChanges`, named so both callers can speak about it. */
export type AudienceRow = ReturnType<typeof summariseAudienceChanges>[number];

/**
 * The audience rows for a mutation, asked from either side of the guard.
 *
 * Shared by the executed path and the preview so the two can never disagree about who
 * could reach a page — they diverged once already, and the preview was the side that
 * said nothing. The pages arrive as loose arguments rather than as a
 * `ViewMutationDecision` because a preview reaches this through a refusal's untyped
 * `details`, not through a decision.
 */
async function readAudienceChanges(
    ctx: KnackContext,
    app: AppConfig,
    input: {
        action: string;
        sceneKey: string;
        childPageKeys: string[];
        transferredPages: ClassifiedLinkTarget[];
    },
    targetSceneKey: string | undefined,
): Promise<AudienceRow[]> {
    // The pre-mutation tree is the right "before": it says what the audience was, and
    // where the pages are headed.
    const metadata = await ctx.getRuntimeMetadata(app);
    return summariseAudienceChanges(input, {
        scenes: metadata ? parseRuntimeScenes(metadata) : null,
        profileNames: buildProfileNameIndex(metadata),
        targetSceneKey,
    });
}

/**
 * What a preview reports about audience.
 *
 * The executed path omits `audienceChanges` when no row exists and `audienceWarning`
 * when every row reads `same`, on the grounds that a response should not carry keys
 * about nothing. Whether a preview should follow that rule is a separate question:
 * a preview is read to decide, and silence there does not distinguish "checked, the
 * audience is unchanged" from "did not check".
 *
 * @param audienceChanges Rows from `readAudienceChanges`; empty when nothing re-parents.
 * @returns Keys to merge into the PREVIEW_ONLY response.
 */
export function describePreviewAudience(
    audienceChanges: AudienceRow[],
): Record<string, unknown> {
    const moved = audienceChanges.filter((row) => row.change === 'changed');
    const unreadable = audienceChanges.filter(
        (row) => row.change === 'unknown',
    );

    const warning = [
        moved.length > 0
            ? `${moved.length} page(s) would be reachable by a different set of users: a page's login and permitted roles follow its parent, and this changes their parent.`
            : null,
        // Kept apart from `moved` deliberately. compareAudience answers 'unknown' when
        // either side could not be resolved, on the rule that not having read an
        // audience must never collapse into having read it and found no change. A
        // preview is where that distinction is acted on, so it is stated separately
        // rather than counted in with the pages whose new audience is known.
        unreadable.length > 0
            ? `${unreadable.length} page(s) have a destination this server could not resolve, so their new audience is unknown rather than unchanged — read them in the builder before accepting.`
            : null,
        moved.length > 0 || unreadable.length > 0
            ? 'Nothing has been sent. Narrowing matters as much as widening: losing a page is silent, with no error and no empty state.'
            : null,
    ]
        .filter((line): line is string => line !== null)
        .join(' ');

    return {
        // Always present, empty array included — the one place this departs from the
        // executed path, which omits the key when no page re-parents. A write receipt
        // carrying a key about nothing is noise; a preview is read to decide, and an
        // absent key cannot separate "checked, nothing re-parents" from "never looked".
        // That ambiguity is what hid this gap in the first place.
        audienceChanges,
        ...(warning ? { audienceWarning: warning } : {}),
    };
}

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
    /** See makeViewMutationDeps — a move names its target scene here. */
    audience?: { targetSceneKey?: string },
): Promise<Record<string, unknown>> {
    const deps = await makeViewMutationDeps(
        ctx,
        app,
        prefetchedMetadata,
        audience,
    );
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
        const refused = {
            ok: false,
            ...identity,
            error: outcome.code,
            message: outcome.message,
            ...(outcome.details ?? {}),
        };
        // A preview is the one refusal that is an answer rather than a stop, so it is
        // the one that has to carry the audience reading as well. Leaving it out made
        // the safe way to look the only way that could not see a silent re-parent —
        // exactly the case summariseAudienceChanges exists for.
        if (outcome.code !== 'PREVIEW_ONLY') return refused;

        const details = outcome.details ?? {};
        const audienceChanges = await readAudienceChanges(
            ctx,
            app,
            {
                action: request.action,
                sceneKey: request.sceneKey,
                childPageKeys: Array.isArray(details.acknowledgedPages)
                    ? (details.acknowledgedPages as string[])
                    : [],
                transferredPages: Array.isArray(details.transferredPages)
                    ? (details.transferredPages as ClassifiedLinkTarget[])
                    : [],
            },
            audience?.targetSceneKey,
        );

        // The same check a write gets, on the body a write would have sent. It ran
        // only on `outcome.result.ok` before, so the one route that looks before it
        // leaps was the one route that could not see a link pointing at no page —
        // the same shape of gap as the audience reading above.
        const previewDangling = await findDanglingLinks(
            deps,
            (details.effectiveBody ?? null) as Record<string, unknown> | null,
        );

        return {
            ...refused,
            ...describePreviewAudience(audienceChanges),
            ...(previewDangling.length > 0
                ? {
                      danglingLinks: previewDangling,
                      danglingLinkWarning: `${previewDangling.length} link(s) in the body this would send point at a page this server cannot find (${previewDangling
                          .map((link) => link.ref)
                          .join(
                              ', ',
                          )}). Knack would store each one and it would open nothing. Check the slug against knack_list_scenes, or create the page with a page specification instead.`,
                  }
                : {}),
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

    const audienceChanges = await readAudienceChanges(
        ctx,
        app,
        {
            action: request.action,
            sceneKey: request.sceneKey,
            childPageKeys: outcome.acknowledgedPages,
            transferredPages: outcome.transferredPages,
        },
        audience?.targetSceneKey,
    );

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
        ...(audienceChanges.length > 0
            ? {
                  audienceChanges,
                  ...(audienceChanges.some((row) => row.change !== 'same')
                      ? {
                            audienceWarning: `${audienceChanges.filter((row) => row.change !== 'same').length} page(s) changing parent here may become reachable by a different set of users — a page's login and permitted roles follow its parent. Verify in the builder. Narrowing matters as much as widening: losing a page is silent, with no error and no empty state.`,
                        }
                      : {}),
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
