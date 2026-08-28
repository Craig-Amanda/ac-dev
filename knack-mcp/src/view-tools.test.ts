import assert from 'node:assert/strict';
import { beforeEach, describe, it } from 'node:test';

import {
    buildAcknowledgementSentence,
    runGuardedViewMutation,
    type FetchViewResult,
    type SceneNode,
    type ViewMutationDeps,
    type ViewMutationRequest,
    type PageDeletionConfirmation,
    type ViewUpdatePolicy,
} from './view-safety.js';

/**
 * These tests drive the same `runGuardedViewMutation` the six view tools in server.ts
 * route through, with a spy standing in for the Knack transport. The assertion that
 * matters throughout is `spy.mutations.length === 0`: a refusal must return before the
 * request is built, not merely report a warning alongside it.
 */

type Spy = {
    /** Every request the guard allowed through to the transport. */
    mutations: string[];
    /** Preflight reads, which are expected and harmless. */
    reads: string[];
    /** Cascade-delete prompts put to the human. */
    prompts: string[];
    snapshots: string[];
    deps: ViewMutationDeps;
    perform: (context: { snapshotPath: string }) => Promise<{ sent: true }>;
};

const SCENES: SceneNode[] = [
    { sceneKey: 'scene_1', sceneName: 'Contacts' },
    {
        sceneKey: 'scene_101',
        sceneName: 'Edit contact',
        parentSceneKey: 'scene_1',
    },
    {
        sceneKey: 'scene_102',
        sceneName: 'Contact history',
        parentSceneKey: 'scene_101',
    },
];

const MENU_VIEW = {
    key: 'view_5',
    type: 'menu',
    name: 'Main navigation',
    links: [
        { name: 'Contacts', type: 'scene', scene: 'scene_1' },
        { name: 'Reports', type: 'scene', scene: 'scene_2' },
    ],
};

/** A view type that is not on the default allowlist. */
const MAP_VIEW = {
    key: 'view_12',
    type: 'map',
    name: 'Site locations',
};

const RICH_TEXT_VIEW = {
    key: 'view_9',
    type: 'rich_text',
    name: 'Intro copy',
    content: '<p>Welcome</p>',
};

/** A details view whose only link column sits inside groups[] — invisible to a flat read. */
const NESTED_LINK_VIEW = {
    key: 'view_7',
    type: 'details',
    name: 'Contact detail',
    columns: [{ type: 'field', field: { key: 'field_1' } }],
    groups: [
        {
            columns: [
                {
                    columns: [
                        { type: 'link', header: 'Edit', scene: 'scene_101' },
                    ],
                },
            ],
        },
    ],
};

function makeSpy(
    options: {
        fetchView?: FetchViewResult;
        snapshotError?: string;
        /** Omitted, the client cannot prompt a human at all. */
        confirm?: PageDeletionConfirmation;
    } = {},
): Spy {
    const spy: Spy = {
        mutations: [],
        reads: [],
        prompts: [],
        snapshots: [],
        deps: {} as ViewMutationDeps,
        perform: async () => {
            throw new Error('perform not initialised');
        },
    };

    spy.deps = {
        fetchView: async (sceneKey, viewKey) => {
            spy.reads.push(`GET ${sceneKey}/${viewKey}`);
            return (
                options.fetchView ?? {
                    ok: true,
                    status: 200,
                    body: { view: RICH_TEXT_VIEW },
                }
            );
        },
        listScenes: async () => SCENES,
        writeSnapshot: async ({ action, viewKey }) => {
            if (options.snapshotError) {
                return { ok: false, error: options.snapshotError };
            }
            const path = `/snapshots/${action}-${viewKey ?? 'scene'}.json`;
            spy.snapshots.push(path);
            return { ok: true, path };
        },
        builderUrlForScene: (sceneKey) =>
            `https://builder.knack.com/acme/app/pages/${sceneKey}`,
        confirmPageDeletion: async ({ childPages, unresolvedLinkCount }) => {
            spy.prompts.push(
                `${childPages.map((page) => page.sceneKey).join(',')}|unresolved=${unresolvedLinkCount}`,
            );
            return options.confirm ?? { supported: false };
        },
    };

    spy.perform = async () => {
        spy.mutations.push('WRITE');
        return { sent: true };
    };

    return spy;
}

async function run(spy: Spy, request: ViewMutationRequest) {
    return runGuardedViewMutation(spy.deps, request, spy.perform);
}

/** The shipped default: menus denied, nothing else. */
const DEFAULT_POLICY: ViewUpdatePolicy = {
    deniedViewTypes: ['menu'],
    deniedKeys: [],
    cascadeConfirmationFallback: 'refuse',
};

/** An app that has opted into the typed-acknowledgement route. */
const FALLBACK_POLICY: ViewUpdatePolicy = {
    ...DEFAULT_POLICY,
    cascadeConfirmationFallback: 'acknowledgement',
};

describe('menu views are never updatable', () => {
    let spy: Spy;
    beforeEach(() => {
        spy = makeSpy({
            fetchView: { ok: true, status: 200, body: MENU_VIEW },
        });
    });

    it('blocks an update to a menu view and sends nothing', async () => {
        const result = await run(spy, {
            action: 'update_view',
            sceneKey: 'scene_1',
            viewKey: 'view_5',
            updates: JSON.stringify({ name: 'Renamed menu' }),
        });

        assert.equal(result.ok, false);
        assert.equal(
            result.ok === false && result.code,
            'BLOCKED_MENU_VIEW_UPDATE',
        );
        assert.deepEqual(spy.mutations, []);
        assert.deepEqual(spy.snapshots, []);
    });

    it('blocks a menu update that only touches an allowlisted key', async () => {
        // The view's type disqualifies it — the payload's contents are irrelevant.
        const result = await run(spy, {
            action: 'update_view',
            sceneKey: 'scene_1',
            viewKey: 'view_5',
            updates: JSON.stringify({ title: 'Nav' }),
            policy: {
                deniedViewTypes: [],
                deniedKeys: [],
                cascadeConfirmationFallback: 'refuse',
            },
        });

        assert.equal(
            result.ok === false && result.code,
            'BLOCKED_MENU_VIEW_UPDATE',
        );
        assert.deepEqual(spy.mutations, []);
    });

    it('offers no override parameter that unblocks a menu', async () => {
        const result = await run(spy, {
            action: 'update_view',
            sceneKey: 'scene_1',
            viewKey: 'view_5',
            updates: JSON.stringify({ name: 'Renamed menu' }),
            acknowledgeDeletionOfPages: buildAcknowledgementSentence([
                'scene_1',
                'scene_2',
            ]),
        });

        assert.equal(
            result.ok === false && result.code,
            'BLOCKED_MENU_VIEW_UPDATE',
        );
        assert.deepEqual(spy.mutations, []);
    });

    it('points the caller at the builder', async () => {
        const result = await run(spy, {
            action: 'update_view',
            sceneKey: 'scene_1',
            viewKey: 'view_5',
            updates: JSON.stringify({ name: 'Renamed menu' }),
        });

        assert.match(
            result.ok === false ? result.message : '',
            /builder\.knack\.com\/acme\/app\/pages\/scene_1/,
        );
    });

    it('blocks moving a menu to another scene', async () => {
        const result = await run(spy, {
            action: 'move_view',
            sceneKey: 'scene_1',
            viewKey: 'view_5',
        });

        assert.equal(
            result.ok === false && result.code,
            'BLOCKED_MENU_VIEW_MOVE',
        );
        assert.deepEqual(spy.mutations, []);
    });
});

describe('links payloads are refused for every view type', () => {
    it('blocks a links payload on a non-menu view', async () => {
        const spy = makeSpy();
        const result = await run(spy, {
            action: 'update_view',
            sceneKey: 'scene_1',
            viewKey: 'view_9',
            updates: JSON.stringify({
                links: [{ name: 'Home', scene: 'scene_1' }],
            }),
        });

        assert.equal(
            result.ok === false && result.code,
            'BLOCKED_LINKS_PAYLOAD',
        );
        assert.deepEqual(spy.mutations, []);
    });

    it('blocks links nested under attributes', async () => {
        const spy = makeSpy();
        const result = await run(spy, {
            action: 'update_view',
            sceneKey: 'scene_1',
            viewKey: 'view_9',
            updates: JSON.stringify({ attributes: { links: [] } }),
        });

        assert.equal(
            result.ok === false && result.code,
            'BLOCKED_LINKS_PAYLOAD',
        );
        assert.deepEqual(spy.mutations, []);
    });

    it('refuses before reading the view, so nothing is even fetched', async () => {
        const spy = makeSpy();
        await run(spy, {
            action: 'update_view',
            sceneKey: 'scene_1',
            viewKey: 'view_9',
            updates: JSON.stringify({ links: [] }),
        });

        assert.deepEqual(spy.reads, []);
        assert.deepEqual(spy.mutations, []);
    });

    it('blocks creating a menu view through the REST endpoint', async () => {
        const spy = makeSpy();
        const result = await run(spy, {
            action: 'create_view',
            sceneKey: 'scene_1',
            updates: JSON.stringify({ type: 'menu', links: [] }),
        });

        assert.equal(
            result.ok === false && result.code,
            'BLOCKED_LINKS_PAYLOAD',
        );
        assert.deepEqual(spy.mutations, []);
    });
});

describe('preflight fails closed', () => {
    it('refuses when the view cannot be read', async () => {
        const spy = makeSpy({ fetchView: { ok: false, status: 503 } });
        const result = await run(spy, {
            action: 'update_view',
            sceneKey: 'scene_1',
            viewKey: 'view_9',
            updates: JSON.stringify({ title: 'New' }),
        });

        assert.equal(
            result.ok === false && result.code,
            'COULD_NOT_VERIFY_VIEW',
        );
        assert.deepEqual(spy.mutations, []);
    });

    it('refuses a delete when the view cannot be read', async () => {
        const spy = makeSpy({ fetchView: { ok: false, status: 404 } });
        const result = await run(spy, {
            action: 'delete_view',
            sceneKey: 'scene_1',
            viewKey: 'view_9',
        });

        assert.equal(
            result.ok === false && result.code,
            'COULD_NOT_VERIFY_VIEW',
        );
        assert.deepEqual(spy.mutations, []);
    });

    it('refuses an unparseable payload rather than forwarding it', async () => {
        const spy = makeSpy();
        const result = await run(spy, {
            action: 'update_view',
            sceneKey: 'scene_1',
            viewKey: 'view_9',
            updates: '{not json',
        });

        assert.equal(
            result.ok === false && result.code,
            'INVALID_UPDATES_JSON',
        );
        assert.deepEqual(spy.mutations, []);
    });
});

describe('the legacy override no longer works', () => {
    it('refuses confirmDestructive instead of honouring it', async () => {
        const spy = makeSpy({
            fetchView: { ok: true, status: 200, body: NESTED_LINK_VIEW },
        });
        const result = await run(spy, {
            action: 'update_view',
            sceneKey: 'scene_1',
            viewKey: 'view_7',
            updates: JSON.stringify({ columns: [] }),
            confirmDestructive: true,
            policy: DEFAULT_POLICY,
        });

        assert.equal(
            result.ok === false && result.code,
            'CONFIRMATION_UPGRADE_REQUIRED',
        );
        assert.deepEqual(spy.mutations, []);
    });

    it('no longer lets the override skip the preflight read', async () => {
        // The old guard skipped the GET entirely when confirmDestructive was set.
        const spy = makeSpy({
            fetchView: { ok: true, status: 200, body: NESTED_LINK_VIEW },
        });
        await run(spy, {
            action: 'update_view',
            sceneKey: 'scene_1',
            viewKey: 'view_7',
            updates: JSON.stringify({ columns: [] }),
            confirmDestructive: true,
            policy: DEFAULT_POLICY,
        });

        assert.deepEqual(spy.mutations, []);
    });
});

describe('cascade acknowledgement (opted-in fallback only)', () => {
    let spy: Spy;
    beforeEach(() => {
        spy = makeSpy({
            fetchView: { ok: true, status: 200, body: NESTED_LINK_VIEW },
        });
    });

    it('detects a link column nested in groups[] and blocks the columns replacement', async () => {
        const result = await run(spy, {
            action: 'update_view',
            sceneKey: 'scene_1',
            viewKey: 'view_7',
            updates: JSON.stringify({ columns: [] }),
            policy: FALLBACK_POLICY,
        });

        assert.equal(
            result.ok === false && result.code,
            'BLOCKED_LINK_COLUMN_LOSS',
        );
        assert.deepEqual(spy.mutations, []);
    });

    it('names every descendant page, not just the linked one', async () => {
        const result = await run(spy, {
            action: 'update_view',
            sceneKey: 'scene_1',
            viewKey: 'view_7',
            updates: JSON.stringify({ columns: [] }),
            policy: FALLBACK_POLICY,
        });

        const required =
            result.ok === false
                ? (result.details?.requiredAcknowledgement as string)
                : '';
        assert.match(required, /scene_101/);
        assert.match(required, /scene_102/);
    });

    it('rejects an acknowledgement that misses a descendant', async () => {
        const result = await run(spy, {
            action: 'update_view',
            sceneKey: 'scene_1',
            viewKey: 'view_7',
            updates: JSON.stringify({ columns: [] }),
            acknowledgeDeletionOfPages: buildAcknowledgementSentence([
                'scene_101',
            ]),
            policy: FALLBACK_POLICY,
        });

        assert.equal(
            result.ok === false && result.code,
            'ACKNOWLEDGEMENT_MISMATCH',
        );
        assert.deepEqual(spy.mutations, []);
    });

    it('rejects a bare true-ish string that names no pages', async () => {
        const result = await run(spy, {
            action: 'update_view',
            sceneKey: 'scene_1',
            viewKey: 'view_7',
            updates: JSON.stringify({ columns: [] }),
            acknowledgeDeletionOfPages: 'yes, I accept',
            policy: FALLBACK_POLICY,
        });

        assert.equal(
            result.ok === false && result.code,
            'ACKNOWLEDGEMENT_MISMATCH',
        );
        assert.deepEqual(spy.mutations, []);
    });

    it('allows the update once the exact pages are acknowledged', async () => {
        const result = await run(spy, {
            action: 'update_view',
            sceneKey: 'scene_1',
            viewKey: 'view_7',
            updates: JSON.stringify({ columns: [] }),
            acknowledgeDeletionOfPages: buildAcknowledgementSentence([
                'scene_101',
                'scene_102',
            ]),
            policy: FALLBACK_POLICY,
        });

        assert.equal(result.ok, true);
        assert.deepEqual(spy.mutations, ['WRITE']);
    });

    it('requires acknowledgement to delete a view carrying a link column', async () => {
        const result = await run(spy, {
            action: 'delete_view',
            sceneKey: 'scene_1',
            viewKey: 'view_7',
            policy: FALLBACK_POLICY,
        });

        assert.equal(
            result.ok === false && result.code,
            'BLOCKED_LINK_COLUMN_LOSS',
        );
        assert.deepEqual(spy.mutations, []);
    });

    it('needs a human, not an acknowledgement, on the default policy', async () => {
        const result = await run(spy, {
            action: 'delete_view',
            sceneKey: 'scene_1',
            viewKey: 'view_7',
        });

        assert.equal(
            result.ok === false && result.code,
            'HUMAN_CONFIRMATION_UNAVAILABLE',
        );
        assert.deepEqual(spy.mutations, []);
    });

    it('does not demand acknowledgement when no columns are replaced', async () => {
        const result = await run(spy, {
            action: 'update_view',
            sceneKey: 'scene_1',
            viewKey: 'view_7',
            updates: JSON.stringify({ title: 'Renamed' }),
            policy: FALLBACK_POLICY,
        });

        assert.equal(result.ok, true);
        assert.deepEqual(spy.mutations, ['WRITE']);
    });
});

describe('the view-type and key denylist', () => {
    it('admits a view type nothing denies', async () => {
        const spy = makeSpy({
            fetchView: { ok: true, status: 200, body: MAP_VIEW },
        });
        const result = await run(spy, {
            action: 'update_view',
            sceneKey: 'scene_1',
            viewKey: 'view_12',
            updates: JSON.stringify({ title: 'Renamed' }),
        });

        assert.equal(result.ok, true);
        assert.deepEqual(spy.mutations, ['WRITE']);
    });

    it('blocks a view type an app has denied', async () => {
        const spy = makeSpy({
            fetchView: { ok: true, status: 200, body: MAP_VIEW },
        });
        const result = await run(spy, {
            action: 'update_view',
            sceneKey: 'scene_1',
            viewKey: 'view_12',
            updates: JSON.stringify({ title: 'Renamed' }),
            policy: {
                deniedViewTypes: ['map', 'menu'],
                deniedKeys: [],
                cascadeConfirmationFallback: 'refuse',
            },
        });

        assert.equal(result.ok === false && result.code, 'BLOCKED_VIEW_TYPE');
        assert.deepEqual(spy.mutations, []);
    });

    it('blocks a key an app has denied', async () => {
        const spy = makeSpy();
        const result = await run(spy, {
            action: 'update_view',
            sceneKey: 'scene_1',
            viewKey: 'view_9',
            updates: JSON.stringify({ columns: [] }),
            policy: {
                deniedViewTypes: ['menu'],
                deniedKeys: ['columns'],
                cascadeConfirmationFallback: 'refuse',
            },
        });

        assert.equal(result.ok === false && result.code, 'BLOCKED_UPDATE_KEY');
        assert.deepEqual(spy.mutations, []);
    });

    it('names the app.json path to change the policy', async () => {
        const spy = makeSpy();
        const result = await run(spy, {
            action: 'update_view',
            sceneKey: 'scene_1',
            viewKey: 'view_9',
            updates: JSON.stringify({ columns: [] }),
            policy: {
                deniedViewTypes: ['menu'],
                deniedKeys: ['columns'],
                cascadeConfirmationFallback: 'refuse',
            },
        });

        assert.equal(
            result.ok === false && result.details?.appJsonPath,
            'viewUpdatePolicy.deniedKeys',
        );
    });

    it('refuses a view that declares no type at all', async () => {
        // Readable but unidentifiable: it could be anything, including a menu.
        const spy = makeSpy({
            fetchView: { ok: true, status: 200, body: { key: 'view_99' } },
        });
        const result = await run(spy, {
            action: 'update_view',
            sceneKey: 'scene_1',
            viewKey: 'view_99',
            updates: JSON.stringify({ title: 'Renamed' }),
        });

        assert.equal(result.ok === false && result.code, 'UNKNOWN_VIEW_TYPE');
        assert.deepEqual(spy.mutations, []);
    });

    it('admits a details view on the default policy', async () => {
        const spy = makeSpy({
            fetchView: { ok: true, status: 200, body: NESTED_LINK_VIEW },
        });
        const result = await run(spy, {
            action: 'update_view',
            sceneKey: 'scene_1',
            viewKey: 'view_7',
            updates: JSON.stringify({ title: 'Renamed' }),
        });

        assert.equal(result.ok, true);
        assert.deepEqual(spy.mutations, ['WRITE']);
    });

    it('allows the default case: a rich_text title change', async () => {
        const spy = makeSpy();
        const result = await run(spy, {
            action: 'update_view',
            sceneKey: 'scene_1',
            viewKey: 'view_9',
            updates: JSON.stringify({ title: 'Welcome' }),
        });

        assert.equal(result.ok, true);
        assert.deepEqual(spy.mutations, ['WRITE']);
    });

    it('does not apply the key denylist to a delete', async () => {
        const spy = makeSpy();
        const result = await run(spy, {
            action: 'delete_view',
            sceneKey: 'scene_1',
            viewKey: 'view_9',
        });

        assert.equal(result.ok, true);
        assert.deepEqual(spy.mutations, ['WRITE']);
    });
});

describe('snapshots gate the mutation', () => {
    it('sends nothing when the snapshot cannot be written', async () => {
        const spy = makeSpy({
            snapshotError: 'ENOSPC: no space left on device',
        });
        const result = await run(spy, {
            action: 'update_view',
            sceneKey: 'scene_1',
            viewKey: 'view_9',
            updates: JSON.stringify({ title: 'Welcome' }),
        });

        assert.equal(result.ok === false && result.code, 'SNAPSHOT_FAILED');
        assert.deepEqual(spy.mutations, []);
    });

    it('writes the snapshot before the mutation, never after', async () => {
        const order: string[] = [];
        const spy = makeSpy();
        spy.deps.writeSnapshot = async () => {
            order.push('snapshot');
            return { ok: true, path: '/snapshots/x.json' };
        };
        spy.perform = async () => {
            order.push('mutation');
            return { sent: true };
        };

        await run(spy, {
            action: 'update_view',
            sceneKey: 'scene_1',
            viewKey: 'view_9',
            updates: JSON.stringify({ title: 'Welcome' }),
        });

        assert.deepEqual(order, ['snapshot', 'mutation']);
    });

    it('reports the snapshot path on success so recovery is findable', async () => {
        const spy = makeSpy();
        const result = await run(spy, {
            action: 'update_view',
            sceneKey: 'scene_1',
            viewKey: 'view_9',
            updates: JSON.stringify({ title: 'Welcome' }),
        });

        assert.equal(
            result.ok === true && result.snapshotPath,
            '/snapshots/update_view-view_9.json',
        );
    });
});

describe('human confirmation for cascade deletes', () => {
    const risky = {
        action: 'update_view',
        sceneKey: 'scene_1',
        viewKey: 'view_7',
        updates: JSON.stringify({ columns: [] }),
        policy: DEFAULT_POLICY,
    } as const;

    const withLinkView = (confirm?: PageDeletionConfirmation) =>
        makeSpy({
            fetchView: { ok: true, status: 200, body: NESTED_LINK_VIEW },
            confirm,
        });

    it('asks the human, naming every page at stake', async () => {
        const spy = withLinkView({ supported: true, accepted: true });
        await run(spy, { ...risky });

        assert.deepEqual(spy.prompts, ['scene_101,scene_102|unresolved=0']);
    });

    it('proceeds once a human accepts', async () => {
        const spy = withLinkView({ supported: true, accepted: true });
        const result = await run(spy, { ...risky });

        assert.equal(result.ok, true);
        assert.deepEqual(spy.mutations, ['WRITE']);
    });

    it('sends nothing when a human declines', async () => {
        const spy = withLinkView({
            supported: true,
            accepted: false,
            outcome: 'decline',
        });
        const result = await run(spy, { ...risky });

        assert.equal(
            result.ok === false && result.code,
            'HUMAN_CONFIRMATION_DECLINED',
        );
        assert.deepEqual(spy.mutations, []);
    });

    it('treats a cancelled prompt as a refusal', async () => {
        const spy = withLinkView({
            supported: true,
            accepted: false,
            outcome: 'cancel',
        });
        const result = await run(spy, { ...risky });

        assert.equal(
            result.ok === false && result.code,
            'HUMAN_CONFIRMATION_DECLINED',
        );
        assert.deepEqual(spy.mutations, []);
    });

    it('cannot be bypassed by a caller-supplied acknowledgement', async () => {
        // The whole point: an agent that knows the page keys still cannot answer
        // for the user when the client can actually ask them.
        const spy = withLinkView({
            supported: true,
            accepted: false,
            outcome: 'decline',
        });
        const result = await run(spy, {
            ...risky,
            acknowledgeDeletionOfPages: buildAcknowledgementSentence([
                'scene_101',
                'scene_102',
            ]),
        });

        assert.equal(
            result.ok === false && result.code,
            'HUMAN_CONFIRMATION_DECLINED',
        );
        assert.deepEqual(spy.mutations, []);
    });

    it('refuses when the client cannot prompt and the app has not opted in', async () => {
        const spy = withLinkView();
        const result = await run(spy, { ...risky });

        assert.equal(
            result.ok === false && result.code,
            'HUMAN_CONFIRMATION_UNAVAILABLE',
        );
        assert.deepEqual(spy.mutations, []);
    });

    it('names the app.json path that enables the fallback', async () => {
        const spy = withLinkView();
        const result = await run(spy, { ...risky });

        assert.equal(
            result.ok === false && result.details?.appJsonPath,
            'viewUpdatePolicy.cascadeConfirmationFallback',
        );
    });

    it('treats a failed elicitation as unavailable, never as consent', async () => {
        const spy = withLinkView({
            supported: false,
            reason: 'transport closed',
        });
        const result = await run(spy, { ...risky });

        assert.equal(
            result.ok === false && result.code,
            'HUMAN_CONFIRMATION_UNAVAILABLE',
        );
        assert.deepEqual(spy.mutations, []);
    });

    it('falls back to the acknowledgement only where an app opted in', async () => {
        const spy = withLinkView();
        const result = await run(spy, {
            ...risky,
            policy: FALLBACK_POLICY,
        });

        assert.equal(
            result.ok === false && result.code,
            'BLOCKED_LINK_COLUMN_LOSS',
        );
        assert.deepEqual(spy.mutations, []);
    });

    it('accepts the exact acknowledgement on an opted-in app', async () => {
        const spy = withLinkView();
        const result = await run(spy, {
            ...risky,
            policy: FALLBACK_POLICY,
            acknowledgeDeletionOfPages: buildAcknowledgementSentence([
                'scene_101',
                'scene_102',
            ]),
        });

        assert.equal(result.ok, true);
        assert.deepEqual(spy.mutations, ['WRITE']);
    });

    it('does not prompt when there is nothing to cascade', async () => {
        const spy = makeSpy({ confirm: { supported: true, accepted: true } });
        const result = await run(spy, {
            action: 'update_view',
            sceneKey: 'scene_1',
            viewKey: 'view_9',
            updates: JSON.stringify({ title: 'Welcome' }),
            policy: DEFAULT_POLICY,
        });

        assert.equal(result.ok, true);
        assert.deepEqual(spy.prompts, []);
    });
});

describe('degenerate view shapes fail closed', () => {
    /** A link column whose target scene cannot be read — shape unknown, not absent. */
    const UNRESOLVED_LINK_VIEW = {
        key: 'view_31',
        type: 'details',
        columns: [{ type: 'link', header: 'Edit', scene: { id: 99 } }],
    };

    /** Readable, but declares no type at all. */
    const UNTYPED_VIEW = { key: 'view_32', name: 'Mystery' };

    it('still requires confirmation when a link target cannot be resolved', async () => {
        // The old guard keyed off childSceneKeys being non-empty, so an unreadable
        // scene reference skipped confirmation entirely and deleted pages silently.
        const spy = makeSpy({
            fetchView: { ok: true, status: 200, body: UNRESOLVED_LINK_VIEW },
            confirm: { supported: true, accepted: false, outcome: 'decline' },
        });
        const result = await run(spy, {
            action: 'delete_view',
            sceneKey: 'scene_1',
            viewKey: 'view_31',
            policy: DEFAULT_POLICY,
        });

        assert.equal(
            result.ok === false && result.code,
            'HUMAN_CONFIRMATION_DECLINED',
        );
        assert.deepEqual(spy.mutations, []);
    });

    it('tells the human that unlisted pages may also be destroyed', async () => {
        const spy = makeSpy({
            fetchView: { ok: true, status: 200, body: UNRESOLVED_LINK_VIEW },
            confirm: { supported: true, accepted: true },
        });
        await run(spy, {
            action: 'delete_view',
            sceneKey: 'scene_1',
            viewKey: 'view_31',
            policy: DEFAULT_POLICY,
        });

        assert.deepEqual(spy.prompts, ['|unresolved=1']);
    });

    it('refuses the acknowledgement fallback when pages cannot be named', async () => {
        const spy = makeSpy({
            fetchView: { ok: true, status: 200, body: UNRESOLVED_LINK_VIEW },
        });
        const result = await run(spy, {
            action: 'delete_view',
            sceneKey: 'scene_1',
            viewKey: 'view_31',
            policy: FALLBACK_POLICY,
        });

        assert.equal(
            result.ok === false && result.code,
            'UNRESOLVED_LINK_TARGET',
        );
        assert.deepEqual(spy.mutations, []);
    });

    it('refuses to move an untyped view, which could be a menu', async () => {
        // The untyped refusal used to sit inside the update_view branch, so a move
        // slipped past both it and the menu check.
        const spy = makeSpy({
            fetchView: { ok: true, status: 200, body: UNTYPED_VIEW },
        });
        const result = await run(spy, {
            action: 'move_view',
            sceneKey: 'scene_1',
            viewKey: 'view_32',
            policy: DEFAULT_POLICY,
        });

        assert.equal(result.ok === false && result.code, 'UNKNOWN_VIEW_TYPE');
        assert.deepEqual(spy.mutations, []);
    });

    it('refuses to delete an untyped view', async () => {
        const spy = makeSpy({
            fetchView: { ok: true, status: 200, body: UNTYPED_VIEW },
        });
        const result = await run(spy, {
            action: 'delete_view',
            sceneKey: 'scene_1',
            viewKey: 'view_32',
            policy: DEFAULT_POLICY,
        });

        assert.equal(result.ok === false && result.code, 'UNKNOWN_VIEW_TYPE');
        assert.deepEqual(spy.mutations, []);
    });

    it('refuses to update an untyped view', async () => {
        const spy = makeSpy({
            fetchView: { ok: true, status: 200, body: UNTYPED_VIEW },
        });
        const result = await run(spy, {
            action: 'update_view',
            sceneKey: 'scene_1',
            viewKey: 'view_32',
            updates: JSON.stringify({ title: 'X' }),
            policy: DEFAULT_POLICY,
        });

        assert.equal(result.ok === false && result.code, 'UNKNOWN_VIEW_TYPE');
        assert.deepEqual(spy.mutations, []);
    });
});

describe('the key denylist holds at any depth', () => {
    const DENY_COLUMNS: ViewUpdatePolicy = {
        deniedViewTypes: ['menu'],
        deniedKeys: ['columns'],
        cascadeConfirmationFallback: 'refuse',
    };

    it('blocks a denied key nested inside groups', async () => {
        const spy = makeSpy();
        const result = await run(spy, {
            action: 'update_view',
            sceneKey: 'scene_1',
            viewKey: 'view_9',
            updates: JSON.stringify({ groups: [{ columns: [] }] }),
            policy: DENY_COLUMNS,
        });

        assert.equal(result.ok === false && result.code, 'BLOCKED_UPDATE_KEY');
        assert.deepEqual(spy.mutations, []);
    });

    it('blocks a denied key nested under attributes', async () => {
        const spy = makeSpy();
        const result = await run(spy, {
            action: 'update_view',
            sceneKey: 'scene_1',
            viewKey: 'view_9',
            updates: JSON.stringify({ attributes: { columns: [] } }),
            policy: DENY_COLUMNS,
        });

        assert.equal(result.ok === false && result.code, 'BLOCKED_UPDATE_KEY');
        assert.deepEqual(spy.mutations, []);
    });

    it('still allows a payload that never mentions the denied key', async () => {
        const spy = makeSpy();
        const result = await run(spy, {
            action: 'update_view',
            sceneKey: 'scene_1',
            viewKey: 'view_9',
            updates: JSON.stringify({ title: 'Welcome' }),
            policy: DENY_COLUMNS,
        });

        assert.equal(result.ok, true);
        assert.deepEqual(spy.mutations, ['WRITE']);
    });
});
