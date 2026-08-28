import assert from 'node:assert/strict';
import { beforeEach, describe, it } from 'node:test';

import {
    buildAcknowledgementSentence,
    runGuardedViewMutation,
    type FetchViewResult,
    type SceneNode,
    type ViewMutationDeps,
    type ViewMutationRequest,
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
    options: { fetchView?: FetchViewResult; snapshotError?: string } = {},
): Spy {
    const spy: Spy = {
        mutations: [],
        reads: [],
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

const WIDE_POLICY: ViewUpdatePolicy = {
    allowedViewTypes: ['details', 'table', 'rich_text'],
    allowedKeys: ['name', 'title', 'columns', 'groups'],
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
            policy: { allowedViewTypes: ['menu'], allowedKeys: ['title'] },
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
            policy: WIDE_POLICY,
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
            policy: WIDE_POLICY,
        });

        assert.deepEqual(spy.mutations, []);
    });
});

describe('cascade acknowledgement', () => {
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
            policy: WIDE_POLICY,
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
            policy: WIDE_POLICY,
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
            policy: WIDE_POLICY,
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
            policy: WIDE_POLICY,
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
            policy: WIDE_POLICY,
        });

        assert.equal(result.ok, true);
        assert.deepEqual(spy.mutations, ['WRITE']);
    });

    it('requires acknowledgement to delete a view carrying a link column', async () => {
        const result = await run(spy, {
            action: 'delete_view',
            sceneKey: 'scene_1',
            viewKey: 'view_7',
        });

        assert.equal(
            result.ok === false && result.code,
            'BLOCKED_LINK_COLUMN_LOSS',
        );
        assert.deepEqual(spy.mutations, []);
    });

    it('does not demand acknowledgement when no columns are replaced', async () => {
        const result = await run(spy, {
            action: 'update_view',
            sceneKey: 'scene_1',
            viewKey: 'view_7',
            updates: JSON.stringify({ title: 'Renamed' }),
            policy: WIDE_POLICY,
        });

        assert.equal(result.ok, true);
        assert.deepEqual(spy.mutations, ['WRITE']);
    });
});

describe('the proven-safe allowlist', () => {
    it('blocks a view type outside the allowlist', async () => {
        const spy = makeSpy({
            fetchView: { ok: true, status: 200, body: NESTED_LINK_VIEW },
        });
        const result = await run(spy, {
            action: 'update_view',
            sceneKey: 'scene_1',
            viewKey: 'view_7',
            updates: JSON.stringify({ title: 'Renamed' }),
        });

        assert.equal(
            result.ok === false && result.code,
            'VIEW_TYPE_NOT_PROVEN_SAFE',
        );
        assert.deepEqual(spy.mutations, []);
    });

    it('blocks a key outside the allowlist on an allowed view type', async () => {
        const spy = makeSpy();
        const result = await run(spy, {
            action: 'update_view',
            sceneKey: 'scene_1',
            viewKey: 'view_9',
            updates: JSON.stringify({ rows_per_page: 50 }),
        });

        assert.equal(result.ok === false && result.code, 'KEY_NOT_PROVEN_SAFE');
        assert.deepEqual(spy.mutations, []);
    });

    it('names the app.json path to widen the list', async () => {
        const spy = makeSpy();
        const result = await run(spy, {
            action: 'update_view',
            sceneKey: 'scene_1',
            viewKey: 'view_9',
            updates: JSON.stringify({ rows_per_page: 50 }),
        });

        assert.equal(
            result.ok === false && result.details?.appJsonPath,
            'viewUpdatePolicy.allowedKeys',
        );
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

    it('does not apply the key allowlist to a delete', async () => {
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
