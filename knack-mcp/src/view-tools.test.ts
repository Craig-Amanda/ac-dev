import assert from 'node:assert/strict';
import { beforeEach, describe, it } from 'node:test';

import {
    runGuardedViewMutation,
    type FetchViewResult,
    type SceneNode,
    type ViewMutationDeps,
    type ViewMutationRequest,
    type PageDeletionConfirmation,
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
    /**
     * What the prompt was actually given, as data.
     *
     * `prompts` renders only childPages, so asserting against it that an external page
     * is absent from the doomed list cannot fail — the string never contained external
     * pages either way. Both halves have to be checked separately.
     */
    promptInputs: Array<{ doomed: string[]; external: (string | null)[] }>;
    snapshots: string[];
    deps: ViewMutationDeps;
    perform: (context: { snapshotPath?: string }) => Promise<{ sent: true }>;
};

const SCENES: SceneNode[] = [
    { sceneKey: 'scene_1', sceneName: 'Contacts' },
    {
        sceneKey: 'scene_101',
        sceneName: 'Edit contact',
        parentRef: 'scene_1',
    },
    {
        sceneKey: 'scene_102',
        sceneName: 'Contact history',
        parentRef: 'scene_101',
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

/** An ordinary non-menu view type. */
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
        /** Omitted, the scene tree reads cleanly. */
        sceneTree?:
            { ok: true; scenes: SceneNode[] } | { ok: false; reason: string };
    } = {},
): Spy {
    const spy: Spy = {
        mutations: [],
        reads: [],
        prompts: [],
        promptInputs: [],
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
        listScenes: async () =>
            options.sceneTree ?? { ok: true as const, scenes: SCENES },
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
        confirmPageDeletion: async ({
            childPages,
            externalPages,
            unresolvedLinkCount,
        }) => {
            spy.prompts.push(
                `${childPages.map((page) => page.sceneKey).join(',')}|unresolved=${unresolvedLinkCount}`,
            );
            spy.promptInputs.push({
                doomed: childPages.map((page) => page.sceneKey),
                external: externalPages.map((page) => page.sceneKey),
            });
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

describe('real Knack shapes reach the guard', () => {
    /** Slugs in `scene` and `parent`, keys in `key` — as the live API returns them. */
    const SLUG_SCENES: SceneNode[] = [
        { sceneKey: 'scene_11', sceneSlug: 'view-rolls-login' },
        {
            sceneKey: 'scene_500',
            sceneName: 'DF Details',
            sceneSlug: 'view-df-details',
            parentRef: 'view-rolls-login',
        },
        {
            sceneKey: 'scene_522',
            sceneName: 'Edit DF',
            sceneSlug: 'edit-df',
            parentRef: 'view-df-details',
        },
        {
            sceneKey: 'scene_405',
            sceneName: 'DF History',
            sceneSlug: 'df-history',
            parentRef: 'edit-df',
        },
    ];

    /** A details view as Knack actually stores one: `scene_link`, slug target. */
    const DETAILS_SCENE_LINK_VIEW = {
        key: 'view_109',
        type: 'details',
        columns: [
            {
                groups: [
                    {
                        columns: [
                            [{ type: 'scene_link', scene: 'view-df-details' }],
                        ],
                    },
                ],
            },
        ],
    };

    /** A form whose Link/URL field input is also `type: "link"` — but no page. */
    const FORM_WITH_URL_INPUT = {
        key: 'view_77',
        type: 'form',
        groups: [
            {
                columns: [
                    { inputs: [{ type: 'link', field: { key: 'field_812' } }] },
                ],
            },
        ],
    };

    it('prompts for a details view whose link is typed scene_link', async () => {
        // Before the fix this returned no link columns at all, so the layout
        // replacement went straight to the PUT with no prompt and no snapshot.
        const spy = makeSpy({
            fetchView: { ok: true, status: 200, body: DETAILS_SCENE_LINK_VIEW },
            sceneTree: { ok: true, scenes: SLUG_SCENES },
            confirm: { supported: true, accepted: false, outcome: 'decline' },
        });
        const result = await run(spy, {
            action: 'update_view',
            sceneKey: 'scene_11',
            viewKey: 'view_109',
            updates: JSON.stringify({ groups: [] }),
        });

        assert.equal(
            result.ok === false && result.code,
            'HUMAN_CONFIRMATION_DECLINED',
        );
        assert.deepEqual(spy.mutations, []);
    });

    it('names every descendant when refs and parents are slugs', async () => {
        const spy = makeSpy({
            fetchView: { ok: true, status: 200, body: DETAILS_SCENE_LINK_VIEW },
            sceneTree: { ok: true, scenes: SLUG_SCENES },
            confirm: { supported: true, accepted: false, outcome: 'decline' },
        });
        await run(spy, {
            action: 'delete_view',
            sceneKey: 'scene_11',
            viewKey: 'view_109',
        });

        // scene_405 must be in the prompt: it dies with edit-df, which dies with
        // view-df-details. A key-based walk dropped it.
        assert.deepEqual(spy.prompts, [
            'scene_500,scene_522,scene_405|unresolved=0',
        ]);
    });

    it('lets a form with a URL-field input be edited normally', async () => {
        const spy = makeSpy({
            fetchView: { ok: true, status: 200, body: FORM_WITH_URL_INPUT },
        });
        const result = await run(spy, {
            action: 'update_view',
            sceneKey: 'scene_11',
            viewKey: 'view_77',
            updates: JSON.stringify({ groups: [{ label: 'Details' }] }),
        });

        assert.equal(result.ok, true);
        assert.deepEqual(spy.prompts, []);
        assert.deepEqual(spy.mutations, ['WRITE']);
    });

    it('refuses an empty payload rather than sending an unexamined PUT', async () => {
        const spy = makeSpy({
            fetchView: { ok: true, status: 200, body: DETAILS_SCENE_LINK_VIEW },
        });
        const result = await run(spy, {
            action: 'update_view',
            sceneKey: 'scene_11',
            viewKey: 'view_109',
            updates: '{}',
        });

        assert.equal(
            result.ok === false && result.code,
            'EMPTY_UPDATE_PAYLOAD',
        );
        assert.deepEqual(spy.mutations, []);
    });

    it('counts a link target that matches no scene as unresolved risk', async () => {
        const spy = makeSpy({
            fetchView: {
                ok: true,
                status: 200,
                body: {
                    key: 'view_9',
                    type: 'details',
                    columns: [{ type: 'scene_link', scene: 'ghost-page' }],
                },
            },
            sceneTree: { ok: true, scenes: SLUG_SCENES },
            confirm: { supported: true, accepted: false, outcome: 'decline' },
        });
        await run(spy, {
            action: 'delete_view',
            sceneKey: 'scene_11',
            viewKey: 'view_9',
        });

        // No page can be named, but the prompt must still say something is at risk.
        assert.deepEqual(spy.prompts, ['|unresolved=1']);
    });
});

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
            confirmDestructive: true,
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

    it('blocks menus by type and nothing else', async () => {
        // `menu` is the only view type this server refuses on type alone. There was
        // once a configurable deniedViewTypes list alongside it; this asserts that
        // removing it did not leave some other type quietly blocked.
        const mapSpy = makeSpy({
            fetchView: { ok: true, status: 200, body: MAP_VIEW },
        });
        const result = await run(mapSpy, {
            action: 'update_view',
            sceneKey: 'scene_1',
            viewKey: 'view_12',
            updates: JSON.stringify({ title: 'Sites' }),
        });

        assert.equal(result.ok, true);
        assert.deepEqual(mapSpy.mutations, ['WRITE']);
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

    it('allows a create carrying links, since it replaces nothing', async () => {
        // Every payload knack_get_view_payload_template emits carries `links: []`,
        // so refusing here blocked view creation outright.
        const spy = makeSpy();
        const result = await run(spy, {
            action: 'create_view',
            sceneKey: 'scene_1',
            updates: JSON.stringify({
                name: 'Grid',
                type: 'table',
                links: [],
                groups: [],
                columns: [],
            }),
        });

        assert.equal(result.ok, true);
        assert.deepEqual(spy.mutations, ['WRITE']);
    });

    it('still blocks an empty links array on an update', async () => {
        // `links: []` on an existing view is the most destructive payload of all:
        // it clears every link and takes their child pages with them.
        const spy = makeSpy();
        const result = await run(spy, {
            action: 'update_view',
            sceneKey: 'scene_1',
            viewKey: 'view_9',
            updates: JSON.stringify({ links: [] }),
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
        });

        assert.deepEqual(spy.mutations, []);
        assert.deepEqual(spy.reads, ['GET scene_1/view_7']);
    });

    it('does not block a harmless update from an existing client', async () => {
        const spy = makeSpy();
        const result = await run(spy, {
            action: 'update_view',
            sceneKey: 'scene_1',
            viewKey: 'view_9',
            updates: JSON.stringify({ title: 'Renamed' }),
            confirmDestructive: true,
        });

        assert.equal(result.ok, true);
        assert.deepEqual(spy.mutations, ['WRITE']);
    });
});

describe('snapshots gate the mutation', () => {
    it('does not make creates, copies, or sorting depend on snapshot storage', async () => {
        for (const request of [
            {
                action: 'create_view' as const,
                sceneKey: 'scene_1',
                updates: JSON.stringify({ type: 'table', links: [] }),
            },
            {
                action: 'copy_view' as const,
                sceneKey: 'scene_1',
                viewKey: 'view_9',
            },
            {
                action: 'update_view_order' as const,
                sceneKey: 'scene_1',
                updates: JSON.stringify({ order: [], pageGroups: [] }),
            },
        ]) {
            const spy = makeSpy({ snapshotError: 'ENOSPC' });
            const result = await run(spy, request);

            assert.equal(result.ok, true);
            assert.deepEqual(spy.reads, []);
            assert.deepEqual(spy.snapshots, []);
            assert.deepEqual(spy.mutations, ['WRITE']);
        }
    });

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

    it('refuses when the client cannot prompt, with no second route', async () => {
        // There was once a typed-acknowledgement fallback here, satisfiable by the
        // caller from the refusal message alone. No human, no deletion.
        const spy = withLinkView();
        const result = await run(spy, { ...risky });

        assert.equal(
            result.ok === false && result.code,
            'HUMAN_CONFIRMATION_UNAVAILABLE',
        );
        assert.deepEqual(spy.mutations, []);
    });

    it('offers no override the caller can satisfy on its own', async () => {
        const spy = withLinkView();
        const result = await run(spy, { ...risky });
        const message = result.ok === false ? result.message : '';

        assert.match(message, /no override/i);
        // Nothing in the refusal may hand back a phrase that unlocks a retry.
        assert.doesNotMatch(message, /pass .* exactly as/i);
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

    it('does not prompt when there is nothing to cascade', async () => {
        const spy = makeSpy({ confirm: { supported: true, accepted: true } });
        const result = await run(spy, {
            action: 'update_view',
            sceneKey: 'scene_1',
            viewKey: 'view_9',
            updates: JSON.stringify({ title: 'Welcome' }),
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
        });

        assert.deepEqual(spy.prompts, ['|unresolved=1']);
    });

    it('refuses when pages cannot be named and no human can be asked', async () => {
        const spy = makeSpy({
            fetchView: { ok: true, status: 200, body: UNRESOLVED_LINK_VIEW },
        });
        const result = await run(spy, {
            action: 'delete_view',
            sceneKey: 'scene_1',
            viewKey: 'view_31',
        });

        assert.equal(
            result.ok === false && result.code,
            'HUMAN_CONFIRMATION_UNAVAILABLE',
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
        });

        assert.equal(result.ok === false && result.code, 'UNKNOWN_VIEW_TYPE');
        assert.deepEqual(spy.mutations, []);
    });
});

describe('a structural write is what triggers the cascade check, not a `columns` key', () => {
    // Regression. The trigger used to be "does this payload replace a `columns` array?",
    // which a details view's groups[].columns[] layout walks straight around: clearing
    // `groups` destroys the link columns inside it, and the word `columns` never appears
    // in the payload. Discovery of nested link columns was already recursive — it was the
    // decision to *look* that was flat, so these all reached the live PUT unconfirmed.
    const cases: Array<[string, Record<string, unknown>]> = [
        ['a wholesale groups replacement', { groups: [] }],
        ['a groups write with no columns key', { groups: [{ label: 'x' }] }],
        ['columns sent as an object', { columns: { '0': { type: 'link' } } }],
        ['an unfamiliar layout key', { rows: [] }],
        [
            'a scalar edit mixed with a structural one',
            { title: 'x', groups: [] },
        ],
    ];

    for (const [label, payload] of cases) {
        it(`refuses ${label} and sends nothing`, async () => {
            const spy = makeSpy({
                fetchView: { ok: true, status: 200, body: NESTED_LINK_VIEW },
            });
            const result = await run(spy, {
                action: 'update_view',
                sceneKey: 'scene_1',
                viewKey: 'view_7',
                updates: JSON.stringify(payload),
            });

            assert.equal(result.ok, false);
            assert.equal(
                result.ok === false && result.code,
                'HUMAN_CONFIRMATION_UNAVAILABLE',
            );
            assert.deepEqual(spy.mutations, []);
            assert.deepEqual(spy.snapshots, []);
        });
    }

    it('puts a groups replacement to the human, naming the whole page tree', async () => {
        const spy = makeSpy({
            fetchView: { ok: true, status: 200, body: NESTED_LINK_VIEW },
            confirm: { supported: true, accepted: false, outcome: 'decline' },
        });
        const result = await run(spy, {
            action: 'update_view',
            sceneKey: 'scene_1',
            viewKey: 'view_7',
            updates: JSON.stringify({ groups: [] }),
        });

        assert.equal(
            result.ok === false && result.code,
            'HUMAN_CONFIRMATION_DECLINED',
        );
        // scene_102 hangs off scene_101, so it dies with it and must be named too.
        assert.deepEqual(spy.prompts, ['scene_101,scene_102|unresolved=0']);
        assert.deepEqual(spy.mutations, []);
    });

    it('still lets a scalar-only edit through without a prompt', async () => {
        // The point of the allowlist is that widening the trigger must not turn every
        // ordinary edit into a confirmation. A title change on this same view is safe.
        const spy = makeSpy({
            fetchView: { ok: true, status: 200, body: NESTED_LINK_VIEW },
        });
        const result = await run(spy, {
            action: 'update_view',
            sceneKey: 'scene_1',
            viewKey: 'view_7',
            updates: JSON.stringify({ title: 'Contact detail' }),
        });

        assert.equal(result.ok, true);
        assert.deepEqual(spy.mutations, ['WRITE']);
        assert.deepEqual(spy.prompts, []);
    });
});

describe('incomplete information is refused, not assumed benign', () => {
    /** Nests past MAX_WALK_DEPTH so every walker would run out of depth. */
    const deeplyNested = (depth: number) => {
        let node: Record<string, unknown> = { links: [{ scene: 'scene_9' }] };
        for (let i = 0; i < depth; i++) node = { wrap: [node] };
        return node;
    };

    it('refuses a payload nested deeper than the checks can walk', async () => {
        // Past the cap payloadTouchesLinks returns false, so this links payload
        // would previously have read as clean.
        const spy = makeSpy();
        const result = await run(spy, {
            action: 'update_view',
            sceneKey: 'scene_1',
            viewKey: 'view_9',
            updates: JSON.stringify(deeplyNested(30)),
        });

        assert.equal(result.ok === false && result.code, 'STRUCTURE_TOO_DEEP');
        assert.deepEqual(spy.mutations, []);
    });

    it('still catches a links payload at ordinary nesting', async () => {
        const spy = makeSpy();
        const result = await run(spy, {
            action: 'update_view',
            sceneKey: 'scene_1',
            viewKey: 'view_9',
            updates: JSON.stringify(deeplyNested(2)),
        });

        assert.equal(
            result.ok === false && result.code,
            'BLOCKED_LINKS_PAYLOAD',
        );
        assert.deepEqual(spy.mutations, []);
    });

    it('refuses a fetched view too deep to search for link columns', async () => {
        const spy = makeSpy({
            fetchView: {
                ok: true,
                status: 200,
                body: { type: 'details', ...deeplyNested(30) },
            },
        });
        const result = await run(spy, {
            action: 'delete_view',
            sceneKey: 'scene_1',
            viewKey: 'view_7',
        });

        assert.equal(result.ok === false && result.code, 'STRUCTURE_TOO_DEEP');
        assert.deepEqual(spy.mutations, []);
    });

    it('refuses when the page tree cannot be read', async () => {
        // An unreadable tree used to arrive as [], indistinguishable from a view
        // whose links have no descendants — so the prompt under-reported the damage.
        const spy = makeSpy({
            fetchView: { ok: true, status: 200, body: NESTED_LINK_VIEW },
            sceneTree: { ok: false, reason: 'runtime metadata unavailable' },
        });
        const result = await run(spy, {
            action: 'delete_view',
            sceneKey: 'scene_1',
            viewKey: 'view_7',
        });

        assert.equal(
            result.ok === false && result.code,
            'SCENE_TREE_UNAVAILABLE',
        );
        assert.deepEqual(spy.mutations, []);
    });

    it('never prompts a human off an unreadable page tree', async () => {
        const spy = makeSpy({
            fetchView: { ok: true, status: 200, body: NESTED_LINK_VIEW },
            sceneTree: { ok: false, reason: 'runtime metadata unavailable' },
            confirm: { supported: true, accepted: true },
        });
        await run(spy, {
            action: 'delete_view',
            sceneKey: 'scene_1',
            viewKey: 'view_7',
        });

        assert.deepEqual(spy.prompts, []);
        assert.deepEqual(spy.mutations, []);
    });
});

describe('external links are not treated as unknown risk', () => {
    /** A view whose only link points outside the app: no scene, and none expected. */
    const URL_LINK_VIEW = {
        key: 'view_41',
        type: 'details',
        links: [{ name: 'Docs', type: 'url', url: 'https://example.com' }],
    };

    it('deletes a view holding only a url link without prompting', async () => {
        // A url link has no child scene by definition. Counting that as "could not
        // resolve" made such views permanently risky, with nothing the user could do.
        const spy = makeSpy({
            fetchView: { ok: true, status: 200, body: URL_LINK_VIEW },
            confirm: { supported: true, accepted: true },
        });
        const result = await run(spy, {
            action: 'delete_view',
            sceneKey: 'scene_1',
            viewKey: 'view_41',
        });

        assert.equal(result.ok, true);
        assert.deepEqual(spy.prompts, []);
        assert.deepEqual(spy.mutations, ['WRITE']);
    });

    it('still flags a scene link whose target cannot be read', async () => {
        const spy = makeSpy({
            fetchView: {
                ok: true,
                status: 200,
                body: {
                    key: 'view_42',
                    type: 'details',
                    links: [{ name: 'Edit', type: 'scene', scene: { id: 7 } }],
                },
            },
        });
        const result = await run(spy, {
            action: 'delete_view',
            sceneKey: 'scene_1',
            viewKey: 'view_42',
        });

        assert.equal(
            result.ok === false && result.code,
            'HUMAN_CONFIRMATION_UNAVAILABLE',
        );
        assert.deepEqual(spy.mutations, []);
    });
});

/**
 * Knack creates a child page *because* a view links to it, and deletes it when that
 * link goes. A page already living elsewhere in the tree does not owe its existence to
 * the link, so removing the link removes navigation and nothing else — measured on a
 * real app, where both the external page and its connection survived a view update.
 *
 * Counting those as doomed made the prompt overstate badly: one table's six link
 * columns predicted 25 of a 60-page app. A prompt that exaggerates is one people learn
 * to click past, which costs more safety than it buys.
 *
 * The downgrade only applies where there is positive evidence: a parent that resolves
 * to a different, real scene. Every evidence-free shape stays at risk.
 */
describe('owned child pages versus links to pages elsewhere', () => {
    /** scene_9 hangs off scene_400, not off scene_1, so it survives. */
    const SCENES_WITH_EXTERNAL: SceneNode[] = [
        { sceneKey: 'scene_1', sceneName: 'Contacts', sceneSlug: 'contacts' },
        { sceneKey: 'scene_400', sceneName: 'Reports', sceneSlug: 'reports' },
        {
            sceneKey: 'scene_9',
            sceneName: 'Monthly report',
            sceneSlug: 'monthly-report',
            parentRef: 'reports',
        },
    ];

    const LINK_TO_EXTERNAL = {
        key: 'view_7',
        type: 'table',
        name: 'Contacts',
        columns: [{ type: 'link', header: 'Report', scene: 'monthly-report' }],
    };

    const structural = {
        action: 'update_view',
        sceneKey: 'scene_1',
        viewKey: 'view_7',
        updates: JSON.stringify({ columns: [] }),
    } as const;

    it('does not prompt when every linked page lives elsewhere', async () => {
        const spy = makeSpy({
            fetchView: { ok: true, status: 200, body: LINK_TO_EXTERNAL },
            sceneTree: { ok: true, scenes: SCENES_WITH_EXTERNAL },
            confirm: { supported: true, accepted: false, outcome: 'decline' },
        });

        const result = await run(spy, structural);

        assert.equal(
            spy.prompts.length,
            0,
            'nothing dies, so nothing to confirm',
        );
        assert.equal(result.ok, true);
        assert.equal(spy.mutations.length, 1);
    });

    it('still writes a snapshot on that path', async () => {
        // The downgrade trusts Knack's metadata about parentage. A restore point costs
        // nothing set against that assumption being wrong.
        const spy = makeSpy({
            fetchView: { ok: true, status: 200, body: LINK_TO_EXTERNAL },
            sceneTree: { ok: true, scenes: SCENES_WITH_EXTERNAL },
        });

        await run(spy, structural);
        assert.equal(spy.snapshots.length, 1);
    });

    it('still prompts when a linked page hangs off the page being changed', async () => {
        const owned: SceneNode[] = [
            ...SCENES_WITH_EXTERNAL,
            {
                sceneKey: 'scene_2',
                sceneName: 'Contact detail',
                sceneSlug: 'contact-detail',
                parentRef: 'contacts',
            },
        ];
        const spy = makeSpy({
            fetchView: {
                ok: true,
                status: 200,
                body: {
                    key: 'view_7',
                    type: 'table',
                    columns: [
                        { type: 'link', scene: 'monthly-report' },
                        { type: 'link', scene: 'contact-detail' },
                    ],
                },
            },
            sceneTree: { ok: true, scenes: owned },
            confirm: { supported: true, accepted: false, outcome: 'decline' },
        });

        const result = await run(spy, structural);

        assert.equal(
            result.ok === false && result.code,
            'HUMAN_CONFIRMATION_DECLINED',
        );
        assert.equal(spy.mutations.length, 0);
        // Both halves, as data. Asserting against the rendered string could not fail:
        // it only ever contained childPages, so an external page was absent from it
        // whatever the classification did.
        const [input] = spy.promptInputs;
        assert.deepEqual(input.doomed, ['scene_2']);
        assert.deepEqual(input.external, ['scene_9']);
    });

    it('treats a page with no parent as at risk, not as external', async () => {
        // No parent is indistinguishable from a parent missing from the metadata, and
        // guessing "safe" is how a human loses more than they agreed to.
        const spy = makeSpy({
            fetchView: { ok: true, status: 200, body: LINK_TO_EXTERNAL },
            sceneTree: {
                ok: true,
                scenes: [
                    { sceneKey: 'scene_1', sceneSlug: 'contacts' },
                    { sceneKey: 'scene_9', sceneSlug: 'monthly-report' },
                ],
            },
            confirm: { supported: true, accepted: false, outcome: 'decline' },
        });

        const result = await run(spy, structural);

        assert.equal(
            result.ok === false && result.code,
            'HUMAN_CONFIRMATION_DECLINED',
        );
        assert.equal(spy.mutations.length, 0);
    });

    it('treats an unresolvable parent as at risk', async () => {
        const spy = makeSpy({
            fetchView: { ok: true, status: 200, body: LINK_TO_EXTERNAL },
            sceneTree: {
                ok: true,
                scenes: [
                    { sceneKey: 'scene_1', sceneSlug: 'contacts' },
                    {
                        sceneKey: 'scene_9',
                        sceneSlug: 'monthly-report',
                        parentRef: 'a-page-that-does-not-exist',
                    },
                ],
            },
            confirm: { supported: true, accepted: false, outcome: 'decline' },
        });

        const result = await run(spy, structural);
        assert.equal(
            result.ok === false && result.code,
            'HUMAN_CONFIRMATION_DECLINED',
        );
        assert.equal(spy.mutations.length, 0);
    });

    it('treats an unresolvable link reference as at risk', async () => {
        const spy = makeSpy({
            fetchView: {
                ok: true,
                status: 200,
                body: {
                    key: 'view_7',
                    type: 'table',
                    columns: [{ type: 'link', scene: 'no-such-page' }],
                },
            },
            sceneTree: { ok: true, scenes: SCENES_WITH_EXTERNAL },
            confirm: { supported: true, accepted: false, outcome: 'decline' },
        });

        const result = await run(spy, structural);
        assert.equal(
            result.ok === false && result.code,
            'HUMAN_CONFIRMATION_DECLINED',
        );
        assert.equal(spy.mutations.length, 0);
    });

    it('does not let an external link bypass the unconditional menu block', async () => {
        // The downgrade is about what a cascade destroys. It must not reach a rule that
        // never depended on cascade analysis in the first place.
        const spy = makeSpy({
            fetchView: {
                ok: true,
                status: 200,
                body: {
                    key: 'view_7',
                    type: 'menu',
                    links: [{ type: 'scene', scene: 'monthly-report' }],
                },
            },
            sceneTree: { ok: true, scenes: SCENES_WITH_EXTERNAL },
        });

        const result = await run(spy, structural);
        assert.equal(
            result.ok === false && result.code,
            'BLOCKED_MENU_VIEW_UPDATE',
        );
        assert.equal(spy.mutations.length, 0);
    });
});

/**
 * A page can be external by parentage and doomed anyway. Page Q, linked directly from
 * this view, whose parent is owned page P: Q's parent is not the page being changed, so
 * it classifies external — but it dies when P does, as P's descendant.
 *
 * Unfiltered it appeared in the doomed list *and* under "NOT being deleted". The prompt
 * is the one artefact that must never contradict itself, and of the two claims the
 * destructive one is the one with consequences.
 */
describe('a page that is external by parentage but doomed as a descendant', () => {
    const SCENES: SceneNode[] = [
        { sceneKey: 'scene_1', sceneName: 'Contacts', sceneSlug: 'contacts' },
        {
            sceneKey: 'scene_P',
            sceneName: 'Owned detail',
            sceneSlug: 'owned-detail',
            parentRef: 'contacts',
        },
        {
            sceneKey: 'scene_Q',
            sceneName: 'Grandchild',
            sceneSlug: 'grandchild',
            parentRef: 'owned-detail',
        },
    ];

    /** Links at both P and Q directly, so Q is a seed as well as a descendant. */
    const VIEW = {
        key: 'view_7',
        type: 'table',
        columns: [
            { type: 'link', scene: 'owned-detail' },
            { type: 'link', scene: 'grandchild' },
        ],
    };

    const structural = {
        action: 'update_view',
        sceneKey: 'scene_1',
        viewKey: 'view_7',
        updates: JSON.stringify({ columns: [] }),
    } as const;

    it('names it as doomed and not as surviving', async () => {
        const spy = makeSpy({
            fetchView: { ok: true, status: 200, body: VIEW },
            sceneTree: { ok: true, scenes: SCENES },
            confirm: { supported: true, accepted: false, outcome: 'decline' },
        });

        const result = await run(spy, structural);

        assert.equal(
            result.ok === false && result.code,
            'HUMAN_CONFIRMATION_DECLINED',
        );
        const [input] = spy.promptInputs;
        assert.ok(input.doomed.includes('scene_Q'), 'Q dies with its parent');
        assert.ok(
            !input.external.includes('scene_Q'),
            'Q must not also be listed as surviving',
        );
    });

    it('leaves no page in both halves of the prompt', async () => {
        const spy = makeSpy({
            fetchView: { ok: true, status: 200, body: VIEW },
            sceneTree: { ok: true, scenes: SCENES },
            confirm: { supported: true, accepted: false, outcome: 'decline' },
        });

        await run(spy, structural);

        const [input] = spy.promptInputs;
        const overlap = input.external.filter(
            (key) => key !== null && input.doomed.includes(key),
        );
        assert.deepEqual(overlap, []);
    });
});
