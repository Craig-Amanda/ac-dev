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
    promptInputs: Array<{
        doomed: string[];
        external: (string | null)[];
        transferred: (string | null)[];
    }>;
    snapshots: string[];
    /** The body the guard handed to the transport, so a test can assert on it. */
    sent: Array<Record<string, unknown> | null>;
    deps: ViewMutationDeps;
    perform: (context: {
        snapshotPath?: string;
        outgoingBody: Record<string, unknown> | null;
    }) => Promise<{ sent: true }>;
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

/** A non-menu view that genuinely carries navigation links. */
const VIEW_WITH_NAV_LINKS = {
    key: 'view_9',
    type: 'rich_text',
    name: 'Intro copy',
    links: [{ name: 'Contacts', type: 'scene', scene: 'scene_1' }],
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
        sent: [],
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
            transferredPages,
            unresolvedLinkCount,
        }) => {
            spy.prompts.push(
                `${childPages.map((page) => page.sceneKey).join(',')}|unresolved=${unresolvedLinkCount}`,
            );
            spy.promptInputs.push({
                doomed: childPages.map((page) => page.sceneKey),
                external: externalPages.map((page) => page.sceneKey),
                transferred: transferredPages.map((page) => page.sceneKey),
            });
            return options.confirm ?? { supported: false };
        },
    };

    spy.perform = async ({ outgoingBody }) => {
        spy.mutations.push('WRITE');
        spy.sent.push(outgoingBody ?? null);
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
        // This view nests its link inside `columns`, so `columns` is the key that
        // removes it — a `groups` write would leave the link in the merged body and
        // is correctly no longer treated as destructive.
        const spy = makeSpy({
            fetchView: { ok: true, status: 200, body: DETAILS_SCENE_LINK_VIEW },
            sceneTree: { ok: true, scenes: SLUG_SCENES },
            confirm: { supported: true, accepted: false, outcome: 'decline' },
        });
        const result = await run(spy, {
            action: 'update_view',
            sceneKey: 'scene_11',
            viewKey: 'view_109',
            updates: JSON.stringify({ columns: [] }),
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

describe('a menu is promptable, not impossible', () => {
    /**
     * Menus used to be refused on their type alone, with no override. Two further
     * rules propped that up: any payload carrying a links array was refused, and so
     * was any view whose type could not be read, since it might be a menu. All three
     * are gone. A menu now answers the same question as every other view — which pages
     * lose their last link — and goes to a human with the answer.
     *
     * It asks for exactly what a table would, too. The narrowing that spares a re-sent
     * link column applies to a re-sent menu link: measured on a live seven-link menu,
     * where omitting one entry deleted that page and its two descendants and the six
     * re-sent links kept theirs — three of them owned and singly referenced, so their
     * survival was not a second referrer doing the work.
     */
    let spy: Spy;
    beforeEach(() => {
        spy = makeSpy({
            fetchView: { ok: true, status: 200, body: MENU_VIEW },
        });
    });

    it('lets a scalar edit through, because the merged body keeps every link', async () => {
        // Refused outright before menus were unblocked; then prompted, while the
        // links container was still unmeasured. Now it is a rename, and it behaves
        // like a rename — the body the guard sends carries all of the menu's links.
        const result = await run(spy, {
            action: 'update_view',
            sceneKey: 'scene_1',
            viewKey: 'view_5',
            updates: JSON.stringify({ name: 'Renamed menu' }),
        });

        assert.equal(result.ok, true);
        assert.deepEqual(spy.prompts, []);
        assert.deepEqual(spy.sent[0]?.links, MENU_VIEW.links);
    });

    it('proceeds once a human accepts, and keeps the menu links in the body', async () => {
        // The reason this could not be allowed before the body builder was fixed:
        // a complete definition that omits `links` sends a menu with no navigation
        // at all, which is worse than anything a caller could have asked for.
        const accepting = makeSpy({
            fetchView: { ok: true, status: 200, body: MENU_VIEW },
            confirm: { supported: true, accepted: true, outcome: 'accept' },
        });

        const result = await run(accepting, {
            action: 'update_view',
            sceneKey: 'scene_1',
            viewKey: 'view_5',
            updates: JSON.stringify({ name: 'Renamed menu' }),
        });

        assert.equal(result.ok, true);
        assert.deepEqual(accepting.sent[0]?.links, MENU_VIEW.links);
        assert.equal(accepting.sent[0]?.name, 'Renamed menu');
    });

    it('asks when the payload drops one of the menu links', async () => {
        // The half that still stops: MENU_VIEW links to scene_1 and scene_2, and this
        // body re-sends only the first.
        const result = await run(spy, {
            action: 'update_view',
            sceneKey: 'scene_1',
            viewKey: 'view_5',
            updates: JSON.stringify({
                links: [{ name: 'Contacts', type: 'scene', scene: 'scene_1' }],
            }),
        });

        assert.equal(
            result.ok === false && result.code,
            'HUMAN_CONFIRMATION_UNAVAILABLE',
        );
        assert.deepEqual(spy.mutations, []);
    });

    it('still refuses the legacy override rather than treating it as consent', async () => {
        const result = await run(spy, {
            action: 'update_view',
            sceneKey: 'scene_1',
            viewKey: 'view_5',
            updates: JSON.stringify({ name: 'Renamed menu' }),
            confirmDestructive: true,
        });

        assert.equal(
            result.ok === false && result.code,
            'CONFIRMATION_UPGRADE_REQUIRED',
        );
        assert.deepEqual(spy.mutations, []);
    });

    it('puts a menu move to a human, since a move re-sends no links at all', async () => {
        const result = await run(spy, {
            action: 'move_view',
            sceneKey: 'scene_1',
            viewKey: 'view_5',
        });

        assert.equal(
            result.ok === false && result.code,
            'HUMAN_CONFIRMATION_UNAVAILABLE',
        );
        assert.deepEqual(spy.mutations, []);
    });

    it('leaves a view with no links alone, whatever its type', async () => {
        // The rule keys on what the view actually carries, not on what it is called.
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

    it('no longer needs a view type to decide anything', async () => {
        // An untyped view used to be refused outright on the grounds that it might be
        // a menu. Nothing reads the type to make this decision now — an untyped view
        // carrying links is judged by its links, like everything else.
        const untyped = makeSpy({
            fetchView: {
                ok: true,
                status: 200,
                body: { key: 'view_32', title: 'Mystery' },
            },
        });
        const result = await run(untyped, {
            action: 'update_view',
            sceneKey: 'scene_1',
            viewKey: 'view_32',
            updates: JSON.stringify({ title: 'X' }),
        });

        assert.equal(result.ok, true);
        assert.deepEqual(untyped.mutations, ['WRITE']);
    });
});

describe('a links array is judged, not banned', () => {
    it('lets a create carry links, since it replaces nothing', async () => {
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

    it('puts an emptied links array to a human — the incident payload', async () => {
        // `links: []` sent to a view carrying navigation is what cleared five pages on
        // 28 August. It is no longer refused outright, but it cannot happen without a
        // person seeing the list first.
        const spy = makeSpy({
            fetchView: { ok: true, status: 200, body: VIEW_WITH_NAV_LINKS },
            confirm: { supported: true, accepted: false, outcome: 'decline' },
        });
        const result = await run(spy, {
            action: 'update_view',
            sceneKey: 'scene_1',
            viewKey: 'view_9',
            updates: JSON.stringify({ links: [] }),
        });

        assert.deepEqual(spy.reads, ['GET scene_1/view_9']);
        assert.equal(
            result.ok === false && result.code,
            'HUMAN_CONFIRMATION_DECLINED',
        );
        assert.deepEqual(spy.mutations, []);
    });

    it('allows adding navigation to a view that had none', async () => {
        // Nothing is being removed, so nothing can cascade. This used to be refused
        // as "still a navigation change", which cost a prompt to protect no page.
        const spy = makeSpy({
            fetchView: { ok: true, status: 200, body: RICH_TEXT_VIEW },
        });
        const result = await run(spy, {
            action: 'update_view',
            sceneKey: 'scene_1',
            viewKey: 'view_9',
            updates: JSON.stringify({
                links: [{ name: 'Home', scene: 'scene_1' }],
            }),
        });

        assert.equal(result.ok, true);
        assert.deepEqual(spy.mutations, ['WRITE']);
    });

    it('allows an empty links array when the view holds none', async () => {
        // Nothing to clear, so nothing to cascade. Refusing here made a byte-for-byte
        // round trip impossible, because a view's own definition carries `links: []`.
        const spy = makeSpy({
            fetchView: { ok: true, status: 200, body: RICH_TEXT_VIEW },
        });
        const result = await run(spy, {
            action: 'update_view',
            sceneKey: 'scene_1',
            viewKey: 'view_9',
            updates: JSON.stringify({ links: [], title: 'Intro' }),
        });

        assert.equal(result.ok, true);
        assert.deepEqual(spy.mutations, ['WRITE']);
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
    // This view's only link column sits inside `groups`, so `groups` is what has to
    // be replaced to remove it. Clearing `columns` leaves the link in the merged body
    // and no longer risks the page — which is the measured behaviour, not a relaxation.
    const risky = {
        action: 'update_view',
        sceneKey: 'scene_1',
        viewKey: 'view_7',
        updates: JSON.stringify({ groups: [] }),
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

    it('lets an untyped view through when it carries no links', async () => {
        // All three actions used to be refused on an unreadable type, because the
        // view might be a menu. Nothing decides on type now, so what matters is
        // whether the view reaches any page — and this one reaches none.
        for (const action of [
            'update_view',
            'move_view',
            'delete_view',
        ] as const) {
            const spy = makeSpy({
                fetchView: { ok: true, status: 200, body: UNTYPED_VIEW },
            });
            const result = await run(spy, {
                action,
                sceneKey: 'scene_1',
                viewKey: 'view_32',
                ...(action === 'update_view'
                    ? { updates: JSON.stringify({ title: 'X' }) }
                    : {}),
            });

            assert.equal(result.ok, true, action);
            assert.deepEqual(spy.mutations, ['WRITE'], action);
        }
    });

    it('still guards an untyped view that does carry links', async () => {
        // The protection the type check was standing in for, done directly. A view
        // whose type cannot be read but whose links can is judged on the links.
        const spy = makeSpy({
            fetchView: {
                ok: true,
                status: 200,
                body: {
                    key: 'view_32',
                    name: 'Mystery',
                    links: [{ type: 'scene', scene: 'scene_101' }],
                },
            },
        });
        const result = await run(spy, {
            action: 'delete_view',
            sceneKey: 'scene_1',
            viewKey: 'view_32',
        });

        assert.equal(
            result.ok === false && result.code,
            'HUMAN_CONFIRMATION_UNAVAILABLE',
        );
        assert.deepEqual(spy.mutations, []);
    });
});

describe('losing the link is what triggers the cascade check, not the shape of the payload', () => {
    // Regression, twice over. The trigger was first "does this payload replace a
    // `columns` array?", which a details view's groups[].columns[] layout walks
    // straight around. Widening it to "is this payload structural?" fixed that and
    // overshot: a structural write that leaves the link columns alone removes nothing,
    // and a live app confirmed it — a complete definition re-sending every link
    // deleted no pages, while the same body one column short deleted exactly that
    // column's page. What decides the risk is which links survive in the merged body.
    //
    // NESTED_LINK_VIEW keeps its only link inside `groups`.
    const removesTheLink: Array<[string, Record<string, unknown>]> = [
        ['a wholesale groups replacement', { groups: [] }],
        ['a groups write with no columns key', { groups: [{ label: 'x' }] }],
        [
            'a scalar edit mixed with a structural one',
            { title: 'x', groups: [] },
        ],
    ];

    for (const [label, payload] of removesTheLink) {
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

    // Structural by any reading, and destructive by none: the merged body still
    // carries `groups`, so the link column is still there when Knack replaces the
    // definition. Refusing these was costing every such edit a confirmation, or a
    // hard refusal on a client that cannot prompt, to protect a page in no danger.
    const leavesTheLink: Array<[string, Record<string, unknown>]> = [
        ['columns sent as an object', { columns: { '0': { type: 'link' } } }],
        ['an unfamiliar layout key', { rows: [] }],
        ['a filter change', { filter_fields: 'view' }],
    ];

    for (const [label, payload] of leavesTheLink) {
        it(`allows ${label}, since the link survives it`, async () => {
            const spy = makeSpy({
                fetchView: { ok: true, status: 200, body: NESTED_LINK_VIEW },
            });
            const result = await run(spy, {
                action: 'update_view',
                sceneKey: 'scene_1',
                viewKey: 'view_7',
                updates: JSON.stringify(payload),
            });

            assert.equal(result.ok, true);
            assert.deepEqual(spy.prompts, []);
            assert.deepEqual(spy.mutations, ['WRITE']);
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

    it('still finds links in a live view at ordinary nesting', async () => {
        // collectLinkTargets has the same depth cap, so it needs the same regression:
        // a links array two levels down must still be discovered. Dropping it is what
        // makes that visible — a payload that clears the wrapper removes the link,
        // and a walk that could not see it would report nothing at stake.
        const spy = makeSpy({
            fetchView: {
                ok: true,
                status: 200,
                body: { type: 'details', ...deeplyNested(2) },
            },
        });
        const result = await run(spy, {
            action: 'update_view',
            sceneKey: 'scene_1',
            viewKey: 'view_9',
            updates: JSON.stringify({ wrap: [] }),
        });

        assert.equal(
            result.ok === false && result.code,
            'HUMAN_CONFIRMATION_UNAVAILABLE',
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

    it('spares an external page reached through a menu link', async () => {
        // Parentage decides what removing a link destroys, and it does not depend on
        // which array the link lived in. This body clears the menu's links, so the
        // route really is severed — and the page at the far end still survives it.
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

        const result = await run(spy, {
            action: 'update_view',
            sceneKey: 'scene_1',
            viewKey: 'view_7',
            updates: JSON.stringify({ links: [] }),
        });
        assert.equal(result.ok, true);
        assert.deepEqual(
            result.ok === true &&
                result.externalPages.map((page) => page.sceneKey),
            ['scene_9'],
        );
        // Nothing was destroyed, so nothing was put to a human — but the severed
        // route is still reported, because "done" is a poor account of a menu that
        // no longer reaches a page.
        assert.deepEqual(spy.prompts, []);
        assert.deepEqual(spy.mutations, ['WRITE']);
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

/**
 * The guard builds its link targets from the current view and never from the payload,
 * so it reported every one of them as severed. Measured on a real app: a same-columns
 * update named two links as removed while both the links and their pages stayed
 * exactly where they were. A field that says "removed" about something still present
 * is worse than no field — it is the account a caller repeats to the user.
 *
 * This narrows what is *reported*, not what is treated as at risk. Narrowing the risk
 * assessment would rest on re-sending a link column preserving its page, which is the
 * founding premise and still unmeasured.
 */
describe('reporting only what the payload actually severs', () => {
    const SCENES: SceneNode[] = [
        { sceneKey: 'scene_1', sceneName: 'Contacts', sceneSlug: 'contacts' },
        { sceneKey: 'scene_400', sceneName: 'Reports', sceneSlug: 'reports' },
        {
            sceneKey: 'scene_9',
            sceneName: 'Monthly report',
            sceneSlug: 'monthly-report',
            parentRef: 'reports',
        },
        {
            sceneKey: 'scene_10',
            sceneName: 'Quarterly report',
            sceneSlug: 'quarterly-report',
            parentRef: 'reports',
        },
    ];

    const TWO_EXTERNAL_LINKS = {
        key: 'view_7',
        type: 'table',
        columns: [
            { type: 'link', scene: 'monthly-report' },
            { type: 'link', scene: 'quarterly-report' },
        ],
    };

    const spyFor = (updates: string) => ({
        spy: makeSpy({
            fetchView: { ok: true, status: 200, body: TWO_EXTERNAL_LINKS },
            sceneTree: { ok: true, scenes: SCENES },
        }),
        request: {
            action: 'update_view' as const,
            sceneKey: 'scene_1',
            viewKey: 'view_7',
            updates,
        },
    });

    it('reports nothing severed when the payload re-sends both links', async () => {
        // The case measured on the real app: same columns back, nothing removed, yet
        // both links were named as removed.
        const { spy, request } = spyFor(
            JSON.stringify({ columns: TWO_EXTERNAL_LINKS.columns }),
        );
        const result = await run(spy, request);

        assert.equal(result.ok, true);
        assert.deepEqual(
            result.ok === true
                ? result.externalPages.map((p) => p.sceneKey)
                : null,
            [],
        );
    });

    it('reports only the link the payload drops', async () => {
        const { spy, request } = spyFor(
            JSON.stringify({
                columns: [{ type: 'link', scene: 'monthly-report' }],
            }),
        );
        const result = await run(spy, request);

        assert.deepEqual(
            result.ok === true
                ? result.externalPages.map((p) => p.sceneKey)
                : null,
            ['scene_10'],
        );
    });

    it('reports both when the payload drops the layout entirely', async () => {
        // Knack's PUT replaces rather than patches, so an omitted columns array is a
        // removal, not a no-op.
        const { spy, request } = spyFor(JSON.stringify({ columns: [] }));
        const result = await run(spy, request);

        assert.deepEqual(
            result.ok === true
                ? result.externalPages.map((p) => p.sceneKey).sort()
                : null,
            ['scene_10', 'scene_9'],
        );
    });

    it('matches a link re-sent by scene key rather than slug', async () => {
        const { spy, request } = spyFor(
            JSON.stringify({
                columns: [
                    { type: 'link', scene: 'MONTHLY-REPORT' },
                    { type: 'link', scene: 'quarterly-report' },
                ],
            }),
        );
        const result = await run(spy, request);

        assert.deepEqual(
            result.ok === true
                ? result.externalPages.map((p) => p.sceneKey)
                : null,
            [],
            'reference matching must not be case-sensitive',
        );
    });

    it('spares an owned page whose link the payload re-sends', async () => {
        // This assertion used to run the other way: the page stayed at risk even
        // though the payload re-sent its link, because whether re-sending preserved
        // it was the founding premise and it had never been measured. It has now.
        // On a live app a complete definition re-sending every link column deleted
        // nothing, and the same body one column short deleted exactly that column's
        // page — so the cascade follows the removed link, and a re-sent link is not
        // a removed one.
        const owned: SceneNode[] = [
            ...SCENES,
            {
                sceneKey: 'scene_2',
                sceneName: 'Contact detail',
                sceneSlug: 'contact-detail',
                parentRef: 'contacts',
            },
        ];
        const view = {
            key: 'view_7',
            type: 'table',
            columns: [{ type: 'link', scene: 'contact-detail' }],
        };
        const spy = makeSpy({
            fetchView: { ok: true, status: 200, body: view },
            sceneTree: { ok: true, scenes: owned },
            confirm: { supported: true, accepted: false, outcome: 'decline' },
        });

        const result = await run(spy, {
            action: 'update_view',
            sceneKey: 'scene_1',
            viewKey: 'view_7',
            updates: JSON.stringify({ columns: view.columns }),
        });

        assert.equal(result.ok, true);
        assert.deepEqual(spy.prompts, []);
        assert.deepEqual(spy.mutations, ['WRITE']);
    });

    it('still dooms it when the payload drops the link', async () => {
        // The other half of the same rule, and the reason the one above is safe to
        // relax: identical fixture, identical everything, one link column removed.
        const owned: SceneNode[] = [
            ...SCENES,
            {
                sceneKey: 'scene_2',
                sceneName: 'Contact detail',
                sceneSlug: 'contact-detail',
                parentRef: 'contacts',
            },
        ];
        const view = {
            key: 'view_7',
            type: 'table',
            columns: [{ type: 'link', scene: 'contact-detail' }],
        };
        const spy = makeSpy({
            fetchView: { ok: true, status: 200, body: view },
            sceneTree: { ok: true, scenes: owned },
            confirm: { supported: true, accepted: false, outcome: 'decline' },
        });

        const result = await run(spy, {
            action: 'update_view',
            sceneKey: 'scene_1',
            viewKey: 'view_7',
            updates: JSON.stringify({ columns: [] }),
        });

        assert.equal(
            result.ok === false && result.code,
            'HUMAN_CONFIRMATION_DECLINED',
        );
        assert.deepEqual(spy.promptInputs[0].doomed, ['scene_2']);
        assert.deepEqual(spy.mutations, []);
    });
});

/** A scene list with no link graph — what every deps implementation supplied before
 *  the referrer rule existed, and what an unreadable metadata payload still gives. */
function withoutLinkGraph(scenes: SceneNode[]): SceneNode[] {
    return scenes.map((scene) => {
        const copy = { ...scene };
        delete copy.views;
        return copy;
    });
}

describe('a page dies with its last link, not with any link', () => {
    /**
     * scene_95 hangs off scene_90, and both view_232 (on scene_90) and view_233 (on
     * scene_91) link to it. This is the topology the two-arm test ran on: removing
     * view_232's link column left the page alive and reachable from view_233.
     */
    const SHARED: SceneNode[] = [
        {
            sceneKey: 'scene_90',
            sceneName: 'Dashboard',
            sceneSlug: 'dashboard',
            views: [
                { viewKey: 'view_232', childSceneRefs: ['detail'] },
                { viewKey: 'view_240', childSceneRefs: [] },
            ],
        },
        {
            sceneKey: 'scene_91',
            sceneName: 'Reports',
            sceneSlug: 'reports',
            views: [{ viewKey: 'view_233', childSceneRefs: ['detail'] }],
        },
        {
            sceneKey: 'scene_95',
            sceneName: 'Detail',
            sceneSlug: 'detail',
            parentRef: 'dashboard',
            views: [],
        },
    ];

    /** The same tree with view_233's link gone, so view_232 holds the only one. */
    const SOLE: SceneNode[] = SHARED.map((scene) =>
        scene.sceneKey === 'scene_91' ? { ...scene, views: [] } : scene,
    );

    const LINKING_VIEW = {
        key: 'view_232',
        type: 'table',
        columns: [{ type: 'link', scene: 'detail' }],
    };

    /** Drops the link column, which is what a link removal actually looks like. */
    const DROP_THE_LINK = JSON.stringify({
        columns: [{ type: 'field', field: { key: 'field_1' } }],
    });

    it('does not prompt when the page keeps a link from another view', async () => {
        // Both arms of the live test named two pages in the prompt and destroyed
        // neither. Counting links from the mutating view alone is what produced that.
        const spy = makeSpy({
            fetchView: { ok: true, status: 200, body: LINKING_VIEW },
            sceneTree: { ok: true, scenes: SHARED },
            confirm: { supported: true, accepted: false, outcome: 'decline' },
        });

        const result = await run(spy, {
            action: 'update_view',
            sceneKey: 'scene_90',
            viewKey: 'view_232',
            updates: DROP_THE_LINK,
        });

        assert.equal(result.ok, true);
        assert.deepEqual(spy.prompts, []);
        assert.equal(spy.mutations.length, 1);
    });

    it('names the page as moved, and the view it moves to', async () => {
        // Not deleted is not the same as unchanged. The page changes parent, and a
        // caller told only "done" leaves someone hunting for it.
        const spy = makeSpy({
            fetchView: { ok: true, status: 200, body: LINKING_VIEW },
            sceneTree: { ok: true, scenes: SHARED },
        });

        const result = await run(spy, {
            action: 'update_view',
            sceneKey: 'scene_90',
            viewKey: 'view_232',
            updates: DROP_THE_LINK,
        });

        assert.equal(result.ok, true);
        if (!result.ok) return;
        assert.deepEqual(
            result.transferredPages.map((page) => page.sceneKey),
            ['scene_95'],
        );
        assert.deepEqual(result.transferredPages[0].otherReferrers, [
            { sceneKey: 'scene_91', viewKey: 'view_233' },
        ]);
        assert.deepEqual(result.acknowledgedPages, []);
    });

    it("still prompts when this view holds the page's last link", async () => {
        // The rule spares a page with another referrer. It must not spare one without,
        // or the guard has been turned off rather than corrected — and scene_93, which
        // had exactly one referring view, is the page this app actually lost.
        const spy = makeSpy({
            fetchView: { ok: true, status: 200, body: LINKING_VIEW },
            sceneTree: { ok: true, scenes: SOLE },
            confirm: { supported: true, accepted: false, outcome: 'decline' },
        });

        const result = await run(spy, {
            action: 'update_view',
            sceneKey: 'scene_90',
            viewKey: 'view_232',
            updates: DROP_THE_LINK,
        });

        assert.equal(
            result.ok === false && result.code,
            'HUMAN_CONFIRMATION_DECLINED',
        );
        assert.deepEqual(spy.promptInputs[0].doomed, ['scene_95']);
        assert.equal(spy.mutations.length, 0);
    });

    it('reports no transfer when the payload keeps the link', async () => {
        // Nothing is severed, so nothing moves. Reporting a transfer here would
        // misdescribe an edit that left navigation exactly as it was.
        const spy = makeSpy({
            fetchView: { ok: true, status: 200, body: LINKING_VIEW },
            sceneTree: { ok: true, scenes: SHARED },
        });

        const result = await run(spy, {
            action: 'update_view',
            sceneKey: 'scene_90',
            viewKey: 'view_232',
            updates: JSON.stringify({ columns: LINKING_VIEW.columns }),
        });

        assert.equal(result.ok, true);
        assert.deepEqual(result.ok && result.transferredPages, []);
    });

    it('tells the human about a move alongside a deletion', async () => {
        // A prompt that lists only what dies cannot be read as an account of the
        // change. Two links, one page losing its last referrer and one not.
        const twoLinks = {
            key: 'view_232',
            type: 'table',
            columns: [
                { type: 'link', scene: 'detail' },
                { type: 'link', scene: 'summary' },
            ],
        };
        const scenes: SceneNode[] = [
            ...SHARED.map((scene) =>
                scene.sceneKey === 'scene_90'
                    ? {
                          ...scene,
                          views: [
                              {
                                  viewKey: 'view_232',
                                  childSceneRefs: ['detail', 'summary'],
                              },
                          ],
                      }
                    : scene,
            ),
            {
                sceneKey: 'scene_96',
                sceneName: 'Summary',
                sceneSlug: 'summary',
                parentRef: 'dashboard',
                views: [],
            },
        ];

        const spy = makeSpy({
            fetchView: { ok: true, status: 200, body: twoLinks },
            sceneTree: { ok: true, scenes },
            confirm: { supported: true, accepted: false, outcome: 'decline' },
        });

        await run(spy, {
            action: 'update_view',
            sceneKey: 'scene_90',
            viewKey: 'view_232',
            updates: DROP_THE_LINK,
        });

        assert.deepEqual(spy.promptInputs[0].doomed, ['scene_96']);
        assert.deepEqual(spy.promptInputs[0].transferred, ['scene_95']);
    });

    it('keeps the old pessimism when the scene tree carries no link graph', async () => {
        // Every existing caller and test supplies scenes without `views`. Those must
        // behave exactly as before: no referrer data, no page spared.
        const spy = makeSpy({
            fetchView: { ok: true, status: 200, body: LINKING_VIEW },
            sceneTree: {
                ok: true,
                scenes: withoutLinkGraph(SHARED),
            },
            confirm: { supported: true, accepted: false, outcome: 'decline' },
        });

        const result = await run(spy, {
            action: 'update_view',
            sceneKey: 'scene_90',
            viewKey: 'view_232',
            updates: DROP_THE_LINK,
        });

        assert.equal(
            result.ok === false && result.code,
            'HUMAN_CONFIRMATION_DECLINED',
        );
        assert.deepEqual(spy.promptInputs[0].doomed, ['scene_95']);
    });
});

describe('the guard sends the body it judged', () => {
    /**
     * A table whose one link column owns a singly-referenced child page — the shape
     * the live premise test ran on, reduced to its essentials.
     */
    const SCENES_WITH_CHILD: SceneNode[] = [
        {
            sceneKey: 'scene_97',
            sceneSlug: 'premise-test',
            views: [
                { viewKey: 'view_239', childSceneRefs: ['book-assessment'] },
            ],
        },
        {
            sceneKey: 'scene_106',
            sceneName: 'Book Assessment',
            sceneSlug: 'book-assessment',
            parentRef: 'premise-test',
            views: [],
        },
    ];

    const LINKED_TABLE = {
        key: 'view_239',
        _id: 'abc123',
        type: 'table',
        title: 'Admissions Clients',
        rows_per_page: '10',
        keyword_search: true,
        columns: [
            { type: 'field', field: { key: 'field_47' } },
            { type: 'link', scene: 'book-assessment', header: 'Book Assess' },
        ],
    };

    const spyForTable = (confirm?: PageDeletionConfirmation) =>
        makeSpy({
            fetchView: { ok: true, status: 200, body: LINKED_TABLE },
            sceneTree: { ok: true, scenes: SCENES_WITH_CHILD },
            confirm,
        });

    it('turns a scalar edit on a linked view into a complete body', async () => {
        // Sending the caller's `{title}` fragment to a route that replaces would wipe
        // the columns and take the child page with them, unprompted — the payload is
        // not structural, so nothing would have stopped it. The guard now merges it
        // into the live definition and sends that instead.
        const spy = spyForTable();

        const result = await run(spy, {
            action: 'update_view',
            sceneKey: 'scene_97',
            viewKey: 'view_239',
            updates: JSON.stringify({ title: 'PREMISE-RESEND' }),
        });

        assert.equal(result.ok, true);
        assert.deepEqual(spy.prompts, []);

        const body = spy.sent[0];
        assert.equal(body?.title, 'PREMISE-RESEND');
        assert.deepEqual(body?.columns, LINKED_TABLE.columns);
        assert.equal(body?.rows_per_page, '10');
        assert.equal(body?.keyword_search, true);
        assert.ok(!('key' in (body ?? {})));
        assert.ok(!('_id' in (body ?? {})));
    });

    it('prompts, and sends the short body, when the payload drops the link', async () => {
        const spy = spyForTable({
            supported: true,
            accepted: true,
            outcome: 'accept',
        });

        const result = await run(spy, {
            action: 'update_view',
            sceneKey: 'scene_97',
            viewKey: 'view_239',
            updates: JSON.stringify({
                title: 'PREMISE-OMIT',
                columns: [{ type: 'field', field: { key: 'field_47' } }],
            }),
        });

        assert.equal(result.ok, true);
        assert.deepEqual(spy.promptInputs[0].doomed, ['scene_106']);
        assert.equal((spy.sent[0]?.columns as unknown[]).length, 1);
    });

    it('judges the merged body, not the fragment', async () => {
        // The two assertions above with the reasoning made explicit: identical view,
        // identical action, and the only difference is whether the merged body still
        // carries the link. One prompts, one does not.
        const resent = spyForTable();
        await run(resent, {
            action: 'update_view',
            sceneKey: 'scene_97',
            viewKey: 'view_239',
            updates: JSON.stringify({ rows_per_page: '25' }),
        });

        const dropped = spyForTable({ supported: false });
        const result = await run(dropped, {
            action: 'update_view',
            sceneKey: 'scene_97',
            viewKey: 'view_239',
            updates: JSON.stringify({ rows_per_page: '25', columns: [] }),
        });

        assert.deepEqual(resent.prompts, []);
        assert.equal(resent.mutations.length, 1);
        assert.equal(
            result.ok === false && result.code,
            'HUMAN_CONFIRMATION_UNAVAILABLE',
        );
        assert.deepEqual(dropped.mutations, []);
    });
});

describe('the container makes no difference', () => {
    /**
     * Two views, identical in every way that matters except where they keep their
     * link: one in `columns`, one in `links`. Both point at the same singly-referenced
     * owned page.
     *
     * This pair used to disagree, because `links` was untested and got no credit for
     * re-sending. A live seven-link menu settled it — omit one entry and that page and
     * its descendants go, re-send the rest and they stay — so both now behave the
     * same, and the distinction the guard used to draw is gone.
     */
    const SCENES_ONE_CHILD: SceneNode[] = [
        { sceneKey: 'scene_1', sceneName: 'Contacts', sceneSlug: 'contacts' },
        {
            sceneKey: 'scene_101',
            sceneName: 'Edit contact',
            sceneSlug: 'edit-contact',
            parentRef: 'contacts',
        },
    ];

    const IN_COLUMNS = {
        key: 'view_7',
        type: 'table',
        title: 'Contacts',
        columns: [{ type: 'link', scene: 'edit-contact' }],
    };

    const IN_LINKS = {
        key: 'view_7',
        type: 'menu',
        title: 'Contacts',
        links: [{ type: 'scene', scene: 'edit-contact' }],
    };

    const scalarEdit = {
        action: 'update_view',
        sceneKey: 'scene_1',
        viewKey: 'view_7',
        updates: JSON.stringify({ title: 'Renamed' }),
    } as const;

    it('lets the column view through when the merged body re-sends its link', async () => {
        const spy = makeSpy({
            fetchView: { ok: true, status: 200, body: IN_COLUMNS },
            sceneTree: { ok: true, scenes: SCENES_ONE_CHILD },
        });

        const result = await run(spy, scalarEdit);

        assert.equal(result.ok, true);
        assert.deepEqual(spy.prompts, []);
    });

    it('lets the links view through on the same edit', async () => {
        const spy = makeSpy({
            fetchView: { ok: true, status: 200, body: IN_LINKS },
            sceneTree: { ok: true, scenes: SCENES_ONE_CHILD },
        });

        const result = await run(spy, scalarEdit);

        assert.equal(result.ok, true);
        assert.deepEqual(spy.prompts, []);
        assert.deepEqual(spy.sent[0]?.links, IN_LINKS.links);
    });

    it('stops both of them when the link is actually dropped', async () => {
        // The symmetry has to hold in the other direction too, or the rule has been
        // switched off for menus rather than extended to them.
        for (const [label, view, payload] of [
            ['columns', IN_COLUMNS, { columns: [] }],
            ['links', IN_LINKS, { links: [] }],
        ] as const) {
            const spy = makeSpy({
                fetchView: { ok: true, status: 200, body: view },
                sceneTree: { ok: true, scenes: SCENES_ONE_CHILD },
                confirm: {
                    supported: true,
                    accepted: false,
                    outcome: 'decline',
                },
            });

            const result = await run(spy, {
                action: 'update_view',
                sceneKey: 'scene_1',
                viewKey: 'view_7',
                updates: JSON.stringify(payload),
            });

            assert.equal(
                result.ok === false && result.code,
                'HUMAN_CONFIRMATION_DECLINED',
                label,
            );
            assert.deepEqual(spy.promptInputs[0].doomed, ['scene_101'], label);
        }
    });

    it('is untroubled by a view that carries an empty links array', async () => {
        // A view's own definition often carries `links: []`. There is nothing there to
        // lose, and the guard should not read one as navigation at stake.
        const spy = makeSpy({
            fetchView: {
                ok: true,
                status: 200,
                body: { ...IN_COLUMNS, links: [] },
            },
            sceneTree: { ok: true, scenes: SCENES_ONE_CHILD },
        });

        const result = await run(spy, scalarEdit);

        assert.equal(result.ok, true);
        assert.deepEqual(spy.prompts, []);
    });
});

describe('the body sent is always the body judged', () => {
    /**
     * The invariant everything else rests on. The guard reasons about a merged body
     * and hands that same object to the transport, so there is no second reasoner and
     * no gap between what was examined and what Knack receives.
     *
     * One payload used to slip through it. `buildEffectiveUpdateBody` can only merge
     * into an object, and a JSON array that happens to contain layout keys clears the
     * empty-payload check — the key walk finds `columns` inside the array — so the raw
     * array went to Knack unmerged.
     */
    const cannotMerge = [
        ['an array carrying layout keys', '[{"columns":[]}]'],
        ['an array of scalars', '[1,2,3]'],
        ['a bare string', '"columns"'],
    ] as const;

    for (const [label, updates] of cannotMerge) {
        it(`refuses ${label} rather than forwarding it unmerged`, async () => {
            const spy = makeSpy({
                fetchView: { ok: true, status: 200, body: NESTED_LINK_VIEW },
            });

            const result = await run(spy, {
                action: 'update_view',
                sceneKey: 'scene_1',
                viewKey: 'view_7',
                updates,
            });

            assert.equal(
                result.ok === false && result.code,
                'INVALID_UPDATES_JSON',
            );
            assert.deepEqual(spy.mutations, []);
        });
    }

    it('still accepts an ordinary object payload', async () => {
        const spy = makeSpy({
            fetchView: { ok: true, status: 200, body: NESTED_LINK_VIEW },
        });

        const result = await run(spy, {
            action: 'update_view',
            sceneKey: 'scene_1',
            viewKey: 'view_7',
            updates: JSON.stringify({ title: 'Fine' }),
        });

        assert.equal(result.ok, true);
        assert.equal(spy.sent[0]?.title, 'Fine');
    });

    it('never hands the transport a null body on an update', async () => {
        // The other half of the invariant: if a payload reaches perform() at all, the
        // guard built the body for it. A null here would mean something was sent that
        // nothing merged.
        const spy = makeSpy({
            fetchView: { ok: true, status: 200, body: NESTED_LINK_VIEW },
        });

        await run(spy, {
            action: 'update_view',
            sceneKey: 'scene_1',
            viewKey: 'view_7',
            updates: JSON.stringify({ rows_per_page: '50' }),
        });

        assert.equal(spy.sent.length, 1);
        assert.notEqual(spy.sent[0], null);
    });
});
