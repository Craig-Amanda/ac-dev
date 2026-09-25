import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import {
    makeApp,
    makeFakeContext,
    payloadOf,
} from '../testing/fake-context.js';
import type { RuntimeMetadata } from '../types.js';
import {
    addPageRules,
    createPage,
    deletePage,
    editPageRules,
    assignPageRuleKeys,
    PAGE_SETTINGS,
    updatePageSettings,
} from './scene-mutations.js';

/** The rule captured from the Builder on NPS Test App scene_220, verbatim. */
const EXISTING_RULE = {
    action: 'hide_views',
    criteria: [
        {
            field: 'all_users-field_104',
            operator: 'does not contain',
            value: 'profile_8',
        },
    ],
    message: '',
    close_link: false,
    existing_page: 'clients',
    view_keys: ['view_571'],
    url: '',
    type: 'neutral',
    key: 'submit_0',
};

const NEW_RULE = {
    action: 'message',
    criteria: [
        {
            field: 'all_users-field_104',
            operator: 'is not',
            value: 'profile_8',
        },
    ],
    message: 'Finance staff only',
    close_link: false,
    view_keys: [],
    type: 'neutral',
};

/** scene_220 and whatever else a test adds, as the fakes mutate them. */
const scenesOf = (metadata: RuntimeMetadata) =>
    (metadata.application as { scenes: Array<Record<string, unknown>> }).scenes;

function makeMetadata(rules: unknown[]): RuntimeMetadata {
    return {
        application: {
            name: 'Demo',
            slug: 'demo',
            objects: [],
            scenes: [
                {
                    key: 'scene_220',
                    name: 'Finance',
                    slug: 'finance2',
                    rules,
                    views: [{ key: 'view_571', name: 'Service Charges' }],
                },
            ],
        },
    };
}

/**
 * A fake Knack whose POST really replaces the stored rules, so the read-back sees what
 * was sent. `store` lets a test make Knack keep something else instead.
 */
function makeCtx(
    rules: unknown[] = [EXISTING_RULE],
    store: (sent: unknown[]) => unknown[] = (sent) => sent,
) {
    const app = makeApp();
    const metadata = makeMetadata(rules);
    const scene = scenesOf(metadata)[0];
    return makeFakeContext({
        apps: [app],
        runtimeMetadata: { [app.appKey]: metadata },
        responses: (apiPath, init) => {
            if (
                apiPath === '/scenes/scene_220/rules' &&
                init?.method === 'POST'
            ) {
                const body = JSON.parse(init.body as string) as {
                    rules: unknown[];
                };
                scene.rules = store(body.rules);
                return { ok: true, status: 200, body: { success: true } };
            }
            return { ok: false, status: 404, body: {} };
        },
    });
}

describe('assignPageRuleKeys', () => {
    it('numbers from the highest existing key, not the count', () => {
        const keyed = assignPageRuleKeys(
            [{ key: 'submit_0' }, { key: 'submit_2' }],
            [{}, {}],
        );
        assert.deepEqual(
            keyed.map((rule) => rule.key),
            ['submit_3', 'submit_4'],
        );
    });

    it('starts at submit_0 on a page with no rules, ignoring non-submit keys', () => {
        assert.equal(assignPageRuleKeys([], [{}])[0].key, 'submit_0');
        assert.equal(
            assignPageRuleKeys([{ key: 'legacy' }], [{}])[0].key,
            'submit_0',
        );
    });

    it('keeps a caller key that is free', () => {
        assert.equal(
            assignPageRuleKeys([{ key: 'submit_0' }], [{ key: 'custom' }])[0]
                .key,
            'custom',
        );
    });

    it('refuses a key already on the page, or repeated in the request', () => {
        assert.throws(
            () =>
                assignPageRuleKeys(
                    [{ key: 'submit_0' }],
                    [{ key: 'submit_0' }],
                ),
            /already used/,
        );
        assert.throws(
            () => assignPageRuleKeys([], [{ key: 'a' }, { key: 'a' }]),
            /already used/,
        );
    });

    it('refuses a non-string key', () => {
        assert.throws(
            () => assignPageRuleKeys([], [{ key: 7 }]),
            /must be a string/,
        );
    });
});

describe('knack_add_page_rules', () => {
    /** One call with NEW_RULE on scene_220 unless the test says otherwise. */
    const runAdd = (
        ctx: ReturnType<typeof makeCtx>['ctx'],
        args: Record<string, unknown> = {},
    ) =>
        addPageRules
            .handler(
                {
                    appKey: 'Demo',
                    sceneKey: 'scene_220',
                    rules: JSON.stringify([NEW_RULE]),
                    ...args,
                } as Parameters<typeof addPageRules.handler>[0],
                ctx,
            )
            .then(payloadOf);
    it('is view-access, like every other layout write', () => {
        assert.equal(addPageRules.access, 'view');
    });

    it('POSTs the existing rules verbatim plus the new one, and verifies the read-back', async () => {
        const { ctx, requests } = makeCtx();
        const result = await runAdd(ctx);

        assert.equal(result.ok, true, JSON.stringify(result));
        assert.equal(result.verified, true);
        assert.equal(result.ruleCountBefore, 1);
        assert.equal(result.ruleCountAfter, 2);
        assert.deepEqual(result.addedKeys, ['submit_1']);
        assert.deepEqual(result.rulesBefore, [EXISTING_RULE]);

        assert.equal(requests.length, 1);
        assert.equal(requests[0].method, 'POST');
        assert.equal(requests[0].apiPath, '/scenes/scene_220/rules');
        // The whole array, existing rule untouched — every field, not just the ones
        // knack_get_scene displays.
        assert.deepEqual(requests[0].body, {
            rules: [EXISTING_RULE, { ...NEW_RULE, key: 'submit_1' }],
        });
    });

    it('reports verified:false when Knack stores something other than what was sent', async () => {
        const { ctx } = makeCtx([EXISTING_RULE], (sent) => sent.slice(1));
        const result = await runAdd(ctx);

        assert.equal(result.ok, true);
        assert.equal(result.verified, false);
        assert.match(String(result.warning), /differ from what was sent/);
        assert.deepEqual(result.rulesBefore, [EXISTING_RULE]);
    });

    it('previewOnly sends nothing', async () => {
        const { ctx, requests } = makeCtx();
        const result = await runAdd(ctx, { previewOnly: true });

        assert.equal(result.ok, true);
        assert.equal(result.previewOnly, true);
        assert.equal((result.rules as unknown[]).length, 2);
        assert.equal(requests.length, 0);
    });

    it('refuses a rule naming a view that is not on the page', async () => {
        const { ctx, requests } = makeCtx();
        const result = await runAdd(ctx, {
            rules: JSON.stringify([
                { ...NEW_RULE, action: 'hide_views', view_keys: ['view_999'] },
            ]),
        });

        assert.equal(result.ok, false);
        assert.equal(result.error, 'VIEW_NOT_ON_PAGE');
        assert.equal(requests.length, 0);
    });

    it('refuses an unknown page', async () => {
        const { ctx, requests } = makeCtx();
        const result = await runAdd(ctx, { sceneKey: 'scene_1' });

        assert.equal(result.error, 'SCENE_NOT_FOUND');
        assert.equal(requests.length, 0);
    });

    it('works on a page with no rules yet', async () => {
        const { ctx, requests } = makeCtx([]);
        const result = await runAdd(ctx);

        assert.equal(result.verified, true);
        assert.deepEqual(requests[0].body, {
            rules: [{ ...NEW_RULE, key: 'submit_0' }],
        });
    });
});

describe('knack_update_page_settings', () => {
    /**
     * Two pages, and a fake Knack that merges a PUT into the page as the real one was
     * measured to, reporting the menu view it repoints when the slug changes.
     */
    function makeSettingsCtx(options: { ignore?: string } = {}) {
        const app = makeApp();
        const metadata = makeMetadata([EXISTING_RULE]);
        const scenes = scenesOf(metadata);
        Object.assign(scenes[0], { modal: false, print: false });
        scenes.push({
            key: 'scene_38',
            name: 'Shared area',
            slug: 'shared-area',
            views: [],
        });
        const scene = scenes[0];
        return {
            scene,
            ...makeFakeContext({
                apps: [app],
                runtimeMetadata: { [app.appKey]: metadata },
                responses: (apiPath, init) => {
                    if (
                        apiPath !== '/scenes/scene_220' ||
                        init?.method !== 'PUT'
                    ) {
                        return { ok: false, status: 404, body: {} };
                    }
                    const sent = JSON.parse(init.body as string) as Record<
                        string,
                        unknown
                    >;
                    const kept = Object.fromEntries(
                        Object.entries(sent).filter(
                            ([key]) => key !== options.ignore,
                        ),
                    );
                    Object.assign(scene, kept);
                    const repointed =
                        'slug' in sent
                            ? [
                                  {
                                      scene: { key: 'scene_38' },
                                      view: {
                                          key: 'view_370',
                                          type: 'menu',
                                          links: [],
                                      },
                                  },
                              ]
                            : [];
                    return {
                        ok: true,
                        status: 200,
                        body: {
                            scene,
                            changes: {
                                updates: {
                                    scenes: repointed.map(() => ({
                                        key: 'scene_38',
                                    })),
                                    views: repointed,
                                },
                            },
                        },
                    };
                },
            }),
        };
    }

    const run = (
        ctx: ReturnType<typeof makeSettingsCtx>['ctx'],
        args: Record<string, unknown>,
    ) =>
        updatePageSettings
            .handler(
                {
                    appKey: 'Demo',
                    sceneKey: 'scene_220',
                    ...args,
                } as Parameters<typeof updatePageSettings.handler>[0],
                ctx,
            )
            .then(payloadOf);

    it('sends only the changed settings, mapped to Knack names, never views', async () => {
        const { ctx, requests } = makeSettingsCtx();
        const result = await run(ctx, {
            name: 'Finance Text',
            modal: true,
            keepModalOpen: true,
            print: false, // already false: not sent
        });

        assert.equal(result.ok, true, JSON.stringify(result));
        assert.equal(result.verified, true);
        assert.equal(requests.length, 1);
        assert.equal(requests[0].method, 'PUT');
        assert.deepEqual(requests[0].body, {
            name: 'Finance Text',
            modal: true,
            modal_prevent_background_click_close: true,
        });
        assert.deepEqual(result.before, {
            name: 'Finance',
            modal: false,
            keepModalOpen: null,
        });
        assert.equal('slugNote' in result, false);
    });

    it('can never send an access property: authenticated: false deletes a public page', () => {
        // Measured live on 25 September. Any of these on the wire is either a 500 or,
        // for authenticated: false, a silent delete of the page.
        const sent = Object.values(PAGE_SETTINGS) as string[];
        for (const forbidden of [
            'authenticated',
            'login_vars',
            'allowed_profiles',
            'limit_profile_access',
            'views',
            'parent',
        ]) {
            assert.equal(sent.includes(forbidden), false, forbidden);
        }
        const inputs = Object.keys(updatePageSettings.input);
        assert.equal(inputs.includes('authenticated'), false);
    });

    it('passes back the views Knack repointed after a slug change', async () => {
        const { ctx } = makeSettingsCtx();
        const result = await run(ctx, { slug: 'finance-url' });

        assert.equal(result.verified, true);
        assert.deepEqual(result.knackUpdated, {
            scenes: ['scene_38'],
            views: [{ sceneKey: 'scene_38', viewKey: 'view_370' }],
        });
        assert.match(String(result.slugNote), /"finance2" to "finance-url"/);
    });

    it('refuses a slug another page already has, or one Knack would never produce', async () => {
        const { ctx, requests } = makeSettingsCtx();
        assert.equal(
            (await run(ctx, { slug: 'shared-area' })).error,
            'SLUG_IN_USE',
        );
        assert.equal(
            (await run(ctx, { slug: 'Finance URL' })).error,
            'INVALID_SLUG',
        );
        assert.equal(
            (await run(ctx, { slug: 'finance--url' })).error,
            'INVALID_SLUG',
        );
        assert.equal(requests.length, 0);
    });

    it('refuses keepModalOpen on a page that is not a modal', async () => {
        const { ctx, requests } = makeSettingsCtx();
        const result = await run(ctx, { keepModalOpen: true });
        assert.equal(result.error, 'NOT_A_MODAL');
        assert.equal(requests.length, 0);
    });

    it('sends nothing when every value is already set, or when nothing is passed', async () => {
        const { ctx, requests } = makeSettingsCtx();
        const same = await run(ctx, { name: 'Finance', print: false });
        assert.equal(same.ok, true);
        assert.equal(same.unchanged, true);
        assert.equal((await run(ctx, {})).error, 'NOTHING_TO_CHANGE');
        assert.equal(requests.length, 0);
    });

    it('previewOnly shows the body without sending it', async () => {
        const { ctx, requests } = makeSettingsCtx();
        const result = await run(ctx, {
            slug: 'finance-url',
            previewOnly: true,
        });
        assert.deepEqual(result.wouldSend, { slug: 'finance-url' });
        assert.match(String(result.slugNote), /Bookmarks/);
        assert.equal(requests.length, 0);
    });

    it('reports verified:false when Knack does not keep a value', async () => {
        const { ctx } = makeSettingsCtx({ ignore: 'print' });
        const result = await run(ctx, { print: true, name: 'Finance Text' });
        assert.equal(result.ok, true);
        assert.equal(result.verified, false);
        assert.match(String(result.warning), /print did not take/);
    });
});

describe('knack_edit_page_rules', () => {
    const SECOND_RULE = { ...NEW_RULE, key: 'submit_1' };
    const runEdit = (
        ctx: ReturnType<typeof makeCtx>['ctx'],
        args: Record<string, unknown>,
    ) =>
        editPageRules
            .handler(
                {
                    appKey: 'Demo',
                    sceneKey: 'scene_220',
                    ...args,
                } as Parameters<typeof editPageRules.handler>[0],
                ctx,
            )
            .then(payloadOf);

    it('removes one rule and POSTs the rest verbatim', async () => {
        const { ctx, requests } = makeCtx([EXISTING_RULE, SECOND_RULE]);
        const result = await runEdit(ctx, { removeKeys: ['submit_0'] });

        assert.equal(result.ok, true, JSON.stringify(result));
        assert.equal(result.verified, true);
        assert.deepEqual(result.removedKeys, ['submit_0']);
        assert.deepEqual(requests[0].body, { rules: [SECOND_RULE] });
        assert.deepEqual(result.rulesBefore, [EXISTING_RULE, SECOND_RULE]);
    });

    it('replaces a rule in place', async () => {
        const { ctx, requests } = makeCtx([EXISTING_RULE, SECOND_RULE]);
        const replacement = { ...SECOND_RULE, message: 'Changed' };
        const result = await runEdit(ctx, {
            replaceRules: JSON.stringify([replacement]),
        });

        assert.equal(result.verified, true);
        assert.deepEqual(requests[0].body, {
            rules: [EXISTING_RULE, replacement],
        });
    });

    it('refuses an unknown key and sends nothing', async () => {
        const { ctx, requests } = makeCtx();
        const result = await runEdit(ctx, { removeKeys: ['submit_7'] });

        assert.equal(result.error, 'INVALID_EDIT');
        assert.match(String(result.message), /submit_7/);
        assert.equal(requests.length, 0);
    });

    it('refuses a replacement naming a view that is not on the page', async () => {
        const { ctx, requests } = makeCtx();
        const result = await runEdit(ctx, {
            replaceRules: JSON.stringify([
                { ...EXISTING_RULE, view_keys: ['view_999'] },
            ]),
        });

        assert.equal(result.error, 'VIEW_NOT_ON_PAGE');
        assert.equal(requests.length, 0);
    });

    it('previewOnly sends nothing', async () => {
        const { ctx, requests } = makeCtx();
        const result = await runEdit(ctx, {
            removeKeys: ['submit_0'],
            previewOnly: true,
        });

        assert.equal(result.previewOnly, true);
        assert.deepEqual(result.rules, []);
        assert.equal(requests.length, 0);
    });
});

describe('knack_create_page and knack_delete_page', () => {
    /**
     * A small tree: a public page with a child, a login guarding one page, a login
     * guarding two, and the home page. Parents are slugs, as Knack writes them.
     */
    function makePageMetadata(): RuntimeMetadata {
        return {
            application: {
                name: 'Demo',
                slug: 'demo',
                home_scene: { key: 'scene_1', slug: 'home' },
                objects: [
                    {
                        key: 'object_1',
                        name: 'Accounts',
                        profile_key: 'all_users',
                    },
                    {
                        key: 'object_2',
                        name: 'Staff',
                        profile_key: 'profile_2',
                    },
                ],
                scenes: [
                    { key: 'scene_1', name: 'Home', slug: 'home', views: [] },
                    {
                        key: 'scene_2',
                        name: 'Reports',
                        slug: 'reports',
                        views: [
                            {
                                key: 'view_2',
                                type: 'table',
                                columns: [
                                    {
                                        type: 'link',
                                        scene: 'report-detail',
                                    },
                                ],
                            },
                        ],
                    },
                    {
                        key: 'scene_3',
                        name: 'Report detail',
                        slug: 'report-detail',
                        parent: 'reports',
                        views: [],
                    },
                    {
                        key: 'scene_4',
                        name: 'Finance Login',
                        slug: 'finance-login',
                        type: 'authentication',
                        views: [
                            {
                                key: 'view_4',
                                type: 'login',
                                allowed_profiles: ['profile_2'],
                                limit_profile_access: true,
                            },
                        ],
                    },
                    {
                        key: 'scene_5',
                        name: 'Finance',
                        slug: 'finance',
                        parent: 'finance-login',
                        views: [],
                    },
                    {
                        key: 'scene_6',
                        name: 'Shared Login',
                        slug: 'shared-login',
                        type: 'authentication',
                        views: [{ key: 'view_6', type: 'login' }],
                    },
                    {
                        key: 'scene_7',
                        name: 'A',
                        slug: 'a',
                        parent: 'shared-login',
                        views: [],
                    },
                    {
                        key: 'scene_8',
                        name: 'B',
                        slug: 'b',
                        parent: 'shared-login',
                        views: [],
                    },
                ],
            },
        };
    }

    const scenes = (metadata: RuntimeMetadata) =>
        (metadata.application as { scenes: Array<Record<string, unknown>> })
            .scenes;

    /** A fake Knack whose POST adds the scenes and whose DELETE removes a subtree. */
    function setupPages(tweak?: (metadata: RuntimeMetadata) => void) {
        const metadata = makePageMetadata();
        tweak?.(metadata);
        const fake = makeFakeContext({
            apps: [makeApp()],
            runtimeMetadata: { Demo: metadata },
            responses: (apiPath, init) => {
                const list = scenes(metadata);
                if (apiPath === '/scenes' && init?.method === 'POST') {
                    const sent = JSON.parse(init.body as string) as Record<
                        string,
                        unknown
                    >;
                    const loginVars = sent.login_vars as Record<
                        string,
                        unknown
                    > | null;
                    const page = {
                        key: 'scene_20',
                        name: sent.name,
                        slug: 'new-page',
                        views: [],
                        ...(loginVars ? { parent: 'new-page-login' } : {}),
                    };
                    const inserts = loginVars
                        ? [
                              {
                                  key: 'scene_21',
                                  slug: 'new-page-login',
                                  type: 'authentication',
                                  views: [
                                      {
                                          key: 'view_21',
                                          type: 'login',
                                          allowed_profiles:
                                              loginVars.allowed_profiles,
                                          limit_profile_access:
                                              loginVars.limit_profile_access,
                                      },
                                  ],
                              },
                          ]
                        : [];
                    list.push(page, ...inserts);
                    return {
                        ok: true,
                        status: 200,
                        body: {
                            scene: page,
                            changes: { inserts: { scenes: inserts } },
                        },
                    };
                }
                const deleted = /^\/scenes\/(scene_\d+)$/.exec(apiPath)?.[1];
                if (deleted && init?.method === 'DELETE') {
                    const root = list.find((scene) => scene.key === deleted);
                    const gone = list.filter(
                        (scene) =>
                            scene === root || scene.parent === root?.slug,
                    );
                    for (const scene of gone)
                        list.splice(list.indexOf(scene), 1);
                    // As measured live: a login page left guarding nothing goes too.
                    const parent = list.find(
                        (scene) => scene.slug === root?.parent,
                    );
                    if (
                        parent?.type === 'authentication' &&
                        !list.some((scene) => scene.parent === parent.slug)
                    ) {
                        list.splice(list.indexOf(parent), 1);
                        gone.push(parent);
                    }
                    return {
                        ok: true,
                        status: 200,
                        body: {
                            changes: {
                                deletes: {
                                    scenes: gone.map((scene) => ({
                                        key: scene.key,
                                    })),
                                },
                            },
                        },
                    };
                }
                return { ok: false, status: 404, body: {} };
            },
        });
        return fake;
    }

    const create = (
        ctx: ReturnType<typeof setupPages>['ctx'],
        args: Record<string, unknown>,
    ) =>
        createPage
            .handler(
                {
                    appKey: 'Demo',
                    name: 'New page',
                    ...args,
                } as Parameters<typeof createPage.handler>[0],
                ctx,
            )
            .then(payloadOf);
    const remove = (
        ctx: ReturnType<typeof setupPages>['ctx'],
        args: Record<string, unknown>,
    ) =>
        deletePage
            .handler(
                {
                    appKey: 'Demo',
                    confirm: false,
                    ...args,
                } as Parameters<typeof deletePage.handler>[0],
                ctx,
            )
            .then(payloadOf);

    it('creates a public page with the body the Builder sends, and verifies it', async () => {
        const { ctx, requests } = setupPages();
        const result = await create(ctx, {});
        assert.equal(result.ok, true, JSON.stringify(result));
        assert.equal(result.sceneKey, 'scene_20');
        assert.equal(result.verified, true);
        assert.deepEqual(requests[0].body, {
            name: 'New page',
            type: 'page',
            views: [],
            authenticated: false,
            login_vars: null,
            menu_pages: null,
            parent: null,
        });
    });

    it('creates a page behind a login for one role, and reads the roles back', async () => {
        const { ctx, requests } = setupPages();
        const result = await create(ctx, { login: { roles: ['profile_2'] } });
        assert.equal(result.ok, true, JSON.stringify(result));
        assert.equal(result.loginSceneKey, 'scene_21');
        assert.equal(result.verified, true);
        assert.deepEqual(
            (requests[0].body as Record<string, unknown>).login_vars,
            {
                authenticated: true,
                allowed_profiles: ['profile_2'],
                limit_profile_access: true,
            },
        );
    });

    it('refuses an unknown role, or a login naming neither roles nor any user', async () => {
        const { ctx, requests } = setupPages();
        assert.equal(
            (await create(ctx, { login: { roles: ['profile_9'] } })).error,
            'UNKNOWN_ROLE',
        );
        assert.equal((await create(ctx, { login: {} })).error, 'INVALID_LOGIN');
        assert.equal(requests.length, 0);
    });

    it('previews a leaf page delete, then deletes it and verifies', async () => {
        const { ctx, requests } = setupPages();
        const preview = await remove(ctx, { sceneKey: 'scene_3' });
        assert.equal(preview.action, 'delete_page_preflight');
        assert.deepEqual(preview.deletes, ['scene_3']);
        assert.equal((preview.linkedFrom as unknown[]).length, 1);
        assert.equal(requests.length, 0);

        const done = await remove(ctx, { sceneKey: 'scene_3', confirm: true });
        assert.equal(done.ok, true, JSON.stringify(done));
        assert.equal(done.verified, true);
        assert.equal(requests[0].apiPath, '/scenes/scene_3');
    });

    it('deletes a page with its own login by deleting the page; Knack removes the login', async () => {
        const { ctx, requests } = setupPages();
        const done = await remove(ctx, { sceneKey: 'scene_5', confirm: true });
        assert.equal(done.ok, true, JSON.stringify(done));
        assert.equal(requests[0].apiPath, '/scenes/scene_5');
        assert.deepEqual([...(done.deleted as string[])].sort(), [
            'scene_4',
            'scene_5',
        ]);
        assert.equal(done.verified, true);
    });

    it('refuses a delete that would take other pages, and the home page', async () => {
        const { ctx, requests } = setupPages();
        assert.equal(
            (await remove(ctx, { sceneKey: 'scene_2', confirm: true })).error,
            'WOULD_DELETE_OTHER_PAGES',
        );
        assert.equal(
            (await remove(ctx, { sceneKey: 'scene_6', confirm: true })).error,
            'WOULD_DELETE_OTHER_PAGES',
        );
        assert.equal(
            (await remove(ctx, { sceneKey: 'scene_1', confirm: true })).error,
            'HOME_PAGE',
        );
        assert.equal(requests.length, 0);
    });

    it('refuses to delete the only page behind a login that is the home page', async () => {
        const { ctx, requests } = setupPages((metadata) => {
            (metadata.application as Record<string, unknown>).home_scene = {
                key: 'scene_4',
                slug: 'finance-login',
            };
        });
        const refused = await remove(ctx, {
            sceneKey: 'scene_5',
            confirm: true,
        });
        assert.equal(refused.error, 'HOME_PAGE');
        assert.match(String(refused.message), /home page too/);
        assert.equal(requests.length, 0);
    });

    it('deletes one of two pages under a shared login without touching the login', async () => {
        const { ctx, requests } = setupPages();
        const done = await remove(ctx, { sceneKey: 'scene_7', confirm: true });
        assert.equal(done.ok, true, JSON.stringify(done));
        assert.equal(requests[0].apiPath, '/scenes/scene_7');
    });
});
