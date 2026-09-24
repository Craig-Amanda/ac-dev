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
    assignPageRuleKeys,
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
    const scene = (
        metadata.application as { scenes: Array<Record<string, unknown>> }
    ).scenes[0];
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
    it('is view-access, like every other layout write', () => {
        assert.equal(addPageRules.access, 'view');
    });

    it('POSTs the existing rules verbatim plus the new one, and verifies the read-back', async () => {
        const { ctx, requests } = makeCtx();
        const result = payloadOf(
            await addPageRules.handler(
                {
                    appKey: 'Demo',
                    sceneKey: 'scene_220',
                    rules: JSON.stringify([NEW_RULE]),
                },
                ctx,
            ),
        );

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
        const result = payloadOf(
            await addPageRules.handler(
                {
                    appKey: 'Demo',
                    sceneKey: 'scene_220',
                    rules: JSON.stringify([NEW_RULE]),
                },
                ctx,
            ),
        );

        assert.equal(result.ok, true);
        assert.equal(result.verified, false);
        assert.match(String(result.warning), /differ from what was sent/);
        assert.deepEqual(result.rulesBefore, [EXISTING_RULE]);
    });

    it('previewOnly sends nothing', async () => {
        const { ctx, requests } = makeCtx();
        const result = payloadOf(
            await addPageRules.handler(
                {
                    appKey: 'Demo',
                    sceneKey: 'scene_220',
                    rules: JSON.stringify([NEW_RULE]),
                    previewOnly: true,
                },
                ctx,
            ),
        );

        assert.equal(result.ok, true);
        assert.equal(result.previewOnly, true);
        assert.equal((result.rules as unknown[]).length, 2);
        assert.equal(requests.length, 0);
    });

    it('refuses a rule naming a view that is not on the page', async () => {
        const { ctx, requests } = makeCtx();
        const result = payloadOf(
            await addPageRules.handler(
                {
                    appKey: 'Demo',
                    sceneKey: 'scene_220',
                    rules: JSON.stringify([
                        {
                            ...NEW_RULE,
                            action: 'hide_views',
                            view_keys: ['view_999'],
                        },
                    ]),
                },
                ctx,
            ),
        );

        assert.equal(result.ok, false);
        assert.equal(result.error, 'VIEW_NOT_ON_PAGE');
        assert.equal(requests.length, 0);
    });

    it('refuses an unknown page', async () => {
        const { ctx, requests } = makeCtx();
        const result = payloadOf(
            await addPageRules.handler(
                {
                    appKey: 'Demo',
                    sceneKey: 'scene_1',
                    rules: JSON.stringify([NEW_RULE]),
                },
                ctx,
            ),
        );

        assert.equal(result.error, 'SCENE_NOT_FOUND');
        assert.equal(requests.length, 0);
    });

    it('works on a page with no rules yet', async () => {
        const { ctx, requests } = makeCtx([]);
        const result = payloadOf(
            await addPageRules.handler(
                {
                    appKey: 'Demo',
                    sceneKey: 'scene_220',
                    rules: JSON.stringify([NEW_RULE]),
                },
                ctx,
            ),
        );

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
        const scenes = (
            metadata.application as { scenes: Array<Record<string, unknown>> }
        ).scenes;
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
