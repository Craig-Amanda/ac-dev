import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import type { RuntimeMetadata, SceneInfo } from '../types.js';
import { parseRuntimeScenes } from './metadata.js';
import {
    buildProfileNameIndex,
    compareAudience,
    describeAudience,
    resolvePageAccess,
} from './page-access.js';

/**
 * The shape measured on 7 September (TESTING.md Tier 7, T22), keys only. Adding a
 * login to `menu-scene-1` made Knack insert `scene_81` (type authentication, holding
 * login view_69) above it; the protected page kept `authenticated: false`; the pages
 * beneath carry no access fields at all; `parent` is a slug throughout.
 */
function measuredMetadata(): RuntimeMetadata {
    return {
        application: {
            objects: [
                { key: 'object_1', name: 'Accounts', profile_key: 'all_users' },
                { key: 'object_2', name: 'Staff', profile_key: 'profile_2' },
                { key: 'object_3', name: 'Items' },
            ],
            scenes: [
                {
                    key: 'scene_3',
                    slug: 'items',
                    type: 'page',
                    parent: null,
                    authenticated: false,
                    views: [{ key: 'view_3', type: 'table' }],
                },
                {
                    key: 'scene_13',
                    slug: 'items-details',
                    parent: 'items',
                    object: 'object_3',
                    allowed_profiles: [],
                    views: [],
                },
                {
                    key: 'scene_81',
                    slug: 'menu-scene-1-login',
                    type: 'authentication',
                    object: null,
                    parent: null,
                    views: [
                        {
                            key: 'view_69',
                            type: 'login',
                            allowed_profiles: ['profile_2'],
                            registration_type: 'closed',
                            limit_profile_access: true,
                        },
                    ],
                },
                {
                    key: 'scene_59',
                    slug: 'menu-scene-1',
                    type: 'page',
                    parent: 'menu-scene-1-login',
                    authenticated: false,
                    login_vars: null,
                    views: [],
                },
                {
                    key: 'scene_60',
                    slug: 'new-page-1',
                    parent: 'menu-scene-1',
                    views: [],
                },
                {
                    key: 'scene_65',
                    slug: 'edit-table-13',
                    parent: 'new-page-1',
                    object: 'object_3',
                    views: [],
                },
                {
                    key: 'scene_2',
                    slug: 'account-settings',
                    type: 'user',
                    allowed_profiles: [],
                    limit_profile_access: false,
                    views: [],
                },
            ],
        },
    };
}

describe('parseRuntimeScenes keeps the access fields, only where the payload had them', () => {
    const scenes = parseRuntimeScenes(measuredMetadata());
    const byKey = new Map(scenes.map((scene) => [scene.sceneKey, scene]));

    it('copies the login view roles and the scene type', () => {
        const login = byKey.get('scene_81');
        assert.equal(login?.sceneType, 'authentication');
        assert.deepEqual(login?.views[0], {
            viewKey: 'view_69',
            viewName: undefined,
            viewType: 'login',
            allowedProfiles: ['profile_2'],
            limitProfileAccess: true,
            registrationType: 'closed',
        });
    });

    it('keeps authenticated verbatim and leaves absent fields absent', () => {
        assert.equal(byKey.get('scene_59')?.authenticated, false);
        assert.equal(byKey.get('scene_3')?.authenticated, false);
        const child = byKey.get('scene_60');
        assert.equal('authenticated' in (child ?? {}), false);
        assert.equal('allowedProfiles' in (child ?? {}), false);
        assert.equal('sceneType' in (child ?? {}), false);
        // A plain view gains nothing.
        assert.deepEqual(byKey.get('scene_3')?.views[0], {
            viewKey: 'view_3',
            viewName: undefined,
            viewType: 'table',
        });
    });

    it('keeps the user scene fields, so a snapshot preserves them', () => {
        const user = byKey.get('scene_2');
        assert.equal(user?.sceneType, 'user');
        assert.deepEqual(user?.allowedProfiles, []);
        assert.equal(user?.limitProfileAccess, false);
    });
});

describe('resolvePageAccess', () => {
    const scenes = parseRuntimeScenes(measuredMetadata());

    it('a top-level page with no login is public', () => {
        const access = resolvePageAccess('scene_3', scenes);
        assert.equal(access.status, 'public');
        assert.deepEqual(access.ancestry, ['scene_3']);
    });

    it('a child of a public page is public through the walk, not through its own fields', () => {
        const access = resolvePageAccess('scene_13', scenes);
        assert.equal(access.status, 'public');
        assert.deepEqual(access.ancestry, ['scene_13', 'scene_3']);
    });

    it('the page directly under the login is protected despite authenticated: false', () => {
        // The single most important case: this page's own field says false. Reading
        // it would report a protected page as public.
        const access = resolvePageAccess('scene_59', scenes);
        assert.equal(access.status, 'protected');
        if (access.status !== 'protected') return;
        assert.equal(access.loginSceneKey, 'scene_81');
        assert.equal(access.loginViewKey, 'view_69');
        assert.deepEqual(access.roles, ['profile_2']);
        assert.equal(access.anyLoggedInUser, false);
        assert.deepEqual(access.ancestry, ['scene_59', 'scene_81']);
    });

    it('a page three levels down inherits the same login', () => {
        const access = resolvePageAccess('scene_65', scenes);
        assert.equal(access.status, 'protected');
        if (access.status !== 'protected') return;
        assert.equal(access.loginSceneKey, 'scene_81');
        assert.deepEqual(access.roles, ['profile_2']);
        assert.deepEqual(access.ancestry, [
            'scene_65',
            'scene_60',
            'scene_59',
            'scene_81',
        ]);
    });

    it('the authentication scene itself is protected by its own login', () => {
        const access = resolvePageAccess('scene_81', scenes);
        assert.equal(access.status, 'protected');
        assert.match(access.reason, /holds the login itself/);
    });

    it('a login that does not limit profiles admits any registered user', () => {
        const open: SceneInfo[] = [
            {
                sceneKey: 'scene_1',
                sceneName: undefined,
                sceneSlug: 'gate',
                parentRef: undefined,
                sceneType: 'authentication',
                views: [
                    {
                        viewKey: 'view_1',
                        viewName: undefined,
                        viewType: 'login',
                        allowedProfiles: [],
                        limitProfileAccess: false,
                    },
                ],
            },
            {
                sceneKey: 'scene_2',
                sceneName: undefined,
                sceneSlug: 'inside',
                parentRef: 'gate',
                views: [],
            },
        ];
        const access = resolvePageAccess('scene_2', open);
        assert.equal(access.status, 'protected');
        if (access.status !== 'protected') return;
        assert.equal(access.anyLoggedInUser, true);
        assert.deepEqual(access.roles, []);
    });

    it('a login view with no role fields is protected with roles null, never an empty list', () => {
        // Null is "could not read"; [] is "nobody". A restore or a prompt that
        // treated them alike would either invent a lock-out or hide one.
        const bare: SceneInfo[] = [
            {
                sceneKey: 'scene_1',
                sceneName: undefined,
                sceneSlug: 'gate',
                parentRef: undefined,
                views: [
                    {
                        viewKey: 'view_1',
                        viewName: undefined,
                        viewType: 'login',
                    },
                ],
            },
        ];
        const access = resolvePageAccess('scene_1', bare);
        assert.equal(access.status, 'protected');
        if (access.status !== 'protected') return;
        assert.equal(access.roles, null);
        assert.match(access.reason, /cannot be read/);
    });

    it('an unresolvable parent is unknown, not public', () => {
        const orphaned: SceneInfo[] = [
            {
                sceneKey: 'scene_5',
                sceneName: undefined,
                sceneSlug: 'lost',
                parentRef: 'no-such-page',
                views: [],
            },
        ];
        const access = resolvePageAccess('scene_5', orphaned);
        assert.equal(access.status, 'unknown');
        assert.match(access.reason, /matches no page/);
    });

    it('a parent cycle is unknown rather than an endless walk', () => {
        const cycle: SceneInfo[] = [
            {
                sceneKey: 'scene_1',
                sceneName: undefined,
                sceneSlug: 'a',
                parentRef: 'b',
                views: [],
            },
            {
                sceneKey: 'scene_2',
                sceneName: undefined,
                sceneSlug: 'b',
                parentRef: 'a',
                views: [],
            },
        ];
        const access = resolvePageAccess('scene_1', cycle);
        assert.equal(access.status, 'unknown');
        assert.match(access.reason, /loops back/);
    });

    it('a missing page is unknown', () => {
        assert.equal(resolvePageAccess('scene_999', scenes).status, 'unknown');
    });
});

describe('profile names and audience wording', () => {
    const metadata = measuredMetadata();
    const scenes = parseRuntimeScenes(metadata);
    const names = buildProfileNameIndex(metadata);

    it('maps a profile key to the object that defines it', () => {
        assert.deepEqual(names.get('profile_2'), {
            profileKey: 'profile_2',
            objectKey: 'object_2',
            objectName: 'Staff',
        });
        assert.equal(names.get('all_users')?.objectKey, 'object_1');
        assert.equal(names.has('profile_9'), false);
    });

    it('names the roles in the audience sentence, key alongside', () => {
        const text = describeAudience(
            resolvePageAccess('scene_60', scenes),
            names,
        );
        assert.match(text, /only Staff \[profile_2\]/);
        assert.match(text, /login on scene_81/);
        assert.equal(
            describeAudience(resolvePageAccess('scene_3', scenes), names),
            'anyone (no login above it)',
        );
    });

    it('compares audiences, and never lets unknown read as unchanged', () => {
        const protectedPage = resolvePageAccess('scene_60', scenes);
        const publicPage = resolvePageAccess('scene_3', scenes);
        const missing = resolvePageAccess('scene_999', scenes);
        assert.equal(compareAudience(protectedPage, protectedPage), 'same');
        assert.equal(compareAudience(protectedPage, publicPage), 'changed');
        assert.equal(compareAudience(publicPage, protectedPage), 'changed');
        assert.equal(compareAudience(publicPage, publicPage), 'same');
        assert.equal(compareAudience(protectedPage, missing), 'unknown');
        assert.equal(compareAudience(missing, publicPage), 'unknown');
    });
});
