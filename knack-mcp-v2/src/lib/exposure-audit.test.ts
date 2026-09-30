import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import type { RuntimeMetadata } from '../types.js';
import {
    auditExposure,
    findTypedEmails,
    maskEmailAddress,
    typedEmailsInEmailRules,
} from './exposure-audit.js';
import { parseRuntimeScenes } from './metadata.js';

describe('maskEmailAddress', () => {
    it('hides the local part and keeps the domain', () => {
        assert.equal(
            maskEmailAddress('jane.doe@example.com'),
            'j***@example.com',
        );
        assert.equal(maskEmailAddress('@nope'), '***');
    });
});

describe('findTypedEmails', () => {
    it('finds addresses in an email rule and marks them as email settings', () => {
        const rule = {
            key: 'submit_2',
            action: 'email',
            email: {
                subject: 'New entry',
                message: 'Reply to office@example.org or {field_4}',
                recipients: [
                    { recipient_mode: 'to', email: 'jane.doe@example.com' },
                    { recipient_mode: 'cc', field: 'field_9' },
                ],
            },
        };
        assert.deepEqual(findTypedEmails(rule), [
            {
                address: 'o***@example.org',
                path: '$.email.message',
                inEmail: true,
            },
            {
                address: 'j***@example.com',
                path: '$.email.recipients.0.email',
                inEmail: true,
            },
        ]);
    });

    it('never matches a Knack placeholder', () => {
        assert.deepEqual(
            findTypedEmails({
                email: { message: 'Hi {field_4}, {field_5.field_6}' },
            }),
            [],
        );
    });

    it('marks an address outside email settings as plain text', () => {
        assert.deepEqual(findTypedEmails({ title: 'Call help@example.com' }), [
            { address: 'h***@example.com', path: '$.title', inEmail: false },
        ]);
    });

    it('reads a JSON string argument by structure', () => {
        const args = {
            viewKey: 'view_1',
            rules: JSON.stringify([
                { action: 'email', email: { to: 'boss@example.com' } },
            ]),
        };
        assert.deepEqual(typedEmailsInEmailRules(args), [
            {
                address: 'b***@example.com',
                path: '$.rules.0.email.to',
                inEmail: true,
            },
        ]);
    });

    it('stops on a cycle', () => {
        const node: Record<string, unknown> = {
            email: { to: 'a@example.com' },
        };
        node.self = node;
        assert.equal(findTypedEmails(node).length, 1);
    });
});

describe('auditExposure', () => {
    const metadata: RuntimeMetadata = {
        application: {
            objects: [{ key: 'object_1', name: 'Enquiries' }],
            scenes: [
                {
                    key: 'scene_1',
                    slug: 'contact',
                    type: 'page',
                    parent: null,
                    views: [
                        { key: 'view_1', type: 'form', name: 'Contact us' },
                        { key: 'view_2', type: 'rich_text' },
                    ],
                },
                {
                    key: 'scene_2',
                    slug: 'staff-login',
                    type: 'authentication',
                    parent: null,
                    views: [{ key: 'view_3', type: 'login' }],
                },
                {
                    key: 'scene_3',
                    slug: 'staff',
                    parent: 'staff-login',
                    views: [{ key: 'view_4', type: 'form', name: 'Edit' }],
                },
                {
                    key: 'scene_4',
                    slug: 'orphan',
                    parent: 'missing-parent',
                    views: [{ key: 'view_5', type: 'form' }],
                },
            ],
        },
    };
    const scenes = parseRuntimeScenes(metadata);
    const viewMap = {
        view_1: {
            name: 'Contact us',
            type: 'form',
            action: 'insert',
            source: { object: 'object_1' },
            rules: {
                submits: [
                    {
                        action: 'email',
                        email: { recipients: [{ email: 'jane@example.com' }] },
                    },
                ],
            },
        },
        view_2: { type: 'rich_text', content: 'Questions? help@example.com' },
        view_4: { type: 'form', action: 'update' },
        view_5: { type: 'form' },
    };
    const viewScenes = {
        view_1: { sceneKey: 'scene_1' },
        view_2: { sceneKey: 'scene_1' },
    };
    const tasks = [
        {
            key: 'task_1',
            name: 'Weekly digest',
            object_key: 'object_1',
            action: {
                email: { to: 'digest@example.com', message: '{field_1}' },
            },
        },
    ];

    it('lists typed addresses in views and tasks, with where they sit', () => {
        const audit = auditExposure({ viewMap, scenes, viewScenes, tasks });
        assert.deepEqual(
            audit.typedEmails.map((hit) => [
                hit.where,
                hit.viewKey ?? hit.taskKey,
                hit.address,
                hit.inEmail,
            ]),
            [
                ['view', 'view_1', 'j***@example.com', true],
                ['view', 'view_2', 'h***@example.com', false],
                ['task', 'task_1', 'd***@example.com', true],
            ],
        );
        assert.equal(audit.typedEmails[0].sceneKey, 'scene_1');
        assert.equal(audit.typedEmails[2].path, '$.action.email.to');
    });

    it('lists forms on public pages and pages of unknown access, not protected ones', () => {
        const audit = auditExposure({ viewMap, scenes, viewScenes, tasks });
        assert.deepEqual(
            audit.publicForms.map((form) => [
                form.viewKey,
                form.access,
                form.action,
                form.objectKey,
            ]),
            [
                ['view_1', 'public', 'insert', 'object_1'],
                ['view_5', 'unknown', null, null],
            ],
        );
        assert.equal(audit.truncated, false);
    });

    it('caps each list and says so', () => {
        const audit = auditExposure({ viewMap, scenes, viewScenes, tasks }, 1);
        assert.equal(audit.typedEmails.length, 1);
        assert.equal(audit.publicForms.length, 1);
        assert.equal(audit.truncated, true);
    });
});

describe('auditExposure and Knack account pages', () => {
    const scenes = parseRuntimeScenes({
        application: {
            objects: [],
            scenes: [
                {
                    key: 'scene_2',
                    slug: 'account-settings',
                    type: 'user',
                    allowed_profiles: [],
                    limit_profile_access: false,
                    views: [{ key: 'view_1', type: 'form', name: 'Account' }],
                },
                {
                    key: 'scene_9',
                    slug: 'preferences',
                    parent: 'account-settings',
                    views: [{ key: 'view_9', type: 'form', name: 'Prefs' }],
                },
                {
                    key: 'scene_3',
                    slug: 'contact',
                    type: 'page',
                    parent: null,
                    views: [{ key: 'view_3', type: 'form', name: 'Contact' }],
                },
            ],
        },
    });

    it('lists forms on an account page, or beneath one, apart from public forms', () => {
        const audit = auditExposure({
            viewMap: {},
            scenes,
            viewScenes: {},
            tasks: [],
        });
        assert.deepEqual(
            audit.publicForms.map((form) => form.viewKey),
            ['view_3'],
        );
        assert.deepEqual(
            audit.accountForms.map((form) => [form.viewKey, form.access]),
            [
                ['view_1', 'account'],
                ['view_9', 'account'],
            ],
        );
        assert.match(audit.accountForms[0].reason, /only to a logged-in user/);
    });
});
