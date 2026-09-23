import assert from 'node:assert/strict';
import { test } from 'node:test';

import type { KnackApiResult } from '../http.js';
import { makeFakeContext, payloadOf } from '../testing/fake-context.js';
import {
    createObject,
    deleteObject,
    objectTools,
    updateObject,
} from './objects.js';

function setup(responses: Record<string, KnackApiResult> = {}) {
    const made = makeFakeContext({ responses });
    made.ctx.state.activeAppKey = 'Demo';
    return made;
}

const OBJECT_105 = {
    _id: 'abc',
    key: 'object_105',
    name: 'Table 1',
    identifier: 'field_2585',
    sort: { field: 'field_2585', order: 'asc' },
};

test('objectTools carries the three mutation tools at the right access levels', () => {
    assert.deepEqual(
        objectTools.map((t) => [t.name, t.access]),
        [
            ['knack_create_object', 'write'],
            ['knack_update_object', 'write'],
            ['knack_delete_object', 'delete'],
        ],
    );
    assert.ok(objectTools.every((t) => t.description.length <= 160));
});

// -------------------------------------------------------------- knack_create_object

test('knack_create_object posts name, user, isBookableResource, fields and template', async () => {
    const { ctx, requests } = setup({
        'POST /objects': {
            ok: true,
            status: 200,
            body: { object: { key: 'object_106', name: 'New Table 1' } },
        },
    });

    const payload = payloadOf(
        await createObject.handler(
            {
                name: 'New Table 1',
                userTable: false,
                isBookableResource: false,
                template: '',
                dryRun: false,
            },
            ctx,
        ),
    );

    assert.deepEqual(requests, [
        {
            apiPath: '/objects',
            method: 'POST',
            body: {
                name: 'New Table 1',
                user: false,
                isBookableResource: false,
                fields: [],
                template: '',
            },
        },
    ]);
    assert.equal(payload.ok, true);
    assert.equal(payload.action, 'create_object');
    // Small bodies are returned inline as-is (see knack_create_field's own test) —
    // the `object` projection only kicks in once getInlineDetail excludes the body.
    assert.deepEqual(payload.body, {
        object: { key: 'object_106', name: 'New Table 1' },
    });
});

test('knack_create_object dryRun previews without a request', async () => {
    const { ctx, requests } = setup();

    const payload = payloadOf(
        await createObject.handler(
            {
                name: 'New Table 1',
                userTable: false,
                isBookableResource: false,
                template: '',
                dryRun: true,
            },
            ctx,
        ),
    );

    assert.equal(requests.length, 0);
    assert.equal(payload.action, 'create_object_dry_run');
    assert.deepEqual(payload.wouldCreate, {
        name: 'New Table 1',
        user: false,
        isBookableResource: false,
        fields: [],
        template: '',
    });
});

test('knack_create_object rejects a blank name before making a request', async () => {
    const { ctx, requests } = setup();

    const payload = payloadOf(
        await createObject.handler(
            {
                name: '   ',
                userTable: false,
                isBookableResource: false,
                template: '',
                dryRun: false,
            },
            ctx,
        ),
    );

    assert.equal(requests.length, 0);
    assert.equal(payload.ok, false);
    assert.equal(payload.action, 'create_object_preflight');
});

// -------------------------------------------------------------- knack_update_object

test('knack_update_object requires at least one field to change', async () => {
    const { ctx, requests } = setup();

    const payload = payloadOf(
        await updateObject.handler(
            { objectKey: 'object_105', dryRun: false },
            ctx,
        ),
    );

    assert.equal(requests.length, 0);
    assert.equal(payload.ok, false);
    assert.equal(payload.action, 'update_object_preflight');
});

test('knack_update_object merges the rename into the fetched current name/identifier/sort', async () => {
    const { ctx, requests } = setup({
        'GET /objects/object_105': {
            ok: true,
            status: 200,
            body: { object: OBJECT_105 },
        },
        'PUT /objects/object_105': {
            ok: true,
            status: 200,
            body: {
                object: { ...OBJECT_105, name: 'Renaming Table 1' },
            },
        },
    });

    const payload = payloadOf(
        await updateObject.handler(
            {
                objectKey: 'object_105',
                name: 'Renaming Table 1',
                dryRun: false,
            },
            ctx,
        ),
    );

    assert.deepEqual(requests, [
        { apiPath: '/objects/object_105', method: 'GET', body: null },
        {
            apiPath: '/objects/object_105',
            method: 'PUT',
            body: {
                name: 'Renaming Table 1',
                identifier: 'field_2585',
                sort: { field: 'field_2585', order: 'asc' },
            },
        },
    ]);
    assert.equal(payload.ok, true);
    assert.equal(payload.action, 'update_object');
});

test('knack_update_object dryRun previews the merge without a PUT', async () => {
    const { ctx, requests } = setup({
        'GET /objects/object_105': {
            ok: true,
            status: 200,
            body: { object: OBJECT_105 },
        },
    });

    const payload = payloadOf(
        await updateObject.handler(
            { objectKey: 'object_105', name: 'Renaming Table 1', dryRun: true },
            ctx,
        ),
    );

    assert.deepEqual(requests, [
        { apiPath: '/objects/object_105', method: 'GET', body: null },
    ]);
    assert.equal(payload.action, 'update_object_dry_run');
    assert.deepEqual(payload.wouldUpdate, {
        name: 'Renaming Table 1',
        identifier: 'field_2585',
        sort: { field: 'field_2585', order: 'asc' },
    });
});

// -------------------------------------------------------------- knack_delete_object

test('knack_delete_object previews and names the table without deleting when confirm is omitted', async () => {
    const { ctx, requests } = setup({
        'GET /objects/object_105': {
            ok: true,
            status: 200,
            body: {
                object: { ...OBJECT_105, fields: [{ key: 'field_2585' }] },
            },
        },
    });

    const payload = payloadOf(
        await deleteObject.handler(
            { objectKey: 'object_105', confirm: false },
            ctx,
        ),
    );

    assert.deepEqual(requests, [
        { apiPath: '/objects/object_105', method: 'GET', body: null },
    ]);
    assert.equal(payload.ok, false);
    assert.equal(payload.action, 'delete_object_preflight');
    assert.equal(payload.wouldDeleteName, 'Table 1');
    assert.equal(payload.wouldDeleteFieldCount, 1);
    assert.match(payload.message as string, /permanently delete/);
});

test('knack_delete_object deletes once confirm is true', async () => {
    const { ctx, requests } = setup({
        'DELETE /objects/object_105': { ok: true, status: 200, body: {} },
    });

    const payload = payloadOf(
        await deleteObject.handler(
            { objectKey: 'object_105', confirm: true },
            ctx,
        ),
    );

    assert.deepEqual(requests, [
        { apiPath: '/objects/object_105', method: 'DELETE', body: null },
    ]);
    assert.equal(payload.action, 'delete_object');
    assert.equal(payload.ok, true);
});
