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
    fields: [
        { key: 'field_2585', name: 'Name', type: 'name' },
        { key: 'field_2586', name: 'Rank', type: 'number' },
    ],
};

/** A fake Knack whose object PUT merges at the top level, as measured live. */
function setupStateful() {
    const stored: Record<string, unknown> = structuredClone(OBJECT_105);
    const made = makeFakeContext({
        responses: (apiPath, init) => {
            if (apiPath !== '/objects/object_105') {
                return { ok: false, status: 404, body: {} };
            }
            if (init?.method === 'PUT') {
                Object.assign(stored, JSON.parse(init.body as string));
            }
            return { ok: true, status: 200, body: { object: stored } };
        },
    });
    made.ctx.state.activeAppKey = 'Demo';
    return { ...made, stored };
}

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
    const { ctx, requests } = setupStateful();

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
        // The read-back.
        { apiPath: '/objects/object_105', method: 'GET', body: null },
    ]);
    assert.equal(payload.ok, true);
    assert.equal(payload.action, 'update_object');
    assert.equal(payload.verified, true);
});

test('knack_update_object changes the display field and sort, and verifies them', async () => {
    const { ctx, stored } = setupStateful();
    const payload = payloadOf(
        await updateObject.handler(
            {
                objectKey: 'object_105',
                identifier: 'field_2586',
                sortField: 'field_2586',
                sortOrder: 'desc',
                dryRun: false,
            },
            ctx,
        ),
    );
    assert.equal(payload.verified, true, JSON.stringify(payload));
    assert.equal(stored.identifier, 'field_2586');
    assert.deepEqual(stored.sort, { field: 'field_2586', order: 'desc' });
    assert.equal(stored.name, 'Table 1');
});

test('knack_update_object refuses a field that is not on the object, which Knack would store', async () => {
    const { ctx, requests } = setupStateful();
    for (const args of [
        { identifier: 'field_1' },
        { sortField: 'field_99999' },
    ]) {
        const payload = payloadOf(
            await updateObject.handler(
                { objectKey: 'object_105', dryRun: false, ...args },
                ctx,
            ),
        );
        assert.equal(payload.ok, false);
        assert.match(
            JSON.stringify(payload.errors),
            /is not a field on object_105/,
        );
    }
    assert.equal(
        requests.some((request) => request.method === 'PUT'),
        false,
    );
});

test('knack_update_object refuses sortOrder alone when there is no sort field yet', async () => {
    const { ctx, stored, requests } = setupStateful();
    delete stored.sort;
    const payload = payloadOf(
        await updateObject.handler(
            { objectKey: 'object_105', sortOrder: 'desc', dryRun: false },
            ctx,
        ),
    );
    assert.equal(payload.ok, false);
    assert.match(JSON.stringify(payload.errors), /needs sortField/);
    assert.equal(
        requests.some((request) => request.method === 'PUT'),
        false,
    );
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

test('knack_update_object on a table with no default sort sends no sort and verifies', async () => {
    const { ctx, stored, requests } = setupStateful();
    delete stored.sort;
    const payload = payloadOf(
        await updateObject.handler(
            {
                objectKey: 'object_105',
                identifier: 'field_2586',
                dryRun: false,
            },
            ctx,
        ),
    );
    const put = requests.find((request) => request.method === 'PUT');
    assert.equal('sort' in (put?.body as Record<string, unknown>), false);
    assert.equal(payload.verified, true, JSON.stringify(payload));
});
