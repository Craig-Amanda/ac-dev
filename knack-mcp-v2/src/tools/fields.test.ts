import assert from 'node:assert/strict';
import { test } from 'node:test';

import type { KnackApiResult } from '../http.js';
import {
    NESTED_MERGE_UNCERTAINTY_NOTE,
    SCHEMA_CACHE_STALE_NOTE,
} from '../lib/field-payload.js';
import { makeFakeContext, payloadOf } from '../testing/fake-context.js';
import {
    createField,
    deleteField,
    duplicateField,
    fieldTools,
    updateField,
} from './fields.js';
import { RUNTIME_METADATA } from '../testing/schema-fixture.js';

const RAW_FIELDS = [
    {
        key: 'field_1',
        _id: 'abc',
        name: 'Name',
        type: 'short_text',
        required: true,
        meta: { description: 'Customer name _ktlHide' },
    },
    {
        key: 'field_2',
        _id: 'def',
        name: 'Total',
        type: 'equation',
        format: { equation: '{field_3} * 2', equation_type: 'numeric' },
        description: 'Legacy top-level description',
    },
];

const OBJECT_RESPONSE: KnackApiResult = {
    ok: true,
    status: 200,
    body: {
        object: { key: 'object_1', name: 'Customers', fields: RAW_FIELDS },
    },
};

/** A write response Knack pads with the whole application schema (> inline limit). */
function bloatedSchemaResponse(field: Record<string, unknown>): KnackApiResult {
    return {
        ok: true,
        status: 200,
        body: {
            application: {
                objects: [
                    {
                        key: 'object_1',
                        name: 'Customers',
                        fields: [...RAW_FIELDS, field],
                    },
                    { key: 'object_2', name: 'Companies', fields: [] },
                ],
                padding: 'x'.repeat(60 * 1024),
            },
        },
    };
}

function setup(
    responses: Record<string, KnackApiResult> = {},
    metadata = RUNTIME_METADATA,
) {
    const made = makeFakeContext({
        runtimeMetadata: { Demo: metadata },
        responses: { 'GET /objects/object_1': OBJECT_RESPONSE, ...responses },
    });
    made.ctx.state.activeAppKey = 'Demo';
    return made;
}

test('fieldTools carries the four mutation tools at the right access levels', () => {
    assert.deepEqual(
        fieldTools.map((t) => [t.name, t.access]),
        [
            ['knack_create_field', 'write'],
            ['knack_update_field', 'write'],
            ['knack_delete_field', 'delete'],
            ['knack_duplicate_field', 'write'],
        ],
    );
    assert.ok(fieldTools.every((t) => t.description.length <= 120));
});

// ---------------------------------------------------------------- knack_create_field

test('knack_create_field posts the definition with description mirrored into meta', async () => {
    const { ctx, requests } = setup({
        'POST /objects/object_1/fields': {
            ok: true,
            status: 200,
            body: {
                field: {
                    key: 'field_8',
                    name: 'Notes',
                    type: 'paragraph_text',
                },
            },
        },
    });
    const payload = payloadOf(
        await createField.handler(
            {
                objectKey: 'object_1',
                name: 'Notes',
                type: 'paragraph_text',
                required: false,
                unique: false,
                description: 'Free text',
                dryRun: false,
            },
            ctx,
        ),
    );
    assert.deepEqual(requests, [
        {
            apiPath: '/objects/object_1/fields',
            method: 'POST',
            body: {
                name: 'Notes',
                type: 'paragraph_text',
                required: false,
                unique: false,
                description: 'Free text',
                meta: { description: 'Free text' },
            },
        },
    ]);
    assert.equal(payload.ok, true);
    assert.equal(payload.action, 'create_field');
    assert.equal(payload.status, 200);
    assert.equal(payload.cacheNote, SCHEMA_CACHE_STALE_NOTE);
    assert.deepEqual(payload.body, {
        field: { key: 'field_8', name: 'Notes', type: 'paragraph_text' },
    });
});

test('knack_create_field dryRun validates the equation and sends nothing', async () => {
    const { ctx, requests } = setup();
    const payload = payloadOf(
        await createField.handler(
            {
                objectKey: 'object_1',
                name: 'Double',
                type: 'equation',
                required: false,
                unique: false,
                format: JSON.stringify({
                    equation: '{field_3} + {field_4.field_6}',
                }),
                dryRun: true,
            },
            ctx,
        ),
    );
    assert.equal(requests.length, 0);
    assert.equal(payload.ok, true);
    assert.equal(payload.action, 'create_field_dry_run');
    assert.equal(payload.dryRun, true);
    assert.deepEqual(payload.wouldCreate, {
        name: 'Double',
        type: 'equation',
        required: false,
        unique: false,
        format: { equation: '{field_3} + {field_4.field_6}' },
    });
    assert.equal(payload.equationWarnings, undefined);
});

test('knack_create_field blocks an equation referencing an unknown field', async () => {
    const { ctx, requests } = setup();
    const payload = payloadOf(
        await createField.handler(
            {
                objectKey: 'object_1',
                name: 'Broken',
                type: 'equation',
                required: false,
                unique: false,
                format: JSON.stringify({ equation: '{field_6} * 2' }),
                dryRun: false,
            },
            ctx,
        ),
    );
    assert.equal(requests.length, 0);
    assert.equal(payload.ok, false);
    assert.equal(payload.action, 'create_field_preflight');
    assert.match(
        (payload.errors as string[])[0],
        /does not match any field on object_1/,
    );
    assert.match(
        (payload.errors as string[])[0],
        /did you mean \{field_4\.field_6\}/,
    );
});

test('knack_create_field blocks a connection without a target object and bad JSON', async () => {
    const { ctx, requests } = setup();
    const payload = payloadOf(
        await createField.handler(
            {
                objectKey: 'object_1',
                name: 'Link',
                type: 'connection',
                required: false,
                unique: false,
                relationship: '{not json',
                dryRun: false,
            },
            ctx,
        ),
    );
    assert.equal(requests.length, 0);
    assert.equal(payload.ok, false);
    const errors = payload.errors as string[];
    assert.match(errors[0], /relationship must be valid JSON/);
    assert.match(
        errors[1],
        /Connection fields require format.object or relationship.object/,
    );
});

test('knack_create_field warns instead of blocking when no schema is available', async () => {
    const { ctx, requests } = setup(
        {},
        null as unknown as typeof RUNTIME_METADATA,
    );
    const payload = payloadOf(
        await createField.handler(
            {
                objectKey: 'object_1',
                name: 'Unchecked',
                type: 'equation',
                required: false,
                unique: false,
                format: JSON.stringify({ equation: '{field_3}' }),
                dryRun: true,
            },
            ctx,
        ),
    );
    assert.equal(requests.length, 0);
    assert.equal(payload.ok, true);
    assert.match(
        (payload.equationWarnings as string[])[0],
        /going out unchecked/,
    );
});

test('knack_create_field projects a full-schema response down to the created field', async () => {
    const created = { key: 'field_9', name: 'Owner', type: 'connection' };
    const { ctx } = setup({
        'POST /objects/object_1/fields': bloatedSchemaResponse(created),
    });
    const payload = payloadOf(
        await createField.handler(
            {
                objectKey: 'object_1',
                name: 'Owner',
                type: 'connection',
                required: false,
                unique: false,
                relationship: JSON.stringify({
                    object: 'object_2',
                    has: 'one',
                    belongs_to: 'many',
                }),
                dryRun: false,
            },
            ctx,
        ),
    );
    assert.equal(payload.ok, true);
    assert.deepEqual(payload.field, created);
    assert.equal(payload.body, undefined);
    assert.ok(
        typeof payload.bodySizeBytes === 'number' &&
            payload.bodySizeBytes > 48 * 1024,
    );
    assert.ok(payload.bodySummary);
    assert.match(String(payload.note), /projected down to the created field/);
    assert.equal(payload.cacheNote, SCHEMA_CACHE_STALE_NOTE);
});

test('knack_create_field passes a failed write through without a cache note', async () => {
    const { ctx } = setup({
        'POST /objects/object_1/fields': {
            ok: false,
            status: 400,
            body: { errors: ['bad'] },
        },
    });
    const payload = payloadOf(
        await createField.handler(
            {
                objectKey: 'object_1',
                name: 'X',
                type: 'short_text',
                required: false,
                unique: false,
                dryRun: false,
            },
            ctx,
        ),
    );
    assert.equal(payload.ok, false);
    assert.equal(payload.status, 400);
    assert.equal(payload.cacheNote, undefined);
});

// ---------------------------------------------------------------- knack_update_field

const UPDATE_BASE = {
    objectKey: 'object_1',
    fieldKey: 'field_1',
    confirmRemoveKtlKeywords: false,
    dryRun: false,
};

test('knack_update_field refuses an empty update', async () => {
    const { ctx, requests } = setup();
    const payload = payloadOf(
        await updateField.handler({ ...UPDATE_BASE }, ctx),
    );
    assert.equal(requests.length, 0);
    assert.equal(payload.ok, false);
    assert.equal(payload.action, 'update_field_preflight');
    assert.deepEqual(payload.errors, [
        'Provide updates and/or description — nothing to update.',
    ]);
});

test('knack_update_field PUTs a rename without fetching the object first', async () => {
    const { ctx, requests } = setup({
        'PUT /objects/object_1/fields/field_1': {
            ok: true,
            status: 200,
            body: { field: { key: 'field_1' } },
        },
    });
    const payload = payloadOf(
        await updateField.handler(
            { ...UPDATE_BASE, updates: JSON.stringify({ name: 'Full Name' }) },
            ctx,
        ),
    );
    assert.deepEqual(requests, [
        {
            apiPath: '/objects/object_1/fields/field_1',
            method: 'PUT',
            body: { name: 'Full Name' },
        },
    ]);
    assert.equal(payload.ok, true);
    assert.equal(payload.action, 'update_field');
    assert.equal(payload.cacheNote, SCHEMA_CACHE_STALE_NOTE);
    assert.equal(payload.mergeNote, undefined);
});

test('knack_update_field adds the merge note when format is touched', async () => {
    const { ctx, requests } = setup({
        'PUT /objects/object_1/fields/field_2': {
            ok: true,
            status: 200,
            body: { field: { key: 'field_2' } },
        },
    });
    const payload = payloadOf(
        await updateField.handler(
            {
                ...UPDATE_BASE,
                fieldKey: 'field_2',
                updates: JSON.stringify({
                    format: { equation: '{field_3} * 3' },
                }),
            },
            ctx,
        ),
    );
    assert.equal(requests[0].method, 'PUT');
    assert.equal(payload.ok, true);
    assert.equal(payload.mergeNote, NESTED_MERGE_UNCERTAINTY_NOTE);
});

test('knack_update_field blocks an equation that crosses a many connection', async () => {
    const { ctx, requests } = setup();
    const payload = payloadOf(
        await updateField.handler(
            {
                ...UPDATE_BASE,
                fieldKey: 'field_2',
                updates: JSON.stringify({
                    format: { equation: '{field_7.field_6}' },
                }),
            },
            ctx,
        ),
    );
    assert.equal(requests.length, 0);
    assert.equal(payload.ok, false);
    assert.match(
        (payload.errors as string[])[0],
        /allows multiple connected records/,
    );
});

test('knack_update_field dryRun fetches the current field and previews the merge', async () => {
    const { ctx, requests } = setup();
    const payload = payloadOf(
        await updateField.handler(
            {
                ...UPDATE_BASE,
                fieldKey: 'field_2',
                dryRun: true,
                updates: JSON.stringify({
                    name: 'Triple',
                    format: { equation: '{field_3} * 3' },
                }),
            },
            ctx,
        ),
    );
    assert.deepEqual(requests, [
        { apiPath: '/objects/object_1', method: 'GET', body: null },
    ]);
    assert.equal(payload.ok, true);
    assert.equal(payload.action, 'update_field_dry_run');
    assert.equal(payload.dryRun, true);
    assert.deepEqual(payload.currentField, RAW_FIELDS[1]);
    assert.deepEqual(payload.changes, {
        name: { from: 'Total', to: 'Triple' },
        format: {
            from: { equation: '{field_3} * 2', equation_type: 'numeric' },
            to: { equation: '{field_3} * 3', equation_type: 'numeric' },
        },
    });
    assert.equal(payload.mergeNote, NESTED_MERGE_UNCERTAINTY_NOTE);
});

test('knack_update_field dryRun reports a field it cannot fetch', async () => {
    const { ctx } = setup();
    const payload = payloadOf(
        await updateField.handler(
            {
                ...UPDATE_BASE,
                fieldKey: 'field_77',
                dryRun: true,
                updates: JSON.stringify({ name: 'X' }),
            },
            ctx,
        ),
    );
    assert.equal(payload.ok, false);
    assert.equal(payload.action, 'update_field_dry_run');
    assert.equal(
        payload.message,
        'Could not fetch current definition for field_77 on object_1.',
    );
    assert.equal(payload.status, 200);
});

test('knack_update_field blocks a description change that drops a KTL keyword', async () => {
    const { ctx, requests } = setup();
    const payload = payloadOf(
        await updateField.handler(
            { ...UPDATE_BASE, description: 'Customer full name' },
            ctx,
        ),
    );
    assert.deepEqual(
        requests.map((r) => r.method),
        ['GET'],
    );
    assert.equal(payload.ok, false);
    assert.equal(payload.action, 'update_field_preflight');
    assert.match(
        (payload.errors as string[])[0],
        /would drop existing KTL keyword\(s\).*_ktlHide/,
    );
    assert.equal(payload.currentDescription, 'Customer name _ktlHide');
    assert.deepEqual(payload.droppedKtlKeywords, ['_ktlHide']);
});

test('knack_update_field lets a description keep its KTL keyword through', async () => {
    const { ctx, requests } = setup({
        'PUT /objects/object_1/fields/field_1': {
            ok: true,
            status: 200,
            body: { field: { key: 'field_1' } },
        },
    });
    const payload = payloadOf(
        await updateField.handler(
            { ...UPDATE_BASE, description: 'Customer full name (_ktlHide)' },
            ctx,
        ),
    );
    assert.deepEqual(
        requests.map((r) => r.method),
        ['GET', 'PUT'],
    );
    assert.deepEqual(requests[1].body, {
        description: 'Customer full name (_ktlHide)',
        meta: { description: 'Customer full name (_ktlHide)' },
    });
    assert.equal(payload.ok, true);
    assert.equal(payload.ktlKeywordWarnings, undefined);
});

test('knack_update_field drops a KTL keyword only with confirmRemoveKtlKeywords', async () => {
    const { ctx, requests } = setup({
        'PUT /objects/object_1/fields/field_1': {
            ok: true,
            status: 200,
            body: { field: { key: 'field_1' } },
        },
    });
    const payload = payloadOf(
        await updateField.handler(
            {
                ...UPDATE_BASE,
                updates: JSON.stringify({
                    description: 'ignored',
                    name: 'Name',
                }),
                description: '',
                confirmRemoveKtlKeywords: true,
            },
            ctx,
        ),
    );
    assert.deepEqual(
        requests.map((r) => r.method),
        ['GET', 'PUT'],
    );
    // The dedicated parameter wins over the description inside `updates`.
    assert.deepEqual(requests[1].body, {
        name: 'Name',
        description: '',
        meta: { description: '' },
    });
    assert.equal(payload.ok, true);
});

test('knack_update_field dryRun shows the current description from meta', async () => {
    const { ctx } = setup();
    const payload = payloadOf(
        await updateField.handler(
            {
                ...UPDATE_BASE,
                dryRun: true,
                description: 'Customer full name _ktlHide',
            },
            ctx,
        ),
    );
    assert.equal(payload.ok, true);
    const changes = payload.changes as Record<
        string,
        { from: unknown; to: unknown }
    >;
    assert.deepEqual(changes.description, {
        from: 'Customer name _ktlHide',
        to: 'Customer full name _ktlHide',
    });
    assert.deepEqual(changes.meta, {
        from: { description: 'Customer name _ktlHide' },
        to: { description: 'Customer full name _ktlHide' },
    });
});

test('knack_update_field warns when the KTL check cannot run', async () => {
    const { ctx, requests } = setup({
        'GET /objects/object_1': { ok: false, status: 500, body: null },
        'PUT /objects/object_1/fields/field_1': {
            ok: true,
            status: 200,
            body: { field: { key: 'field_1' } },
        },
    });
    const payload = payloadOf(
        await updateField.handler({ ...UPDATE_BASE, description: 'New' }, ctx),
    );
    assert.deepEqual(
        requests.map((r) => r.method),
        ['GET', 'PUT'],
    );
    assert.equal(payload.ok, true);
    assert.match(
        (payload.ktlKeywordWarnings as string[])[0],
        /Could not fetch the current field/,
    );
});

test('knack_update_field projects a full-schema response down to the updated field', async () => {
    const { ctx } = setup({
        'PUT /objects/object_1/fields/field_2': bloatedSchemaResponse({
            key: 'field_9',
        }),
    });
    const payload = payloadOf(
        await updateField.handler(
            {
                ...UPDATE_BASE,
                fieldKey: 'field_2',
                updates: JSON.stringify({
                    relationship: { object: 'object_2' },
                }),
            },
            ctx,
        ),
    );
    assert.equal(payload.ok, true);
    assert.deepEqual(payload.field, RAW_FIELDS[1]);
    assert.equal(payload.body, undefined);
    assert.match(String(payload.note), /projected down to the updated field/);
    assert.equal(payload.cacheNote, SCHEMA_CACHE_STALE_NOTE);
    assert.equal(payload.mergeNote, NESTED_MERGE_UNCERTAINTY_NOTE);
});

test('knack_update_field rejects malformed updates JSON before any request', async () => {
    const { ctx, requests } = setup();
    const payload = payloadOf(
        await updateField.handler({ ...UPDATE_BASE, updates: '[1,2]' }, ctx),
    );
    assert.equal(requests.length, 0);
    assert.equal(payload.ok, false);
    assert.deepEqual(payload.errors, ['updates must be a JSON object.']);
});

// ---------------------------------------------------------------- knack_delete_field

test('knack_delete_field issues a DELETE and adds the cache note on success', async () => {
    const { ctx, requests } = setup({
        'DELETE /objects/object_1/fields/field_3': {
            ok: true,
            status: 200,
            body: { deleted: true },
        },
    });
    const payload = payloadOf(
        await deleteField.handler(
            { objectKey: 'object_1', fieldKey: 'field_3' },
            ctx,
        ),
    );
    assert.deepEqual(requests, [
        {
            apiPath: '/objects/object_1/fields/field_3',
            method: 'DELETE',
            body: null,
        },
    ]);
    assert.equal(payload.ok, true);
    assert.equal(payload.action, 'delete_field');
    assert.equal(payload.cacheNote, SCHEMA_CACHE_STALE_NOTE);
});

test('knack_delete_field passes a failure through without a cache note', async () => {
    const { ctx } = setup();
    const payload = payloadOf(
        await deleteField.handler(
            { objectKey: 'object_1', fieldKey: 'field_3' },
            ctx,
        ),
    );
    assert.equal(payload.ok, false);
    assert.equal(payload.status, 404);
    assert.equal(payload.cacheNote, undefined);
});

// ---------------------------------------------------------------- knack_duplicate_field

test('knack_duplicate_field clones the source without key/_id and mirrors description', async () => {
    const { ctx, requests } = setup({
        'POST /objects/object_1/fields': {
            ok: true,
            status: 200,
            body: { field: { key: 'field_10' } },
        },
    });
    const payload = payloadOf(
        await duplicateField.handler(
            {
                objectKey: 'object_1',
                sourceFieldKey: 'field_2',
                newName: 'Total Copy',
            },
            ctx,
        ),
    );
    assert.deepEqual(requests, [
        { apiPath: '/objects/object_1', method: 'GET', body: null },
        {
            apiPath: '/objects/object_1/fields',
            method: 'POST',
            body: {
                name: 'Total Copy',
                type: 'equation',
                format: { equation: '{field_3} * 2', equation_type: 'numeric' },
                description: 'Legacy top-level description',
                meta: { description: 'Legacy top-level description' },
            },
        },
    ]);
    assert.equal(payload.ok, true);
    assert.equal(payload.action, 'duplicate_field');
    assert.equal(payload.sourceFieldKey, 'field_2');
    assert.equal(payload.newName, 'Total Copy');
    assert.equal(payload.cacheNote, SCHEMA_CACHE_STALE_NOTE);
});

test('knack_duplicate_field reports a missing source field', async () => {
    const { ctx, requests } = setup();
    const payload = payloadOf(
        await duplicateField.handler(
            { objectKey: 'object_1', sourceFieldKey: 'field_77', newName: 'X' },
            ctx,
        ),
    );
    assert.deepEqual(
        requests.map((r) => r.method),
        ['GET'],
    );
    assert.equal(payload.ok, false);
    assert.equal(
        payload.message,
        'Source field field_77 not found on object_1.',
    );
});

test('knack_duplicate_field reports an object it cannot fetch', async () => {
    const { ctx } = setup({
        'GET /objects/object_1': { ok: false, status: 500, body: null },
    });
    const payload = payloadOf(
        await duplicateField.handler(
            { objectKey: 'object_1', sourceFieldKey: 'field_1', newName: 'X' },
            ctx,
        ),
    );
    assert.equal(payload.ok, false);
    assert.equal(payload.message, 'Could not fetch object fields.');
});

test('knack_duplicate_field projects a full-schema response down to the copy', async () => {
    const copy = { key: 'field_11', name: 'Name Copy', type: 'short_text' };
    const { ctx } = setup({
        'POST /objects/object_1/fields': bloatedSchemaResponse(copy),
    });
    const payload = payloadOf(
        await duplicateField.handler(
            {
                objectKey: 'object_1',
                sourceFieldKey: 'field_1',
                newName: 'Name Copy',
            },
            ctx,
        ),
    );
    assert.equal(payload.ok, true);
    assert.deepEqual(payload.field, copy);
    assert.match(
        String(payload.note),
        /projected down to the duplicated field/,
    );
});
