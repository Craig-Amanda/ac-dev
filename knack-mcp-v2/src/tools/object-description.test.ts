import assert from 'node:assert/strict';
import { test } from 'node:test';

import type { KnackApiResult } from '../http.js';
import { makeFakeContext, payloadOf } from '../testing/fake-context.js';
import { deleteField } from './fields.js';
import { createObject, updateObject } from './objects.js';

const STAMP = /_notes=\[Craig on \d{4}-\d{2}-\d{2}\]/;

type Field = Record<string, unknown>;
type World = {
    fields: Field[];
    /** What POST /objects answers with as the new object's fields. */
    createResponseFields: 'listed' | 'omitted';
    /** Make the PUT to the auto-increment field fail. */
    failFieldPut?: boolean;
};

const ok = (body: unknown): KnackApiResult => ({ ok: true, status: 200, body });

/**
 * A fake Knack holding one object, object_106, whose fields the world controls.
 * It stores descriptions the way Knack does (top level and meta) so a read-back sees
 * what was written.
 */
function setup(world: World) {
    const stored = { key: 'object_106', name: 'Clients', fields: world.fields };
    const made = makeFakeContext({
        responses: (apiPath, init) => {
            const method = (init?.method ?? 'GET').toUpperCase();
            const body = init?.body ? JSON.parse(String(init.body)) : {};
            if (method === 'POST' && apiPath === '/objects') {
                return ok({
                    object: {
                        key: 'object_106',
                        name: body.name,
                        fields:
                            world.createResponseFields === 'listed'
                                ? stored.fields
                                : [],
                    },
                });
            }
            if (apiPath === '/objects/object_106') {
                if (method === 'PUT') Object.assign(stored, body);
                return ok({ object: stored });
            }
            if (method === 'POST' && apiPath === '/objects/object_106/fields') {
                const field = {
                    key: `field_${stored.fields.length + 1}`,
                    ...body,
                };
                stored.fields.push(field);
                return ok({ field });
            }
            const put = apiPath.match(/^\/objects\/object_106\/fields\/(\w+)$/);
            if (put && method === 'PUT') {
                if (world.failFieldPut) {
                    return { ok: false, status: 500, body: { error: 'boom' } };
                }
                const field = stored.fields.find((f) => f.key === put[1])!;
                Object.assign(field, body);
                return ok({ field });
            }
            if (put && method === 'DELETE') {
                stored.fields = stored.fields.filter((f) => f.key !== put[1]);
                return ok({});
            }
            return { ok: false, status: 404, body: {} };
        },
    });
    made.ctx.state.activeAppKey = 'Demo';
    return { ...made, stored };
}

const autoIncrement = (extra: Field = {}): Field => ({
    key: 'field_1',
    name: 'ID',
    type: 'auto_increment',
    ...extra,
});

const create = (
    ctx: Parameters<typeof createObject.handler>[1],
    extra: Record<string, unknown> = {},
) =>
    createObject.handler(
        {
            name: 'Clients',
            description: 'People and organisations we work with.',
            notedBy: 'Craig',
            userTable: false,
            isBookableResource: false,
            template: '',
            dryRun: false,
            ...extra,
        },
        ctx,
    );

const written = (
    requests: Array<{ apiPath: string; method: string; body: unknown }>,
) => requests.filter((r) => r.method !== 'GET');

test('knack_create_object writes the description and stamp onto the auto-increment field Knack made', async () => {
    const { ctx, requests, stored } = setup({
        fields: [autoIncrement()],
        createResponseFields: 'listed',
    });
    const payload = payloadOf(await create(ctx));

    assert.equal(payload.ok, true);
    assert.equal(payload.objectKey, 'object_106');
    const outcome = payload.objectDescription as Record<string, unknown>;
    assert.equal(outcome.ok, true);
    assert.equal(outcome.fieldKey, 'field_1');
    assert.equal(outcome.verified, true);
    assert.equal(outcome.addedAutoIncrementField, undefined);
    assert.equal(payload.warning, undefined);

    // The object POST, then the field PUT: no extra field was added.
    assert.deepEqual(
        written(requests).map((r) => `${r.method} ${r.apiPath}`),
        ['POST /objects', 'PUT /objects/object_106/fields/field_1'],
    );
    const description = String(stored.fields[0].description);
    assert.match(description, /^People and organisations we work with\. /);
    assert.match(description, STAMP);
});

test('knack_create_object finds the auto-increment field on the live object when the response lists none', async () => {
    const { ctx, requests } = setup({
        fields: [autoIncrement()],
        createResponseFields: 'omitted',
    });
    const payload = payloadOf(await create(ctx));
    assert.equal(
        (payload.objectDescription as Record<string, unknown>).ok,
        true,
    );
    assert.deepEqual(
        written(requests).map((r) => `${r.method} ${r.apiPath}`),
        ['POST /objects', 'PUT /objects/object_106/fields/field_1'],
    );
});

test('knack_create_object adds an auto-increment field, carrying the description, when the object has none', async () => {
    const { ctx, requests, stored } = setup({
        fields: [],
        createResponseFields: 'omitted',
    });
    const payload = payloadOf(await create(ctx));

    const outcome = payload.objectDescription as Record<string, unknown>;
    assert.equal(outcome.ok, true);
    assert.equal(outcome.addedAutoIncrementField, true);
    assert.equal(outcome.verified, true);
    const posted = written(requests).find(
        (r) => r.apiPath === '/objects/object_106/fields',
    )!;
    const body = posted.body as Record<string, string>;
    assert.equal(body.type, 'auto_increment');
    assert.equal(body.name, 'Record ID');
    assert.match(body.description, /^People and organisations we work with\. /);
    assert.match(body.description, STAMP);
    assert.equal(stored.fields.length, 1);
});

test('knack_create_object refuses without a description or notedBy, sending nothing', async () => {
    const { ctx, requests } = setup({
        fields: [],
        createResponseFields: 'omitted',
    });
    for (const bad of [{ description: '  ' }, { notedBy: ' ' }]) {
        const payload = payloadOf(await create(ctx, bad));
        assert.equal(payload.ok, false);
        assert.equal(payload.action, 'create_object_preflight');
    }
    assert.equal(requests.length, 0);
});

test('knack_create_object previews the description on a dry run and sends nothing', async () => {
    const { ctx, requests } = setup({
        fields: [],
        createResponseFields: 'omitted',
    });
    const payload = payloadOf(await create(ctx, { dryRun: true }));
    assert.equal(payload.action, 'create_object_dry_run');
    const would = payload.wouldWriteDescription as Record<string, unknown>;
    assert.equal(would.description, 'People and organisations we work with.');
    assert.equal(would.notedBy, 'Craig');
    assert.equal(requests.length, 0);
});

test('knack_create_object reports a failed description write without rolling the object back', async () => {
    const { ctx, requests } = setup({
        fields: [autoIncrement()],
        createResponseFields: 'listed',
        failFieldPut: true,
    });
    const payload = payloadOf(await create(ctx));

    assert.equal(payload.ok, true, 'the object itself was created');
    const outcome = payload.objectDescription as Record<string, unknown>;
    assert.equal(outcome.ok, false);
    assert.match(
        String(payload.warning),
        /was created but its description was not written cleanly/,
    );
    assert.match(String(payload.warning), /Nothing was rolled back/);
    assert.ok(!requests.some((r) => r.method === 'DELETE'));
});

test('knack_create_object says so when Knack answers without the new table key', async () => {
    const made = makeFakeContext({
        responses: { 'POST /objects': ok({ object: {} }) },
    });
    made.ctx.state.activeAppKey = 'Demo';
    const payload = payloadOf(await create(made.ctx));
    const outcome = payload.objectDescription as Record<string, unknown>;
    assert.equal(outcome.ok, false);
    assert.match(String(outcome.error), /key could not be found/);
});

// ------------------------------------------------------------ knack_update_object

const update = (
    ctx: Parameters<typeof updateObject.handler>[1],
    extra: Record<string, unknown> = {},
) =>
    updateObject.handler(
        { objectKey: 'object_106', dryRun: false, ...extra },
        ctx,
    );

test('knack_update_object with only a description edits the auto-increment field and never PUTs the object', async () => {
    const { ctx, requests, stored } = setup({
        fields: [
            autoIncrement({
                description: 'Old words. _notes=[Amanda on 2026-09-01]',
            }),
        ],
        createResponseFields: 'listed',
    });
    const payload = payloadOf(
        await update(ctx, { description: 'New words about clients.' }),
    );

    assert.equal(payload.ok, true);
    const outcome = payload.objectDescription as Record<string, unknown>;
    assert.equal(outcome.verified, true);
    assert.deepEqual(
        written(requests).map((r) => `${r.method} ${r.apiPath}`),
        ['PUT /objects/object_106/fields/field_1'],
    );
    const description = String(stored.fields[0].description);
    assert.match(description, /^New words about clients\./);
    // The stamp records who added the note, so a content edit keeps it.
    assert.match(description, /Amanda on 2026-09-01/);
});

test('knack_update_object does not add an auto-increment field to an existing table', async () => {
    const { ctx, requests } = setup({
        fields: [],
        createResponseFields: 'omitted',
    });
    const payload = payloadOf(
        await update(ctx, { description: 'Clients.', notedBy: 'Craig' }),
    );

    assert.equal(payload.ok, false);
    const outcome = payload.objectDescription as Record<string, unknown>;
    assert.match(String(outcome.error), /has no auto-increment field/);
    assert.match(String(outcome.error), /knack_create_field/);
    assert.deepEqual(written(requests), []);
});

test('knack_update_object refuses an empty description and previews without writing', async () => {
    const { ctx, requests } = setup({
        fields: [autoIncrement()],
        createResponseFields: 'listed',
    });
    const empty = payloadOf(await update(ctx, { description: '   ' }));
    assert.equal(empty.ok, false);
    assert.match(String((empty.errors as string[])[0]), /must not be empty/);

    const preview = payloadOf(
        await update(ctx, { description: 'Clients.', dryRun: true }),
    );
    assert.equal(preview.action, 'update_object_dry_run');
    assert.deepEqual(preview.wouldWriteDescription, {
        onField: 'field_1',
        from: '',
        to: 'Clients.',
    });
    assert.deepEqual(written(requests), []);
});

test('knack_update_object can rename and describe in one call', async () => {
    const { ctx, requests } = setup({
        fields: [autoIncrement()],
        createResponseFields: 'listed',
    });
    const payload = payloadOf(
        await update(ctx, {
            name: 'Customers',
            description: 'Paying customers.',
            notedBy: 'Craig',
        }),
    );
    assert.equal(payload.ok, true);
    assert.equal(
        (payload.objectDescription as Record<string, unknown>).ok,
        true,
    );
    assert.deepEqual(
        written(requests).map((r) => `${r.method} ${r.apiPath}`),
        ['PUT /objects/object_106', 'PUT /objects/object_106/fields/field_1'],
    );
});

// ------------------------------------------------------------ knack_delete_field

test('knack_delete_field keeps the table description in its response when it deletes the field holding it', async () => {
    const { ctx } = setup({
        fields: [
            autoIncrement({
                description: 'Holds clients. _notes=[Craig on 2026-09-30]',
            }),
            { key: 'field_2', name: 'Name', type: 'short_text' },
        ],
        createResponseFields: 'listed',
    });
    const holder = payloadOf(
        await deleteField.handler(
            { objectKey: 'object_106', fieldKey: 'field_1' },
            ctx,
        ),
    );
    assert.equal(holder.lostObjectDescription, 'Holds clients.');
    assert.match(String(holder.warning), /held the description of object_106/);

    const other = payloadOf(
        await deleteField.handler(
            { objectKey: 'object_106', fieldKey: 'field_2' },
            ctx,
        ),
    );
    assert.equal(other.lostObjectDescription, undefined);
});
