import assert from 'node:assert/strict';
import { test } from 'node:test';

import type { KnackApiResult } from '../http.js';
import { makeFakeContext, payloadOf } from '../testing/fake-context.js';
import { deleteField, updateField } from './fields.js';
import {
    readHolderKeywords,
    readObjectDescription,
} from '../lib/object-description.js';
import { createObject, updateObject } from './objects.js';

const NOTE =
    /^_notes=\[People and organisations we work with\. \| Craig on \d{4}-\d{2}-\d{2}\]$/;

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
    // The words and who and when sit together inside one note.
    assert.match(description, NOTE);
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
    assert.equal(body.name, 'AI');
    assert.match(body.description, NOTE);
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
                description: '_notes=[Old words. | Amanda on 2026-09-01]',
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
    assert.equal(
        description,
        '_notes=[New words about clients. | Amanda on 2026-09-01]',
    );
    // The stamp records who added the note, so a content edit keeps it.
});

test('knack_update_object does not add an auto-increment field to an existing table, and sends nothing', async () => {
    const { ctx, requests } = setup({
        fields: [],
        createResponseFields: 'omitted',
    });
    for (const extra of [{}, { name: 'Customers' }]) {
        const payload = payloadOf(
            await update(ctx, {
                description: 'Clients.',
                notedBy: 'Craig',
                ...extra,
            }),
        );
        assert.equal(payload.ok, false);
        assert.equal(payload.action, 'update_object_preflight');
        const message = String((payload.errors as string[])[0]);
        assert.match(message, /has no auto-increment field/);
        assert.match(message, /knack_create_field/);
        // A rename asked for in the same call is not left half done.
        assert.deepEqual(written(requests), []);
    }
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
    assert.equal(preview.action, 'update_object_dry_run');
    const would = preview.wouldWriteDescription as Record<string, unknown>;
    assert.equal(would.onField, 'field_1');
    assert.equal(would.from, '');
    assert.equal(would.to, 'Clients.');
    // The field has no note yet, so the preview says a stamp needs notedBy.
    assert.match(String(would.notedByNeeded), /notedBy is needed/);
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
                description: '_notes=[Holds clients. | Craig on 2026-09-30]',
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

// ------------------------------------------- keywords on the auto-increment field

const holderWith = (description: string) =>
    setup({
        fields: [autoIncrement({ description })],
        createResponseFields: 'listed',
    });

test('a schema-locked auto-increment field refuses a description change, sending nothing', async () => {
    const { ctx, requests, stored } = holderWith(
        '_notes=[Old words. | Amanda on 2026-09-01] _mcp_schemalock',
    );
    const payload = payloadOf(await update(ctx, { description: 'New words.' }));
    assert.equal(payload.ok, false);
    assert.equal(payload.action, 'update_object_preflight');
    assert.match(
        JSON.stringify(payload.errors),
        /schema-locked|_mcp_schemalock/,
    );
    assert.deepEqual(written(requests), []);
    assert.equal(
        stored.fields[0].description,
        '_notes=[Old words. | Amanda on 2026-09-01] _mcp_schemalock',
    );
});

test('other keywords on the auto-increment field are kept when its words change', async () => {
    const { ctx, stored } = holderWith(
        '_notes=[Old words. | Amanda on 2026-09-01] _ktlHide',
    );
    const payload = payloadOf(await update(ctx, { description: 'New words.' }));
    assert.equal(payload.ok, true, JSON.stringify(payload));
    assert.equal(
        stored.fields[0].description,
        '_notes=[New words. | Amanda on 2026-09-01] _ktlHide',
    );
});

test('a table-locked table refuses a description change before anything is sent', async () => {
    const { ctx, requests } = holderWith(
        '_notes=[Old words. | Amanda on 2026-09-01] _mcp_tablelock',
    );
    const payload = payloadOf(await update(ctx, { description: 'New words.' }));
    assert.equal(payload.ok, false);
    assert.match(JSON.stringify(payload), /table-locked/);
    assert.deepEqual(written(requests), []);
});

test("a schema lock on the table's auto-increment field also stops other fields being deleted", async () => {
    const { ctx, requests } = setup({
        fields: [
            autoIncrement({
                description:
                    '_notes=[Clients. | Amanda on 2026-09-01] _mcp_schemalock',
            }),
            { key: 'field_2', name: 'Name', type: 'short_text' },
        ],
        createResponseFields: 'listed',
    });
    const payload = payloadOf(
        await deleteField.handler(
            { objectKey: 'object_106', fieldKey: 'field_2' },
            ctx,
        ),
    );
    assert.equal(payload.ok, false);
    assert.match(
        JSON.stringify(payload.errors),
        /field_2 is covered by _mcp_schemalock on field_1, the table's auto-increment field/,
    );
    assert.deepEqual(written(requests), []);
});

// ---------------------------- descriptions the Builder saved, in HTML

test('a description the Builder saved as HTML keeps its keyword, and the preview shows exactly what would be stored', async () => {
    const { ctx, requests, stored } = setup({
        fields: [
            autoIncrement({ description: '<p>_mcp_nodata</p>' }),
            { key: 'field_2', name: 'Name', type: 'short_text' },
        ],
        createResponseFields: 'listed',
    });

    const preview = payloadOf(
        await update(ctx, {
            description: 'Keyword preservation probe.',
            notedBy: 'Craig',
            dryRun: true,
        }),
    );
    const would = preview.wouldWriteDescription as Record<string, unknown>;
    assert.equal(would.onField, 'field_1');
    // The HTML around the keyword is not mistaken for words.
    assert.equal(would.from, '');
    assert.equal(would.keywordsKept, '_mcp_nodata');
    assert.match(
        String(would.wouldStore),
        /^_notes=\[Keyword preservation probe\. \| Craig on \d{4}-\d{2}-\d{2}\] _mcp_nodata$/,
    );
    assert.deepEqual(written(requests), []);

    // And a real write stores exactly that, with the keyword still there.
    const done = payloadOf(
        await update(ctx, {
            description: 'Keyword preservation probe.',
            notedBy: 'Craig',
        }),
    );
    assert.equal(done.ok, true, JSON.stringify(done));
    assert.equal(
        String(stored.fields[0].description),
        String(would.wouldStore),
    );
});

test('the table description reads through Builder HTML, without the keyword or the tags', () => {
    const fields = [
        autoIncrement({
            description:
                '<p>Holds clients.&nbsp;Handle with care.</p><p>_notes=[Craig on 2026-09-30] _mcp_nodata</p>',
        }),
    ];
    assert.equal(
        readObjectDescription(fields).text,
        'Holds clients. Handle with care.',
    );
    assert.equal(readHolderKeywords(fields), '_mcp_nodata');
});

// ------------- a description the builder edited: meta.description is the real one

const STAMPED = '_notes=[Second write. | Test agent on 2026-09-30]';

/** A holder whose two copies disagree, as after a builder edit (found on the playground). */
const builderEdited = (topLevel: string, meta: string) =>
    setup({
        fields: [
            autoIncrement({
                description: topLevel,
                meta: { description: meta },
            }),
            { key: 'field_2', name: 'Name', type: 'short_text' },
        ],
        createResponseFields: 'listed',
    });

test('a schema lock only meta.description shows still refuses the write, and nothing is lost', async () => {
    const before = {
        top: `<p>${STAMPED} <br>_mcp_nodata<br></p>`,
        meta: `<p>${STAMPED} <br>_mcp_nodata<br>_mcp_schemalock<br></p>`,
    };
    const { ctx, requests, stored } = builderEdited(before.top, before.meta);
    const payload = payloadOf(
        await update(ctx, { description: 'Third write.' }),
    );
    assert.equal(payload.ok, false, JSON.stringify(payload));
    assert.match(JSON.stringify(payload), /_mcp_schemalock/);
    assert.deepEqual(written(requests), []);
    assert.equal(stored.fields[0].description, before.top);
    assert.deepEqual(stored.fields[0].meta, { description: before.meta });
});

test('a keyword a person removed is gone, even though the top-level copy still shows it', async () => {
    const { ctx, stored } = builderEdited(
        `<p>${STAMPED} <br>_mcp_nodata<br>_mcp_schemalock<br></p>`,
        `<p>${STAMPED} <br>_mcp_nodata<br></p>`,
    );
    const payload = payloadOf(
        await update(ctx, { description: 'Third write.' }),
    );
    assert.equal(payload.ok, true, JSON.stringify(payload));
    // Only the keyword the builder still shows is carried, and both copies now agree.
    const expected =
        '_notes=[Third write. | Test agent on 2026-09-30] _mcp_nodata';
    assert.equal(stored.fields[0].description, expected);
    assert.deepEqual(stored.fields[0].meta, { description: expected });
});

test('the keyword-drop guard reads the copy the builder edits', async () => {
    // meta.description carries _mcp_nodata; the stale top-level copy does not.
    const { ctx, requests } = builderEdited(
        `<p>${STAMPED}</p>`,
        `<p>${STAMPED} <br>_mcp_nodata<br></p>`,
    );
    const payload = payloadOf(
        await updateField.handler(
            {
                objectKey: 'object_106',
                fieldKey: 'field_1',
                description: 'Words with no keyword',
                notedBy: 'Craig',
                restampNote: false,
                confirmRemoveKtlKeywords: true,
                dryRun: false,
            },
            ctx,
        ),
    );
    assert.equal(payload.ok, false, JSON.stringify(payload));
    assert.match(JSON.stringify(payload), /_mcp_nodata/);
    assert.deepEqual(written(requests), []);
});
