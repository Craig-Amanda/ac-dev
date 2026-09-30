import assert from 'node:assert/strict';
import { test } from 'node:test';

import { makeFakeContext, payloadOf } from '../testing/fake-context.js';
import { updateObject } from './objects.js';

/**
 * The exact shapes reported from the playground on 30 September: a table's
 * auto-increment field after a person added `_mcp_nodata` and `_mcp_schemalock` in the
 * builder. Only `meta.description` carries them; the top-level copy, and the cached
 * schema that was loaded before the edit, do not.
 */
const NOTE =
    '_notes=[Probe for the two description copies. | Test agent on 2026-09-30]';
const FRESH_META = `<p>${NOTE}<br>_mcp_nodata<br>_mcp_schemalock<br>\n</p>`;

function playground(liveMeta: string = FRESH_META) {
    const holder = (description: string, meta: string) => ({
        key: 'field_1',
        name: 'AI',
        type: 'auto_increment',
        description,
        meta: { description: meta },
    });
    const name = { key: 'field_2', name: 'Name', type: 'short_text' };
    const live = {
        key: 'object_9',
        name: 'MCP probe 4',
        fields: [holder(NOTE, liveMeta), name],
    };
    const cached = {
        objects: [
            {
                key: 'object_9',
                name: 'MCP probe 4',
                fields: [holder(NOTE, NOTE), name],
            },
        ],
    };
    const made = makeFakeContext({
        runtimeMetadata: { Demo: cached as never },
        responses: () => ({ ok: true, status: 200, body: { object: live } }),
    });
    made.ctx.state.activeAppKey = 'Demo';
    return made;
}

const update = (ctx: ReturnType<typeof playground>['ctx'], dryRun: boolean) =>
    updateObject.handler(
        {
            objectKey: 'object_9',
            description: 'Second write.',
            notedBy: 'Test agent',
            dryRun,
        },
        ctx,
    );

test('the dry run refuses a schema-locked holder too, instead of previewing a write that would fail', async () => {
    const { ctx, requests } = playground();
    const payload = payloadOf(await update(ctx, true));
    assert.equal(payload.ok, false, JSON.stringify(payload));
    assert.equal(payload.action, 'update_object_preflight');
    assert.match(JSON.stringify(payload), /field_1 carries _mcp_schemalock/);
    assert.equal(payload.wouldWriteDescription, undefined);
    assert.deepEqual(
        requests.filter((request) => request.method !== 'GET'),
        [],
    );
});

test('the real write is refused for the schema lock, sends nothing, and clears no cache', async () => {
    const { ctx, requests } = playground();
    const payload = payloadOf(await update(ctx, false));
    assert.equal(payload.ok, false, JSON.stringify(payload));
    assert.equal(payload.action, 'update_object_preflight');
    assert.match(JSON.stringify(payload), /field_1 carries _mcp_schemalock/);
    // A refusal changed nothing, so it must not say the cache was cleared.
    assert.equal(payload.cacheNote, undefined);
    assert.deepEqual(
        requests.filter((request) => request.method !== 'GET'),
        [],
    );
});

test('without the lock the dry run sees the keyword the builder added and would keep it', async () => {
    const made = playground(`<p>${NOTE}<br>_mcp_nodata<br>\n</p>`);
    const payload = payloadOf(await update(made.ctx, true));
    const would = payload.wouldWriteDescription as Record<string, unknown>;
    assert.equal(would.from, 'Probe for the two description copies.');
    assert.equal(would.keywordsKept, '_mcp_nodata');
    assert.equal(
        would.wouldStore,
        '_notes=[Second write. | Test agent on 2026-09-30] _mcp_nodata',
    );
});

test('a rename asked for with a locked description is not half done', async () => {
    const { ctx, requests } = playground();
    const payload = payloadOf(
        await updateObject.handler(
            {
                objectKey: 'object_9',
                name: 'Renamed',
                description: 'Second write.',
                notedBy: 'Test agent',
                dryRun: false,
            },
            ctx,
        ),
    );
    assert.equal(payload.ok, false);
    assert.deepEqual(
        requests.filter((request) => request.method !== 'GET'),
        [],
    );
});
