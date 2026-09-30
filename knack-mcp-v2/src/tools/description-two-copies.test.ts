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

function playground() {
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
        fields: [holder(NOTE, FRESH_META), name],
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

test('the dry run sees the keywords the builder added and would keep both', async () => {
    const { ctx } = playground();
    const payload = payloadOf(await update(ctx, true));
    const would = payload.wouldWriteDescription as Record<string, unknown>;
    assert.equal(would.from, 'Probe for the two description copies.');
    assert.equal(would.keywordsKept, '_mcp_nodata _mcp_schemalock');
    assert.equal(
        would.wouldStore,
        '_notes=[Second write. | Test agent on 2026-09-30] _mcp_nodata _mcp_schemalock',
    );
});

test('the real write is refused for the schema lock, and sends nothing', async () => {
    const { ctx, requests } = playground();
    const payload = payloadOf(await update(ctx, false));
    assert.equal(payload.ok, false, JSON.stringify(payload));
    assert.match(JSON.stringify(payload), /field_1 carries _mcp_schemalock/);
    assert.deepEqual(
        requests.filter((request) => request.method !== 'GET'),
        [],
    );
});
