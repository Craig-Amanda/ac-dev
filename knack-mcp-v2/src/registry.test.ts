import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { z } from 'zod';

import { makeCacheEntry } from './lib/cache.js';
import { type AnyToolDef, defineTool, registerTools } from './registry.js';
import { makeTextResponse } from './response.js';
import { makeApp, makeFakeContext, payloadOf } from './testing/fake-context.js';

type Registered = {
    name: string;
    config: { description?: string };
    handler: (args: Record<string, unknown>) => Promise<{
        content: Array<{ type: 'text'; text: string }>;
        isError?: boolean;
    }>;
};

/** A stand-in for McpServer that records registrations. */
function fakeServer() {
    const registered: Registered[] = [];
    return {
        registered,
        server: {
            registerTool(
                name: string,
                config: { description?: string },
                handler: Registered['handler'],
            ) {
                registered.push({ name, config, handler });
            },
        } as unknown as Parameters<typeof registerTools>[0],
    };
}

const echo = defineTool({
    name: 'knack_echo',
    description: 'Echo.',
    access: 'read',
    input: { appKey: z.string().optional(), value: z.string() },
    handler: async (args) => makeTextResponse({ ok: true, value: args.value }),
});
const write = defineTool({
    name: 'knack_write',
    description: 'Write.',
    access: 'write',
    input: { appKey: z.string().optional() },
    handler: async () => makeTextResponse({ ok: true, wrote: true }),
});
const diag = defineTool({
    name: 'knack_diag',
    description: 'Diag.',
    access: 'diagnostic',
    input: { appKey: z.string().optional() },
    handler: async () => makeTextResponse({ ok: true }),
});
const boom = defineTool({
    name: 'knack_boom',
    description: 'Throws.',
    access: 'read',
    input: {},
    handler: async () => {
        throw new Error('nope');
    },
});

describe('registerTools', () => {
    it('advertises only the levels some app opted into', () => {
        const { ctx } = makeFakeContext({
            apps: [makeApp({ allowDiagnostics: false })],
        });
        const { server, registered } = fakeServer();
        const summary = registerTools(server, ctx, [echo, write, diag]);
        assert.deepEqual(summary.advertised, ['knack_echo', 'knack_write']);
        assert.deepEqual(summary.withheld, ['knack_diag']);
        assert.deepEqual(
            registered.map((entry) => entry.name),
            ['knack_echo', 'knack_write'],
        );
    });

    it('withholds everything but reads in enforced read-only mode', () => {
        const { ctx } = makeFakeContext({ options: { readOnly: true } });
        const { server } = fakeServer();
        const summary = registerTools(server, ctx, [echo, write, diag]);
        assert.deepEqual(summary.advertised, ['knack_echo']);
    });

    it("enforces the selected app's own toggle at call time", async () => {
        const { ctx } = makeFakeContext({
            apps: [
                makeApp({ appKey: 'Open' }),
                makeApp({ appKey: 'Locked', readonly: true }),
            ],
        });
        const { server, registered } = fakeServer();
        registerTools(server, ctx, [write]);
        const tool = registered[0];
        assert.equal(
            payloadOf(await tool.handler({ appKey: 'Open' })).wrote,
            true,
        );
        const refused = await tool.handler({ appKey: 'Locked' });
        assert.equal(refused.isError, true);
        assert.match(
            payloadOf(refused).error as string,
            /"Locked" is readonly/,
        );
        // Falls back to the session app.
        ctx.state.activeAppKey = 'Locked';
        assert.equal((await tool.handler({})).isError, true);
    });

    it('turns a thrown error into a compact JSON error response', async () => {
        const { ctx } = makeFakeContext();
        const { server, registered } = fakeServer();
        registerTools(server, ctx, [boom]);
        const result = await registered[0].handler({});
        assert.equal(result.isError, true);
        assert.deepEqual(payloadOf(result), {
            ok: false,
            tool: 'knack_boom',
            error: 'nope',
        });
    });

    it('rejects a duplicate tool name at registration', () => {
        const { ctx } = makeFakeContext();
        const { server } = fakeServer();
        assert.throws(
            () => registerTools(server, ctx, [echo, echo as AnyToolDef]),
            /Duplicate tool name/,
        );
    });
});

describe('registerTools API cost note', () => {
    const spender = (calls: number, remaining: number) =>
        defineTool({
            name: 'knack_spender',
            description: 'Spends calls.',
            access: 'read',
            input: { appKey: z.string().optional() },
            handler: async (_args, ctx) => {
                for (let i = 0; i < calls; i++) {
                    ctx.usage.record(
                        'Demo',
                        {
                            plan: {
                                limit: 75000,
                                remaining,
                                resetsAt: Date.now() + 3_600_000,
                            },
                        },
                        Date.now(),
                    );
                }
                return makeTextResponse({ ok: true });
            },
        });

    const run = async (tool: AnyToolDef) => {
        const { ctx } = makeFakeContext();
        const { server, registered } = fakeServer();
        registerTools(server, ctx, [tool]);
        return registered[0].handler({});
    };

    it('adds nothing to a cheap request', async () => {
        const result = await run(spender(3, 40000));
        assert.equal(result.content.length, 1);
    });

    it('appends a note after the payload for an expensive request', async () => {
        const result = await run(spender(30, 40000));
        assert.equal(result.content.length, 2);
        assert.deepEqual(payloadOf(result), { ok: true });
        assert.match(
            result.content[1].text,
            /Demo: this request made 30 API calls/,
        );
    });

    it('appends a warning to a cheap request when the allowance is low', async () => {
        const result = await run(spender(1, 10000));
        assert.match(result.content[1].text, /Running low/);
    });
});

describe('registerTools cache invalidation', () => {
    const changing = (text: unknown) =>
        defineTool({
            name: 'knack_changing',
            description: 'Writes.',
            access: 'write',
            input: { appKey: z.string().optional() },
            handler: async () => makeTextResponse(text),
        });

    const run = async (tool: AnyToolDef, appKey = 'Demo') => {
        const made = makeFakeContext({
            apps: [makeApp({ appKey: 'Demo' }), makeApp({ appKey: 'Other' })],
        });
        for (const key of ['Demo', 'Other']) {
            made.ctx.caches.runtimeMetadata.set(
                key,
                makeCacheEntry({ objects: [] } as never, 'runtime'),
            );
            made.ctx.caches.schema.set(
                key,
                makeCacheEntry({ objects: [] }, 'runtime'),
            );
        }
        const { server, registered } = fakeServer();
        registerTools(server, made.ctx, [tool]);
        await registered[0].handler({ appKey });
        return made.ctx;
    };

    it("drops the app's cached metadata after a write that reports a cacheNote, and only that app's", async () => {
        const ctx = await run(changing({ ok: true, cacheNote: 'cleared' }));
        assert.equal(ctx.caches.runtimeMetadata.has('Demo'), false);
        assert.equal(ctx.caches.schema.has('Demo'), false);
        assert.equal(ctx.caches.runtimeMetadata.has('Other'), true);
    });

    it('leaves the cache alone when the write did not happen (a dry run or a refusal)', async () => {
        const ctx = await run(changing({ ok: false, errors: ['refused'] }));
        assert.equal(ctx.caches.runtimeMetadata.has('Demo'), true);
    });

    it('never clears the cache after a read', async () => {
        const reader = defineTool({
            name: 'knack_reading',
            description: 'Reads.',
            access: 'read',
            input: { appKey: z.string().optional() },
            handler: async () => makeTextResponse({ cacheNote: 'x' }),
        });
        const ctx = await run(reader);
        assert.equal(ctx.caches.runtimeMetadata.has('Demo'), true);
    });
});
