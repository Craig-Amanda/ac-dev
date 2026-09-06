import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { z } from 'zod';

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
