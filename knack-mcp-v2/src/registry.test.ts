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

    it("says in the response when the app's metadata was withheld for its key", async () => {
        const { ctx } = makeFakeContext({ secrets: {} });
        const readsSchema = defineTool({
            name: 'knack_reads_schema',
            description: 'Reads the schema.',
            access: 'read',
            input: { appKey: z.string().optional() },
            handler: async (_args, context) => {
                const { source } = await context.getSchema(
                    context.getApp('Demo'),
                );
                return makeTextResponse({ ok: true, source }, 'Existing note.');
            },
        });
        const { server, registered } = fakeServer();
        registerTools(server, ctx, [readsSchema, boom]);

        const result = await registered[0].handler({ appKey: 'Demo' });
        assert.deepEqual(payloadOf(result), { ok: true, source: null });
        assert.equal(result.content.length, 2);
        assert.match(result.content[1].text, /^Existing note\.\n\n/);
        assert.match(
            result.content[1].text,
            /No API key found for appKey "Demo".*may be out of date\./,
        );

        ctx.state.activeAppKey = 'Demo';
        const failed = await registered[1].handler({});
        assert.equal(failed.isError, true);
        assert.match(failed.content[1].text, /No API key found/);
    });

    it('adds no key note when the key is fine or the tool lists apps', async () => {
        const { ctx } = makeFakeContext();
        const { server, registered } = fakeServer();
        registerTools(server, ctx, [echo]);
        const result = await registered[0].handler({
            appKey: 'Demo',
            value: 'x',
        });
        assert.equal(result.content.length, 1);

        const { ctx: noKey } = makeFakeContext({ secrets: {} });
        await noKey.getRuntimeMetadata(noKey.getApp('Demo'));
        const listApps = defineTool({
            name: 'knack_list_apps',
            description: 'List.',
            access: 'read',
            input: {},
            handler: async () => makeTextResponse({ ok: true }),
        });
        const { server: other, registered: listed } = fakeServer();
        registerTools(other, noKey, [listApps]);
        noKey.state.activeAppKey = 'Demo';
        assert.equal((await listed[0].handler({})).content.length, 1);
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

describe('registerTools typed email note', () => {
    const rule = JSON.stringify([
        {
            action: 'email',
            email: { recipients: [{ email: 'jane@example.com' }] },
        },
    ]);
    const addsRule = defineTool({
        name: 'knack_add_rule',
        description: 'Adds a rule.',
        access: 'view',
        input: { appKey: z.string().optional(), rules: z.string() },
        handler: async () => makeTextResponse({ ok: true }, 'Rule added.'),
    });
    const refusesRule = defineTool({
        name: 'knack_refuse_rule',
        description: 'Refuses.',
        access: 'view',
        input: { appKey: z.string().optional(), rules: z.string() },
        handler: async () => {
            throw new Error('refused');
        },
    });
    const writesRecord = defineTool({
        name: 'knack_create_record',
        description: 'Creates a record.',
        access: 'write',
        input: { appKey: z.string().optional(), values: z.string() },
        handler: async () => makeTextResponse({ ok: true }),
    });

    it('flags a typed address in an email the change adds, with the local part hidden', async () => {
        const { ctx } = makeFakeContext();
        const { server, registered } = fakeServer();
        registerTools(server, ctx, [addsRule]);
        const result = await registered[0].handler({
            appKey: 'Demo',
            rules: rule,
        });
        assert.equal(result.content.length, 2);
        assert.match(result.content[1].text, /^Rule added\.\n\n/);
        assert.match(
            result.content[1].text,
            /typed email address into an email: j\*\*\*@example\.com at \$\.rules\.0\.email\.recipients\.0\.email\./,
        );
        assert.doesNotMatch(result.content[1].text, /jane@/);
    });

    it('says nothing for a refused change, a record write, or an email sent to a field', async () => {
        const { ctx } = makeFakeContext();
        const { server, registered } = fakeServer();
        registerTools(server, ctx, [refusesRule, writesRecord, addsRule]);
        const refused = await registered[0].handler({
            appKey: 'Demo',
            rules: rule,
        });
        assert.equal(refused.isError, true);
        assert.equal(refused.content.length, 1);

        const record = await registered[1].handler({
            appKey: 'Demo',
            values: JSON.stringify({ field_5: { email: 'jane@example.com' } }),
        });
        assert.equal(record.content.length, 1);

        const toField = await registered[2].handler({
            appKey: 'Demo',
            rules: JSON.stringify([
                {
                    action: 'email',
                    email: { recipients: [{ field: 'field_5' }] },
                },
            ]),
        });
        assert.equal(toField.content.length, 2);
        assert.doesNotMatch(toField.content[1].text, /typed email/);
    });
});

describe('registerTools typed email note on previews, refusals and the real email paths', () => {
    const emailsRule = JSON.stringify({
        rules: {
            emails: [
                {
                    action: 'email',
                    email: {
                        from_email: 'office@example.org',
                        recipients: [{ email: 'jane@example.com' }],
                    },
                },
            ],
        },
    });
    function viewTool(
        name: string,
        answer: Record<string, unknown>,
    ): AnyToolDef {
        return defineTool({
            name,
            description: 'Changes a view.',
            access: 'view',
            input: { appKey: z.string().optional(), payload: z.string() },
            handler: async () => makeTextResponse(answer),
        });
    }

    it('says what a preview would do, and nothing for another refusal', async () => {
        const { ctx } = makeFakeContext();
        const { server, registered } = fakeServer();
        registerTools(server, ctx, [
            viewTool('knack_preview_view', {
                ok: false,
                error: 'PREVIEW_ONLY',
                preview: true,
            }),
            viewTool('knack_refused_view', {
                ok: false,
                error: 'VIEW_NOT_FOUND',
            }),
        ]);
        const preview = await registered[0].handler({
            appKey: 'Demo',
            payload: emailsRule,
        });
        assert.match(
            preview.content[1].text,
            /^This change would put a typed email address into an email: o\*\*\*@example\.org at \$\.payload\.rules\.emails\.0\.email\.from_email; j\*\*\*@example\.com at \$\.payload\.rules\.emails\.0\.email\.recipients\.0\.email\./,
        );
        const refused = await registered[1].handler({
            appKey: 'Demo',
            payload: emailsRule,
        });
        assert.equal(refused.content.length, 1);
    });

    it("flags Knack's own email rules on a view update, and a task's email", async () => {
        const { ctx } = makeFakeContext();
        const createTask = defineTool({
            name: 'knack_create_task',
            description: 'Creates a task.',
            access: 'write',
            input: { appKey: z.string().optional(), action: z.string() },
            handler: async () => makeTextResponse({ ok: true }),
        });
        const { server, registered } = fakeServer();
        registerTools(server, ctx, [
            viewTool('knack_update_view', { ok: true }),
            createTask,
        ]);
        const update = await registered[0].handler({
            appKey: 'Demo',
            payload: emailsRule,
        });
        assert.match(update.content[1].text, /^This change puts a typed/);

        const task = await registered[1].handler({
            appKey: 'Demo',
            action: JSON.stringify({
                action: 'email',
                email: { recipients: [{ email: 'boss@example.com' }] },
            }),
        });
        assert.match(
            task.content[1].text,
            /b\*\*\*@example\.com at \$\.action\.email/,
        );
    });
});
