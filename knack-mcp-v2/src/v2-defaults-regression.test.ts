/**
 * Regression test for the v1 SDK's `.default()` bug, run against the *real* MCP wire
 * protocol rather than by calling a tool's handler directly.
 *
 * The bug: `@modelcontextprotocol/sdk` v1's internal zod-compat shim re-wrapped every
 * tool's raw zod shape through `zod/v4-mini`'s `object()`, whose `.default()` handling
 * does not match classic zod's — a caller that omitted a defaulted parameter (e.g.
 * `includeRaw` on knack_get_view, `dryRun` on knack_create_records) got a hard
 * `Invalid input: expected nonoptional, received undefined` before the tool handler
 * ever ran, even though the advertised JSON schema showed the parameter as optional
 * with a default. A unit test that calls `getView.handler(...)` directly never
 * exercises this, because it bypasses the SDK's schema validation entirely — the SDK
 * layer is exactly what was broken. So this test builds the real server with
 * `createServer`, wires it to a real `Client` over an in-memory transport pair, and
 * calls tools through `client.callTool`, the same path a real MCP client uses.
 */
import assert from 'node:assert/strict';
import { after, before, describe, it } from 'node:test';

import { Client } from '@modelcontextprotocol/client';
import { InMemoryTransport } from '@modelcontextprotocol/server';

import type { ToolResult } from './response.js';
import { createServer } from './server.js';
import { makeApp, makeFakeContext, payloadOf } from './testing/fake-context.js';
import type { RuntimeMetadata } from './types.js';

/**
 * `payloadOf` is typed against the handler-level `ToolResult`; `client.callTool()`
 * returns the SDK's `CallToolResult`, whose `content` is a broader union (most of which
 * carry no `.text`). Every call in this suite is a text-only tool response in practice,
 * so the cast is safe here even though the two result types aren't structurally
 * assignable to each other.
 */
function extractPayload(result: unknown): Record<string, unknown> {
    return payloadOf(result as ToolResult);
}

/** One object with one field, one scene with one view — just enough for knack_get_view
 * and knack_create_records to have something real to resolve against. */
function makeMetadata(): RuntimeMetadata {
    return {
        application: {
            name: 'Demo',
            slug: 'demo',
            account: { slug: 'acme' },
            objects: [
                {
                    key: 'object_1',
                    name: 'Contact',
                    fields: [
                        {
                            key: 'field_1',
                            name: 'Name',
                            type: 'short_text',
                            required: true,
                        },
                    ],
                },
            ],
            scenes: [
                {
                    key: 'scene_1',
                    name: 'Contacts',
                    slug: 'contacts',
                    views: [
                        {
                            key: 'view_1',
                            name: 'Contacts table',
                            type: 'table',
                            title: 'Contacts',
                            source: {
                                object: 'object_1',
                                criteria: {
                                    match: 'all',
                                    rules: [],
                                    groups: [],
                                },
                                sort: [],
                                limit: '',
                            },
                            columns: [
                                {
                                    type: 'field',
                                    field: { key: 'field_1' },
                                    header: 'Name',
                                },
                            ],
                            links: [],
                            groups: [],
                            inputs: [],
                            no_data_text: 'No Contact Records',
                        },
                    ],
                },
            ],
        },
        // Matches the shape the other fixtures in this suite build; only the fields
        // the tools under test actually read need to be present.
    } as unknown as RuntimeMetadata;
}

/** A connected Client talking to a real server built by createServer, over an
 * in-memory transport pair — no stdio, no process, no network. */
async function connectClient(ctx: ReturnType<typeof makeFakeContext>['ctx']) {
    const { server } = createServer(ctx);
    const [clientTransport, serverTransport] =
        InMemoryTransport.createLinkedPair();
    const client = new Client({
        name: 'verify-v2-migration',
        version: '1.0.0',
    });
    await Promise.all([
        server.connect(serverTransport),
        client.connect(clientTransport),
    ]);
    return { client, server };
}

describe('v2 SDK: a tool call omitting a defaulted parameter uses the default', () => {
    let client: Client;
    let closeAll: () => Promise<void>;

    before(async () => {
        const app = makeApp();
        const { ctx } = makeFakeContext({
            apps: [app],
            runtimeMetadata: { [app.appKey]: makeMetadata() },
            responses: {
                'POST /objects/object_1/records': {
                    ok: true,
                    status: 200,
                    body: { record: { id: 'record_1', field_1: 'Ada' } },
                },
            },
        });
        const { client: c, server } = await connectClient(ctx);
        client = c;
        closeAll = async () => {
            await client.close();
            await server.close();
        };
    });

    after(async () => {
        await closeAll();
    });

    it('knack_get_view: omitting detail and includeRaw does not throw a schema error', async () => {
        // Only appKey and viewKey are sent — `detail` (default 'context') and
        // `includeRaw` (default false) are both omitted, which is exactly the shape of
        // call that the v1 SDK's zod-compat shim rejected before the handler ran.
        const result = await client.callTool({
            name: 'knack_get_view',
            arguments: { appKey: 'Demo', viewKey: 'view_1' },
        });

        assert.equal(
            result.isError,
            undefined,
            `expected no protocol-level error, got: ${JSON.stringify(result)}`,
        );
        // The exact shape of a `detail: 'context'` response is covered by
        // views.test.ts; this only needs to prove the call reached the handler at all.
        assert.equal(extractPayload(result).ok, true);
    });

    it('knack_get_view: detail "attributes" with includeRaw omitted defaults it to false', async () => {
        const result = await client.callTool({
            name: 'knack_get_view',
            arguments: {
                appKey: 'Demo',
                viewKey: 'view_1',
                detail: 'attributes',
            },
        });

        assert.equal(
            result.isError,
            undefined,
            `expected no protocol-level error, got: ${JSON.stringify(result)}`,
        );
        const payload = extractPayload(result);
        assert.equal(payload.ok, true);
        // includeRaw defaulted to false: the handler took the "note" branch rather than
        // attaching the full raw view JSON. The note's exact wording is covered by
        // views.test.ts; this only needs to prove which branch the default sent it down.
        assert.equal(payload.attributeDetail, undefined);
    });

    it('knack_create_records: omitting dryRun defaults it to false and actually creates', async () => {
        // Only appKey, objectKey and records are sent — `dryRun` (default false) is
        // omitted. If the SDK still had the v1 bug this call would fail before the
        // handler ran; with the fix it should reach the handler, see dryRun === false,
        // and go through the real (fake-backed) create path rather than the dry run.
        const result = await client.callTool({
            name: 'knack_create_records',
            arguments: {
                appKey: 'Demo',
                objectKey: 'object_1',
                records: [{ field_1: 'Ada' }],
            },
        });

        assert.equal(
            result.isError,
            undefined,
            `expected no protocol-level error, got: ${JSON.stringify(result)}`,
        );
        const payload = extractPayload(result);
        assert.equal(payload.ok, true);
        // action is 'batch_create_records', not 'batch_create_records_dry_run' — proof
        // dryRun defaulted to false rather than to some other/undefined value. The
        // success/failure counts for this exact fixture are records.test.ts's job.
        assert.equal(payload.action, 'batch_create_records');
    });
});
