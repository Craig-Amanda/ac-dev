/**
 * Assemble the MCP server: tools from the registry, plus the per-app metadata resource.
 */
import {
    McpServer,
    ResourceTemplate,
} from '@modelcontextprotocol/sdk/server/mcp.js';

import type { KnackContext } from './context.js';
import { type RegistrationSummary, registerTools } from './registry.js';
import { ALL_TOOLS } from './tools/index.js';

export const SERVER_NAME = 'knack-mcp';

export function createServer(ctx: KnackContext): {
    server: McpServer;
    tools: RegistrationSummary;
} {
    const server = new McpServer({ name: SERVER_NAME, version: '2.0.0' });
    ctx.server = server;

    const tools = registerTools(server, ctx, ALL_TOOLS);

    // knack://<AppKey>/schema | fieldMap | viewMap
    server.registerResource(
        'knack_metadata',
        new ResourceTemplate('knack://{appKey}/{kind}', { list: undefined }),
        {
            description:
                'Cached schema, field map or view map for one app as JSON.',
            mimeType: 'application/json',
        },
        async (uri, variables) => {
            const appKey = String(variables.appKey ?? '');
            const kind = String(variables.kind ?? '');
            const app = ctx.findApp(appKey);
            const reply = (payload: unknown) => ({
                contents: [
                    {
                        uri: uri.toString(),
                        mimeType: 'application/json',
                        text: JSON.stringify(payload),
                    },
                ],
            });
            if (!app) return reply({ ok: false, message: 'Unknown appKey' });
            switch (kind) {
                case 'schema':
                    return reply(
                        (await ctx.getSchema(app)).schema ?? {
                            ok: false,
                            message:
                                'No schema available from runtime API or schema.json.',
                        },
                    );
                case 'fieldMap':
                    return reply(
                        (await ctx.getFieldMap(app)).fieldMap ?? {
                            ok: false,
                            message:
                                'No field map available from runtime API or fieldMap.json.',
                        },
                    );
                case 'viewMap':
                    return reply(
                        (await ctx.getViewMap(app)).viewMap ?? {
                            ok: false,
                            message:
                                'No view map available from runtime API or viewMap.json.',
                        },
                    );
                default:
                    return reply({
                        ok: false,
                        message: 'Unknown resource type',
                    });
            }
        },
    );

    return { server, tools };
}
