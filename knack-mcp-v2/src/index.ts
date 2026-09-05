#!/usr/bin/env node
/**
 * Entry point. `--readonly` pins the whole server read-only regardless of app.json.
 * stdout is JSON-RPC; everything human-facing goes to stderr.
 */
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js';

import type { ServerOptions } from './config.js';
import { KnackContext } from './context.js';
import {
    describeServerBuild,
    summariseServerBuild,
} from './lib/build-identity.js';
import { createServer } from './server.js';

export async function main(options: ServerOptions = {}): Promise<void> {
    // Logged before anything that can fail: a server that does not start is exactly
    // the case where knowing which build is running matters.
    console.error(
        `[knack-mcp] ${summariseServerBuild(describeServerBuild(options.readOnly === true))}`,
    );
    const ctx = KnackContext.fromEnvironment(options);
    const { server, tools } = createServer(ctx);
    console.error(
        `[knack-mcp] ${tools.advertised.length} tools advertised` +
            (tools.withheld.length
                ? `, ${tools.withheld.length} withheld by app.json permissions`
                : ''),
    );
    await server.connect(new StdioServerTransport());
}

const readOnly =
    process.argv.includes('--readonly') ||
    process.env.KNACK_MCP_READONLY === '1';
main({ readOnly }).catch((error) => {
    console.error(
        `[knack-mcp] startup failed: ${error instanceof Error ? error.message : String(error)}`,
    );
    if (error instanceof Error && error.stack) console.error(error.stack);
    process.exit(1);
});
