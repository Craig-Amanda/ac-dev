#!/usr/bin/env node
/**
 * Entry point. `--readonly` pins the whole server read-only regardless of app.json.
 * stdout is JSON-RPC; everything human-facing goes to stderr.
 */
import { pathToFileURL } from 'node:url';

import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js';

import type { ServerOptions } from './config.js';
import { KnackContext } from './context.js';
import {
    describeServerBuild,
    summariseServerBuild,
} from './lib/build-identity.js';
import { describeError, isEnabledEnv } from './lib/util.js';
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

// Starting the server is a side effect that belongs to running this file directly, not
// to importing it — a test, or any future wrapper that imports `main` to pass its own
// ServerOptions, would otherwise spawn a stdio server bound to the process's own
// stdin/stdout the moment it evaluated this module.
const isDirectExecution = (() => {
    const entryPath = process.argv[1];
    return entryPath
        ? import.meta.url === pathToFileURL(entryPath).href
        : false;
})();

if (isDirectExecution) {
    const readOnly =
        process.argv.includes('--readonly') ||
        isEnabledEnv(process.env.KNACK_MCP_READONLY, false);
    main({ readOnly }).catch((error) => {
        console.error(`[knack-mcp] startup failed: ${describeError(error)}`);
        if (error instanceof Error && error.stack) console.error(error.stack);
        process.exit(1);
    });
}
