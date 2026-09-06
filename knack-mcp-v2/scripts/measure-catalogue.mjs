#!/usr/bin/env node
/**
 * Boot the built server against a stub app folder and measure the tool catalogue a
 * client is sent on every turn. Run after `npm run build`:
 *
 *   node scripts/measure-catalogue.mjs            # full and readonly modes
 *   node scripts/measure-catalogue.mjs --list     # also print each tool's size
 *
 * Tokens are estimated at four characters each, the same convention as the README.
 */
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

import { Client } from '@modelcontextprotocol/sdk/client/index.js';
import { StdioClientTransport } from '@modelcontextprotocol/sdk/client/stdio.js';

const here = path.dirname(fileURLToPath(import.meta.url));
const entry = path.resolve(here, '..', 'dist', 'index.js');
if (!fs.existsSync(entry)) {
    console.error('dist/index.js not found. Run `npm run build` first.');
    process.exit(1);
}

const stub = fs.mkdtempSync(path.join(os.tmpdir(), 'knack-mcp-catalogue-'));
fs.mkdirSync(path.join(stub, 'apps', 'Demo', 'schema'), { recursive: true });
fs.writeFileSync(
    path.join(stub, 'apps', 'Demo', 'schema', 'app.json'),
    JSON.stringify({
        appKey: 'Demo',
        appName: 'Demo',
        appId: '000000000000000000000000',
        readonly: false,
        allowViewMutation: true,
        allowDelete: true,
        allowDiagnostics: true,
    }),
);
fs.writeFileSync(
    path.join(stub, 'secrets.json'),
    JSON.stringify({ Demo: 'x' }),
);

async function measure(args) {
    const transport = new StdioClientTransport({
        command: process.execPath,
        args: [entry, ...args],
        env: {
            ...process.env,
            KNACK_APPS_DIR: path.join(stub, 'apps'),
            KNACK_MCP_SECRETS_PATH: path.join(stub, 'secrets.json'),
        },
        stderr: 'pipe',
    });
    const client = new Client({ name: 'measure-catalogue', version: '0' });
    await client.connect(transport);
    const { tools } = await client.listTools();
    await client.close();
    return tools;
}

const listEach = process.argv.includes('--list');
const rows = [];
for (const [label, args] of [
    ['full', []],
    ['readonly', ['--readonly']],
]) {
    const tools = await measure(args);
    const bytes = JSON.stringify(tools).length;
    const descChars = tools.reduce(
        (n, t) => n + (t.description?.length ?? 0),
        0,
    );
    const schemaChars = tools.reduce(
        (n, t) => n + JSON.stringify(t.inputSchema).length,
        0,
    );
    rows.push({
        mode: label,
        tools: tools.length,
        bytes,
        tokens: Math.round(bytes / 4),
        descChars,
        schemaChars,
    });
    if (listEach && label === 'full') {
        for (const t of tools.sort(
            (a, b) => JSON.stringify(b).length - JSON.stringify(a).length,
        )) {
            console.log(
                `${String(JSON.stringify(t).length).padStart(6)}  ${t.name}`,
            );
        }
    }
}
console.table(rows);
fs.rmSync(stub, { recursive: true, force: true });
