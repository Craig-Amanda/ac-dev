#!/usr/bin/env node
// PreToolUse hook on Read. Stops the reads that cost the most tokens for the
// least use in ac-dev:
//
//  1. Always refused: any package-lock.json, anything under node_modules/,
//     dist/ or coverage/ (build and test output of knack-mcp-v2).
//  2. A text file over LIMIT_BYTES read whole, with neither offset nor limit.
//     TESTING.md (~54k tokens), view-mutations.test.ts (~53k) and
//     lib/view-safety.ts (~38k) are the usual offenders; a session almost
//     always wants one tool, function or test from them. Passing an offset or
//     a limit is the way through, deliberately: reading the whole file stays
//     possible, it just has to be asked for.
//
// Images, PDFs and notebooks are left alone: the Read tool pages or scales
// those itself. Fails open on a payload it cannot parse or a file it cannot
// stat, so a broken hook never blocks a read.

import { readFileSync, statSync } from 'node:fs';

const LIMIT_BYTES = 40 * 1024;

let input;
try {
    input = JSON.parse(readFileSync(0, 'utf8') || '{}');
} catch {
    process.exit(0);
}
const ti = input.tool_input || {};
const file = String(ti.file_path || '');
if (!file) process.exit(0);
const norm = file.replace(/\\/g, '/');

const refuse = (why) => {
    console.error(`Refused: ${why}`);
    process.exit(2);
};

if (/(^|\/)package-lock\.json$/.test(norm)) {
    refuse(
        'package-lock.json is tens of thousands of tokens. Use `npm ls <package>` for a version.',
    );
}
if (
    /(^|\/)(node_modules|dist|coverage)\//.test(norm) ||
    /\.min\.js$/.test(norm)
) {
    refuse(
        'installed, built or generated code. Read the source under src/, or grep for the one symbol you need.',
    );
}

if (ti.offset !== undefined || ti.limit !== undefined || ti.pages !== undefined)
    process.exit(0);
if (/\.(png|jpe?g|gif|webp|bmp|ico|svg|pdf|ipynb)$/i.test(norm))
    process.exit(0);

let bytes;
try {
    bytes = statSync(file).size;
} catch {
    process.exit(0);
}
if (bytes <= LIMIT_BYTES) process.exit(0);

const tokens = Math.round(bytes / 4 / 1000);
const hint = /\.(md|html)$/.test(norm)
    ? 'grep -n "^#" (or the tool name) for the section you need'
    : /\.(js|mjs|cjs|ts)$/.test(norm)
      ? 'grep -n for the tool name (knack_...), function or test name you need; .codemap/ lists what each file holds'
      : 'grep the file for what you need';
refuse(
    `${norm.split('/').slice(-3).join('/')} is about ${tokens}k tokens. ${hint}, then Read with offset and limit. ` +
        'To read the whole file anyway, pass an explicit offset or limit.',
);
