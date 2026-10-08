#!/usr/bin/env node
/**
 * PreToolUse hook for `gh pr create`: refuse to open a pull request that changes tool
 * code in knack-mcp-v2 without touching docs/FEATURES.html.
 *
 * CLAUDE.md requires the catalogue to be documented by hand in the README, FEATURES.html
 * and (sometimes) MIGRATION.md. `src/docs-drift.test.ts` catches tool names and counts;
 * this catches the case it cannot, a tool change whose FEATURES.html prose was never
 * revisited. The `update-features-doc` skill says what to update.
 *
 * Exit 2 blocks the call and shows stderr to Claude. Any other outcome allows it.
 * Bypass for a change that needs no doc update (a bug fix inside a tool): put
 * `SKIP_FEATURES_CHECK=1` in front of the command.
 */
import { execFileSync } from 'node:child_process';
import process from 'node:process';

const FEATURES = 'knack-mcp-v2/docs/FEATURES.html';
const TOOL_CODE = /^knack-mcp-v2\/src\/tools\/(?!.*\.test\.ts$).+\.ts$/;

const git = (...args) =>
    execFileSync('git', args, {
        encoding: 'utf8',
        stdio: ['ignore', 'pipe', 'ignore'],
    })
        .split('\n')
        .map((line) => line.trim())
        .filter(Boolean);

function changedFiles() {
    for (const base of ['origin/main', 'main']) {
        try {
            git('rev-parse', '--verify', '--quiet', base);
        } catch {
            continue;
        }
        return new Set([
            ...git('diff', '--name-only', `${base}...HEAD`),
            ...git('diff', '--name-only', 'HEAD'),
        ]);
    }
    return null;
}

let input = '';
for await (const chunk of process.stdin) input += chunk;

let command = '';
try {
    command = String(JSON.parse(input)?.tool_input?.command ?? '');
} catch {
    process.exit(0);
}
if (!/\bgh\s+pr\s+create\b/.test(command)) process.exit(0);
if (/\bSKIP_FEATURES_CHECK=1\b/.test(command)) process.exit(0);

let files;
try {
    files = changedFiles();
} catch {
    process.exit(0);
}
if (!files) process.exit(0);

const toolFiles = [...files].filter((file) => TOOL_CODE.test(file));
if (toolFiles.length === 0 || files.has(FEATURES)) process.exit(0);

process.stderr.write(
    [
        `This pull request changes tool code but not ${FEATURES}:`,
        ...toolFiles.map((file) => `  ${file}`),
        '',
        'Run the update-features-doc skill and update the catalogue row, counts and any prose the change affects, then commit and retry.',
        'If this change needs no FEATURES.html update (for example a bug fix inside an existing tool), retry with SKIP_FEATURES_CHECK=1 in front of the gh command and say why in the PR body.',
    ].join('\n') + '\n',
);
process.exit(2);
