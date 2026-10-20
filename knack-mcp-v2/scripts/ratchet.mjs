#!/usr/bin/env node
// Ratchets for patterns the codebase is moving away from (scripts/ratchet.json).
// A long-running migration is forgotten because most of the code still shows
// the old pattern, and the old pattern is what gets copied. This makes the
// migration a check rather than a memory: a match in a file not on the rule's
// list fails, and a file on the `pending` list that no longer matches fails too,
// so the list only ever shrinks. `exceptions` are permanent and carry a reason.
// Paths in ratchet.json are relative to the repository root.
//
//   node knack-mcp-v2/scripts/ratchet.mjs          check every rule (CI)
//   node knack-mcp-v2/scripts/ratchet.mjs --fix    drop pending files that are now clean
import { execFileSync } from 'node:child_process';
import { existsSync, readFileSync, writeFileSync } from 'node:fs';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const here = dirname(fileURLToPath(import.meta.url));
const root = join(here, '..', '..');
const configPath = join(here, 'ratchet.json');
const config = JSON.parse(readFileSync(configPath, 'utf8'));
const fix = process.argv.includes('--fix');

// Untracked-but-not-ignored files count, so a new file is checked before its first commit.
const files = execFileSync(
    'git',
    ['ls-files', '--cached', '--others', '--exclude-standard'],
    { cwd: root, encoding: 'utf8' },
)
    .split('\n')
    .filter((f) => f && existsSync(join(root, f)));

const problems = [];
let changed = false;
for (const rule of config.rules) {
    const include = new RegExp(rule.include);
    const pattern = new RegExp(rule.pattern);
    const exceptions = new Set((rule.exceptions ?? []).map((e) => e.path));
    const pending = new Set(rule.pending ?? []);
    const offenders = new Map();
    for (const f of files) {
        if (!include.test(f)) continue;
        const lines = readFileSync(join(root, f), 'utf8').split('\n');
        const hit = lines.findIndex((l) => pattern.test(l));
        if (hit >= 0) offenders.set(f, hit + 1);
    }
    for (const [f, line] of offenders) {
        if (exceptions.has(f) || pending.has(f)) continue;
        problems.push(
            `${f}:${line}: ${rule.id} — ${rule.message}${rule.doc ? ` See ${rule.doc}.` : ''}`,
        );
    }
    for (const f of pending) {
        if (offenders.has(f)) continue;
        if (fix) {
            rule.pending = rule.pending.filter((p) => p !== f);
            changed = true;
            console.log(`${rule.id}: ${f} is clean, removed from pending`);
        } else {
            problems.push(
                `${f}: ${rule.id} — clean now; remove it from "pending" in knack-mcp-v2/scripts/ratchet.json (or run with --fix)`,
            );
        }
    }
}

if (changed) writeFileSync(configPath, JSON.stringify(config, null, 4) + '\n');
if (problems.length) {
    console.error(problems.join('\n'));
    process.exit(1);
}
const left = config.rules
    .map((r) => `${r.id} ${r.pending?.length ?? 0} pending`)
    .join(', ');
console.log(`ratchets clean: ${left}`);
