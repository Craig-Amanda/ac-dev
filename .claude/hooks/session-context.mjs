#!/usr/bin/env node
// SessionStart hook. Puts the least context that lets a fresh session start
// work without re-exploring the repository:
//
//  1. Rebuilds the code map (.codemap/, .claude/scripts/codemap.mjs) when it
//     is stale, and prints one line saying where it is. The map is how a
//     session finds a tool, function, test or doc section with one grep.
//  2. After /clear: prints the saved hand-off note (LATEST.md, written by the
//     handoff skill), and any AUTO note newer than it (written by
//     session-end.mjs when a session ended without a save).
//  3. On a fresh start or resume: prints one pointer line, so the note costs
//     nothing unless it is asked for. After a compaction: nothing about notes,
//     because the summary already carries the work.
//
// Stdout from a SessionStart hook goes into the session's context. Never
// fails the session: every error exits 0, and the map build has a timeout.

import { spawnSync } from 'node:child_process';
import { existsSync, readdirSync, readFileSync, statSync } from 'node:fs';
import { join } from 'node:path';
import { handoffDir } from './handoff-dir.mjs';

let source = 'startup';
let cwd = process.env.CLAUDE_PROJECT_DIR || process.cwd();
try {
    const input = JSON.parse(readFileSync(0, 'utf8') || '{}');
    source = input.source || source;
    cwd = input.cwd || cwd;
} catch {
    // no payload: treat as a fresh start
}

const out = [];
try {
    const root = process.env.CLAUDE_PROJECT_DIR || cwd;
    const script = join(root, '.claude', 'scripts', 'codemap.mjs');
    if (process.env.AC_DEV_SKIP_CODEMAP !== '1' && existsSync(script)) {
        const r = spawnSync('node', [script], {
            cwd: root,
            encoding: 'utf8',
            timeout: 20000,
        });
        const line = (r.stdout || '').trim().split('\n').pop();
        if (line)
            out.push(
                `${line}. Start at .codemap/INDEX.md; grep the area files rather than crawling the tree.`,
            );
    }

    const dir = handoffDir(cwd);
    const latest = dir ? join(dir, 'LATEST.md') : null;
    const hasLatest = Boolean(latest && existsSync(latest));
    const autos =
        dir && existsSync(dir)
            ? readdirSync(dir)
                  .filter((f) => /^AUTO-.*\.md$/.test(f))
                  .sort()
            : [];
    const newestAuto = autos.length ? join(dir, autos[autos.length - 1]) : null;
    const autoIsNewer = Boolean(
        newestAuto &&
        (!hasLatest || statSync(newestAuto).mtimeMs > statSync(latest).mtimeMs),
    );

    if (source === 'clear') {
        if (hasLatest)
            out.push(
                '',
                `## Hand-off note (${latest})`,
                readFileSync(latest, 'utf8').trim(),
            );
        if (autoIsNewer)
            out.push(
                '',
                `## Newer AUTO note: the last session ended without a save (${newestAuto})`,
                readFileSync(newestAuto, 'utf8').trim(),
            );
        if (hasLatest || autoIsNewer) {
            out.push(
                '',
                'Run `node .claude/skills/handoff/state.mjs` to see what moved since, then start Next steps item 1 unless it needs a person.',
            );
        }
    } else if (source !== 'compact' && (hasLatest || autoIsNewer)) {
        out.push(
            `A hand-off note exists${autoIsNewer ? ' (and a newer AUTO note)' : ''}: run /handoff resume to pick it up.`,
        );
    }
} catch {
    // never block a session over context
}

if (out.length) console.log(out.join('\n'));
process.exit(0);
