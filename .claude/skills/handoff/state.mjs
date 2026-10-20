#!/usr/bin/env node
// Prints the live working state in about 20 lines, so a hand-off note never
// has to restate what git and GitHub already know.
//
//   node .claude/skills/handoff/state.mjs
//
// Every worktree of this clone (branch, uncommitted files, commits ahead of
// origin/main, last commit), then the open PRs on Craig-Amanda/ac-dev with
// their merge state, then where the hand-off notes are. Read-only: no fetch,
// no checkout, so it is safe while other sessions work in the other worktrees.

import { execFileSync } from 'node:child_process';
import { existsSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { autoNotes, handoffDir } from '../../hooks/handoff-dir.mjs';

const GH_REPO = 'Craig-Amanda/ac-dev';
const repo = dirname(dirname(dirname(dirname(fileURLToPath(import.meta.url)))));

/** Trimmed stdout of a command, or '' on any failure. */
function run(cmd, args, timeout = 8000) {
    try {
        return execFileSync(cmd, args, {
            encoding: 'utf8',
            timeout,
            stdio: ['ignore', 'pipe', 'ignore'],
        }).trim();
    } catch {
        return '';
    }
}

const lines = [];
const main = run('git', ['-C', repo, 'rev-parse', '--short', 'origin/main']);
lines.push(
    `## Live state (${new Date().toISOString().slice(0, 16)}Z) — origin/main @ ${main || '?'}`,
);

const worktrees = run('git', ['-C', repo, 'worktree', 'list', '--porcelain'])
    .split(/\n\s*\n/)
    .map((block) => ({
        dir: (block.match(/^worktree (.+)$/m) || [])[1],
        branch: (
            (block.match(/^branch (.+)$/m) || [])[1] || '(detached)'
        ).replace('refs/heads/', ''),
    }))
    .filter((w) => w.dir);

lines.push(
    '',
    '### Worktrees (dirty = uncommitted files, ahead = commits not on origin/main)',
);
for (const w of worktrees) {
    if (!existsSync(w.dir)) {
        lines.push(`- ! ${w.dir} [${w.branch}] folder missing (prunable)`);
        continue;
    }
    const dirty = run('git', ['-C', w.dir, 'status', '--porcelain'])
        .split('\n')
        .filter(Boolean).length;
    const ahead = run('git', [
        '-C',
        w.dir,
        'rev-list',
        '--count',
        'origin/main..HEAD',
    ]);
    const last = run('git', [
        '-C',
        w.dir,
        'log',
        '-1',
        '--format=%cr: %s',
    ]).slice(0, 90);
    const quiet = dirty === 0 && ahead === '0';
    lines.push(
        `- ${quiet ? '·' : '*'} ${w.dir} [${w.branch}] dirty ${dirty}, ahead ${ahead || '?'} — ${last}`,
    );
}

const json = run(
    'gh',
    [
        'pr',
        'list',
        '--repo',
        GH_REPO,
        '--state',
        'open',
        '--limit',
        '30',
        '--json',
        'number,title,headRefName,isDraft,mergeStateStatus',
    ],
    20000,
);
lines.push('', `### Open PRs on ${GH_REPO}`);
if (!json) {
    lines.push('- (gh unavailable or timed out)');
} else {
    const prs = JSON.parse(json);
    if (!prs.length) lines.push('- none');
    for (const p of prs)
        lines.push(
            `- #${p.number} ${p.isDraft ? '[draft] ' : ''}${p.mergeStateStatus} ${p.headRefName} — ${p.title.slice(0, 80)}`,
        );
}

const dir = handoffDir(repo);
const hasLatest = Boolean(dir && existsSync(join(dir, 'LATEST.md')));
lines.push(
    '',
    `### Hand-off notes: ${dir || '(not a git checkout)'}`,
    `- ${hasLatest ? 'LATEST.md' : 'no LATEST.md'}, ${autoNotes(dir).length} AUTO note(s)`,
);

console.log(lines.join('\n'));
