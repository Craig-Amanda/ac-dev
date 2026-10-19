#!/usr/bin/env node
// Builds a grep-able map of this checkout into .codemap/ (gitignored), so a
// session finds the tool, function, test or doc section it needs with one grep
// instead of crawling the tree or reading a 50k-token file whole.
//
//   node .claude/scripts/codemap.mjs            build if stale, print one line
//   node .claude/scripts/codemap.mjs --force    always rebuild
//   node .claude/scripts/codemap.mjs --check    exit 1 if stale, build nothing
//
// No LLM and no network: it reads the files git would track (tracked plus
// untracked-but-not-ignored, so a file is mapped before its first commit), so
// dist/, node_modules/ and every .env stay out of it. Purpose lines come from
// each file's first comment block. The map holds code structure and document
// headings, never data. Stale means HEAD moved, the file list changed, or a
// tracked file is newer than the map.

import { execFileSync } from 'node:child_process';
import {
    existsSync,
    mkdirSync,
    readFileSync,
    statSync,
    writeFileSync,
} from 'node:fs';
import { basename, dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const root = dirname(dirname(dirname(fileURLToPath(import.meta.url))));
const out = join(root, '.codemap');
const MAX = 140;
const args = new Set(process.argv.slice(2));

const git = (...a) =>
    execFileSync('git', ['-C', root, ...a], { encoding: 'utf8' }).trim();
const files = git('ls-files', '--cached', '--others', '--exclude-standard')
    .split('\n')
    .filter(Boolean)
    .filter((f) => existsSync(join(root, f)));
const sha = git('rev-parse', '--short', 'HEAD');
const branch = git('rev-parse', '--abbrev-ref', 'HEAD');
const read = (f) => {
    try {
        return readFileSync(join(root, f), 'utf8');
    } catch {
        return '';
    }
};
const size = (f) => statSync(join(root, f)).size;

// ---------- staleness ----------
const stampFile = join(out, '.stamp.json');
function isStale() {
    let stamp;
    try {
        stamp = JSON.parse(readFileSync(stampFile, 'utf8'));
    } catch {
        return true;
    }
    if (stamp.sha !== sha || stamp.count !== files.length) return true;
    return files.some((f) => statSync(join(root, f)).mtimeMs > stamp.builtAt);
}

if (args.has('--check')) process.exit(isStale() ? 1 : 0);
if (!args.has('--force') && !isStale()) {
    console.log(`codemap current (${branch} @ ${sha}): grep .codemap/*.md`);
    process.exit(0);
}

// ---------- extractors ----------
function clip(text) {
    const t = text.replace(/\s+/g, ' ').trim();
    if (t.length <= MAX) return t;
    const cut = t.slice(0, MAX);
    const stop = cut.lastIndexOf('. ');
    return stop > 60 ? cut.slice(0, stop + 1) : cut.replace(/\s\S*$/, '') + '…';
}

/**
 * Comment blocks in a file, each with the first paragraph of its text and the
 * code line that follows it.
 */
function commentBlocks(file) {
    const lines = read(file).split('\n').slice(0, 400);
    const blocks = [];
    let cur = null;
    const isComment = (l) => /^(\/\*|\*|\/\/|#(?!!))/.test(l);
    for (let i = 0; i < lines.length; i++) {
        const line = lines[i].trim();
        if (isComment(line)) {
            if (!cur) cur = { start: i, parts: [], done: false };
            const text = line
                .replace(/^(?:\/\*\*?|\*\/?|\/\/+|#)\s?/, '')
                .replace(/\*\/\s*$/, '')
                .trim();
            if (cur.done) continue;
            if (text.startsWith('@') || /^(eslint|prettier|@ts-)/.test(text)) {
                if (cur.parts.length) cur.done = true;
                continue;
            }
            if (text === '' || /^[=\-─*#]+$/.test(text)) {
                if (cur.parts.length) cur.done = true;
                continue;
            }
            cur.parts.push(text);
            continue;
        }
        if (cur) {
            if (line === '' && !cur.parts.length) {
                cur = null;
                continue;
            }
            cur.next = line;
            cur.text = clip(cur.parts.join(' '));
            if (cur.text) blocks.push(cur);
            cur = null;
        }
    }
    return blocks;
}

/**
 * A file's purpose, in order of preference: the comment above the export the
 * file is named after (view-safety.ts → viewSafety); a comment that opens the
 * file before any code but imports; the comment above the first exported
 * function or class. Inline comments deeper in the file are what the order
 * avoids.
 */
function purpose(file) {
    const blocks = commentBlocks(file);
    if (!blocks.length) return '';
    const stem = basename(file).replace(/(\.test)?\.[cm]?[tj]s$/, '');
    const named = stem
        .split(/[.\-_]/)
        .join('')
        .toLowerCase();
    const exportName = (b) =>
        ((b.next || '').match(
            /^export\s+(?:default\s+)?(?:async\s+)?(?:class|function|const|interface|type|enum)\s+(\w+)/,
        ) || [])[1];
    const byName = blocks.find(
        (b) => exportName(b) && exportName(b).toLowerCase() === named,
    );
    if (byName) return byName.text;
    const src = read(file).split('\n');
    const firstImport = src.findIndex((l) => /^import\b/.test(l.trim()));
    const top = blocks.find(
        (b) => b.start <= 2 || (firstImport !== -1 && b.start < firstImport),
    );
    if (top) return top.text;
    const firstExport = blocks.find((b) =>
        /^export\s+(default\s+)?(async\s+)?(class|function|const)\b/.test(
            b.next || '',
        ),
    );
    return firstExport ? firstExport.text : '';
}

const mdTitle = (f) => clip((read(f).match(/^#\s+(.+)$/m) || [null, ''])[1]);
const tok = (f) => Math.round(size(f) / 4 / 100) / 10; // thousands of tokens, roughly
const row = (f, text) => `- \`${f}\` ${text || '(no header comment)'}`;
const within = (prefix, re) =>
    files.filter((f) => f.startsWith(prefix) && (!re || re.test(f)));
const isTest = (f) => /\.test\.ts$/.test(f);
const bigNote = (f) =>
    tok(f) >= 8 ? ` [~${tok(f)}k tokens: read with offset/limit]` : '';

/** Tools a source file registers: `name: 'knack_x'` → name :line. */
function toolNames(file) {
    return read(file)
        .split('\n')
        .map((l, i) => [l.match(/^\s*name:\s*'(knack_\w+)'/), i + 1])
        .filter(([m]) => m)
        .map(([m, n]) => `  ${m[1]} :${n}`);
}

/** Top-level exported functions, classes and constants → name :line. */
function exportsOf(file) {
    return read(file)
        .split('\n')
        .map((l, i) => [
            l.match(
                /^export\s+(?:default\s+)?(?:async\s+)?(?:function\*?|class|const|interface|type|enum)\s+(\w+)/,
            ),
            i + 1,
        ])
        .filter(([m]) => m)
        .map(([m, n]) => `${m[1]} :${n}`);
}

const SRC = 'knack-mcp-v2/src/';
const sourceFiles = within(SRC, /\.ts$/).filter((f) => !isTest(f));

// ---------- sections ----------
const sections = {};

sections['tools.md'] = [
    '# Tools — src/tools/<group>.ts → purpose, then each registered tool name :line',
    '',
    ...sourceFiles
        .filter((f) => f.startsWith(`${SRC}tools/`))
        .flatMap((f) => [row(f, purpose(f) + bigNote(f)), ...toolNames(f)]),
];

sections['src.md'] = [
    '# Source — src/, src/lib/, src/testing/ → purpose; exports with lines',
    '',
    ...sourceFiles
        .filter((f) => !f.startsWith(`${SRC}tools/`))
        .flatMap((f) => {
            const ex = exportsOf(f);
            return [
                row(f, purpose(f) + bigNote(f)),
                ...(ex.length ? [`  exports: ${ex.join(' · ')}`] : []),
            ];
        }),
];

/** A test file's purpose: its header comment, else its top-level describe()/test() titles. */
function testPurpose(f) {
    const head = purpose(f);
    if (head) return head;
    const titles = [
        ...read(f).matchAll(/^(?:describe|test)\(\s*(['"`])(.+?)\1/gm),
    ].map((m) => m[2]);
    return titles.length ? clip('titles: ' + titles.join(' · ')) : '';
}
sections['tests.md'] = [
    '# Tests — file → header comment or top-level describe()/test() titles',
    '',
    ...files.filter(isTest).map((f) => row(f, testPurpose(f) + bigNote(f))),
];

/** Headings with line numbers: markdown ## / ###, or HTML <h2>/<h3>. */
function headings(f) {
    const html = f.endsWith('.html');
    return read(f)
        .split('\n')
        .map((l, i) => [
            html
                ? l.match(/<h([23])[^>]*>(.*?)<\/h\1>/)
                : l.match(/^(#{2,3})\s+(.+)$/),
            i + 1,
        ])
        .filter(([m]) => m)
        .map(([m, n]) => {
            const level = html ? '#'.repeat(Number(m[1])) : m[1];
            return `  ${level} ${clip(m[2].replace(/<[^>]+>/g, ''))} :${n}`;
        });
}
sections['docs.md'] = [
    '# Docs — file → title (size); documents over ~5k tokens list their headings with lines',
    '',
    ...files
        .filter(
            (f) =>
                /\.(md|html)$/.test(f) &&
                !f.startsWith('.claude/') &&
                !f.startsWith('knack-mcp/'),
        )
        .flatMap((f) => {
            const title = f.endsWith('.html')
                ? clip((read(f).match(/<title>(.*?)<\/title>/) || [])[1] || '')
                : mdTitle(f);
            const head = row(f, `${title} (~${tok(f)}k tokens)`);
            return tok(f) < 5 ? [head] : [head, ...headings(f)];
        }),
];

const fm = (f, key) =>
    clip(
        (
            (read(f).match(new RegExp(`^${key}:\\s*(.+)$`, 'm')) || [
                null,
                '',
            ])[1] || ''
        ).replace(/^["']|["']$/g, ''),
    );
sections['tooling.md'] = [
    '# Tooling — scripts, CI, Claude hooks and skills',
    '',
    '## knack-mcp-v2/scripts/ and Scripts/',
    ...within('knack-mcp-v2/scripts/').map((f) => row(f, purpose(f))),
    ...within('Scripts/').map((f) => row(f, purpose(f))),
    '',
    '## .github/',
    ...within('.github/').map((f) => row(f, fm(f, 'name') || purpose(f))),
    '',
    '## .claude/hooks and .claude/scripts',
    ...within('.claude/hooks/').map((f) => row(f, purpose(f))),
    ...within('.claude/scripts/').map((f) => row(f, purpose(f))),
    '',
    '## .claude/skills',
    ...within('.claude/skills/', /SKILL\.md$/).map(
        (f) => `- ${f.split('/')[2]}: ${fm(f, 'description')}`,
    ),
];

// ---------- index ----------
const big = files
    .filter((f) => !/package-lock\.json$/.test(f) && size(f) > 40 * 1024)
    .sort((a, b) => size(b) - size(a))
    .map((f) => `${f} ~${tok(f)}k`);
const index = [
    `# ac-dev code map — ${branch} @ ${sha}, built ${new Date().toISOString().slice(0, 16)}Z`,
    '',
    'Generated by `node .claude/scripts/codemap.mjs`, rebuilt at session start when stale. Do not edit.',
    '**Grep the area files; never read them whole.** e.g. `grep -i count_records .codemap/*.md`',
    '',
    '## Area files',
    ...Object.keys(sections).map(
        (s) =>
            `- ${s} (${sections[s].length} lines) — ${sections[s][0].replace(/^#\s*/, '').slice(0, 100)}`,
    ),
    '',
    '## Shape',
    '- `knack-mcp-v2/` the MCP server: `src/server.ts` wires `src/registry.ts`, tool groups in `src/tools/<group>.ts`, pure rules in `src/lib/`, HTTP in `src/http.ts`',
    '- `knack-mcp-v2/docs/ARCHITECTURE.md` module map and the steps for adding a tool; README.md and docs/FEATURES.html are the hand-kept catalogue',
    '- Patterns being migrated away from: `knack-mcp-v2/scripts/ratchet.json` (CI fails on a new match)',
    '- `dist/` is the build: gitignored, not mapped, never read',
    '',
    '## Large files (~k tokens) — find the line here, then Read with offset/limit',
    big.join(' · '),
    '',
];

mkdirSync(out, { recursive: true });
writeFileSync(join(out, 'INDEX.md'), index.join('\n'));
for (const [name, lines] of Object.entries(sections))
    writeFileSync(join(out, name), lines.join('\n') + '\n');
writeFileSync(
    stampFile,
    JSON.stringify({ sha, count: files.length, builtAt: Date.now() }),
);

const sizes = ['INDEX.md', ...Object.keys(sections)].map(
    (n) => `${n} ~${Math.round(statSync(join(out, n)).size / 4 / 100) / 10}k`,
);
console.log(`codemap built (${branch} @ ${sha}): ${sizes.join(', ')} tokens`);
