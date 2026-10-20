#!/usr/bin/env node
// SessionEnd hook. A no-LLM safety net for a /clear or exit without the
// handoff skill run first: it writes AUTO-<stamp>.md beside the hand-off
// notes (see handoff-dir.mjs) from the transcript tail — the person's last
// prompts, the files edited, PR links — and session-context.mjs shows it
// after the clear. Keeps the newest 10.
//
// The note holds prompts and file paths, never tool output, so records or
// file contents read during the session do not land in it. It stays inside
// .git and is never committed.
//
// Reads the hook JSON ({transcript_path, reason, session_id, cwd}) on stdin.
// Never fails the session: every error exits 0.

import {
    closeSync,
    existsSync,
    mkdirSync,
    openSync,
    readFileSync,
    readSync,
    statSync,
    unlinkSync,
    writeFileSync,
} from 'node:fs';
import { join } from 'node:path';
import { autoNotes, handoffDir } from './handoff-dir.mjs';

const TAIL_BYTES = 4 * 1024 * 1024;
const KEEP = 10;

/** The last TAIL_BYTES of the transcript as complete JSONL lines; transcripts reach tens of MB. */
function tailLines(file) {
    const size = statSync(file).size;
    const start = Math.max(0, size - TAIL_BYTES);
    const fd = openSync(file, 'r');
    const buf = Buffer.alloc(size - start);
    readSync(fd, buf, 0, buf.length, start);
    closeSync(fd);
    const lines = buf.toString('utf8').split('\n');
    return (start > 0 ? lines.slice(1) : lines).filter(Boolean);
}

/**
 * The AUTO note for this session, or null when there is nothing worth keeping:
 * no prompts or edits, or the hand-off note was saved after the last prompt
 * (the usual /handoff then /clear, which already left a better note).
 */
function buildNote(hook, lines, savedAt) {
    const prompts = [];
    const edited = new Set();
    const prs = new Set();
    let title = '';
    let lastPromptAt = 0;
    for (const line of lines) {
        let j;
        try {
            j = JSON.parse(line);
        } catch {
            continue;
        }
        if (j.type === 'custom-title' && j.customTitle) title = j.customTitle;
        if (j.type === 'pr-link')
            (JSON.stringify(j).match(/pull\/\d+/g) || []).forEach((p) =>
                prs.add('#' + p.slice(5)),
            );
        if (j.isSidechain || !j.message) continue;
        const content = j.message.content;
        if (j.type === 'user' && !j.isMeta) {
            const text =
                typeof content === 'string'
                    ? content
                    : ((content || []).find((c) => c.type === 'text') || {})
                          .text;
            // Tagged text is a notification or command echo, not something a person typed
            if (text && !text.trimStart().startsWith('<')) {
                prompts.push(text.replace(/\s+/g, ' ').trim().slice(0, 300));
                lastPromptAt = Date.parse(j.timestamp) || lastPromptAt;
            }
        }
        if (j.type === 'assistant' && Array.isArray(content)) {
            for (const c of content) {
                if (
                    c.type === 'tool_use' &&
                    /^(Edit|Write|MultiEdit|NotebookEdit)$/.test(c.name) &&
                    c.input?.file_path
                ) {
                    edited.add(c.input.file_path.replace(/\\/g, '/'));
                }
            }
        }
    }
    if (!prompts.length && !edited.size) return null;
    if (savedAt && savedAt >= lastPromptAt) return null;
    return [
        `# AUTO note (the handoff skill was not run) — ${new Date().toISOString().slice(0, 16)}Z`,
        `Session ${hook.session_id} ended (${hook.reason})${title ? ` — "${title}"` : ''}. Mechanical extract; check before trusting.`,
        '',
        '## Last prompts (oldest first)',
        ...prompts.slice(-8).map((p) => `- ${p}`),
        '',
        '## Files edited (tail of session)',
        ...[...edited].slice(-25).map((f) => `- ${f}`),
        ...(prs.size ? ['', `## PRs linked: ${[...prs].join(' ')}`] : []),
        '',
    ].join('\n');
}

try {
    const hook = JSON.parse(readFileSync(0, 'utf8') || '{}');
    const dir = handoffDir(
        hook.cwd || process.env.CLAUDE_PROJECT_DIR || process.cwd(),
    );
    if (dir && hook.transcript_path && existsSync(hook.transcript_path)) {
        const latest = join(dir, 'LATEST.md');
        const savedAt = existsSync(latest) ? statSync(latest).mtimeMs : 0;
        const note = buildNote(hook, tailLines(hook.transcript_path), savedAt);
        if (note) {
            mkdirSync(dir, { recursive: true });
            // To the second: worktree sessions share this folder and can end in the same minute.
            const stamp = new Date()
                .toISOString()
                .slice(0, 19)
                .replace(/[-:T]/g, '');
            writeFileSync(join(dir, `AUTO-${stamp}.md`), note);
            const autos = autoNotes(dir);
            autos
                .slice(0, Math.max(0, autos.length - KEEP))
                .forEach((f) => unlinkSync(join(dir, f)));
        }
    }
} catch {
    // A hook must never break /clear or exit
}
process.exit(0);
