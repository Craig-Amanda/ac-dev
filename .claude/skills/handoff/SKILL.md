---
name: handoff
description: Save a hand-off note before /clear (the default), or `resume` to pick one up. Writes LATEST.md under the clone's .git/claude-handoff/, shared by every worktree and never committed; the SessionStart hook prints it after /clear.
argument-hint: '[resume]'
---

# Hand-off

Clearing often is the cheapest way to keep token use down, and this skill is how the work survives the clear. A note costs a few hundred tokens after `/clear`; re-exploring the repository to find where things stood costs tens of thousands.

Notes live in `$(git rev-parse --git-common-dir)/claude-handoff/`: inside `.git`, so never committed, and shared by every worktree of the clone. `node .claude/skills/handoff/state.mjs` prints the exact folder.

This note is for one stretch of work. The owner's standing constraints for live Knack testing stay in `ARC-KNACK-CODE/HANDOFF.md` outside the repository; don't copy them in.

## `/handoff` (save, the default)

1. Run `node .claude/skills/handoff/state.mjs` and `date -u +%Y-%m-%dT%H:%MZ`. Use the stamp as printed; never guess a time.
2. If `LATEST.md` exists in the notes folder, move it to `archive/<its first-line stamp as YYYYMMDD-HHMM>.md` there (create `archive/`).
3. Write a new `LATEST.md` from the template below. **60 lines at most.** It is read into context on every `/clear`, so every line earns its place:
    - facts from this session only: no copy of git or PR state (the state script re-runs on resume), no repository rules (they live in CLAUDE.md and the skills);
    - exact paths, branches, PR numbers, tool names, view/scene/field keys and commands; no narrative;
    - "Next steps" item 1 must be something a fresh session can start on immediately;
    - **never record data or app names**: nothing a Knack query returned. An appKey alias, object or view key is fine.
4. A durable lesson goes to a memory file, not the note; link it by name.
5. Say in one line: "Hand-off saved, safe to /clear." Do no more work after it.

### Template

```markdown
# <one-line title of what we were doing> — <date -u stamp>

## Goal

<1–2 lines: the outcome the person wants>

## Done this session

- <PR/commit/branch — what, merged or open>

## In flight

- <worktree path> [<branch>] — <state: edited/tested/pushed> — next: <exact step>

## Next steps (in order)

1. <runnable first step>

## Waiting on a person

- <who — question or sign-off>

## Ruled out / don't redo

- <what was checked and why it is not the answer>

## Open first

- <path:line — why> (5 at most)
```

## `/handoff resume`

Read `LATEST.md`, and the newest `AUTO-*.md` if it is newer: an AUTO note means a session ended without a save, and it lists only the last prompts and edited files. Run `state.mjs` to see what moved since. Then say in three to five lines where things stand, and start "Next steps" item 1 unless it needs a person.

The notes are written and read by the `SessionEnd` and `SessionStart` hooks (`.claude/hooks/session-end.mjs`, `session-context.mjs`); their header comments say what each does.
