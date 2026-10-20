# Long-running code migrations

A change that takes days or weeks (retiring a keyword, moving every tool to a shared helper) is forgotten between sessions for one reason: most of the code still shows the old way, and the old way is what gets copied. Chat history and personal memory do not survive a new session, a new worktree or a second person. Three things do, because they are in the repository:

1. **A ratchet** in `knack-mcp-v2/scripts/ratchet.json`: the pattern being retired, the files not yet moved (`pending`), and any permanent `exceptions` with a reason. CI fails on a new match outside the list and on a listed file that is now clean, so the list only shrinks and the progress is the list's length. `node knack-mcp-v2/scripts/ratchet.mjs --fix` drops files that are now clean.
2. **One file here** per initiative, `docs/migrations/<slug>.md`, from the template below. It holds what the new way is and how to move one file.
3. **One line** in `ac-dev/CLAUDE.md` under "Migrations in progress", so the rule is in front of every session.

Delete the file here and the ratchet's `pending` key when the list reaches zero; keep the rule as a fence if the pattern must never return.

## Template

```markdown
# <What is moving> → <to what>

Ratchet: `<rule id>` in knack-mcp-v2/scripts/ratchet.json. Started <date>.

## The new way

<2–5 lines, or the path of a file already converted to copy>

## Moving one file

1. <step>

## Not in scope

- <what looks related but is not part of this>
```
