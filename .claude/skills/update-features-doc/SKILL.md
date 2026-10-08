---
name: update-features-doc
description: Update knack-mcp-v2's hand-written catalogue docs (README.md and docs/FEATURES.html) for a tool change. Use before opening any pull request that adds, renames, removes or changes the behaviour of a tool.
---

# Update the catalogue docs

`knack-mcp-v2/README.md` and `knack-mcp-v2/docs/FEATURES.html` list every tool by hand.
`src/docs-drift.test.ts` fails on a missing or stale tool name or a wrong count. It does
not read prose, so descriptions and examples are yours to keep accurate. A hook on
`gh pr create` refuses a pull request that changes `src/tools/*.ts` without touching
FEATURES.html.

## 1. Find what changed

- `git diff main...HEAD --stat -- knack-mcp-v2/src/tools` and read the tool definitions
  that changed: `name`, `access`, `input` and what the handler returns.
- Decide which case applies: **added**, **renamed**, **removed**, **behaviour changed**
  (new parameter, new limit, new refusal), or **no doc-visible change** (a bug fix).

## 2. README.md

- The tool's row in its group table. Keep the access column, and say what is new in the
  last column.
- The "N tools in full mode, N in read-only mode" line. A `read` tool counts in both; a
  `write`, `delete`, `view`, `view-delete` or `diagnostic` tool counts in full mode only.

## 3. docs/FEATURES.html

Check each of these. A tool change usually touches more than the table row.

- **Catalogue table row** for the tool, in the matching `<h3>` section. Copy a neighbouring
  row's markup, including the access badge (`b-read`, `b-write`, `b-diag` and so on).
- **The count in that section's heading**, e.g. `Records and files (13)`.
- **"At a glance"**: both stat tiles, full mode and read-only mode.
- **Side-by-side table**: the "Tool count" row, e.g. `74 (40 read-only)`.
- **"What it does not do"**: remove or reword a bullet if the tool closes a gap it lists.
- **Worked examples and prose** that name a tool. Grep for the tool's name and for its
  neighbours: an example that recommends a heavier tool for a job the new one now does
  should say so.
- **Removed or renamed tool**: delete or rename every mention, not just the row.

## 4. MIGRATION.md

Only when a legacy tool's mapping changes. A new tool with no legacy equivalent needs
nothing here.

## 5. Check

Run from the repository root:

```bash
npx prettier --write knack-mcp-v2/README.md knack-mcp-v2/docs/FEATURES.html
npm run test
```

`docs-drift.test.ts` must pass. Then run `npm run build && npm run catalogue` in
`knack-mcp-v2` and put the token change in the PR body.

## When no update is needed

For a bug fix inside an existing tool that changes nothing a reader of the docs would
see, the hook can be bypassed by putting `SKIP_FEATURES_CHECK=1` in front of the
`gh pr create` command. Say why in the PR body.
