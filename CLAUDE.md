# ac-dev

Read `README.md` first: it covers the workspace layout, the single root lockfile and the
commands that reproduce CI. The rules below are the ones that are easy to miss.

## Where changes go

- All work goes into `knack-mcp-v2`. `knack-mcp` (v1) has been removed from the repository.
- Run `npm install` from the repository root only, never inside a workspace folder.
- `knack-mcp-v2/docs/ARCHITECTURE.md` has the module map, the token-budget rules for tool
  descriptions and the steps for adding a tool.
- To find a tool, function, test or doc section, grep the code map first:
  `grep -i <word> .codemap/*.md`. It is rebuilt at session start.

## Working rules

- Edit files with the Edit and Write tools, never through the shell: `sed -i`, a heredoc
  into a file or a `node -e` that writes nests quoting layers and mangles silently, so
  `.claude/hooks/block-shell-edits.mjs` refuses them.
- `.claude/hooks/guard-large-read.mjs` refuses a whole read of a file over 40 KB: find the
  line in the code map or with grep, then Read with offset and limit.
- UK English in new text, comments and names. Keep public APIs and Knack's own names.
- Reuse an existing helper before adding one; no thin wrappers or one-off abstractions.
- Ask before adding a backward-compatibility layer, alias or dual code path.
- Doc comments on new functions; other comments explain why, not what.
- Keep edits to the task and never revert changes you did not make.

## Adding, renaming or removing a tool

The catalogue is documented by hand in three places. Update all of them in the same pull
request as the code:

1. `knack-mcp-v2/README.md`: the tool's row in its group table, and the
   "N tools in full mode, N in read-only mode" line.
2. `knack-mcp-v2/docs/FEATURES.html`: the tool's row in the matching catalogue table, that
   table's count in its heading, both counts in "At a glance", the "Tool count" row of the
   side-by-side, and "What it does not do" when the tool closes a gap listed there.
3. `knack-mcp-v2/MIGRATION.md`: only when a legacy tool's mapping changes.

The `update-features-doc` skill (`.claude/skills/update-features-doc/SKILL.md`) lists every
spot to check. A hook on `gh pr create` (`.claude/hooks/check-features-doc.mjs`) refuses a
pull request that changes `knack-mcp-v2/src/tools/*.ts` without touching `FEATURES.html`;
for a change that needs no doc update, put `SKIP_FEATURES_CHECK=1` in front of the command.

`src/docs-drift.test.ts` fails when a registered tool is missing from the README or from
FEATURES.html, when either still lists a tool that no longer exists, or when the stated
counts differ from the registry. It checks names and counts, not prose: keep the
descriptions and examples accurate yourself.

## Migrations in progress

`knack-mcp-v2/scripts/ratchet.mjs` (run in CI) fails on a new use of a retired pattern;
`knack-mcp-v2/docs/migrations/` says how to move a file.

- `old-mcp-keywords`: write `_mcp_nodata _mcp_schemalock` / `_mcp_nodata _mcp_allowwrite`,
  never `_mcp_hidden` / `_mcp_writeonly`, in code, fixtures and docs.

## Before committing

A pre-commit hook (husky + lint-staged) runs `eslint --fix` and Prettier on staged files,
so formatting is fixed on the way in. It does not run the type check or the tests. Before
pushing, run what CI runs:

```bash
npm run typecheck
npm run test
npm run build
```

`npm run format` and `npm run lint` fix or report the whole workspace if the hook was
skipped.
