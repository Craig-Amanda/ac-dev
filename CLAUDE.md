# ac-dev

Read `README.md` first: it covers the workspace layout, the single root lockfile and the
commands that reproduce CI. The rules below are the ones that are easy to miss.

## Where changes go

- All work goes into `knack-mcp-v2`. `knack-mcp` (v1) is frozen until it is deleted on
  12 October 2026; do not edit it.
- Run `npm install` from the repository root only, never inside a workspace folder.
- `knack-mcp-v2/docs/ARCHITECTURE.md` has the module map, the token-budget rules for tool
  descriptions and the steps for adding a tool.

## Adding, renaming or removing a tool

The catalogue is documented by hand in three places. Update all of them in the same pull
request as the code:

1. `knack-mcp-v2/README.md`: the tool's row in its group table, and the
   "N tools in full mode, N in read-only mode" line.
2. `knack-mcp-v2/docs/FEATURES.html`: the tool's row in the matching catalogue table, that
   table's count in its heading, both counts in "At a glance", the "Tool count" row of the
   side-by-side, and "What it does not do" when the tool closes a gap listed there.
3. `knack-mcp-v2/MIGRATION.md`: only when a legacy tool's mapping changes.

`src/docs-drift.test.ts` fails when a registered tool is missing from the README or from
FEATURES.html, when either still lists a tool that no longer exists, or when the stated
counts differ from the registry. It checks names and counts, not prose: keep the
descriptions and examples accurate yourself.

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
