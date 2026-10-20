# `_mcp_hidden` / `_mcp_writeonly` → `_mcp_nodata` with `_mcp_schemalock` / `_mcp_allowwrite`

Ratchet: `old-mcp-keywords` in knack-mcp-v2/scripts/ratchet.json. Started 2026-10-19, after the keyword redesign in PR 79.

## The new way

- `_mcp_hidden` → `_mcp_nodata _mcp_schemalock`
- `_mcp_writeonly` → `_mcp_nodata _mcp_allowwrite`

The old names keep working as aliases (`src/lib/field-exclusion.ts`) and warn when read. New code, fixtures and docs use the new names.

## Moving one file

1. Replace each old keyword in a fixture description with its expansion above.
2. Update any assertion that matches the old name in a message (`/_mcp_writeonly/`) to the message the new keywords produce.
3. Run `npm run test`, then `node knack-mcp-v2/scripts/ratchet.mjs --fix` to drop the file from `pending`.

A test that exists to prove the alias still works belongs in `field-exclusion.test.ts`, or goes in `exceptions` with that reason.

## Not in scope

- Removing the aliases themselves: Knack apps in the field still carry the old names in their field descriptions.
