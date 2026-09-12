# ac-dev

This folder is a lightweight workspace for related development projects.

## Structure

- Keep each project self-contained in its own folder.
- Add shared code only when at least two projects genuinely need the same behavior.
- Prefer extracting shared code into its own package rather than importing files across project folders.

## Current Workspace

- `knack-mcp` (v1): the original Knack MCP server (single-file). **Deprecated — use at your own risk.** See the notice below.
- `knack-mcp-v2`: the same server rebuilt as small modules with a smaller tool catalogue. **New development happens here.** It does the same work with fewer tools, so every turn costs fewer tokens — about a third less in full mode. See `knack-mcp-v2/README.md` and `knack-mcp-v2/MIGRATION.md` for the move.

## Deprecation Notice: `knack-mcp` (v1)

> **Warning: `knack-mcp` (v1) is deprecated and will be removed on 12 October 2026 (30 days from 12 September 2026).**
>
> - **Use v1 at your own risk.** It receives no further fixes or updates.
> - **Do not use v1 to update any frontend views.** All view changes must go through `knack-mcp-v2`; the v1 view tools are no longer maintained or supported.
> - **Switch to `knack-mcp-v2` as soon as possible.** Every v1 tool maps to a v2 tool: see `knack-mcp-v2/MIGRATION.md` for the mapping and `knack-mcp-v2/README.md` for setup.
> - After the removal date, the `knack-mcp` folder and its `server-readonly.js` / `server-full.js` entry points will no longer exist in this repository, and any MCP client configuration still pointing at them will stop working.
>
> **Note for contributors:** `knack-mcp` (v1) is frozen. Do not open pull requests against it, add features to it, or fix bugs in it; every change, including bug fixes, goes into `knack-mcp-v2`. Pull requests that touch `knack-mcp` will be closed. If you still depend on v1 for anything, migrate it before 12 October 2026, because the folder is deleted on that date.

## Recommended Pattern

Use this folder as a workspace container, not as one large application.

- Project-specific runtime code, configs, and dependencies should stay inside each project.
- Shared utilities should live in a dedicated workspace package once they stabilize.
- Avoid creating a shared folder too early; it tends to collect one-off code that should remain local.

## Useful Commands

**Install from this folder only.** The workspace has one lockfile — the root `package-lock.json` — and it covers every workspace project's dependencies:

- `npm install` installs the whole workspace. Dependencies hoist into the root `node_modules`, and each workspace folder is linked in from there.
- `npm ci` for an exact lockfile install, which is what CI runs.

Never run `npm install` inside a workspace folder. It writes a second lockfile there that goes stale against this one, and anything resolving against the stale copy has to re-fetch dependency trees the root already holds.

These reproduce the CI jobs exactly, so running them here before pushing tells you what the pipeline will say:

- `npm run build` builds every workspace project that defines a build script.
- `npm run test` runs tests for every workspace project that defines one.
- `npm run lint` lints the workspace, then each project.
- `npm run format:check` checks formatting; `npm run format` writes it.
- `npm run audit` fails on a high-severity advisory.

`build` and `test` also run from inside a workspace folder for a quick iteration — neither needs registry access. Install is the only one that has to come from here.

To add another Node-based project later, place it in a child folder and add it to `workspaces` in `package.json`. Do not commit a lockfile inside it.
