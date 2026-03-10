# ac-dev

This folder is a lightweight workspace for related development projects.

## Structure

- Keep each project self-contained in its own folder.
- Add shared code only when at least two projects genuinely need the same behavior.
- Prefer extracting shared code into its own package rather than importing files across project folders.

## Current Workspace

- `knack-mcp`: MCP server project.

## Recommended Pattern

Use this folder as a workspace container, not as one large application.

- Project-specific runtime code, configs, and dependencies should stay inside each project.
- Shared utilities should live in a dedicated workspace package once they stabilize.
- Avoid creating a shared folder too early; it tends to collect one-off code that should remain local.

## Useful Commands

From this folder:

- `npm run build` to build all workspace projects that define a build script.
- `npm run test` to run tests for all workspace projects that define a test script.
- `npm run lint` to lint all workspace projects that define a lint script.

To add another Node-based project later, place it in a child folder and add it to `workspaces` in `package.json`.