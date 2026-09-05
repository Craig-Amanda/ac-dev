# Architecture

`knack-mcp-v2` is the Knack MCP server rebuilt as a set of small modules with one job
each. It exposes the same capabilities as `knack-mcp` (the single-file server it
replaces) with a smaller tool catalogue and testable handlers.

## Layout

```
src/
  index.ts           entry point: --readonly flag, startup banner, stdio transport
  server.ts          createServer(ctx): registers tools and the knack:// resource
  context.ts         KnackContext — apps, secrets, session state, caches, HTTP, metadata
  config.ts          environment, AppConfig, app discovery, secrets
  access.ts          permission model: read | write | delete | view | view-delete | diagnostic
  registry.ts        defineTool / registerTools — gating, logging, error shaping, once
  response.ts        makeTextResponse, size caps, overflow summaries, Knack `changes` compaction
  records.ts         dataAccess read policy and record helpers
  attachments.ts     file/image fields: resolve, download with a byte cap, extract text
  view-mutation.ts   wiring from the view tools to the safety guard: fresh metadata,
                     snapshots, human confirmation via elicitation, response shaping
  http.ts            fetch with a streaming byte cap and JSON parsing
  types.ts           cached schema / view / scene types
  tools/             one file per tool group; each exports an array of tool definitions
  lib/               pure logic, no I/O: metadata parsing, view templates, view safety,
                     field shapes, field references, seed CSVs, analysis, builder URLs
  testing/           makeFakeContext and helpers for handler tests
scripts/
  measure-catalogue.mjs   boots the built server and reports catalogue bytes per mode
```

## The three ideas

**Context, not closure.** Everything a handler needs arrives as one `KnackContext`
argument. A test builds one with `makeFakeContext` and stubs `request` and
`getRuntimeMetadata`, so every tool handler is a unit test away from a real call.

**A tool is data.** `defineTool({ name, description, access, input, handler })`. The
registry decides whether to advertise a tool (any app opted into its access level),
resolves the app and enforces the app's own toggles before the handler runs, logs the
call, and turns a thrown `Error` into a compact `{ ok: false, error }` response. A
handler contains only the tool's own logic.

**Pure logic in `lib/`.** The view-safety guard, metadata parsing, template builders and
shape catalogues have no I/O and are tested directly. `lib/view-safety.ts` is unchanged
from the legacy server: the behaviour it encodes was measured live and is recorded in
`knack-mcp/TESTED.md`.

## Token budget

The tool catalogue is sent to the model on every turn, so it is the one response the
server pays for continuously. Rules:

- One-sentence descriptions, at most 120 characters. Guidance lives in the README.
- `.describe()` only where the value format is not obvious, and at most 60 characters.
- One tool per job, with a mode parameter where legacy had several near-duplicates.
- Compact JSON responses; large raw payloads inline only under `KNACK_MCP_MAX_INLINE_DETAIL_BYTES`.
- `npm run catalogue` measures the result. Compare before merging a catalogue change.

## Adding a tool

1. Add a `defineTool` to the matching `tools/<group>.ts` and append it to that file's
   exported array.
2. Pick the lowest `access` level that covers every request the handler makes.
3. Throw plain `Error`s for validation failures; the registry shapes them.
4. Add a test in `tools/<group>.test.ts` driving `tool.handler(args, ctx)` through
   `makeFakeContext`, asserting on the payload and on the recorded REST calls.
5. Run `npm run build && npm run catalogue` and check the catalogue did not grow more
   than the tool is worth.

## Permissions

Per app, in `app.json`: `readonly: false` enables writes; `allowDelete`,
`allowViewMutation` and `allowDiagnostics` are separate opt-ins; `dataAccess` restricts
which objects and fields record tools may return. `--readonly` (or
`KNACK_MCP_READONLY=1`) pins the whole server read-only regardless of app.json. A level
is advertised when at least one app opts in; every call still checks the selected app.
