# knack-mcp-v2

An MCP (Model Context Protocol) server that exposes Knack application data to AI coding
assistants: schemas, records, fields, scenes and views, with guarded view mutations.
It is the `knack-mcp` server rebuilt as small modules, with a smaller tool catalogue
so every turn costs fewer tokens. Capabilities are unchanged; where several tools did one
job they are now one tool with a mode parameter. `MIGRATION.md` maps every old name to
its new one.

## Setup

Install from the repository root (one lockfile covers every workspace):

```bash
cd ..            # the ac-dev root
npm install
npm run build -w knack-mcp-v2
```

Then point your MCP client at the built entry:

```json
{
    "mcpServers": {
        "knack": {
            "command": "node",
            "args": ["/absolute/path/to/ac-dev/knack-mcp-v2/dist/index.js"],
            "env": {
                "KNACK_APPS_DIR": "/absolute/path/to/KnackApps",
                "KNACK_MCP_SECRETS_PATH": "/absolute/path/to/.knack-mcp-secrets.json"
            }
        }
    }
}
```

Add `"--readonly"` to `args` for a launcher that must never write.

### Apps folder

`KNACK_APPS_DIR` holds one folder per app with `schema/app.json` (legacy `app.json` at
the app root is still read):

```json
{
    "appKey": "ARC",
    "appName": "ARC Portal",
    "appId": "5f1e...",
    "readonly": false,
    "allowViewMutation": true,
    "allowDelete": false,
    "allowDiagnostics": false,
    "builderAccountSlug": "my-account",
    "builderAppSlug": "arc-portal",
    "dataAccess": {
        "allowedObjectKeys": ["object_1"],
        "allowedFieldKeys": { "object_1": ["field_1", "field_2"] },
        "redactedFieldKeys": ["field_9"],
        "maxRecordsPerQuery": 200
    }
}
```

Only `appKey` and `appId` are required. Writes need `readonly: false`; deletes, view
mutations and raw diagnostics are separate opt-ins. `dataAccess` is optional and
restricts what record tools may return.

Optional cache files beside `app.json` (`schema.json`, `fieldMap.json`, `viewMap.json`,
`fieldReferenceIndex.json`) are used when the runtime API is unavailable and are written
by `knack_cache` with `refresh: true, persistFiles: true`. View mutations write restore
points to `schema/snapshots/`.

### Secrets

`KNACK_MCP_SECRETS_PATH` (default `~/.knack-mcp-secrets.json`) maps app keys to REST API
keys:

```json
{ "ARC": "your-rest-api-key" }
```

## Environment variables

| Variable                             | Default                     | Meaning                                                        |
| ------------------------------------ | --------------------------- | -------------------------------------------------------------- |
| `KNACK_APPS_DIR`                     | required                    | Folder of app folders                                          |
| `KNACK_MCP_SECRETS_PATH`             | `~/.knack-mcp-secrets.json` | App key to REST key map                                        |
| `KNACK_MCP_READONLY`                 | unset                       | `1` pins the server read-only, same as `--readonly`            |
| `DEBUG`                              | `false`                     | `1`/`true` logs each call and request to stderr                |
| `KNACK_CACHE_TTL_MS`                 | `300000`                    | In-memory metadata cache lifetime                              |
| `KNACK_MAX_RESPONSE_BYTES`           | `20971520`                  | Largest upstream body read                                     |
| `KNACK_MCP_MAX_TOOL_TEXT_BYTES`      | `262144`                    | Largest tool response; larger ones become a structural summary |
| `KNACK_MCP_MAX_INLINE_DETAIL_BYTES`  | `49152`                     | Largest raw payload inlined inside a response                  |
| `KNACK_MCP_MAX_EXTRACTED_TEXT_BYTES` | `196608`                    | Longest attachment text returned by `knack_read_file`          |
| `KNACK_MCP_BATCH_CONCURRENCY`        | `5`                         | Concurrent requests for batch record tools (max 10)            |
| `KNACK_MCP_PRETTY_TOOL_JSON`         | `false`                     | Pretty-print responses (costs tokens)                          |

`KNACK_MCP_COMPACT_TOOL_METADATA` from `knack-mcp` is gone: descriptions are written
short at source instead of being trimmed at startup.

## Token cost

The tool catalogue is sent with every request. `npm run build && npm run catalogue`
measures it against a stub app in both modes. Numbers at the time of writing are in
`MIGRATION.md`.

Responses are compact JSON, sized to what was asked: list tools omit builder URLs and
per-item detail unless requested, raw payloads inline only under the inline-detail cap,
and every view mutation returns Knack's `changes` block reduced to keys and page
identities.

## Tools

<!-- TOOLS -->

## View safety

Knack's view `PUT` replaces rather than patches, and a page is destroyed when the
definition Knack receives no longer carries the last link to it. Every view mutation
runs through one guard: it reads fresh metadata, works out which pages would lose their
last link, writes a snapshot, and puts any destruction to a human through MCP
elicitation. A client that cannot prompt a human cannot cascade-delete through this
server. The rules, their evidence and the corrections made along the way are in
`../knack-mcp/TESTED.md`; the guard itself is `src/lib/view-safety.ts`, unchanged.

## Development

```bash
npm run build -w knack-mcp-v2
npm test -w knack-mcp-v2
npm run catalogue -w knack-mcp-v2
```

See `docs/ARCHITECTURE.md` for the module map and how to add a tool.
