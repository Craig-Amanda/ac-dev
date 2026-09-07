# knack-mcp-v2

An MCP (Model Context Protocol) server that exposes Knack application data to AI coding
assistants: schemas, records, fields, scenes and views, with guarded view mutations.
It is the `knack-mcp` server rebuilt as small modules, with a smaller tool catalogue
so every turn costs fewer tokens. Capabilities are unchanged; where several tools did one
job they are now one tool with a mode parameter. `MIGRATION.md` maps every old name to
its new one.

This is where development happens from now on. `knack-mcp` stays in the repository and
keeps working, with no retirement date; it is simply the more expensive server to run,
since its larger catalogue is sent with every request.

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

49 tools in full mode, 34 in read-only mode. A level is advertised when at least one
app opts into it in `app.json`; every call still checks the selected app. `appKey` is
optional everywhere once `knack_set_context` has selected an app.

### Orientation

| Tool                | Access | What it does                                                                                             |
| ------------------- | ------ | -------------------------------------------------------------------------------------------------------- |
| `knack_list_apps`   | read   | Lists the apps in the folder (re-scanned), the build identity and whether this client can prompt a human |
| `knack_set_context` | read   | Selects the active app by key, or infers it from a file or folder path                                   |
| `knack_cache`       | read   | Cache and file status; with `refresh: true` clears and re-warms, `persistFiles` writes the JSON files    |

### Schema

| Tool                                | Access | What it does                                                                                                                                                    |
| ----------------------------------- | ------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `knack_list_objects`                | read   | Objects with key, name and field count                                                                                                                          |
| `knack_get_object`                  | read   | One object. `detail`: `fields` (default), `summary`, `types`, `raw` (REST object payload) or `rawMetadata` (runtime payload). Raw modes need `allowDiagnostics` |
| `knack_get_field`                   | read   | Complete raw definition of one field from the REST API                                                                                                          |
| `knack_resolve`                     | read   | Field key or fieldMap alias → key, name, type, object and Builder URL                                                                                           |
| `knack_get_object_connections`      | read   | Connection fields of an object and the objects they link to                                                                                                     |
| `knack_describe_field_shape`        | read   | Record value shapes and definition shape for a field type                                                                                                       |
| `knack_validate_field_mapping`      | read   | Validates a name → key/alias mapping                                                                                                                            |
| `knack_generate_snapshot_structure` | read   | Empty snapshot templates keyed by field key and name                                                                                                            |
| `knack_check_duplicate_field_usage` | read   | Fields referenced by more than one alias or mapping key                                                                                                         |

### Records and files

| Tool                               | Access     | What it does                                                                                        |
| ---------------------------------- | ---------- | --------------------------------------------------------------------------------------------------- |
| `knack_get_record`                 | read       | One record by id                                                                                    |
| `knack_find_records`               | read       | Filters, paging, sorting; `includeSchema` adds the object's field schema to the response            |
| `knack_get_related_records`        | read       | Records connected to a record, forward or reverse, limited to approved fields                       |
| `knack_aggregate_records`          | read       | Count and sum with grouping and date buckets; returns aggregates only                               |
| `knack_verify_record_field_shapes` | diagnostic | Compares a live record's values against the documented shapes                                       |
| `knack_create_records`             | write      | One request per record, limited concurrency, retry on 429 only; `dryRun` validates without creating |
| `knack_update_records`             | write      | Same shape for updates                                                                              |
| `knack_delete_records`             | delete     | Previews until `confirm: true`                                                                      |
| `knack_upload_asset`               | write      | Uploads a local file as a file or image asset                                                       |
| `knack_download_file`              | read       | Downloads an attachment to a temporary path under a byte cap                                        |
| `knack_read_file`                  | read       | Downloads and extracts bounded text from PDF, DOCX and text-like attachments                        |

### Views

| Tool                              | Access      | What it does                                                                                                                                               |
| --------------------------------- | ----------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `knack_list_scenes`               | read        | Scenes with key, name, slug and view count; `includeViews`, `includeBuilderUrls` opt in                                                                    |
| `knack_list_views`                | read        | Views with scene context and type; filter by scene or type                                                                                                 |
| `knack_get_view`                  | read        | One view. `detail`: `context` (default), `fields` (configured field settings) or `attributes` (needs `allowDiagnostics`; `includeRaw` inlines the payload) |
| `knack_plan_view_repoint`         | read        | Every connection reference in a view, split into rescope and retarget edits; changes nothing                                                               |
| `knack_get_view_payload_template` | read        | Starter create-view payload from a view type, or a clone of `fromViewKey` with identifiers stripped — never sent to Knack                                  |
| `knack_snapshot_app`              | read        | Writes a restore point to the local app folder: scene tree with its access fields, profile map, schema pointer, optionally one view — never sent to Knack  |
| `knack_list_page_referrers`       | read        | Views linking to a page and what removing each link would do to it; `includeDescendants` adds the pages beneath                                            |
| `knack_get_page_access`           | read        | Who can reach a page: walks up to the nearest login and lists the roles it admits — public, protected or unknown                                           |
| `knack_create_view`               | view        | Creates a view from a full definition                                                                                                                      |
| `knack_update_view_order`         | view        | Reorders views and page groups on a scene                                                                                                                  |
| `knack_update_view`               | view        | Merges changes into the live definition and sends it whole; a dropped last link goes to the human                                                          |
| `knack_copy_view`                 | view        | Knack's copy (`sharePages: false`) or a create from the source definition that keeps child pages shared (`sharePages: true`)                               |
| `knack_move_view`                 | view        | Moves a view; owned child pages go to the human                                                                                                            |
| `knack_delete_view`               | view-delete | Deletes a view; pages reached only through it go to the human                                                                                              |

### Analysis

| Tool                          | Access | What it does                                                                                                                   |
| ----------------------------- | ------ | ------------------------------------------------------------------------------------------------------------------------------ |
| `knack_get_context_bundle`    | read   | Selected object schemas, aliases and view context in one call                                                                  |
| `knack_get_app_overview`      | read   | Every object with counts, types and relationships                                                                              |
| `knack_analyze_data_model`    | read   | Design feedback on the data model                                                                                              |
| `knack_app_deep_dive`         | read   | One-call onboarding snapshot                                                                                                   |
| `knack_list_field_references` | read   | References to a field across schema, aliases and views; `classification` filters (e.g. `viewRecordRule`), `groupByView` groups |
| `knack_search_ktl_keywords`   | read   | KTL underscore keywords in view titles and descriptions                                                                        |
| `knack_search_emails`         | read   | Email rules and actions in views                                                                                               |
| `knack_generate_seed_csvs`    | read   | Import-ready seed CSV content per object                                                                                       |

### Fields

| Tool                    | Access | What it does                                                                                  |
| ----------------------- | ------ | --------------------------------------------------------------------------------------------- |
| `knack_create_field`    | write  | Creates a field; `dryRun` validates the definition                                            |
| `knack_update_field`    | write  | Merges changed properties; protects KTL keywords in descriptions; `dryRun` previews the merge |
| `knack_delete_field`    | delete | Deletes a field                                                                               |
| `knack_duplicate_field` | write  | Copies a field under a new name                                                               |

The MCP resource `knack://<AppKey>/schema`, `.../fieldMap` and `.../viewMap` serve the
cached JSON documents directly.

## Finding what points at a page

`knack_list_page_referrers` answers the direction a snapshot cannot. A snapshot reads
downward — a page and what hangs off it; this reads upward, which is the direction that
decides consequences. Use it before a delete to see what breaks, and after a rebuild to
find references left pointing at a key that no longer exists (a rebuilt page always
carries a new key — Knack assigns them).

It runs on the same referrer index the cascade guard already trusts, so it is the guard's
own answer asked from the other end. Where two or more views link to a page it says the
transfer destination is **unmeasured** rather than guessing: a transfer has only been
observed with a single referrer left. To make a destination certain, remove the links you
do not want the page under first, so exactly one referrer remains when the owning link
goes.

## Who can reach a page

`knack_get_page_access` answers a question no scene field answers on its own. Measured
on 7 September (`TESTING.md` Tier 7): when a login is added to a page, Knack does not
mark that page — it inserts a new scene of `type: "authentication"` above it, holding a
`login` view, and re-parents the page under it. The roles live on that login view
(`allowed_profiles`, `limit_profile_access`) and nowhere else; no page beneath carries
any. The page directly under the login even carries `authenticated: false`, the same as
a public one. So the tool walks the parent chain upward to the nearest login and reports
what it found: `public`, `protected` with the roles (each mapped to the user object that
defines it, since a profile key alone tells a person nothing), or `unknown` with the
reason — a parent that matches no page, a loop, a login view without its role fields.
Unknown is never reported as public.

The same resolution feeds two places that used to ask instead of answer:

- The cascade prompt on a move or a transfer now names the audience on both sides —
  who reaches each page now, who reaches its replacement or its new parent — and says
  `CHANGES`, `unchanged`, or `UNKNOWN — verify in the builder`. Where one side cannot be
  resolved it falls back to the old `CHECK THE AUDIENCE` wording; unknown is never
  rounded to unchanged.
- Snapshots are `snapshotVersion: 3`: each scene carries `sceneType`, `authenticated`
  and, on a login view, `allowedProfiles` / `limitProfileAccess`, and the file carries a
  `profiles` map from profile key to object. A page rebuilt from a version 2 snapshot came
  back with no access control and nothing in the file saying what it had been.

## View safety

Knack's view `PUT` replaces rather than patches, and a page is destroyed when the
definition Knack receives no longer carries the last link to it. Every view mutation
runs through one guard: it reads fresh metadata, works out which pages would lose their
last link, writes a snapshot, and puts any destruction to a human through MCP
elicitation. A client that cannot prompt a human cannot cascade-delete through this
server. The rules, their evidence and the corrections made along the way are in
`../knack-mcp/TESTED.md`; the guard itself is `src/lib/view-safety.ts`, which tracks
the legacy guard rule for rule — the two are fixed together so the differential pass
stays meaningful.

## Development

```bash
npm run build -w knack-mcp-v2
npm test -w knack-mcp-v2
npm run catalogue -w knack-mcp-v2
```

See `docs/ARCHITECTURE.md` for the module map and how to add a tool.
