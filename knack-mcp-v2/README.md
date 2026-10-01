# knack-mcp-v2

An MCP (Model Context Protocol) server that exposes Knack application data to AI coding
assistants: schemas, records, fields, scenes and views, with guarded view mutations.
It is the `knack-mcp` server rebuilt as small modules, with a smaller tool catalogue
so every turn costs fewer tokens. Capabilities are unchanged; where several tools did one
job they are now one tool with a mode parameter. `MIGRATION.md` maps every old name to
its new one.

This is where development happens from now on. `knack-mcp` (v1) is deprecated: use it
at your own risk, never for updating frontend views, and switch to this server as soon
as possible. v1 will be removed from the repository on 12 October 2026 (30 days from
12 September 2026).

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
    "allowAudit": false,
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
mutations, raw diagnostics and the exposure audit (`allowAudit`) are separate opt-ins. `dataAccess` is optional and
restricts what record tools may return.

### Field exclusion keywords

A person can limit what the model reads or changes by putting a KTL-style keyword in a
field's description in the Knack builder. They work with or without a `dataAccess` block,
and need no restart. Every field stays visible in the schema, so the model can still be
asked to work with it: the keywords limit its data and its definition, not whether the
model knows it exists.

| Keyword           | Record reads                            | Record writes | Filter, sort, aggregate, download | Field definition                                                                                    |
| ----------------- | --------------------------------------- | ------------- | --------------------------------- | --------------------------------------------------------------------------------------------------- |
| `_mcp_nodata`     | Value and `_raw` read as `"[redacted]"` | Refused       | Refused                           | Editable                                                                                            |
| `_mcp_allowwrite` | With `_mcp_nodata`: still redacted      | Allowed       | Refused                           | Editable                                                                                            |
| `_mcp_schemalock` | Normal                                  | Allowed       | Allowed                           | `update_field`, `delete_field`, `duplicate_field` and `delete_object` refuse                        |
| `_mcp_tablelock`  | Normal                                  | Allowed       | Allowed                           | The whole table: no field added, edited, duplicated or deleted, and the table not edited or deleted |

- **Combining:** keywords add up. `_mcp_nodata _mcp_schemalock` on one field means no
  data and no schema changes, while the field is still visible and usable.
- **Old names (deprecated):** `_mcp_writeonly` still works and means
  `_mcp_nodata _mcp_allowwrite`. `_mcp_hidden` still works and means
  `_mcp_nodata _mcp_schemalock`; it no longer hides the field, but its data is no less
  protected. Both are flagged: `knack_get_object` gives each field that still carries
  one a `keywordWarnings` entry naming the replacement, and `create_field` and
  `update_field` warn when a description they write contains one. Nothing is refused, and
  the model cannot swap them itself, since that means removing a keyword: a person
  replaces them in the builder.
- **Table lock:** Knack tables have no description, so `_mcp_tablelock` goes in the
  description of any one field on the table and locks all of it.
- **Keywords for a whole table:** a table's own keywords go in the description of its
  auto-increment field, after its `_notes`, and apply to **every field on the table**:
  `_notes=[Payroll | Craig on 2026-09-30] _mcp_nodata` makes every field on that table
  no-data. Any of the keywords above works this way, including `_mcp_schemalock`, which
  protects the definition of every existing field on the table (editing, duplicating,
  deleting them, and deleting the table) but does not stop a new field being added: that is
  what `_mcp_tablelock` is for. That
  field is also where the table's description lives (see "Table descriptions").
  `knack_update_object` carries the keywords along when it changes the words and never adds
  or removes one; the rules above for `update_field` still apply to the field itself. A keyword on an auto-increment
  field therefore now covers its whole table, not just itself. To find any already there,
  run `knack_search_ktl_keywords` and look for auto-increment fields.
- **Retired setting:** `dataAccess.objectKeywords` in `app.json` no longer applies. An app
  that still sets it is **refused** (every tool that selects it returns an error naming
  the tables and what to do), and `knack_list_apps` shows a `configProblem` for it,
  because ignoring it would silently drop the protection it was written for. To migrate:
  add each table's keywords to its auto-increment field's description in the builder,
  then delete `objectKeywords` from `app.json`.
- **Formulas and copies:** an equation, text formula or sum/min/max/average that reads a
  no-data field reads as no-data too, and so does a field whose conditional rule copies
  one in (a "record" value's `input`). A count field whose filters test one is left
  alone: it reads no values, only a match count.
- **Display fields:** when an object's display field is redacted, connections to it keep
  their record ids but show `"[redacted]"` for the linked records' display values.
- **Rules and tasks:** a field, view or page rule, or a task, that reads a no-data or
  redacted field is refused (`NO_DATA_FIELD`): a criterion (a per-record equality
  probe), a value copied through `input`, half of a `field_1.field_2` connection path, or
  `{field_N}` in an email or message. One that writes a no-data field as a
  `values[].field` target is refused too (`NO_WRITE_FIELD`), unless it has
  `_mcp_allowwrite`. Display rules are the exception: they only change what a person sees
  in the live app, so they may test, show or hide any no-data field.
- **Duplicates:** `duplicate_field` sends the source's description, checks that the copy
  kept every `_mcp_*` keyword, and writes them back once if Knack dropped them. If that
  fails it answers `COPY_NOT_PROTECTED` with the copy's key, for a person to fix.
- **Adding and removing:** the model may add a keyword that tightens a limit, and may set
  `_mcp_nodata` and `_mcp_allowwrite` together on a field that has neither. It may not
  add `_mcp_allowwrite` (or `_mcp_writeonly`) to a field that already has `_mcp_nodata`:
  only a person can let the model write data it cannot see. `update_field` never drops an
  `_mcp_*` keyword, even with `confirmRemoveKtlKeywords`, and still refuses when the live
  field cannot be fetched but the cache shows the keyword. Only a person in the builder
  can lift an exclusion.
- **Case:** keywords match in any case, in descriptions and in app.json alike, so
  `_MCP_NoData` protects the field too.
- **Freshness:** keywords are read from the cached schema (five-minute TTL (time to live)
  by default) and, for the schema locks (`_mcp_schemalock`, `_mcp_tablelock`), from the
  live field wherever a write tool reads it, and **the live field wins**: `delete_field`,
  `delete_object`, `duplicate_field`, `update_object`, `edit_field_rules` and
  `create_field` (except a dry run, which sends nothing) read the live table first, and
  `update_field` does the same whenever the cache says a field is locked, before it
  refuses. So a lock added in the builder is seen at once, and a lock taken off no longer
  refuses writes while the cache catches up. The data limits (`_mcp_nodata`) come from the
  cache only, so a change to one applies after the TTL, or straight away after
  `knack_cache` with `refresh: true`; the cache is also cleared by any successful change
  made through this server. A stale cache errs on the side of protecting data.

A bulk update or delete by filter goes through the same read policy, so it cannot filter
on a no-data or redacted field either. Deleting whole records is allowed: it reveals
nothing about a protected value.

This limits what the model reads through these tools. It is not a security boundary: the
server still holds the REST API key.

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

Every tool that reads an app from Knack needs that app's key, including the ones that only
read its structure (objects, fields, pages, views, rules and tasks). Before the first such
read in a session the server checks the key with one authenticated request. A key Knack
rejects (401 or 403) is not tried again for five minutes, unless another call with it
succeeds first; a 429 or 5xx leaves it unconfirmed, the structure is served, and the key
is checked again after a minute. With no key, or a rejected one, the
schema tools fall back to the cache files on disk (`schema.json` and the others below
`app.json`), and every tool response for that app ends with a note naming the key problem
and saying that the structure may be out of date. `knack_list_apps` shows each app's key
as `missing`, `unchecked`, `accepted` or `rejected`.

Knack itself serves an app's structure to anyone who has its application ID, because the
live app loads from it. Treat anything in that structure as visible: keep personal data
out of email rules, view filters and other page settings, and send emails to an address
held in a record rather than one typed into the rule.

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

72 tools in full mode, 38 in read-only mode. A level is advertised when at least one
app opts into it in `app.json`; every call still checks the selected app. `appKey` is
optional everywhere once `knack_set_context` has selected an app.

### Orientation

| Tool                | Access | What it does                                                                                                                                                           |
| ------------------- | ------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `knack_list_apps`   | read   | Lists the apps in the folder (re-scanned), each app's latest API usage, the build identity and whether this client can prompt a human                                  |
| `knack_set_context` | read   | Selects the active app by key, or infers it from a file or folder path                                                                                                 |
| `knack_cache`       | read   | Cache and file status; with `refresh: true` clears and re-warms, `persistFiles` writes the JSON files; a successful change to an app clears that app's cache by itself |

### Schema

| Tool                                | Access | What it does                                                                                                                                                                               |
| ----------------------------------- | ------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `knack_list_objects`                | read   | Objects with key, name, field count and a short description                                                                                                                                |
| `knack_get_object`                  | read   | One object, with its full description. `detail`: `fields` (default), `summary`, `types`, `raw` (REST object payload) or `rawMetadata` (runtime payload). Raw modes need `allowDiagnostics` |
| `knack_get_field`                   | read   | Complete raw definition of one field from the REST API                                                                                                                                     |
| `knack_resolve`                     | read   | Field key or fieldMap alias → key, name, type, object and Builder URL                                                                                                                      |
| `knack_get_object_connections`      | read   | Connection fields of an object and the objects they link to                                                                                                                                |
| `knack_describe_field_shape`        | read   | Record value shapes and definition shape for a field type                                                                                                                                  |
| `knack_validate_field_mapping`      | read   | Validates a name → key/alias mapping                                                                                                                                                       |
| `knack_generate_snapshot_structure` | read   | Empty snapshot templates keyed by field key and name                                                                                                                                       |
| `knack_check_duplicate_field_usage` | read   | Fields referenced by more than one alias or mapping key                                                                                                                                    |

### Records and files

| Tool                               | Access     | What it does                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| ---------------------------------- | ---------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `knack_get_record`                 | read       | One record by id                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| `knack_find_records`               | read       | Filters, paging, sorting; `includeSchema` adds the object's field schema to the response                                                                                                                                                                                                                                                                                                                                                                                              |
| `knack_get_related_records`        | read       | Records connected to a record, forward or reverse, limited to approved fields                                                                                                                                                                                                                                                                                                                                                                                                         |
| `knack_aggregate_records`          | read       | Count, sum, average, min and max with grouping and date buckets; returns aggregates only                                                                                                                                                                                                                                                                                                                                                                                              |
| `knack_verify_record_field_shapes` | diagnostic | Compares a live record's values against the documented shapes                                                                                                                                                                                                                                                                                                                                                                                                                         |
| `knack_create_records`             | write      | One request per record, limited concurrency, retry on 429 only, refused up front if the account's daily API allowance cannot cover it; `dryRun` validates without creating and says how each date was read. A date is checked in its own field's order (`dateFormat` in `knack_get_object`): an impossible one is refused, an ambiguous one is flagged, and an ISO date (`2026-09-03`) is sent in that order. After a write, a stored day that differs from the one meant is reported |
| `knack_update_records`             | write      | Same shape for updates; or `where` (filters + data) updates every match, previewing until `confirm`; dates are checked and read back as in create                                                                                                                                                                                                                                                                                                                                     |
| `knack_delete_records`             | delete     | By ids or by `filters`; previews until `confirm: true`                                                                                                                                                                                                                                                                                                                                                                                                                                |
| `knack_upload_asset`               | write      | Uploads a local file as a file or image asset                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| `knack_download_file`              | read       | Downloads an attachment to a temporary path under a byte cap                                                                                                                                                                                                                                                                                                                                                                                                                          |
| `knack_read_file`                  | read       | Downloads and extracts bounded text from PDF, DOCX and text-like attachments                                                                                                                                                                                                                                                                                                                                                                                                          |

### Views

| Tool                              | Access      | What it does                                                                                                                                                                                                                                                                                                              |
| --------------------------------- | ----------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `knack_list_scenes`               | read        | Scenes with key, name, slug and view count; `includeViews`, `includeBuilderUrls` opt in                                                                                                                                                                                                                                   |
| `knack_get_scene`                 | read        | One page's rules (conditional show/hide); view keys and connection-traversal field criteria resolved to names, dangling references flagged                                                                                                                                                                                |
| `knack_list_views`                | read        | Views with scene context and type; filter by scene or type                                                                                                                                                                                                                                                                |
| `knack_get_view`                  | read        | One view. `detail`: `context` (default), `fields` (configured field settings) or `attributes` (needs `allowDiagnostics`; `includeRaw` inlines the payload). In `fieldSettings`, a form input reports its field's own input type and options, since the copy stored on the form goes stale                                 |
| `knack_plan_view_repoint`         | read        | Every connection reference in a view, split into rescope and retarget edits; changes nothing                                                                                                                                                                                                                              |
| `knack_get_view_payload_template` | read        | Starter create-view payload for grid/table, form, details, list, search, menu, rich text (`content`) or calendar (`eventField` date, `labelField`), or a clone of `fromViewKey` with identifiers stripped — never sent to Knack                                                                                           |
| `knack_snapshot_app`              | read        | Writes a restore point to the local app folder: scene tree with its access fields, profile map, schema pointer, optionally one view — never sent to Knack                                                                                                                                                                 |
| `knack_list_page_referrers`       | read        | Views linking to a page and what removing each link would do to it; `includeDescendants` adds the pages beneath                                                                                                                                                                                                           |
| `knack_get_page_access`           | read        | Who can reach a page: walks up to the nearest login and lists the roles it admits — public, protected or unknown                                                                                                                                                                                                          |
| `knack_create_view`               | view        | Creates a view from a full definition                                                                                                                                                                                                                                                                                     |
| `knack_update_view_order`         | view        | Reorders views and page groups on a scene                                                                                                                                                                                                                                                                                 |
| `knack_update_view`               | view        | Merges changes into the live definition and sends it whole; a dropped last link goes to the human; protects KTL keywords in title/description, `keywordEdits` adds/updates one in place                                                                                                                                   |
| `knack_add_view_columns`          | view        | Appends new fields to a table, details or list view's existing columns; reads the live layout itself, so it never needs `allowDiagnostics` or the caller's own copy of the rest — form and search are unsupported shapes and refused                                                                                      |
| `knack_add_action_link`           | view        | Appends action-link column(s) (caller-supplied JSON) to a table, details or list view's existing columns; reads the live layout itself, so it never needs `allowDiagnostics` — form is an unsupported shape and refused                                                                                                   |
| `knack_add_page_link_column`      | view        | Appends page-link column(s) (caller-supplied JSON) to a table, details or list view's existing columns — either an existing scene's key/slug, or a `{name, parent, views}` specification that creates one; reads the live layout itself, so it never needs `allowDiagnostics` — form is an unsupported shape and refused  |
| `knack_add_view_rules`            | view        | Appends record and/or submit rules (caller-supplied JSON) to a view's `rules`, leaving the other array and everything already there untouched; gives each new rule the Builder's key (`"4"`, `submit_2`) so `knack_edit_view_rules` can later edit it — reads the live rules itself, so it never needs `allowDiagnostics` |
| `knack_edit_view_rules`           | view        | Removes or replaces a view's record, submit, display or email rules by key; every other rule set and the rest of the view stay as read, and the form's default submit rule cannot be removed                                                                                                                              |
| `knack_add_page_rules`            | view        | Appends page rules (caller-supplied JSON: hide/show views, message, redirect) to a page; Knack's POST replaces the whole array, so it reads the live rules first, numbers missing keys `submit_N`, refuses key clashes and views not on the page, and reads back to verify                                                |
| `knack_edit_page_rules`           | view        | Removes or replaces a page's rules by key; reads the live array, keeps the order, refuses unknown keys and views not on the page, and reads back to verify                                                                                                                                                                |
| `knack_create_page`               | view        | Creates an empty top-level page, public or behind a login limited to chosen roles (Knack adds the login page itself); checks the roles exist and reads the page's access back to verify                                                                                                                                   |
| `knack_delete_page`               | view-delete | Deletes a page, and its login page when that login guards nothing else, as the Builder does; refuses the home page (including a home login that would go with the page) and any delete that would take other pages; lists views left linking to it; previews unless `confirm` is true                                     |
| `knack_update_page_settings`      | view        | Changes a page's name, URL slug, print link or modal options (the Builder's Page Settings); sends only the values that differ and never the views, reads back to verify, refuses a slug another page has, and after a slug change lists the views Knack repointed                                                         |
| `knack_add_view_links`            | view        | Appends new entries (caller-supplied JSON) to a view's top-level `links` — a menu's nav entries, or another view type's link buttons — reads the live links itself, so it never needs `allowDiagnostics`                                                                                                                  |
| `knack_copy_view`                 | view        | Knack's copy (`sharePages: false`) or a create from the source definition that keeps child pages shared (`sharePages: true`)                                                                                                                                                                                              |
| `knack_move_view`                 | view        | Moves a view; owned child pages go to the human                                                                                                                                                                                                                                                                           |
| `knack_delete_view`               | view-delete | Deletes a view; pages reached only through it go to the human                                                                                                                                                                                                                                                             |

### Analysis

| Tool                             | Access | What it does                                                                                                                          |
| -------------------------------- | ------ | ------------------------------------------------------------------------------------------------------------------------------------- |
| `knack_get_context_bundle`       | read   | Selected object schemas, aliases and view context in one call                                                                         |
| `knack_get_app_overview`         | read   | Every object with description, counts, types and relationships                                                                        |
| `knack_analyze_data_model`       | read   | Design feedback on the data model                                                                                                     |
| `knack_app_deep_dive`            | read   | One-call onboarding snapshot                                                                                                          |
| `knack_list_field_references`    | read   | References to a field across schema, aliases and views; `classification` filters (e.g. `viewRecordRule`), `groupByView` groups        |
| `knack_find_orphaned_field_refs` | read   | Pages, views, rules, formulas and tasks still naming a deleted field, with the path to each; reads fresh metadata; `fieldKey` narrows |
| `knack_search_ktl_keywords`      | read   | KTL underscore keywords in view titles and descriptions                                                                               |
| `knack_search_emails`            | read   | Email rules and actions in views                                                                                                      |
| `knack_audit_exposure`           | audit  | Only when asked: typed email addresses in views and tasks, and forms on pages with no login; needs `allowAudit`                       |
| `knack_generate_seed_csvs`       | read   | Import-ready seed CSV content per object                                                                                              |

### Exposure audit

Knack serves an app's structure (objects, pages, views, rules, tasks) to anyone who has
its application ID. `knack_audit_exposure` lists the two things in it that matter most:

- **The app's own addresses**: anything in its settings, such as `from_email` (the
  default sender) and `technical_contact`, reported once under `settingsEmails`.
- **Typed email addresses**: in email rules, task emails, and any other text a view
  carries. Each hit gives the view or task, the path to the text, whether it sits in an
  email's settings, and the address with its local part hidden (`j***@example.com`). A
  rule whose sender is the app's default sender is not listed again; the Builder fills
  that in on every rule, so only a sender typed over it is.
- **Forms on public pages**: every form, registration, checkout or customer view on a
  page with no login above it, with the table it writes to. A form whose page access cannot be worked out is listed as `unknown`.
  Forms on Knack's account pages (`type: "user"`, such as Account Settings, and pages
  beneath one) are listed apart, under `accountForms`: the login walk finds no login
  above them, but Knack shows an account page only to a logged-in user.

It is off unless the app sets `"allowAudit": true`, it stays available in enforced
read-only mode, and its description tells the model to run it only when the user asks.
Like every read of an app's structure, it needs the app's REST API key: without one it
audits nothing, and the response says why.

Separately, any view, page or task change that puts a typed address into an email's
settings goes through, and its response ends with a note naming the hidden address and
its path, and suggesting an email field on the record instead. A sender equal to the
app's default sender is not flagged. A preview or dry run gets
the same note, saying what the change would do; any other refusal gets none. Emails sent
to a field or a connected record are not flagged, and record writes are never scanned.

### Scheduled tasks

Tasks are read from the public app metadata. Creating one uses the Builder's own
`POST /objects/:key/tasks`, captured from network traffic. `PUT` and `DELETE` on
`/objects/:key/tasks/:taskKey` were measured with the API key: a partial `PUT` clears
`run_status` without applying the change, so updates always send the whole live task.

| Tool                | Access | What it does                                                                                                                                                                  |
| ------------------- | ------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `knack_list_tasks`  | read   | Lists scheduled tasks, per object or app-wide: schedule, running or paused, criteria, values and email                                                                        |
| `knack_create_task` | write  | Creates a task, **paused** unless `runStatus: "running"`; refuses unknown fields, and reading or writing `_mcp_nodata` ones; `previewOnly`; reads back to verify              |
| `knack_update_task` | write  | Changes name, schedule (partially), action or running state; sends the whole live task with only that changed; warns when turning a task on; reads back; `before` restores it |
| `knack_delete_task` | delete | Deletes a task; previews unless `confirm` is true; checks it existed first and is gone after, since Knack answers success either way                                          |

### Objects and fields

Object (table) mutation endpoints are undocumented in Knack's public REST API reference
— captured from Builder UI network traffic, authenticating the same way as every other
request here (app id + REST API key).

| Tool                       | Access | What it does                                                                                                                                                                                                                        |
| -------------------------- | ------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `knack_create_object`      | write  | Creates a table and writes its required `description` (with `notedBy`) onto its auto-increment field, adding that field if Knack did not; reads back to verify; `dryRun` previews — see "Table descriptions"                        |
| `knack_update_object`      | write  | Renames a table, changes its display field (`identifier`) or default sort, and/or sets its `description`; refuses a field that is not on the table (Knack would store it anyway); reads back to verify; `dryRun` previews the merge |
| `knack_delete_object`      | delete | Deletes a table and all of its fields and records; previews unless `confirm` is true                                                                                                                                                |
| `knack_create_field`       | write  | Creates a field; `description` and `notedBy` are both required and stored together as `_notes=[description                                                                                                                          | name on date]` — see below; a date field takes its date order from the app's time zone (`dd/mm/yyyy`outside the US) with no time unless`includeTime`(24-hour),`dateFormat`overrides;`dryRun` validates the definition |
| `knack_update_field`       | write  | Merges changed properties; protects KTL keywords (including `_notes`) in descriptions; warns when a new or changed formula reads a computed field placed after it; `dryRun` previews the merge                                      |
| `knack_edit_field_rules`   | write  | Adds, replaces or removes a field's conditional rules (which set its value) or validation rules (which reject input) by key; sends only that rule set, refuses locked and hidden fields, reads back to verify                       |
| `knack_delete_field`       | delete | Deletes a field; if it held the table's description, the words come back in the response                                                                                                                                            |
| `knack_update_field_order` | write  | Moves fields before or after another field, or sets the full order; sends Knack every field, warns when an equation lands before a computed field it reads, reads back to verify; `dryRun` previews                                 |
| `knack_duplicate_field`    | write  | Copies a field under a new name                                                                                                                                                                                                     |

The MCP resource `knack://<AppKey>/schema`, `.../fieldMap` and `.../viewMap` serve the
cached JSON documents directly.

## API usage

Knack sends its rate limits on every authenticated response, and the server reads them
rather than counting calls itself, so the figures already include the front end, Make and
any other client spending from the same allowance. The allowance belongs to the Knack
account, not to an app, so every app on the account draws on the same one. Apps on one
account share one daily reading: a call on any of them updates what all of them show, and a
batch on one is checked against the freshest figure. The account is the app's
`builderAccountSlug` in `app.json` (case does not matter) or, if that is not set, the
account slug in its runtime metadata once that has been loaded (any tool that reads the
schema loads it). An app with neither is treated as an account of its own, so setting
`builderAccountSlug` on every app is the reliable way to get the sharing. The burst limit
and the call counts stay per app.

| Headers                                     | Meaning                                                                                   |
| ------------------------------------------- | ----------------------------------------------------------------------------------------- |
| `x-planlimit-limit`, `-remaining`, `-reset` | The account's daily API allowance. `reset` is milliseconds until it resets, at 00:00 UTC. |
| `x-ratelimit-limit`, `-remaining`, `-reset` | A short burst limit (10 requests, about one second). `reset` is epoch seconds.            |

- **Where to see it.** `knack_list_apps` returns `apiUsage` for each app: the daily
  `limit`, `remaining`, `used`, `percentUsed` and `resetsAt` (to the nearest second), the
  size of the burst window (`burstLimit`), when the reading was taken (`readAt`) and how
  many calls this server has made (`callsThisSession`). What remains in the burst window
  is not reported: the window lasts about a second, so it would be stale before it was
  read. `plan` is `null` until a tool has made a call to Knack's REST API for that app.
  The first time an app's structure is read in a session (for example by
  `knack_list_objects`), the server makes one authenticated request to check the app's
  REST key, so that tool fills in `plan` and uses one request of the allowance. After
  that, tools that only read cached metadata (`knack_cache`) make no request. A reading
  is a snapshot: other clients keep spending between calls. It is
  dropped once its reset time has passed.
- **What a response says.** Nothing, normally. A response gets a trailing note only when
  that request made 25 or more API calls, or when the daily allowance is 80 percent used
  or more (and a firmer one at 95 percent).
- **Burst pacing.** When the burst window has no requests left, the next request waits for
  it to reset (at most two seconds) instead of drawing a 429. Requests already in flight
  count against the window.
- **429s.** A burst 429 waits for the window to reset and retries. A 429 when the daily
  allowance is spent is not retried; the result is `daily_api_limit_reached` with the
  reset time, since nothing helps before 00:00 UTC.
- **Batches.** `knack_create_records`, `knack_update_records` and `knack_delete_records`
  make one request per record. If the last daily reading shows fewer calls left than the
  batch needs, they refuse before sending anything. With no reading yet they run.
- **Not covered.** Reads that page through an unknown number of records
  (`knack_aggregate_records`, the reference scans) are not estimated up front. File
  downloads come from Knack's CDN and do not count.

## Table descriptions

Knack has no description for a table, and every table has an auto-increment field, so a
table's description lives in that field's description. Anything reading the schema gets a
table's purpose from it: `knack_list_objects` and `knack_get_app_overview` show it (cut to
160 characters, without the `_notes` stamp) and `knack_get_object` returns it whole under
`objectDescription`, with the field it sits on. A table with no description simply shows
none.

- **New tables always get one.** `knack_create_object` requires `description` and
  `notedBy`. After the table is made it writes the description onto the table's
  auto-increment field as `_notes=[<description> | <name> on <date>]`, like any field description,
  and reads it back. Knack does not make one when a table is created through the
  API (measured on the playground: it adds `Name`, a text field called `Record ID` and
  `Created By`, `Updated By` and `Owned By` connections, but no auto-increment field), so
  the server adds one, named `AI` (not `Record ID`, which Knack would rename
  `Record ID Copy`), carrying the description. If the description cannot be written, the table stays, the response
  says so in `warning`, and `knack_update_object` fixes it.
- **Changing it.** `knack_update_object` takes `description` alone or with a rename or
  sort change. The original `_notes` stamp is kept, since it records who added the note.
  Its dry run shows the field, the words before and after, any keywords that will be kept
  and the exact string that would be stored (`wouldStore`).
  An existing table with no auto-increment field is refused, with the exact call that adds
  one: only new tables get a field added for them.
- **Keywords.** The auto-increment field's description can carry the same `_mcp_*`
  keywords as any field, after the note. A table-locked table (`_mcp_tablelock` on any of
  its fields) refuses a description change, as it refuses any change to the table. A
  schema-locked auto-increment field (`_mcp_schemalock`) refuses one too, since only a
  person may change a locked field; both refuse before anything is sent. Any other
  keyword on the field is kept when its words change.
- **Deleting it.** `knack_delete_field` on the auto-increment field that holds the
  description returns the words in `lostObjectDescription`, so they can be restored.
- **Several.** Knack allows more than one auto-increment field on a table. The first one
  with words is the description, and `autoIncrementKeys` lists them all. Deleting the
  holder while another exists loses the words (they are returned, as above); the next
  `knack_update_object` writes them onto the one that is left.
- **Length.** A description of 20,000 characters was accepted and read back whole; no
  limit was found.

There is no backfill: tables that already exist are described when someone next edits
them.

## Field description notes

**This is always on** — every field description written through this server is stored as
one note, `_notes=[<description> | <name> on <date>]`: the words first, then who added
them and when, all inside the brackets. `knack_create_field` requires both `description`
and `notedBy`; so does `knack_update_field` when it sets a description on a field with no
note yet.

```
_notes=[Customer's preferred contact method | Craig on 2026-09-30]
_notes=[Updated on | Craig on 2026-09-30]
```

The words go inside the brackets so the whole description reads in one place, and because
the Builder did not accept a description that was only a bare stamp, every field needs at
least a few words.

**How much to write.** An obvious field needs only a few words (`Updated on`, `Updated by`,
`Person` on a client table). A field whose meaning is not clear from its name and type
needs a sentence: computed fields (equation, concatenation), connections, and anything
with a rule behind it. Whether a field is obvious is a judgement, so ask the person when
unsure. `knack_create_field` nudges (`descriptionWarning`, never a refusal) when an
equation, concatenation or connection is described in fewer than four words.

Other KTL keywords follow the note, so they still form the trailing keyword cluster KTL
requires:

```
_notes=[Customer's preferred contact method | Craig on 2026-09-30] _ktlHide
```

A `]` or `[` in the words becomes a parenthesis, since a bracket would close the note early.

**Older descriptions.** Before 30 September the words sat outside the note
(`Customer name _ktlHide _notes=[Craig on 2026-09-07]`), and a plain stamp before that
(`_notes=Craig on 2026-09-07`). Both are still read correctly, by the schema tools too, and
are rewritten into the form above the next time that field's description is written. So
is HTML: the Builder's rich-text box saves a keyword as `<p>_mcp_nodata</p>`, and the tags
are read as spaces, so the words and keywords are told apart as in plain text (what is
written back is plain text).
Nothing is rewritten in bulk.

A restamp replaces the attribution inside the brackets, and an edit to the description
replaces the words while keeping the original attribution.

**Which copy of a description.** Knack keeps a field's description twice, at the top level
(`description`) and under `meta.description`. The builder edits only `meta.description`, so
after a person changes a description there the top-level copy is left behind (measured on
the playground: a `_mcp_schemalock` added in the builder showed only in `meta.description`).
This server trusts `meta.description` everywhere: for keywords, for the locks, for the
keyword-drop guard, for what a table description change carries along, and for the cached
schema. The top-level copy is used only when a field has no `meta.description`, or its
`meta.description` has no text in it (empty, blank or HTML with nothing in it), so an empty
`meta.description` can never hide a keyword the top-level copy still holds; the cost is that
emptying a description completely in the builder leaves any stale top-level keywords in
force until it is written again. Writes through this server set both copies.

`_notes` records who **added** the note, not who last touched the field:

- The first time a description is set on a field — on create, or on an update where the
  field has no `_notes` stamp yet — `notedBy` is required and gets stamped fresh.
- A later `knack_update_field` edit to that same description preserves the existing stamp
  untouched, wherever it sits relative to other keywords; `notedBy` is not needed for an
  ordinary content edit.
- To re-attribute the note to someone else, pass `restampNote: true` together with
  `notedBy` — this only happens when explicitly asked for.
- Clearing a description (empty string) with `knack_update_field` needs no `notedBy`;
  there is nothing left to attribute. `knack_create_field` does not allow an empty one.

The existing KTL-keyword-drop guard — which blocks a description edit that would silently
lose a token like `_ktlHide` unless `confirmRemoveKtlKeywords: true` is passed — protects
`_notes` the same way, and applies independently of how many other keywords a description
or view carries.

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

**Setting access: only at creation.** `knack_create_page` can create a page behind a
login for chosen roles, and Knack builds the login page itself. Changing access
afterwards is not possible with the REST API key. Measured on NP Place Playground on
25 September, every variant returned 500 and changed nothing:

- adding a login to an existing page (the Builder's "require login" save, with or
  without the page's `views`)
- changing a login view's `allowed_profiles` or `limit_profile_access` (the Builder's
  own body, a minimal body, the whole view, and "any logged-in user")
- removing a login (the Builder's own `PUT /scenes/<login page>` with
  `authenticated: false`)
- deleting a login page directly

The Builder can do all of these because it uses a signed-in session. Change access there.

**Warning:** to Knack, `authenticated: false` means "delete this login page": the Builder
removes a login that way, and Knack lifts the page under it to the top level. Sent to an
ordinary public page with the API key, it returned 200 and **deleted that page**, with its
views. No tool here can send `authenticated`, and a test checks that
`knack_update_page_settings` never will.

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

### Deleted fields

A field deleted in the builder during a conversation is still in any copy of a view the
model read before, and a `groups` or `columns` array built from that copy would put it
back and break the form. So before any view is created or updated, the guard checks
every `field_N` the outgoing view names against the fresh metadata it has just read (no
extra request) and refuses with `UNKNOWN_FIELD_IN_VIEW` when one no longer exists:

- `unknownFieldKeysInUpdates`: the field came in with the change. Read the view again and
  build the change from the current definition.
- `unknownFieldKeysInStoredView`: Knack's own copy of the view still names it. Send the
  property that holds it without it, or remove it in the builder.

It checks against the same public metadata the view is read from, so if Knack were ever
slow to show a builder change there, a field deleted moments earlier could still pass.

Deleting a field in the builder usually removes it everywhere, but not always: on
25 September Knack stripped a deleted field from a record rule and left its input on two
forms, which crashed the builder's rules dialog and stopped their display rules. Run
`knack_find_orphaned_field_refs` after deleting a field to list anything still naming it,
with the path to each reference.

### View KTL keyword guard

A view's `title` or `description` can carry several KTL keywords bunched at the end (see
[Field description notes](#field-description-notes) for why they must trail the text —
same hard requirement here). `knack_update_view` sends the whole definition, not a patch,
so a plain `title`/`description` string replacement silently drops any keyword the new
text does not happen to repeat. The guard now catches that: it refuses with
`KTL_KEYWORDS_WOULD_BE_DROPPED` unless `confirmRemoveKtlKeywords: true` is passed —
naming exactly which keywords, on which property, would be lost.

To add or update a keyword without hand-retyping the whole cluster, pass `keywordEdits` —
JSON of the form `{"title"?: {"_keyword": "value" | null}, "description"?: {...}}`:

- A keyword not already present is appended at the end of the trailing cluster.
- A keyword that already exists has its value replaced in place, in the same position,
  with every sibling keyword left untouched.
- `null` means a bare keyword with no `=value`; a string produces `_keyword=value`.

`keywordEdits` alone is a valid update — `updates` can be omitted entirely if that is the
only change being made. This is separate from `_notes` field-description stamping above:
views get no automatic "who added it" attribution, only the drop-guard and the
add/update-in-place convenience.

## Development

```bash
npm run build -w knack-mcp-v2
npm test -w knack-mcp-v2
npm run catalogue -w knack-mcp-v2
```

See `docs/ARCHITECTURE.md` for the module map and how to add a tool.
