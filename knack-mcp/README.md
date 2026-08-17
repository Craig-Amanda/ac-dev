# knack-mcp

An MCP (Model Context Protocol) server that exposes Knack application data — schemas, records, fields, views, and more — to AI coding assistants such as Claude (via Claude Desktop), Cursor, Copilot, or any other MCP-compatible client.

## Table of Contents

- [Prerequisites](#prerequisites)
- [Setup](#setup)
    - [1. Install dependencies](#1-install-dependencies)
    - [2. Create your KnackApps directory](#2-create-your-knackapps-directory)
    - [3. Add an app.json for each app](#3-add-an-appjson-for-each-app)
    - [4. Create a secrets file](#4-create-a-secrets-file)
    - [5. Build the server](#5-build-the-server)
    - [6. Configure your MCP client](#6-configure-your-mcp-client)
- [Environment Variables](#environment-variables)
- [Optional Cache Files](#optional-cache-files)
- [Usage](#usage)
    - [Context & Discovery Tools](#context--discovery-tools)
    - [Data Read Tools](#data-read-tools)
    - [Schema & Field Tools](#schema--field-tools)
    - [Database Design & Overview Tools](#database-design--overview-tools)
    - [View & Search Tools](#view--search-tools)
    - [Data Model Analysis Tools](#data-model-analysis-tools)
    - [MCP Resources](#mcp-resources)
- [Workflow Tips](#workflow-tips)

---

## Prerequisites

- **Node.js 18+** (Node 24 LTS recommended for optimal performance)
    - Supported runtime: Node 18 and above
    - Node 16 supported on best-effort basis with automatic fetch fallback
- A Knack account with at least one application and a REST API key

---

## Setup

### 1. Install dependencies

From the `knack-mcp` folder:

```bash
npm install
```

### 2. Create your KnackApps directory

Choose any location on your machine to store your app configurations. Each Knack app gets its own subdirectory inside this folder. The subdirectory name becomes the **app key** used throughout the server.

```
KnackApps/
  MyApp/
    schema/
      app.json
  AnotherApp/
    schema/
      app.json
```

> The server also accepts `app.json` at the root of the app folder (i.e., `KnackApps/MyApp/app.json`) if you prefer a flat layout.

### 3. Add an app.json for each app

Each app directory needs an `app.json` that identifies it to the server. Create one at `KnackApps/<AppKey>/schema/app.json`:

```json
{
    "appKey": "MyApp",
    "appName": "My Knack Application",
    "appId": "5f3a1b2c3d4e5f6a7b8c9d0e",
    "apiBase": "https://api.knack.com/v1",
    "builderAccountSlug": "my-account",
    "builderAppSlug": "my-knack-application",
    "readonly": false,
    "allowViewMutation": true,
    "allowDiagnostics": true,
    "notes": "Production app — handle with care"
}
```

| Field                | Required | Description                                                                                                                         |
| -------------------- | -------- | ----------------------------------------------------------------------------------------------------------------------------------- |
| `appKey`             | ✅       | A short identifier for the app (must match the folder name).                                                                        |
| `appId`              | ✅       | Your Knack Application ID (found in the Knack Builder under **Settings → API & Code**).                                             |
| `appName`            | No       | A friendly display name for the app.                                                                                                |
| `apiBase`            | No       | API base URL. Defaults to `https://api.knack.com/v1`.                                                                               |
| `builderAccountSlug` | No       | Knack Builder account slug used for generated Builder URLs.                                                                         |
| `builderAppSlug`     | No       | Knack Builder app slug used for generated Builder URLs.                                                                             |
| `readonly`           | No       | Defaults to `true`. Set to `false` to expose field and record mutation tools for this app and allow write operations.               |
| `allowViewMutation`  | No       | Enables create/update/delete view tools for this app.                                                                               |
| `allowDelete`        | No       | Defaults to `false`. Set to `true` to allow destructive delete tools for this app.                                                  |
| `allowDiagnostics`   | No       | Enables raw inspection and field-shape diagnostic tools for this app.                                                               |
| `dataAccess`         | No       | Optional allowlist/redaction policy for sensitive-data record reads. See [Sensitive-data deployments](#sensitive-data-deployments). |
| `notes`              | No       | Free-text notes visible in `knack_list_apps`.                                                                                       |

If the Builder slugs are omitted, the server falls back to runtime metadata when available, then to a slugified `appName`.

### 4. Create a secrets file

The secrets file maps each `appKey` to its Knack REST API key. By default the server looks for this file at `~/.knack-mcp-secrets.json`.

```json
{
    "MyApp": "knack-rest-api-key-here",
    "AnotherApp": "another-knack-rest-api-key"
}
```

You can find your API key in the Knack Builder under **Settings → API & Code → API Key**.

> **Keep this file outside your project repository** to avoid committing credentials to source control.

### 5. Build the server

```bash
npm run build
```

This compiles the TypeScript source in `src/` to JavaScript in `dist/`.

### 6. Configure your MCP client

Add the server to your MCP client configuration. The exact location of this file depends on your client:

- **Claude Desktop (macOS):** `~/Library/Application Support/Claude/claude_desktop_config.json`
- **Claude Desktop (Windows):** `%APPDATA%\Claude\claude_desktop_config.json`
- **Cursor:** `.cursor/mcp.json` in your project root, or the global Cursor MCP settings

```json
{
    "mcpServers": {
        "knack-mcp-readonly": {
            "command": "node",
            "args": ["/absolute/path/to/knack-mcp/dist/server-readonly.js"],
            "env": {
                "KNACK_APPS_DIR": "/absolute/path/to/KnackApps",
                "KNACK_MCP_SECRETS_PATH": "/absolute/path/to/.knack-mcp-secrets.json"
            }
        },
        "knack-mcp-full": {
            "command": "node",
            "args": ["/absolute/path/to/knack-mcp/dist/server-full.js"],
            "env": {
                "KNACK_APPS_DIR": "/absolute/path/to/KnackApps",
                "KNACK_MCP_SECRETS_PATH": "/absolute/path/to/.knack-mcp-secrets.json"
            }
        }
    }
}
```

Replace the paths with the actual locations on your machine. After saving, restart your MCP client to pick up the new server.

### WSL with nvm

When the MCP client runs in WSL, use the included launcher instead of a bare `node` command. It silently selects Node 24 before starting the server, keeping MCP stdout clean for JSON-RPC and avoiding an outage when the parent process inherited an older Node version.

```json
{
    "mcpServers": {
        "knack-mcp-readonly": {
            "command": "/absolute/path/to/knack-mcp/scripts/start-mcp.sh",
            "args": ["server-readonly.js"],
            "env": {
                "KNACK_APPS_DIR": "/absolute/path/to/KnackApps",
                "KNACK_MCP_SECRETS_PATH": "/absolute/path/to/.knack-mcp-secrets.json"
            }
        }
    }
}
```

The launcher is optional and WSL-specific; it protects WSL users from an inherited older Node version. Mac and Windows users can keep using their existing `node` command on Node 18+.

`server-readonly.js` is an enforced server-wide boundary: it never advertises mutation, view-mutation, or raw diagnostic tools, regardless of any app configuration. Use it for director or other read-only installations. `server-full.js` retains the normal per-app opt-in behaviour.

Tool exposure now comes from each app's `app.json` rather than server-wide mutation or diagnostic env flags. The alternate entry points remain usable, but they no longer change which tool categories are advertised.

---

## Environment Variables

| Variable                                                                                                                                                                                                                                              | Required | Default                     | Description                                                                                                                                                         |
| ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------- | --------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `KNACK_APPS_DIR`                                                                                                                                                                                                                                      | ✅       | —                           | Absolute path to your `KnackApps` directory.                                                                                                                        |
| `KNACK_MCP_SECRETS_PATH`                                                                                                                                                                                                                              | No       | `~/.knack-mcp-secrets.json` | Path to your secrets JSON file.                                                                                                                                     |
| `DEBUG`                                                                                                                                                                                                                                               | No       | `false`                     | Set to `1`, `true`, `yes`, or `on` to write debug logs to stderr.                                                                                                   |
| `KNACK_CACHE_TTL_MS`                                                                                                                                                                                                                                  | No       | `300000` (5 min)            | How long runtime data is cached in memory before re-fetching, in milliseconds.                                                                                      |
| `KNACK_MAX_RESPONSE_BYTES`                                                                                                                                                                                                                            | No       | `20971520` (20 MB)          | Maximum size (in bytes) of an API response the server will process.                                                                                                 |
| `KNACK_MCP_COMPACT_TOOL_METADATA`                                                                                                                                                                                                                     | No       | `true`                      | Shortens verbose MCP tool descriptions before advertising them to the client. Set to `false` to keep the original long descriptions.                                |
| `KNACK_MCP_PRETTY_TOOL_JSON`                                                                                                                                                                                                                          | No       | `false`                     | When `false`, tool responses are returned as compact JSON to reduce token usage. Set to `true` only when human-readable formatting matters more than cost.          |
| `KNACK_MCP_MAX_TOOL_TEXT_BYTES`                                                                                                                                                                                                                       | No       | `262144` (256 KB)           | Maximum serialised tool-response size sent back to the client. Larger payloads are replaced with a compact overflow summary to avoid runaway token use.             |
| `KNACK_MCP_MAX_INLINE_DETAIL_BYTES`                                                                                                                                                                                                                   | No       | `49152` (48 KB)             | Maximum size for inlining raw view/object payload details inside a normal tool response. Larger payloads are replaced with a structural summary plus size metadata. |
| `KNACK_MCP_MAX_EXTRACTED_TEXT_BYTES`                                                                                                                                                                                                                  | No       | `196608` (192 KB)           | Maximum extracted attachment text returned by `knack_read_file`. Longer documents are truncated.                                                                    |
| `KNACK_MCP_BATCH_CONCURRENCY`                                                                                                                                                                                                                         | No       | `5`                         | Maximum concurrent API requests in flight for `knack_batch_create_records`, `knack_batch_update_records`, and `knack_batch_delete_records`. Clamped to 10.          |
| For token-based clients, the default settings are already biased toward lower usage: compact tool metadata, compact JSON responses, and a response-size guardrail. Only relax those defaults if you specifically need more verbose inspection output. |

Some high-volume tools also now default to smaller result windows or less verbose payloads:

- `knack_list_scenes` omits full view lists unless `includeViews` is set to `true`.
- `knack_search_emails` omits message bodies unless `includeMessage` is set to `true`.
- Broad search/list tools such as field references, views, email search, and KTL keyword search default to smaller `maxResults` values to reduce accidental large responses.
- Raw inspection tools such as `knack_get_raw_object`, `knack_get_raw_object_metadata`, and `knack_get_view_attributes` inline the full payload only when it is small enough to stay economical.
- Mutation tools are advertised when at least one app has `"readonly": false` in `app.json`, and each call still enforces the selected app's write toggle.
- View mutation tools are advertised when at least one app has `"allowViewMutation": true` in `app.json`, and each call still enforces the selected app's view-mutation toggle.
- Diagnostic/raw inspection tools are advertised when at least one app has `"allowDiagnostics": true` in `app.json`, and each call still enforces the selected app's diagnostic toggle.

When view mutation tools are enabled, the server also exposes helper operations for common classic-builder workflows:

- `knack_get_view_payload_template` builds starter payloads for common view types. Use `grid` for convenience; the helper normalises it to Knack's stored `table` type.
- `knack_get_view_payload_template` can now auto-derive starter fields from object metadata when you pass `appKey` and omit `fieldKeys`. By default it uses up to 12 fields unless you raise `maxFields`.
- For form templates, auto-derived fields now exclude non-input calculation/system-style field types such as `auto_increment`, `sum`, `count`, and `equation`.
- Both payload helper tools accept `sceneKey` so they can derive `existingViewKeys` from scene metadata instead of making you pass the layout order manually.
- `knack_get_view_payload_template_from_view` clones an existing view from runtime metadata or `viewMap.json`, strips the Knack identifiers, and rebuilds `pageGroups` from the source scene when possible. `targetViewType` supports a same-type clone or `details`/`list` conversion only; other view types need a type-specific payload rather than a cloned layout. Configured columns, including Title/Copy and Divider elements, are retained.
- `knack_update_view_order` wraps `POST /scenes/{sceneKey}/views/sort`.
- `knack_update_view` guards against link-column loss: a `columns` replacement on a view that has a `link` column makes Knack delete that link column and cascade-delete its child scene (even when the link column is re-sent unchanged). The tool now blocks such updates by default and reports the at-risk link columns/scenes; pass `confirmDestructive: true` to override, or edit columns in the Knack builder.
- `knack_copy_view` and `knack_move_view` wrap `POST /scenes/{sourceSceneKey}/copyview`.

**Cache staleness:** none of the mutation tools (field or view) invalidate the in-memory/on-disk schema or scene/view cache automatically. Every successful field-mutation response (`knack_create_field`, `knack_update_field`, `knack_delete_field`, `knack_duplicate_field`) includes a `cacheNote`, and every successful view-mutation response (`knack_create_view`, `knack_update_view`, `knack_update_view_order`, `knack_copy_view`, `knack_move_view`, `knack_delete_view`) includes the equivalent, reminding you to run `knack_refresh_cache` (`warm: true, persistFiles: true`) before trusting cached-schema or cached-view tools to reflect the change. `knack_update_field` also adds a `mergeNote` when the update touches `format`/`relationship`, since whether Knack's PUT merges or fully replaces a partial nested object hasn't been independently verified — check `knack_get_field` afterwards if in doubt.

Token note:
The payload helper tools now return the payload only once, using the standard inline-detail size guard. Larger cloned payloads fall back to a structural summary instead of duplicating both `payload` and `payloadJson` in the response.

---

## Optional Cache Files

To reduce API calls — or to allow the server to work without a live API key — you can place pre-fetched JSON files inside `KnackApps/<AppKey>/schema/`:

| File                       | Description                                                                                                                                           |
| -------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------- |
| `schema.json`              | Full Knack object and field definitions. Used by schema/field tools when the runtime API is unavailable.                                              |
| `fieldMap.json`            | Mapping of friendly aliases (e.g. `object_1.full_name`) to field keys (e.g. `field_42`). Used by alias-resolution tools.                              |
| `viewMap.json`             | View attribute data keyed by view key. Used by view and search tools.                                                                                 |
| `fieldReferenceIndex.json` | Cached reverse index of field-key references found across schema metadata, field aliases, and view metadata. Used by field-reference discovery tools. |

The server checks the runtime Knack API first, then falls back to these files. Use `knack_refresh_cache` (with `persist: true`) to write fresh data to disk.

---

## Usage

Once the server is running and connected to your MCP client, you can ask your AI assistant to call any of the following tools.

### Context & Discovery Tools

#### `knack_list_apps`

Lists all Knack apps discovered from your `KnackApps` folder.

```
List my Knack apps
```

#### `knack_set_context`

Sets the active Knack app using either an explicit app key or by inferring which app a file path belongs to. Most tools use the active app automatically.

```
Set context to /path/to/KnackApps/MyApp/src/someFile.js
```

| Parameter     | Type              | Description                                                                                                                                 |
| ------------- | ----------------- | ------------------------------------------------------------------------------------------------------------------------------------------- |
| `appKey`      | string (optional) | Explicit app key to activate immediately. Useful when the current file path is ambiguous or outside `KnackApps/<AppKey>/...`.               |
| `contextPath` | string (optional) | A file or folder path inside `KnackApps/<AppKey>/...`. The server also tries app-name and folder-name aliases when the path is less direct. |

#### `knack_cache_status`

Reports which app is currently active, which local metadata files exist, and the current cache state.

| Parameter | Type              | Description                 |
| --------- | ----------------- | --------------------------- |
| `appKey`  | string (optional) | Defaults to the active app. |

#### `knack_refresh_cache`

Clears in-memory caches for one or all apps and optionally re-warms them from the API and saves the results to disk.

| Parameter      | Type               | Description                                                                                                                    |
| -------------- | ------------------ | ------------------------------------------------------------------------------------------------------------------------------ |
| `appKey`       | string (optional)  | App to refresh. Omit to refresh all apps.                                                                                      |
| `warm`         | boolean (optional) | Re-fetch data immediately after clearing (default: `false`).                                                                   |
| `persistFiles` | boolean (optional) | Save freshly fetched data to `schema.json`, `fieldMap.json`, `viewMap.json`, and `fieldReferenceIndex.json` (default: `true`). |

#### `knack_get_context_bundle`

Fetches a bounded, task-specific bundle of object schema, friendly aliases, and view context in one call. This is intended for a known implementation surface, not as a replacement for the smaller discovery tools.

| Parameter               | Type                | Description                                                              |
| ----------------------- | ------------------- | ------------------------------------------------------------------------ |
| `objectKeys`            | string[] (optional) | Exact object keys to include (maximum 20).                               |
| `fieldAliases`          | string[] (optional) | Exact `fieldMap.json` aliases to resolve (maximum 100).                  |
| `viewKeys`              | string[] (optional) | Exact view keys to include (maximum 20).                                 |
| `includeViewAttributes` | boolean (optional)  | Include guarded raw attributes for requested views. Defaults to `false`. |
| `appKey`                | string (optional)   | Defaults to the active app.                                              |

At least one object key, alias, or view key is required. Only the requested sources are loaded; the response reports its schema, field-map, and view-map sources so callers can tell whether the data came from runtime metadata or local cache files.

For each requested view, `fieldSettings` is always included. It provides a compact list of configured form, search, and display fields with object-level requiredness plus view-level read-only, default, and rule settings, even when guarded raw attributes are too large to return. An omitted setting is not treated as `false`.

---

### Data Read Tools

> These tools require a valid API key in your secrets file.

`knack_get_record` and `knack_find_records` both include a short `tip` in successful responses reminding you that every field is returned twice — `field_xxx` (formatted) and `field_xxx_raw` (raw) — and to prefer the raw value for connections, dates, and other structured fields.

#### `knack_get_record`

Fetches a single record by object key and record ID.

| Parameter   | Type              | Description                        |
| ----------- | ----------------- | ---------------------------------- |
| `objectKey` | string            | Knack object key, e.g. `object_1`. |
| `recordId`  | string            | The record ID to fetch.            |
| `appKey`    | string (optional) | Defaults to the active app.        |

#### `knack_download_file`

Downloads a file or image attachment from a specific record field to a controlled temporary directory outside the repository. The attachment URL is resolved from the record, so the tool cannot download arbitrary URLs. It honours the app's `dataAccess` object and field policy and is available in enforced read-only mode.

| Parameter   | Type              | Description                                    |
| ----------- | ----------------- | ---------------------------------------------- |
| `objectKey` | string            | Object containing the attachment.              |
| `recordId`  | string            | Record containing the attachment.              |
| `fieldKey`  | string            | File or image field containing the attachment. |
| `appKey`    | string (optional) | Defaults to the active app.                    |

#### `knack_read_file`

Downloads an approved attachment and returns its text for AI review. It supports PDF, DOCX, TXT, CSV, JSON, Markdown, and XML. Extracted text is bounded by `KNACK_MCP_MAX_EXTRACTED_TEXT_BYTES`; unsupported formats are downloaded and reported without extracting text.

| Parameter   | Type              | Description                                    |
| ----------- | ----------------- | ---------------------------------------------- |
| `objectKey` | string            | Object containing the attachment.              |
| `recordId`  | string            | Record containing the attachment.              |
| `fieldKey`  | string            | File or image field containing the attachment. |
| `appKey`    | string (optional) | Defaults to the active app.                    |

#### `knack_find_records`

Searches records for an object with optional full-text search and filter expressions.

| Parameter     | Type                        | Description                             |
| ------------- | --------------------------- | --------------------------------------- |
| `objectKey`   | string                      | Knack object key.                       |
| `q`           | string (optional)           | Full-text search query.                 |
| `filters`     | string \| object (optional) | Knack filter JSON or object.            |
| `page`        | number (optional)           | Page number (default: 1).               |
| `rowsPerPage` | number (optional)           | Rows per page (default: 25, max: 1000). |
| `appKey`      | string (optional)           | Defaults to the active app.             |

#### `knack_get_object_records_with_schema`

Fetches records for an object and includes that object's schema in the same response. The tool has hard-coded defaults (`appKey: "ARC"`, `objectKey: "object_294"`) that are specific to this project — always pass explicit values for other apps or objects.

| Parameter     | Type                        | Description                                                   |
| ------------- | --------------------------- | ------------------------------------------------------------- |
| `objectKey`   | string (optional)           | Knack object key (e.g. `object_1`). Defaults to `object_294`. |
| `appKey`      | string (optional)           | App key to use. Defaults to `ARC`.                            |
| `page`        | number (optional)           | Page number (default: 1).                                     |
| `rowsPerPage` | number (optional)           | Rows per page (default: 25, max: 1000).                       |
| `q`           | string (optional)           | Full-text search query.                                       |
| `filters`     | string \| object (optional) | Knack filter JSON or object.                                  |

#### `knack_get_raw_object_metadata`

Returns the raw runtime metadata object payload for a Knack object before schema normalization. This is intended for diagnostics when you need to verify whether Knack is returning attributes such as field descriptions.

| Parameter   | Type              | Description                 |
| ----------- | ----------------- | --------------------------- |
| `objectKey` | string            | Knack object key.           |
| `appKey`    | string (optional) | Defaults to the active app. |

---

### Schema & Field Tools

#### `knack_get_object_fields`

Returns all fields for an object from the cached schema, including object-level requiredness and descriptions when available.

Field mutation tools (`knack_create_field` and `knack_update_field`) now preflight their JSON locally before calling Knack. They require object-shaped JSON for `format`, `relationship`, and `updates`, reject blank field names/types, and require a valid target object key for a newly declared connection field. Advanced valid Knack settings remain pass-through rather than being artificially restricted.

Both mutation tools also accept a dedicated `description` parameter — a short note on what the field is for, stored as the field's description/help text in the Knack Builder. This is useful documentation for other developers or AI assistants reading the schema later (it shows up wherever field descriptions are already surfaced, e.g. `knack_get_object_fields`, `knack_list_fields`). On `knack_update_field`, `description` takes precedence over any `"description"` key already present in `updates`, and `updates` itself becomes optional if you are only setting the description; pass an empty string to clear an existing description.

**KTL keyword protection:** Knack replaces a field's description outright rather than merging it, so `knack_update_field` guards against accidentally wiping out KTL keyword tokens (underscore-prefixed, e.g. `_hideField`) that are already embedded in the current description. Before applying a description change, the tool fetches the field's current definition and checks whether every existing keyword token is still present in the new text. If one would be dropped, the update is blocked with an error listing the missing keyword(s) — pass `confirmRemoveKtlKeywords: true` only after explicitly confirming the removal with the user. If the current definition can't be fetched (e.g. no API access), the check is skipped and a `ktlKeywordWarnings` note is included in the response instead of blocking the write.

`knack_update_field`'s `dryRun` preview returns `currentField` plus a `changes` object — only the keys your update actually touches, each as `{from, to}` — rather than the full field definition twice, so the preview stays proportional to the size of the edit rather than the size of the field.

**Connection-field write payload size:** Knack's create/update response for a connection field includes the full application schema (every object's field list), not just the field you touched — creating or changing a connection also updates the cross-object relationship graph, and Knack's API reflects that in the response body. On an app with many objects this can run into tens of thousands of characters. Both `knack_create_field` and `knack_update_field` detect an oversized response (over `KNACK_MCP_MAX_INLINE_DETAIL_BYTES`) and project it down to a `field` key holding just the created/updated field, plus a `bodySummary` structural summary and a `note` explaining what happened — call `knack_get_field` afterwards for the full raw definition if you need it. Smaller responses (most field types) are returned as before, unprojected.

| Parameter   | Type              | Description                 |
| ----------- | ----------------- | --------------------------- |
| `objectKey` | string            | Knack object key.           |
| `appKey`    | string (optional) | Defaults to the active app. |

#### `knack_get_object`

Returns an object's metadata (name, key) plus all its fields from the cached schema, including descriptions when available.

| Parameter   | Type              | Description                 |
| ----------- | ----------------- | --------------------------- |
| `objectKey` | string            | Knack object key.           |
| `appKey`    | string (optional) | Defaults to the active app. |

#### `knack_list_fields`

Lists all fields for an object showing field key, name, type, object-level requiredness, and description when available.

| Parameter   | Type              | Description                 |
| ----------- | ----------------- | --------------------------- |
| `objectKey` | string            | Knack object key.           |
| `appKey`    | string (optional) | Defaults to the active app. |

#### `knack_get_field_type`

Returns the type of a specific field by field key or friendly alias.

| Parameter         | Type              | Description                                                         |
| ----------------- | ----------------- | ------------------------------------------------------------------- |
| `fieldKeyOrAlias` | string            | A field key (e.g. `field_42`) or alias (e.g. `object_1.full_name`). |
| `appKey`          | string (optional) | Defaults to the active app.                                         |

#### `knack_list_field_types`

Lists all fields for an object with their types and provides a grouped summary by type.

| Parameter   | Type              | Description                 |
| ----------- | ----------------- | --------------------------- |
| `objectKey` | string            | Knack object key.           |
| `appKey`    | string (optional) | Defaults to the active app. |

#### `knack_resolve_field_alias`

Resolves a friendly alias from `fieldMap.json` to the underlying Knack field key. When resolved from the on-disk cache, the response includes a `note` that the map may be stale if a field was recently created, renamed, or deleted — run `knack_refresh_cache` (`warm: true, persistFiles: true`) to be sure.

| Parameter | Type              | Description                            |
| --------- | ----------------- | -------------------------------------- |
| `alias`   | string            | An alias such as `object_1.full_name`. |
| `appKey`  | string (optional) | Defaults to the active app.            |

#### `knack_resolve_any`

Resolves any identifier — field key or alias — to its field key, name, type, and parent object key.

| Parameter    | Type              | Description                 |
| ------------ | ----------------- | --------------------------- |
| `identifier` | string            | A field key or alias.       |
| `appKey`     | string (optional) | Defaults to the active app. |

#### `validateFieldMapping`

Validates a mapping object by resolving each alias/key and checking that it exists in the schema.

| Parameter       | Type              | Description                                           |
| --------------- | ----------------- | ----------------------------------------------------- |
| `mappingObject` | object            | Key/value pairs of aliases or field keys to validate. |
| `appKey`        | string (optional) | Defaults to the active app.                           |

#### `generateSnapshotStructure`

Generates a snapshot-style object structure for a given Knack object using its schema fields.

| Parameter   | Type              | Description                 |
| ----------- | ----------------- | --------------------------- |
| `objectKey` | string            | Knack object key.           |
| `appKey`    | string (optional) | Defaults to the active app. |

#### `checkForDuplicateFieldUsage`

Checks for duplicate field usage across `fieldMap` aliases and optionally within a provided mapping object.

| Parameter       | Type              | Description                                    |
| --------------- | ----------------- | ---------------------------------------------- |
| `mappingObject` | object (optional) | An additional mapping to check for duplicates. |
| `appKey`        | string (optional) | Defaults to the active app.                    |

#### `knack_list_field_references`

Lists all cached references for a field id across schema metadata, alias mappings, and view metadata.

The response also includes Knack Builder URLs for the field and any matching scene/view references when enough ids are available.

| Parameter    | Type              | Description                                                         |
| ------------ | ----------------- | ------------------------------------------------------------------- |
| `fieldKey`   | string            | Knack field key, e.g. `field_42`.                                   |
| `maxResults` | number (optional) | Maximum number of references to return (default: 1000, max: 10000). |
| `appKey`     | string (optional) | Defaults to the active app.                                         |

---

### Database Design & Overview Tools

These tools give a high-level view of the entire data model and explain the shape of data returned by the Knack API — making it easier to build and reason about Knack applications.

#### `knack_list_objects`

Lists every object in the app schema with its key, name, and field count. Use this as the first step when exploring an unfamiliar app to map out the full data model.

| Parameter | Type              | Description                 |
| --------- | ----------------- | --------------------------- |
| `appKey`  | string (optional) | Defaults to the active app. |

#### `knack_describe_field_shape`

Returns the expected shape of data that the Knack API returns for a given field type — both the formatted value (human-readable) and the raw value (machine-readable). Use this when writing code that reads or processes Knack records.

| Parameter   | Type   | Description                                                                             |
| ----------- | ------ | --------------------------------------------------------------------------------------- |
| `fieldType` | string | Knack field type, e.g. `connection`, `date_time`, `name`, `address`, `multiple_choice`. |

**Example response for `connection` type:**

```json
{
    "fieldType": "connection",
    "summary": "Reference to one or more records in another object.",
    "formattedShape": "\"Record Label A, Record Label B\"",
    "rawShape": "[{ \"id\": \"abc123\", \"identifier\": \"Record Label A\" }, { \"id\": \"def456\", \"identifier\": \"Record Label B\" }]",
    "notes": "Raw is an array of objects with id (record ID) and identifier (display label). Use raw when you need record IDs for further API calls.",
    "tip": "Knack returns both field_xxx (formatted) and field_xxx_raw (raw) for every field. Prefer raw values when you need machine-readable data."
}
```

#### `knack_verify_record_field_shapes`

Fetches a live record and compares each observed field value against the documented field-shape heuristics for that field type. Use this to validate or refine the field-shape docs with real Knack payloads.

| Parameter            | Type               | Description                                                                        |
| -------------------- | ------------------ | ---------------------------------------------------------------------------------- |
| `objectKey`          | string             | Knack object key.                                                                  |
| `recordId`           | string             | The record ID to inspect.                                                          |
| `appKey`             | string (optional)  | Defaults to the active app.                                                        |
| `includeBlankFields` | boolean (optional) | Include fields where both formatted and raw values are blank. Defaults to `false`. |

The response includes per-field status (`match`, `mismatch`, `skipped`, or `unknown`), observed formatted/raw shape classifications, preview values, and any findings.

#### `knack_get_object_connections`

Returns all connection fields for a given object, showing which other objects they link to. Use this to understand relationships between objects and navigate the data graph.

| Parameter   | Type              | Description                 |
| ----------- | ----------------- | --------------------------- |
| `objectKey` | string            | Knack object key.           |
| `appKey`    | string (optional) | Defaults to the active app. |

#### `knack_get_app_overview`

Returns a complete overview of the app schema: all objects with field counts, field type breakdowns, and the full connection graph between objects. Use this to understand the data model at a glance and get a foundation for database design advice.

| Parameter             | Type               | Description                                                                                 |
| --------------------- | ------------------ | ------------------------------------------------------------------------------------------- |
| `appKey`              | string (optional)  | Defaults to the active app.                                                                 |
| `includeFieldDetails` | boolean (optional) | When `true`, include all field names and types for each object (verbose). Default: `false`. |

#### `knack_app_deep_dive`

A one-call onboarding snapshot for an app you haven't explored yet. It combines what would otherwise take several separate calls — `knack_get_app_overview`, `knack_analyze_data_model`, and a `knack_list_scenes`/`knack_list_views` summary — into a single response: the data model (objects, field types, connection graph), design-feedback observations, and a UI-structure summary (scene/view counts and view-type breakdown). Call this first when starting work on an unfamiliar app, then use the more targeted tools for deeper detail on a specific object, scene, or view.

| Parameter                | Type               | Description                                                                                                                                                                  |
| ------------------------ | ------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `appKey`                 | string (optional)  | Defaults to the active app.                                                                                                                                                  |
| `includeFieldDetails`    | boolean (optional) | Include every field name/type per object in the data model section (verbose). Default: `false`.                                                                              |
| `includeScenes`          | boolean (optional) | Include the per-scene list (key, name, slug, view count) under `ui.scenes`. Default: `false` — only totals and the view-type summary.                                        |
| `maxRelationshipsListed` | number (optional)  | Cap on connection relationships listed in full under `dataModel.relationships`. Default: `200`, max `2000`. The total count is always accurate even when the list is capped. |

If scene/view metadata hasn't been cached yet, `ui.available` is `false` with a message pointing at `knack_refresh_cache` — the data-model section is still returned in full.

#### `knack_generate_seed_csvs`

Generates Knack import-ready seed CSV content for new object imports. The response includes one CSV per object, realistic example rows, suggested unique import keys, connection lookup notes, and an import order so parent/lookup objects can be loaded before dependent objects.

| Parameter                             | Type                | Description                                                                                                                                                                  |
| ------------------------------------- | ------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `appKey`                              | string (optional)   | Defaults to the active app.                                                                                                                                                  |
| `objectKeys`                          | string[] (optional) | Restrict generation to a subset of object keys. Defaults to all objects in the schema.                                                                                       |
| `rowsPerObject`                       | number (optional)   | Minimum number of sample rows per object. Default: `4`, min `2`, max `10`.                                                                                                   |
| `useExistingConnectionValues`         | boolean (optional)  | When `true`, the tool plans authenticated API calls to fetch first-page display values for connected parent objects that are not included in `objectKeys`. Default: `false`. |
| `confirmExistingConnectionValueFetch` | boolean (optional)  | Must be set to `true` before the tool performs any API-key-backed parent lookup fetches. Default: `false`.                                                                   |

The generated CSVs follow Knack import-friendly conventions:

- use field names as headers
- generate a stable unique import key per object
- populate connection fields with matching lookup values from the connected object’s generated CSV
- use a single cell with comma-separated values for multi-select and many-to-many examples
- split `name` and `address` fields into separate import columns
- skip non-importable/system fields such as rollups and auto-increment values

### Relationship and reporting tools

#### `knack_get_related_records`

Retrieves selected fields from connected records without requiring the client to reconstruct Knack connection shapes. Use `forward` to follow a connection from a source record, or `reverse` to find records whose connection field points at the source record. Reverse queries use one filtered API request; forward queries fetch each connected record, so keep the limit modest.

The tool requires explicit `fieldKeys` and validates the source object, related object, connection, fields, and any sort field against the current schema.

#### `knack_aggregate_records`

Counts or sums records with optional Knack filters, up to three grouping fields, and an optional day/month/year date bucket. It returns aggregate groups only, never the source records. Set `maxRecords` deliberately; when the scan is capped (`capped: true`), the response also includes an explicit `warning` stating the counts/sums are partial, not the true total — don't report a capped result as final.

### Batch record tools

> These tools require a valid API key in your secrets file and `readonly: false` in `app.json` (`knack_batch_delete_records` also requires `allowDelete: true`).

#### `knack_batch_create_records` / `knack_batch_update_records` / `knack_batch_delete_records`

Create, update, or delete up to 100 records in a Knack object in one call. Requests run with up to `KNACK_MCP_BATCH_CONCURRENCY` (default 5, max 10) in flight at once, and any individual request that gets a `429` (rate limited) or `5xx` response is retried with exponential backoff before being reported as failed. Each item's own result is reported individually in `results`, alongside `successCount`/`failureCount` — one failing record does not abort the rest of the batch, so check each entry rather than assuming an all-or-nothing outcome.

| Parameter   | Type                                   | Description                                                                                                                                   |
| ----------- | -------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------- |
| `appKey`    | string (optional)                      | Defaults to the active app.                                                                                                                   |
| `objectKey` | string                                 | Knack object key.                                                                                                                             |
| `records`   | string[] (create) / object[] (update)  | Create: array of record-data JSON strings (same shape as `knack_create_record`'s `data`). Update: array of `{recordId, data}` pairs. Max 100. |
| `recordIds` | string[] (delete only)                 | Record IDs to delete. Max 100.                                                                                                                |
| `dryRun`    | boolean (optional, create/update only) | Validates every record's JSON and returns the count/preview without writing anything.                                                         |
| `confirm`   | boolean (optional, delete only)        | Must be `true` to actually delete; otherwise returns a preview of what would be deleted. This is destructive and cannot be undone.            |

### Sensitive-data deployments

For apps containing confidential information, configure an explicit `dataAccess` policy in that app's `app.json`. The policy is enforced by the record-read tools and the relationship/reporting tools. It is designed to keep sensitive field selection in local configuration rather than in the public MCP source code.

```json
{
    "appKey": "MyApp",
    "appId": "5f3a1b2c3d4e5f6a7b8c9d0e",
    "readonly": true,
    "allowViewMutation": false,
    "allowDelete": false,
    "allowDiagnostics": false,
    "dataAccess": {
        "allowedObjectKeys": [
            "object_clients",
            "object_referrals",
            "object_assessments"
        ],
        "allowedFieldKeys": {
            "object_clients": ["field_client_reference", "field_client_name"],
            "object_referrals": [
                "field_referral_client",
                "field_referral_status",
                "field_received_date"
            ],
            "object_assessments": [
                "field_assessment_client",
                "field_assessed_date",
                "field_assessor"
            ]
        },
        "redactedFieldKeys": [
            "field_gp_summary",
            "field_address",
            "field_email"
        ],
        "maxRecordsPerQuery": 500
    }
}
```

`allowedObjectKeys` restricts record reads to named objects. `allowedFieldKeys` limits returned fields per object; connection fields used for relationship traversal must also be included. `redactedFieldKeys` wins over an allowlist. For policy-protected apps, filters and sorting may only use approved fields, free-text search is disabled, and list responses are capped by `maxRecordsPerQuery`. Record APIs preserve their normal behaviour when no `dataAccess` policy is configured, to avoid changing existing deployments.

Keep this configuration, the Knack API key, and the director's MCP installation separate from write-capable technical installations. An API key is application-level, so `readonly` and the policy are important safeguards rather than an indication of what the person using Claude is entitled to see.

When `useExistingConnectionValues` is enabled, the tool **does not call the authenticated API immediately**. It first returns:

- whether confirmation is required
- a rough authenticated API call estimate
- which connected parent objects would be queried
- the planned `/objects/<objectKey>/records?page=1&rows_per_page=<n>` requests

Re-run the tool with `confirmExistingConnectionValueFetch: true` only after reviewing that estimate.

When it fetches existing parent display values from Knack, it uses the first non-empty field in this priority order from each returned record: `identifier`, `display`, `name`, `label`, then `id`.

---

### View & Search Tools

#### `knack_get_view_context`

Returns the scene context (scene key, name, and slug) for a given view key.

The response also includes `builderUrls.scene` and `builderUrls.view`.

| Parameter | Type              | Description                    |
| --------- | ----------------- | ------------------------------ |
| `viewKey` | string            | Knack view key, e.g. `view_1`. |
| `appKey`  | string (optional) | Defaults to the active app.    |

#### `knack_get_view_attributes`

Returns view attributes for a view key from runtime metadata or the cached `viewMap.json`. By default this returns `fieldSettings` only — a compact field-level summary of key, type, label, object-level requiredness, read-only settings, defaults, and stored rules — rather than the full raw view JSON, since `fieldSettings` already covers the common case in a much smaller payload. It does not evaluate conditional visibility or other rules against a record.

Pass `includeRawAttributes: true` to also get the full raw `attributes` payload (layout, `pageGroups`, rules) alongside `fieldSettings`, inlined only when small enough to stay economical (see `KNACK_MCP_MAX_INLINE_DETAIL_BYTES`).

The response also includes `builderUrls.scene` and `builderUrls.view`.

| Parameter              | Type               | Description                                                     |
| ---------------------- | ------------------ | --------------------------------------------------------------- |
| `viewKey`              | string             | Knack view key.                                                 |
| `appKey`               | string (optional)  | Defaults to the active app.                                     |
| `includeRawAttributes` | boolean (optional) | Include the full raw view attributes payload. Default: `false`. |

#### `knack_list_view_fields`

Returns the configured form inputs, search fields, and displayed columns for one view without returning its full raw payload. Each result includes its field key, label, type, object-level requiredness, view-level read-only settings, defaults, stored rules (including visibility rules when supplied by Knack), layout role, and source path in the view metadata. View-level rules are included separately and are not evaluated against a record.

| Parameter | Type              | Description                    |
| --------- | ----------------- | ------------------------------ |
| `viewKey` | string            | Knack view key, e.g. `view_1`. |
| `appKey`  | string (optional) | Defaults to the active app.    |

#### `knack_search_ktl_keywords`

Searches view titles and descriptions for KTL-style underscore keywords (e.g. `_myKeyword`).

| Parameter | Type              | Description                                      |
| --------- | ----------------- | ------------------------------------------------ |
| `query`   | string (optional) | Filter results to keywords containing this text. |
| `appKey`  | string (optional) | Defaults to the active app.                      |

#### `knack_search_emails`

Searches views for email-related rules and actions, returning recipient addresses, subjects, and message content.

| Parameter        | Type               | Description                                                    |
| ---------------- | ------------------ | -------------------------------------------------------------- |
| `query`          | string (optional)  | Filter by text found in the recipient, subject, or message.    |
| `includeMessage` | boolean (optional) | Include full message content in results (default: `true`).     |
| `maxResults`     | number (optional)  | Maximum number of results to return (default: 500, max: 5000). |
| `appKey`         | string (optional)  | Defaults to the active app.                                    |

#### `knack_find_views_with_record_rule_field`

Finds views whose record-rule-related metadata references a specific field id.

The response also includes Knack Builder URLs for the field, scene, and view when enough ids are available.

| Parameter    | Type              | Description                                                                 |
| ------------ | ----------------- | --------------------------------------------------------------------------- |
| `fieldKey`   | string            | Knack field key, e.g. `field_42`.                                           |
| `maxResults` | number (optional) | Maximum number of matching references to inspect (default: 500, max: 5000). |
| `appKey`     | string (optional) | Defaults to the active app.                                                 |

#### `knack_list_scenes`

Lists all scenes (pages) in the app with their key, name, slug, view count, and optionally the full list of views per scene. Use this to explore the UI structure of a Knack application and discover what scenes and views exist before querying individual views.

The response includes a `builderUrl` for each scene when enough metadata is available.

| Parameter      | Type               | Description                                                                             |
| -------------- | ------------------ | --------------------------------------------------------------------------------------- |
| `includeViews` | boolean (optional) | When `true`, include each view's key, name, and type under the scene (default: `true`). |
| `appKey`       | string (optional)  | Defaults to the active app.                                                             |

> **Note:** Requires runtime metadata. Run `knack_refresh_cache` with `warm: true` if scene data is missing.

#### `knack_list_views`

Lists all views across the app with their scene context (scene key, name, slug), view type, and a Knack Builder URL. Supports filtering by scene key or view type so you can quickly find, for example, all `form` views or all views in a specific scene.

| Parameter    | Type              | Description                                                                                                    |
| ------------ | ----------------- | -------------------------------------------------------------------------------------------------------------- |
| `sceneKey`   | string (optional) | Filter to views belonging to a specific scene.                                                                 |
| `viewType`   | string (optional) | Filter by view type, e.g. `form`, `grid`, `table`, `report`, `search`, `menu`, `rich_text`, `map`, `calendar`. |
| `maxResults` | number (optional) | Maximum number of views to return (default: 500, max: 5000).                                                   |
| `appKey`     | string (optional) | Defaults to the active app.                                                                                    |

The response includes a `viewTypeSummary` showing the count of each view type across the (filtered) app.

> **Note:** Requires runtime metadata. Run `knack_refresh_cache` with `warm: true` if scene data is missing.

---

### Data Model Analysis Tools

#### `knack_analyze_data_model`

Analyses the app's data model and returns structured design feedback including field-count distribution, connection density, isolated objects (no connections), objects with unusually high or low field counts, field type spread across the whole app, and a plain-English observations list.

| Parameter | Type              | Description                 |
| --------- | ----------------- | --------------------------- |
| `appKey`  | string (optional) | Defaults to the active app. |

**Example response (abbreviated):**

```json
{
    "summary": {
        "totalObjects": 18,
        "totalFields": 312,
        "avgFieldCount": 17,
        "minFieldCount": 2,
        "maxFieldCount": 58,
        "connectedObjectCount": 14,
        "isolatedObjectCount": 4
    },
    "fieldTypeDistribution": [
        { "type": "short_text", "count": 89, "percentage": 29 },
        { "type": "connection", "count": 42, "percentage": 13 }
    ],
    "highFieldCountObjects": [
        {
            "objectKey": "object_5",
            "objectName": "Applications",
            "fieldCount": 58
        }
    ],
    "isolatedObjects": [
        {
            "objectKey": "object_12",
            "objectName": "Lookup Codes",
            "fieldCount": 3
        }
    ],
    "observations": [
        "4 object(s) have no connection fields — they may be standalone lookup tables or unused.",
        "1 object(s) exceed 34 fields — consider whether any could be split into related objects.",
        "78% of objects participate in at least one connection relationship."
    ]
}
```

---

### MCP Resources

In addition to tools, the server exposes read-only resources that can be attached directly as context:

| URI                         | Description                               |
| --------------------------- | ----------------------------------------- |
| `knack://<AppKey>/schema`   | Full object and field schema for the app. |
| `knack://<AppKey>/fieldMap` | Field alias map for the app.              |
| `knack://<AppKey>/viewMap`  | View attribute map for the app.           |

---

## Workflow Tips

- **Start a session** by asking your AI to call `knack_list_apps`, then `knack_set_context` with either your current file path or the explicit `appKey`. All subsequent tool calls will automatically use the right app.
- **Get oriented on an unfamiliar app** by calling `knack_app_deep_dive` — it's the fastest single-call way to see the data model, design observations, and UI structure at once, before drilling into the more targeted tools below.
- **Explore the data model** by calling `knack_get_app_overview` to see all objects, their field counts, and how they connect to each other in one response.
- **Get design feedback** on the data model by calling `knack_analyze_data_model` — it highlights isolated objects, unusually large tables, field type distribution, and connection density in a single structured response.
- **Explore the UI structure** by calling `knack_list_scenes` to discover every page (scene) and the views it contains. Then use `knack_list_views` with `viewType: "form"` (or another type) to filter down to exactly the views you need.
- **Understand returned data** before writing code that reads records — call `knack_describe_field_shape` with the field type (e.g. `connection`, `date_time`, `name`) to see exactly what shape the API returns. Remember that Knack provides both `field_xxx` (formatted) and `field_xxx_raw` (raw) values for every field.
- **Validate shape docs against real payloads** by calling `knack_verify_record_field_shapes` with a known record ID from an object that has representative data. This is the fastest way to spot where the documented shapes need tightening.
- **Trace relationships** by calling `knack_get_object_connections` on any object to see which fields link to other objects and what those objects are named.
- **Persist schema data** by calling `knack_refresh_cache` with `warm: true, persistFiles: true`. This writes `schema.json`, `fieldMap.json`, `viewMap.json`, and `fieldReferenceIndex.json` to disk so the server works even when offline or without an API key. It also populates connection relationship metadata used by `knack_get_object_connections` and `knack_get_app_overview`.
- **Use aliases** — if you have a `fieldMap.json`, prefer aliases like `object_1.full_name` over raw field keys. They are more readable and the server resolves them automatically.
- **Enable debug logging** by setting `DEBUG=1` in the server's environment when troubleshooting. Debug output is written to stderr and will not interfere with the MCP stdio transport.
