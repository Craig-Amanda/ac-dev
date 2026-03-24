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
  - [MCP Resources](#mcp-resources)
- [Workflow Tips](#workflow-tips)

---

## Prerequisites

- **Node.js 18+** (required for native `fetch` support)
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
  "notes": "Production app — handle with care"
}
```

| Field | Required | Description |
|-------|----------|-------------|
| `appKey` | ✅ | A short identifier for the app (must match the folder name). |
| `appId` | ✅ | Your Knack Application ID (found in the Knack Builder under **Settings → API & Code**). |
| `appName` | No | A friendly display name for the app. |
| `apiBase` | No | API base URL. Defaults to `https://api.knack.com/v1`. |
| `notes` | No | Free-text notes visible in `knack_list_apps`. |

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
    "knack-mcp": {
      "command": "node",
      "args": ["/absolute/path/to/knack-mcp/dist/server.js"],
      "env": {
        "KNACK_APPS_DIR": "/absolute/path/to/KnackApps",
        "KNACK_MCP_SECRETS_PATH": "/absolute/path/to/.knack-mcp-secrets.json"
      }
    }
  }
}
```

Replace the paths with the actual locations on your machine. After saving, restart your MCP client to pick up the new server.

---

## Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `KNACK_APPS_DIR` | ✅ | — | Absolute path to your `KnackApps` directory. |
| `KNACK_MCP_SECRETS_PATH` | No | `~/.knack-mcp-secrets.json` | Path to your secrets JSON file. |
| `DEBUG` | No | `false` | Set to `1`, `true`, `yes`, or `on` to write debug logs to stderr. |
| `KNACK_CACHE_TTL_MS` | No | `300000` (5 min) | How long runtime data is cached in memory before re-fetching, in milliseconds. |
| `KNACK_MAX_RESPONSE_BYTES` | No | `20971520` (20 MB) | Maximum size (in bytes) of an API response the server will process. |

---

## Optional Cache Files

To reduce API calls — or to allow the server to work without a live API key — you can place pre-fetched JSON files inside `KnackApps/<AppKey>/schema/`:

| File | Description |
|------|-------------|
| `schema.json` | Full Knack object and field definitions. Used by schema/field tools when the runtime API is unavailable. |
| `fieldMap.json` | Mapping of friendly aliases (e.g. `object_1.full_name`) to field keys (e.g. `field_42`). Used by alias-resolution tools. |
| `viewMap.json` | View attribute data keyed by view key. Used by view and search tools. |

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
Sets the active Knack app by inferring which app a file path belongs to. Most tools use the active app automatically.

```
Set context to /path/to/KnackApps/MyApp/src/someFile.js
```

| Parameter | Type | Description |
|-----------|------|-------------|
| `contextPath` | string | A file or folder path inside `KnackApps/<AppKey>/...`. |

#### `knack_cache_status`
Reports which app is currently active, which local metadata files exist, and the current cache state.

| Parameter | Type | Description |
|-----------|------|-------------|
| `appKey` | string (optional) | Defaults to the active app. |

#### `knack_refresh_cache`
Clears in-memory caches for one or all apps and optionally re-warms them from the API and saves the results to disk.

| Parameter | Type | Description |
|-----------|------|-------------|
| `appKey` | string (optional) | App to refresh. Omit to refresh all apps. |
| `warm` | boolean (optional) | Re-fetch data immediately after clearing (default: `false`). |
| `persistFiles` | boolean (optional) | Save freshly fetched data to `schema.json`, `fieldMap.json`, and `viewMap.json` (default: `true`). |

---

### Data Read Tools

> These tools require a valid API key in your secrets file.

#### `knack_get_record`
Fetches a single record by object key and record ID.

| Parameter | Type | Description |
|-----------|------|-------------|
| `objectKey` | string | Knack object key, e.g. `object_1`. |
| `recordId` | string | The record ID to fetch. |
| `appKey` | string (optional) | Defaults to the active app. |

#### `knack_find_records`
Searches records for an object with optional full-text search and filter expressions.

| Parameter | Type | Description |
|-----------|------|-------------|
| `objectKey` | string | Knack object key. |
| `q` | string (optional) | Full-text search query. |
| `filters` | string \| object (optional) | Knack filter JSON or object. |
| `page` | number (optional) | Page number (default: 1). |
| `rowsPerPage` | number (optional) | Rows per page (default: 25, max: 1000). |
| `appKey` | string (optional) | Defaults to the active app. |

#### `knack_get_object_records_with_schema`
Fetches records for an object and includes that object's schema in the same response. The tool has hard-coded defaults (`appKey: "ARC"`, `objectKey: "object_294"`) that are specific to this project — always pass explicit values for other apps or objects.

| Parameter | Type | Description |
|-----------|------|-------------|
| `objectKey` | string (optional) | Knack object key (e.g. `object_1`). Defaults to `object_294`. |
| `appKey` | string (optional) | App key to use. Defaults to `ARC`. |
| `page` | number (optional) | Page number (default: 1). |
| `rowsPerPage` | number (optional) | Rows per page (default: 25, max: 1000). |
| `q` | string (optional) | Full-text search query. |
| `filters` | string \| object (optional) | Knack filter JSON or object. |

#### `knack_get_raw_object_metadata`
Returns the raw runtime metadata object payload for a Knack object before schema normalization. This is intended for diagnostics when you need to verify whether Knack is returning attributes such as field descriptions.

| Parameter | Type | Description |
|-----------|------|-------------|
| `objectKey` | string | Knack object key. |
| `appKey` | string (optional) | Defaults to the active app. |

---

### Schema & Field Tools

#### `knack_get_object_fields`
Returns all fields for an object from the cached schema, including descriptions when available.

| Parameter | Type | Description |
|-----------|------|-------------|
| `objectKey` | string | Knack object key. |
| `appKey` | string (optional) | Defaults to the active app. |

#### `knack_get_object`
Returns an object's metadata (name, key) plus all its fields from the cached schema, including descriptions when available.

| Parameter | Type | Description |
|-----------|------|-------------|
| `objectKey` | string | Knack object key. |
| `appKey` | string (optional) | Defaults to the active app. |

#### `knack_list_fields`
Lists all fields for an object showing field key, name, type, and description when available.

| Parameter | Type | Description |
|-----------|------|-------------|
| `objectKey` | string | Knack object key. |
| `appKey` | string (optional) | Defaults to the active app. |

#### `knack_get_field_type`
Returns the type of a specific field by field key or friendly alias.

| Parameter | Type | Description |
|-----------|------|-------------|
| `fieldKeyOrAlias` | string | A field key (e.g. `field_42`) or alias (e.g. `object_1.full_name`). |
| `appKey` | string (optional) | Defaults to the active app. |

#### `knack_list_field_types`
Lists all fields for an object with their types and provides a grouped summary by type.

| Parameter | Type | Description |
|-----------|------|-------------|
| `objectKey` | string | Knack object key. |
| `appKey` | string (optional) | Defaults to the active app. |

#### `knack_resolve_field_alias`
Resolves a friendly alias from `fieldMap.json` to the underlying Knack field key.

| Parameter | Type | Description |
|-----------|------|-------------|
| `alias` | string | An alias such as `object_1.full_name`. |
| `appKey` | string (optional) | Defaults to the active app. |

#### `knack_resolve_any`
Resolves any identifier — field key or alias — to its field key, name, type, and parent object key.

| Parameter | Type | Description |
|-----------|------|-------------|
| `identifier` | string | A field key or alias. |
| `appKey` | string (optional) | Defaults to the active app. |

#### `validateFieldMapping`
Validates a mapping object by resolving each alias/key and checking that it exists in the schema.

| Parameter | Type | Description |
|-----------|------|-------------|
| `mappingObject` | object | Key/value pairs of aliases or field keys to validate. |
| `appKey` | string (optional) | Defaults to the active app. |

#### `generateSnapshotStructure`
Generates a snapshot-style object structure for a given Knack object using its schema fields.

| Parameter | Type | Description |
|-----------|------|-------------|
| `objectKey` | string | Knack object key. |
| `appKey` | string (optional) | Defaults to the active app. |

#### `checkForDuplicateFieldUsage`
Checks for duplicate field usage across `fieldMap` aliases and optionally within a provided mapping object.

| Parameter | Type | Description |
|-----------|------|-------------|
| `mappingObject` | object (optional) | An additional mapping to check for duplicates. |
| `appKey` | string (optional) | Defaults to the active app. |

---

### Database Design & Overview Tools

These tools give a high-level view of the entire data model and explain the shape of data returned by the Knack API — making it easier to build and reason about Knack applications.

#### `knack_list_objects`
Lists every object in the app schema with its key, name, and field count. Use this as the first step when exploring an unfamiliar app to map out the full data model.

| Parameter | Type | Description |
|-----------|------|-------------|
| `appKey` | string (optional) | Defaults to the active app. |

#### `knack_describe_field_shape`
Returns the expected shape of data that the Knack API returns for a given field type — both the formatted value (human-readable) and the raw value (machine-readable). Use this when writing code that reads or processes Knack records.

| Parameter | Type | Description |
|-----------|------|-------------|
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

| Parameter | Type | Description |
|-----------|------|-------------|
| `objectKey` | string | Knack object key. |
| `recordId` | string | The record ID to inspect. |
| `appKey` | string (optional) | Defaults to the active app. |
| `includeBlankFields` | boolean (optional) | Include fields where both formatted and raw values are blank. Defaults to `false`. |

The response includes per-field status (`match`, `mismatch`, `skipped`, or `unknown`), observed formatted/raw shape classifications, preview values, and any findings.

#### `knack_get_object_connections`
Returns all connection fields for a given object, showing which other objects they link to. Use this to understand relationships between objects and navigate the data graph.

| Parameter | Type | Description |
|-----------|------|-------------|
| `objectKey` | string | Knack object key. |
| `appKey` | string (optional) | Defaults to the active app. |

#### `knack_get_app_overview`
Returns a complete overview of the app schema: all objects with field counts, field type breakdowns, and the full connection graph between objects. Use this to understand the data model at a glance and get a foundation for database design advice.

| Parameter | Type | Description |
|-----------|------|-------------|
| `appKey` | string (optional) | Defaults to the active app. |
| `includeFieldDetails` | boolean (optional) | When `true`, include all field names and types for each object (verbose). Default: `false`. |

---

### View & Search Tools

#### `knack_get_view_context`
Returns the scene context (scene key, name, and slug) for a given view key.

| Parameter | Type | Description |
|-----------|------|-------------|
| `viewKey` | string | Knack view key, e.g. `view_1`. |
| `appKey` | string (optional) | Defaults to the active app. |

#### `knack_get_view_attributes`
Returns all stored attributes for a view key from runtime metadata or the cached `viewMap.json`.

| Parameter | Type | Description |
|-----------|------|-------------|
| `viewKey` | string | Knack view key. |
| `appKey` | string (optional) | Defaults to the active app. |

#### `knack_search_ktl_keywords`
Searches view titles and descriptions for KTL-style underscore keywords (e.g. `_myKeyword`).

| Parameter | Type | Description |
|-----------|------|-------------|
| `query` | string (optional) | Filter results to keywords containing this text. |
| `appKey` | string (optional) | Defaults to the active app. |

#### `knack_search_emails`
Searches views for email-related rules and actions, returning recipient addresses, subjects, and message content.

| Parameter | Type | Description |
|-----------|------|-------------|
| `query` | string (optional) | Filter by text found in the recipient, subject, or message. |
| `includeMessage` | boolean (optional) | Include full message content in results (default: `true`). |
| `maxResults` | number (optional) | Maximum number of results to return (default: 500, max: 5000). |
| `appKey` | string (optional) | Defaults to the active app. |

---

### MCP Resources

In addition to tools, the server exposes read-only resources that can be attached directly as context:

| URI | Description |
|-----|-------------|
| `knack://<AppKey>/schema` | Full object and field schema for the app. |
| `knack://<AppKey>/fieldMap` | Field alias map for the app. |
| `knack://<AppKey>/viewMap` | View attribute map for the app. |

---

## Workflow Tips

- **Start a session** by asking your AI to call `knack_list_apps`, then `knack_set_context` with your current file path. All subsequent tool calls will automatically use the right app.
- **Explore the data model** by calling `knack_get_app_overview` to see all objects, their field counts, and how they connect to each other in one response.
- **Understand returned data** before writing code that reads records — call `knack_describe_field_shape` with the field type (e.g. `connection`, `date_time`, `name`) to see exactly what shape the API returns. Remember that Knack provides both `field_xxx` (formatted) and `field_xxx_raw` (raw) values for every field.
- **Validate shape docs against real payloads** by calling `knack_verify_record_field_shapes` with a known record ID from an object that has representative data. This is the fastest way to spot where the documented shapes need tightening.
- **Trace relationships** by calling `knack_get_object_connections` on any object to see which fields link to other objects and what those objects are named.
- **Persist schema data** by calling `knack_refresh_cache` with `warm: true, persistFiles: true`. This writes `schema.json`, `fieldMap.json`, and `viewMap.json` to disk so the server works even when offline or without an API key. It also populates connection relationship metadata used by `knack_get_object_connections` and `knack_get_app_overview`.
- **Use aliases** — if you have a `fieldMap.json`, prefer aliases like `object_1.full_name` over raw field keys. They are more readable and the server resolves them automatically.
- **Enable debug logging** by setting `DEBUG=1` in the server's environment when troubleshooting. Debug output is written to stderr and will not interfere with the MCP stdio transport.
