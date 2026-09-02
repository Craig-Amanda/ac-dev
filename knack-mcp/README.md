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
- [View safety rules](#view-safety-rules)
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
- `knack_copy_view` and `knack_move_view` wrap `POST /scenes/{sourceSceneKey}/copyview`.
- Every view mutation runs through the safety guard described in [View safety rules](#view-safety-rules) — a mutation that would leave a child page unreachable goes to a human first, on any view type including menus, and source mutations that can remove a view or its child pages write a snapshot first.

**Cache staleness:** none of the mutation tools (field or view) invalidate the in-memory/on-disk schema or scene/view cache automatically. Every successful field-mutation response (`knack_create_field`, `knack_update_field`, `knack_delete_field`, `knack_duplicate_field`) includes a `cacheNote`, and every successful view-mutation response (`knack_create_view`, `knack_update_view`, `knack_update_view_order`, `knack_copy_view`, `knack_move_view`, `knack_delete_view`) includes the equivalent, reminding you to run `knack_refresh_cache` (`warm: true, persistFiles: true`) before trusting cached-schema or cached-view tools to reflect the change. `knack_update_field` also adds a `mergeNote` when the update touches `format`/`relationship`, since whether Knack's PUT merges or fully replaces a partial nested object hasn't been independently verified — check `knack_get_field` afterwards if in doubt.

Token note:
The payload helper tools now return the payload only once, using the standard inline-detail size guard. Larger cloned payloads fall back to a structural summary instead of duplicating both `payload` and `payloadJson` in the response.

---

## View safety rules

Knack's view `PUT` **replaces rather than patches**, and cascade-deletes the child page behind any link the new definition no longer carries. A link re-sent unchanged is safe — measured, see [Verifying the premise](#verifying-the-premise-against-a-real-app). That holds for a link column and for a menu's `links` entry alike: the container makes no difference. These rules are enforced inside the tools, so they hold regardless of which tool a caller reaches for or what a caller remembers.

All six view tools (`knack_create_view`, `knack_update_view`, `knack_update_view_order`, `knack_copy_view`, `knack_move_view`, `knack_delete_view`) run through the same guard.

### One rule, applied to every view

There is no view-type gate and no unconditional block. Three rules used to stand in for one: a `menu` could never be updated or moved, any payload carrying a `links` array was refused, and a view whose type could not be read was refused on the grounds that it might be a menu. All three said the same thing — a menu's navigation is too dangerous to touch — and all three are replaced by asking the question that actually decides it, for any view:

> **Which pages lose their last link if this goes ahead?**

A menu is now **promptable rather than impossible**. A client that cannot prompt still cannot change one, which is exactly how the old block behaved.

A menu asks for exactly what a table does. That was not always so — while the `links` container was untested, a view holding one got no narrowing at all and every page it reached was treated as at risk. A live seven-link menu settled it: one entry omitted, six re-sent. Knack deleted the omitted link's page and its two descendants, and kept the other six — three of them owned and singly referenced, so their survival was not a second referrer doing the work.

So there is no per-container rule left. A link is a link, wherever it is stored.

### Rules that fail closed

| Rule                                                                                                                                 | Error code                                                      |
| ------------------------------------------------------------------------------------------------------------------------------------ | --------------------------------------------------------------- |
| The view is read before every mutation. An unreadable view refuses the mutation — it is indistinguishable from a view with no links. | `COULD_NOT_VERIFY_VIEW`                                         |
| An `updates` payload that is not valid JSON, or that parses as anything but an object, is refused rather than forwarded unchecked.   | `INVALID_UPDATES_JSON`                                          |
| A payload that writes no properties at all — nothing for any rule to evaluate, and nothing useful to send.                           | `EMPTY_UPDATE_PAYLOAD`                                          |
| Either the payload or the live view nests deeper than the walks will follow, so links could be hiding past the cap.                  | `STRUCTURE_TOO_DEEP`                                            |
| The page tree cannot be read, so the set of pages at stake cannot be worked out. An unreadable tree is not an empty one.             | `SCENE_TREE_UNAVAILABLE`                                        |
| A restore point must be on disk before anything reaches Knack.                                                                       | `SNAPSHOT_FAILED`                                               |
| The removed `confirmDestructive` flag is refused, so callers written against the old signature fail closed.                          | `CONFIRMATION_UPGRADE_REQUIRED`                                 |
| A human declined the prompt, or the client could not raise one.                                                                      | `HUMAN_CONFIRMATION_DECLINED`, `HUMAN_CONFIRMATION_UNAVAILABLE` |

Nine codes, and each names something the guard could not establish rather than a policy it is applying. That is the whole list — there is no code for "this view type is not allowed", because no view type is.

**The body Knack receives is the body the guard judged.** The guard reads the live definition, merges the caller's patch into it, decides on the merged object, and hands that same object to the transport. Nothing is rebuilt at the call site, so the two cannot disagree about what a request does — and a payload that cannot be merged is refused rather than forwarded, which is why `INVALID_UPDATES_JSON` covers more than a parse failure.

The preflight walks `columns[]`, `groups[].columns[]` and `links[]` recursively. A link nested inside a group is found — a single-level read of `columns` misses it, and a link the walk cannot see is a link the guard cannot tell is being dropped.

### Confirming page deletion

When a mutation would leave a child page with no link reaching it — an update whose merged body drops one, or a delete or move that takes every link with it — the guard works out the exact pages destroyed, including descendants, since a doomed child page may own children of its own. It then **asks the human operating the MCP client** to confirm, via MCP elicitation.

That prompt is rendered by the client and answered by a person. The calling model never sees it and cannot answer it, and there is no second route: **if no human can be asked, the mutation is refused.**

An earlier version offered a fallback where the caller typed back a sentence naming the doomed pages. It was removed. The refusal handed over the exact string needed to satisfy it, so an agent could read it and retry in the same turn without surfacing anything to a person — it proved the preflight had been read, not that anyone agreed. A consent mechanism the caller can satisfy alone is not consent.

- Confirmed → the mutation proceeds.
- Declined or cancelled → `HUMAN_CONFIRMATION_DECLINED`, nothing sent to Knack.
- The elicitation request fails, times out, or the client never advertised the capability → treated as **unavailable**, never as consent.

Two degenerate shapes also fail closed rather than being read as "nothing at risk":

- A link column whose target scene **cannot be resolved** still counts as risk. An unreadable reference is not evidence that no child page exists, so the prompt warns that more pages than listed may be destroyed. A `url` link is excluded — it points outside the app and has no child scene by definition.

#### Which links actually destroy a page

Not every severed link kills something. **Knack deletes a child page when its _last_ referring link goes — not when _a_ link to it goes**, which is the same thing the builder does. Each link the mutation would cut is sorted into one of four classes, and only two of them count as damage:

| Class         | When                                                                                               | What happens                                                             |
| ------------- | -------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------ |
| `owned`       | The page hangs off the page being changed, and no other view in the app links to it.               | **Destroyed**, with its descendants. Goes in the confirmation prompt.    |
| `transferred` | The page hangs off the page being changed, but another view links to it too.                       | Survives and **re-parents** onto the view that still links to it.        |
| `external`    | The page's `parent` resolves to a different, real page.                                            | Stays where it is; only this route in is removed.                        |
| `unknown`     | The page declares no parent, its parent resolves to nothing, or the reference resolves to nothing. | **Treated as destroyed.** Absence of evidence is not evidence of safety. |

Working `transferred` out needs the whole app's link graph, not the mutating view's — nothing in one view's definition says whether another view still points at the same page — so the guard builds a referrer index over every view in the app from the same runtime payload the preflight reads.

Two conditions have to hold before a page is spared on that index, and both fail closed:

- The scene list must actually carry per-view links. Missing, it is treated as _not measured_ rather than _nothing links here_, and every page stays at risk.
- The index must contain **the link being cut**. An index built from partial metadata otherwise reads as "no other referrers", which is indistinguishable from a genuine sole referrer.

A page whose parent is unknown is never spared on referrer count. A link graph read from metadata that lost a parent pointer is no sounder than the pointer it lost.

Transfers are reported, not hidden. They appear in the confirmation prompt alongside the doomed pages, and in the tool result as `pagesMovedToAnotherLink`, each naming the view the page is now reached from. Severed links to `external` pages appear as `linksRemovedPagesKept`. Not-deleted is not the same as unchanged: the page changes parent, and someone will go looking for it.

#### Checking which mode you are in

Elicitation is an optional capability, so whether you get a prompt or a refusal depends on the client you are connected with. `knack_list_apps` reports it, so you can check before relying on either path rather than finding out on a real change.

It is reported twice, in two forms. The response carries a plain-text banner as a second text block beside the JSON, because a client-dependent rule buried in a serialised payload is a rule nobody reads. The JSON stays at `content[0]`, so anything parsing that keeps working:

```
Knack apps: 14 discovered in /home/you/ARC-KNACK-CODE/KnackApps. Active app: none.
Writable: Content Operations, GAP-Track, Noah's Place, Spot. View mutation allowed: GAP-Track, Noah's Place.
Cascade deletes: a human is prompted. Client "claude-code 2.1.250" advertised MCP elicitation. A mutation that would delete child pages is put to the user for confirmation. The calling model cannot answer it.
```

The same facts follow as structured fields, so a caller can branch on them:

```json
{
    "humanConfirmation": {
        "available": true,
        "client": "claude-code 2.1.250",
        "message": "This client can prompt a human, so a mutation that would delete child pages is put to the user directly. The calling model cannot answer that prompt."
    },
    "cascadeDeleteBehaviour": {
        "mode": "prompts-human",
        "summary": "A mutation that would delete child pages is put to the user for confirmation. The calling model cannot answer it."
    },
    "apps": [{ "appKey": "MyApp" }]
}
```

Both are reported once per response rather than per app — `humanConfirmation` and `cascadeDeleteBehaviour` sit at the top level, not inside `apps[]`.

### Which build am I talking to?

Three different causes present identically — a missing key in the response — and none can be told apart from the payload alone:

- the branch carrying a feature was never merged, so the code is not there;
- the checkout is right but `dist/` was never rebuilt — the case `sourceNewerThanBuild` exists to catch, because `git.commit` looks current while the code is not;
- both are right, but the client is still talking to a server process that started **before** the `git checkout`.

So the server states its own identity. `knack_list_apps` reports a `serverBuild` object, the banner ends with a one-line form of it, and the same line goes to **stderr at startup** — unconditionally, not behind `DEBUG`, because a stale server is exactly the case where nobody has thought to turn debugging on. Most clients surface stderr in a server log pane:

```
[knack-mcp] Build: knack-mcp 1.0.0, full mode, TypeScript source, main @ c999805, started 2026-08-31T10:13:43.524Z. Loaded from /home/you/ac-dev/knack-mcp/src.
```

On a stale build the same line carries the warning, since this is what reaches stderr and leads the app listing:

```
… Loaded from /home/you/ac-dev/knack-mcp/dist. WARNING: the checkout has changed since this build was compiled, so c999805 describes the source tree and not the code running. Rebuild before trusting it.
```

```json
{
    "serverBuild": {
        "name": "knack-mcp",
        "version": "1.0.0",
        "mode": "full",
        "runtime": "typescript",
        "entryPath": "/home/you/ac-dev/knack-mcp/src/server.ts",
        "moduleDir": "/home/you/ac-dev/knack-mcp/src",
        "git": { "branch": "main", "commit": "c999805" },
        "sourceNewerThanBuild": false,
        "startedAt": "2026-08-31T10:13:43.524Z",
        "features": [
            "cascade-delete-guard",
            "human-confirmation",
            "list-apps-banner",
            "mutation-snapshots",
            "server-build-identity"
        ]
    }
}
```

| Field                       | Answers                                                                                                                                                                                                                                                                                                                                                                                                     |
| --------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `runtime`                   | `typescript` when run under `tsx` from `src/`, `compiled` when run from `dist/`. A `compiled` runtime is the one that needs `npm run build` after a pull.                                                                                                                                                                                                                                                   |
| `moduleDir` / `entryPath`   | **Which clone** this is. If it is not the directory you edited, the client is configured against a different checkout — the usual cause of a fix that "did not take".                                                                                                                                                                                                                                       |
| `git.branch` / `git.commit` | Which commit the **checkout** is on. Read from `.git` directly, never by shelling out, so it cannot hang startup; `null` on a non-git checkout. On a `compiled` runtime this is not necessarily the code running — see the row below.                                                                                                                                                                       |
| `sourceNewerThanBuild`      | Whether `git.commit` describes code that is **not running**. `.git` is read at call time, so a `dist/` built from an older commit still reports the checkout's — this compares the running module's timestamp against `.git/HEAD` and the branch ref. `true` means rebuild before trusting the commit; `false` on a `typescript` runtime, which has no build to fall behind; `null` when it cannot be told. |
| `startedAt`                 | When the process started. **Earlier than your `git checkout` means the server has not been restarted** — the source is only read at startup, so a checkout alone changes nothing a running server does.                                                                                                                                                                                                     |
| `mode`                      | `readonly` for `server-readonly.js`, `full` otherwise.                                                                                                                                                                                                                                                                                                                                                      |
| `features`                  | Whether this build has a given feature, without needing to know commit hashes.                                                                                                                                                                                                                                                                                                                              |

The startup line is printed **before** anything that can fail, so it appears even when the server does not start at all — a missing `KNACK_APPS_DIR`, an unreadable `KnackApps` folder. A server that fails to start never reaches a tool call, which is precisely when knowing which code is failing matters most:

```
[knack-mcp] Build: knack-mcp 1.0.0, full mode, compiled JavaScript, main @ 35beaf7, started ...
Error: Missing env var KNACK_APPS_DIR (absolute path to your KnackApps folder).
```

A missing `serverBuild` is itself the answer: the build predates this field.

The banner's second line reflects what this server will actually accept: started via `server-readonly.js`, it reads `Writes: none. This server was started in enforced read-only mode, so every app is read-only whatever app.json says.` — the per-app `readonly` flags in `apps[]` still echo `app.json` verbatim and do not account for that mode.

> **If these fields are missing from the response**, the client is running older code — see [Which build am I talking to?](#which-build-am-i-talking-to) below. Absent is not the same as `false`: `available` is a real boolean and is always present when the code is.

`cascadeDeleteBehaviour.mode` is the answer to "what would actually happen". It depends only on the connected client — there is no per-app setting, and nothing in `app.json` can change it:

| Mode            | Meaning                                                         |
| --------------- | --------------------------------------------------------------- |
| `prompts-human` | The client can ask; a person confirms each cascade delete.      |
| `refuses`       | The client cannot ask, so cascade deletes are refused outright. |

The same object is written to stderr under `DEBUG=1` as `human_confirmation_status`.

**When the client cannot prompt**, the guard refuses with `HUMAN_CONFIRMATION_UNAVAILABLE` and points you at the Knack builder. There is no override, no per-app opt-out, and no parameter the caller can send to proceed anyway. If you need to restructure a view carrying link columns from such a client, do it in the builder — and take a `knack_snapshot_app` restore point first.

### What is refused, and what is not

There is no configurable policy. The rules are fixed, and the ones with no override are listed in [Rules with no override](#rules-with-no-override) above. Two consequences are worth spelling out:

- **`columns` is writable, and that is fine.** What protects a child page is the confirmation step, and what triggers it is **losing the link**, not the shape of the payload. The guard merges the caller's patch into the view's live definition, and puts to a human only those pages whose link the merged body no longer carries. A payload that re-sends a link — including a scalar edit, which re-sends everything — removes nothing and proceeds without a prompt.

⚠️ **On a client that cannot prompt, only link removals are refused.** This used to be far broader: the trigger was "is this payload structural?", which caught filters, `source`, `rows_per_page`, sorting and layout on any view with link targets — most tables in a mature app (352 of 676 in one production app measured during review). On a client that cannot prompt, that was a hard refusal for nearly every meaningful edit to those views. Now that a re-sent link is measured safe, those edits proceed untouched, and what remains refused is the narrow case that genuinely destroys something: a payload whose merged body drops a link to a page nothing else reaches. Check `cascadeDeleteBehaviour` if you are planning work that removes links.

**What counts as a link.** A node points at a child page when it carries a `scene` property, whatever its declared type. Knack is not consistent here: table and search columns use `type: "link"`, details and calendar columns use `type: "scene_link"`, menu entries use `type: "scene"`. Matching the type string missed details views entirely. Conversely a form's Link/URL field input is also `type: "link"` but carries a `field` and no `scene` — it points at no page, and is ignored.

**References are slugs, not keys.** A link's `scene` and a scene's `parent` both hold slugs (`roll-details3`); `key` holds `scene_N`. Both are resolved through a slug index before the descendant walk, and a reference matching neither a slug nor a key is reported as unresolved risk rather than dropped.

- **A `links` array is refused on updates, not on creates.** The hazard is replacement: Knack rebuilds navigation from what it receives, so `links: []` on an existing view clears every link and takes their child pages with it. A create replaces nothing, and the payloads `knack_get_view_payload_template` produces all carry `links: []` — so creating a view works normally. When updating, send only the properties you are changing rather than round-tripping a whole view.

### Snapshots

Every update, move, and delete writes a timestamped restore point first, and refuses to proceed if it cannot:

```
KnackApps/<AppKey>/schema/snapshots/2026-08-28T14-22-05Z-update_view-view_230.json
```

Each snapshot holds the full scene tree (routes, slugs and **parent pages**) and the target view's complete definition (columns, filters, links, source). Updates, moves, and deletes write one first; creates, copies, and view ordering do not remove the source view or child pages and are not blocked on snapshot storage. The scene tree is always re-fetched rather than served from cache, so it describes the app immediately before the mutation — and it is fetched **once** per mutation, shared between the confirmation prompt and the snapshot, since on a large app that payload is several megabytes.

The object schema is **not** embedded, only referenced by `schemaPath`. Rebuilding a cascade-deleted page needs the scene tree and the view definitions; the object/field list is context, and it does not change when a page is deleted — copying it into every snapshot added hundreds of KB per file, to files nothing prunes.

This is not the same thing as `knack_refresh_cache`, which overwrites `schema.json`/`viewMap.json` in place and never persists scenes at all.

⚠️ **Snapshots are never pruned.** Every update, move, and delete writes the whole scene tree. On a large app that is a few hundred KB each, indefinitely — clear out `schema/snapshots/` periodically.

`knack_snapshot_app` exposes the same writer directly. Run it before Knack **builder** changes too — the server never sees builder-side edits, and a snapshot is the only record that can rebuild a cascade-deleted page tree.

### Running the tests

```bash
npm test
```

Unit tests cover the guard logic against fixture payloads, and the tool-level tests drive the same code path the six view tools use with a spy standing in for the Knack transport. The assertion throughout is that a refusal issues **zero** `PUT`, `POST` or `DELETE` requests.

This proves no destructive request is _issued_. It does not prove Knack's server-side behaviour.

### Verifying the premise against a real app

The guard was built on one claim: that replacing `columns` cascade-deletes the child pages behind a view's link columns, **even when the link column is re-sent unchanged**.

**That claim is false.** It was measured on 1 September against a purpose-built fixture — one table, five child pages, four of them referenced by no other view in the app — in three runs, each a complete definition differing only in which link columns it carried:

| Run                                | Links dropped      | Guard predicted | Knack deleted                            |
| ---------------------------------- | ------------------ | --------------- | ---------------------------------------- |
| Every link re-sent byte-for-byte   | none               | 4               | **0**                                    |
| One link column omitted            | `book-assessment2` | 4               | **1** — exactly that page                |
| A two-referrer page's link omitted | `client-details2`  | 3               | **0** — the page moved to the other view |

The second run took the view from 16 columns to 15, which is what rules out the alternative reading that Knack merged or ignored the array. So:

> **Knack deletes a child page when the definition it receives no longer carries a link to it, and only then.** Re-sending a link column is not destructive.

Every earlier cascade — including the two on a production app that were taken as confirmation — was a page whose link had genuinely stopped being sent. None of them distinguished the two explanations, which is why this went unmeasured for so long.

A companion result, and one gap:

- **A menu's `links` array** was settled separately, on a seven-link live menu. One entry omitted, six re-sent: Knack deleted the omitted link's page and its two descendants and kept the rest, including three that were owned and singly referenced. Same rule, different array — so menus now behave like every other view.
- **A partial body.** The server never sends one now — it merges into the live definition first — but a hand-built partial `PUT` against this route still replaces whatever it omits.

`scripts/verify-cascade-premise.ts` remains useful for re-checking the behaviour on a Knack plan or region you have not tested. It records the app's scene keys, re-sends the view's `columns` array byte-for-byte, then diffs the scene list and reports which pages disappeared.

```bash
# Safe: checks the fixture is suitable, sends no PUT.
KNACK_APP_ID=... KNACK_API_KEY=... \
  npx tsx scripts/verify-cascade-premise.ts --scene scene_1 --view view_2 --dry-run

# Destroys pages if the premise holds. Disposable apps only.
KNACK_APP_ID=... KNACK_API_KEY=... \
  npx tsx scripts/verify-cascade-premise.ts --scene scene_1 --view view_2 --confirm-destructive
```

It refuses to send the `PUT` without `--confirm-destructive`, and needs a view with at least one link column pointing at a child page.

After any real cascade, compare what Knack reports against what the guard predicted. The tool result carries both: `pagesExpectedToBeDeleted` is the guard's list, and `pagesKnackReportsDeleted` is read from `changes.deletes.scenes` in Knack's own response. **A difference between those two is a bug in the guard** — the second is the only account of the damage that does not come from this server's own reasoning.

### Is the public payload the whole view?

Every rebuilt body is assembled from `applications/{appId}`, so the merge presumes that payload holds the complete view. A property the builder kept and the payload omitted would be silently reset on every edit — and reading the view back afterwards could never show it, because the read comes from the same payload that sourced the write.

Settling it needs a different observer. The Knack builder is a web app, and **its own save request carries the definition as Knack's client believes it**: open a view in the builder with devtools on the Network tab, change the title, save, and copy the request body from the `PUT` to `.../views/view_NNN`. Diffing that against `knack_snapshot_app` for the same view enumerates the gap instead of sampling for damage.

> ⚠️ Copy the **request body only**. The headers carry a live builder session cookie.

Done on two tables configured differently — one carrying `options` and `reportType`, the other `allow_limit` and a populated `table_design`. The two agreed on every key but one. Filters, sorts, totals, per-column rules, link designs, action rules with their record and submit rules, and the table design block all appear in the payload with the values the builder sends.

The exception is `design`, which the builder sends and the payload omits. It was `{}` on both views, **including the one with table design fully switched on** — the populated settings live in `table_design`, which the payload does carry. So the one key at risk holds nothing on either side of that toggle.

Two limits. The key set varies per view — Knack omits what does not apply — so this is a per-view check rather than a fact about tables in general. And only tables were checked; details, form and calendar views are unverified. The method is cheap enough to repeat: one builder save and one snapshot.

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

Lists all Knack apps discovered from your `KnackApps` folder. Re-scans the directory on every call, so newly added apps appear immediately.

```
List my Knack apps
```

The response carries a plain-text banner alongside the JSON, summarising the app count, which apps accept writes, and whether this client can confirm a cascade delete or will be refused — see [Checking which mode you are in](#checking-which-mode-you-are-in).

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

**Where the description actually persists:** Knack's fields API doesn't reliably persist a bare top-level `description` on create/update — confirmed in production use, where it silently failed to stick and had to be resent under `meta.description` to actually take effect. Both tools now write to `meta.description` automatically (alongside the top-level key, which is harmless if unused) whenever a description is set — whether via the dedicated `description` parameter or a raw `"description"` key inside `knack_update_field`'s `updates` JSON. Nothing extra to do on your end; this applies transparently. `knack_duplicate_field` does the same normalization on the cloned payload before creating the copy, so duplicating a field whose description only ever landed at the top level (e.g. it predates this fix) doesn't reintroduce the same silently-lost-description problem.

**KTL keyword protection:** Knack replaces a field's description outright rather than merging it, so `knack_update_field` guards against accidentally wiping out KTL keyword tokens (underscore-prefixed, e.g. `_hideField`) that are already embedded in the current description. Before applying a description change, the tool fetches the field's current definition and checks whether every existing keyword token is still present in the new text. If one would be dropped, the update is blocked with an error listing the missing keyword(s) — pass `confirmRemoveKtlKeywords: true` only after explicitly confirming the removal with the user. If the current definition can't be fetched (e.g. no API access), the check is skipped and a `ktlKeywordWarnings` note is included in the response instead of blocking the write. The guard also fires when the description is set via a raw `meta.description` key inside `updates` (not just the dedicated `description` parameter), keyword extraction recognises a keyword wrapped in punctuation (e.g. `(_hideField)`), and "is it still present" is a whole-token match rather than a plain substring check — so an unrelated word that merely contains the same characters (e.g. `_hideFieldWasRemoved`) doesn't count as the keyword being kept.

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

Create, update, or delete up to 100 records in a Knack object in one call. Requests run with up to `KNACK_MCP_BATCH_CONCURRENCY` (default 5, max 10) in flight at once. Update and delete requests retry a `429` (rate limited) or `5xx` response with exponential backoff before being reported as failed; create requests only retry on `429` — Knack has no client-supplied idempotency key, so retrying a create on a lost/delayed `5xx` risks silently producing a duplicate record if the original write actually succeeded server-side. For delete specifically, a `404` immediately after a `5xx`-triggered retry is reported as a success rather than a failure, since it almost certainly means the first attempt's delete was applied and only its response was lost. Each item's own result is reported individually in `results`, alongside `successCount`/`failureCount` — one failing record does not abort the rest of the batch, so check each entry rather than assuming an all-or-nothing outcome.

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

Analyses the app's data model and returns structured design feedback including field-count distribution, connection density, isolated objects, objects with unusually high or low field counts, field type spread across the whole app, and a plain-English observations list. An object counts as isolated only when it has no connections at all in either direction — a pure connection _target_ (e.g. a core "Users" object other objects point at but that has no outgoing connections of its own) is correctly excluded, since it's already counted as connected.

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
