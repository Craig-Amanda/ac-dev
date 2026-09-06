# knack-mcp-v2 — full test plan

This is the acceptance plan for the rewrite before it replaces `knack-mcp` for anyone
else. Run it against a **disposable test app only**; several sections write to Knack or
destroy pages. Never point any of this at a production app.

## How to use this file

- Work top to bottom. Tiers 1–2 need no human at the keyboard and no destructive
  writes; Tiers 3–5 do.
- Every case gets an ID (`T1`, `P1`, …). Log the result in the [results log](#results-log)
  at the end of a run — pass, fail, or blocked — with the exact call and response for
  any failure.
- **You cannot report whether a confirmation prompt appeared.** MCP elicitation goes to
  the client, not back to the model. A human has to say whether they saw the prompt;
  an AI reporting "no prompt was raised" from the tool response alone is exactly the
  mistake that invalidated three runs of the legacy plan (see `knack-mcp/TESTING.md`).
- `lib/view-safety.ts` is byte-for-byte the legacy guard, and its whole evidence base —
  the cascade rule, every C/A/V/P case, the recovery drill — carries over unchanged.
  **Do not re-run `knack-mcp/TESTING.md` here.** Tier 4 below covers only what changed
  in the rewrite: gating order, response shapes, and the tools that now share code.

## What's already proven, and what isn't

| Proven by the automated suite (628 tests, `npm test -w knack-mcp-v2`)                                                  | Not touched by it — needs this plan                                                                                  |
| ---------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------- |
| Every handler's logic against a fake `KnackContext` — gating, error shaping, pagination arithmetic, CSV escaping, etc. | Real Knack API responses: field types, error bodies, rate limiting, pagination behaviour the fake context can't fake |
| That a regression test fails against the pre-fix code (done for every bug fixed in this rewrite)                       | Whether the fix still behaves correctly against the real API's actual response shapes                                |
| Catalogue byte/token counts against a stub app (`npm run catalogue`)                                                   | Whether a real MCP client's context usage matches the measured numbers                                               |
| Typecheck, lint, format                                                                                                | MCP elicitation UX, cascade-delete prompts, recovery from a snapshot                                                 |

## 1. Setup

### 1.1 Test app

Reuse the legacy plan's disposable app if it still exists (`knack-mcp/TESTING.md` §1
lists the shapes it needs); otherwise build one with:

- At least two objects connected by a field with `has: "many"` on one side and
  `has: "one"` on the other (needed for T7 below).
- At least one object with 1,200+ records, or a way to generate them quickly (needed
  for T5 — the aggregate-paging fix only shows a difference past 1,000 scanned rows).
- A field whose value can start with `=`, `+`, `-` or `@` (a short-text field is enough)
  for T6.
- Two aliases that differ only by case for the same field key, in whatever mapping
  `knack_validate_field_mapping` / `knack_resolve` reads (needed for T8).
- `app.json` with `allowViewMutation: true`, `allowDelete: true`, and a `dataAccess`
  block naming a subset of objects/fields plus at least one `redactedFieldKeys` entry
  that also appears in `allowedFieldKeys` (needed for Tier 3 — this exact overlap was a
  bug this rewrite fixed).

### 1.2 Run both servers side by side

Point one MCP client profile at `knack-mcp/dist/server.js` and another at
`knack-mcp-v2/dist/index.js`, both against the **same** `KnackApps` folder and secrets
file, so Tier 2's differential checks compare like against like. Build both first:

```bash
npm run build -w knack-mcp
npm run build -w knack-mcp-v2
```

### 1.3 Two client profiles

Repeat the destructive tiers (3, 4) once with an MCP client that advertises elicitation
and once with one that doesn't (mirrors legacy plan's M1/M2). The no-elicitation profile
should see every cascade-risking call refused with `HUMAN_CONFIRMATION_UNAVAILABLE`,
never silently allowed.

## Tier 1 — Differential smoke test (AI, read-only, no human needed)

The fastest way to catch a rewrite regression that no unit test anticipated: run the
same read-only call against both servers pointed at the same app and diff the results.
Expected differences are listed; anything else is a finding.

For each tool below, call it with the same arguments against both servers and compare:

| Legacy tool                                                                                             | v2 tool                                         | Expected difference                                                         |
| ------------------------------------------------------------------------------------------------------- | ----------------------------------------------- | --------------------------------------------------------------------------- |
| `knack_list_apps`                                                                                       | `knack_list_apps`                               | v2 adds `serverBuild`                                                       |
| `knack_get_object`                                                                                      | `knack_get_object` (`detail: "summary"`)        | none                                                                        |
| `knack_get_object_fields`                                                                               | `knack_get_object` (default `detail: "fields"`) | none                                                                        |
| `knack_list_field_types`                                                                                | `knack_get_object` (`detail: "types"`)          | none                                                                        |
| `knack_get_raw_object`                                                                                  | `knack_get_object` (`detail: "raw"`)            | none (both need `allowDiagnostics`)                                         |
| `knack_resolve_field_alias` / `knack_get_field_type`                                                    | `knack_resolve`                                 | v2 resolves case-insensitively when no exact match exists — see T8          |
| `knack_find_records`                                                                                    | `knack_find_records`                            | none                                                                        |
| `knack_get_object_records_with_schema`                                                                  | `knack_find_records` (`includeSchema: true`)    | fields nested beside records, not under `recordsResponse`                   |
| `knack_get_related_records`                                                                             | `knack_get_related_records`                     | none in content; v2 fetches the forward direction concurrently — same order |
| `knack_aggregate_records`                                                                               | `knack_aggregate_records`                       | identical for object sizes ≤1,000 records; see T5 past that                 |
| `knack_list_scenes`, `knack_list_views`                                                                 | unchanged                                       | none                                                                        |
| `knack_get_view_context`                                                                                | `knack_get_view` (default `detail: "context"`)  | none                                                                        |
| `knack_get_view_attributes`                                                                             | `knack_get_view` (`detail: "attributes"`)       | param renamed `includeRawAttributes` → `includeRaw`                         |
| `knack_get_context_bundle`, `knack_get_app_overview`, `knack_analyze_data_model`, `knack_app_deep_dive` | unchanged                                       | none                                                                        |
| `knack_list_field_references`                                                                           | `knack_list_field_references`                   | none                                                                        |
| `knack_search_ktl_keywords`, `knack_search_emails`                                                      | unchanged                                       | none                                                                        |

**T1** — Run every row above against the real app. Any unexplained field-level
difference (missing key, different value, different ordering where order should be
stable) is a finding — file it with both raw responses.

**T2** — Repeat `knack_create_records` / `knack_update_records` / `knack_delete_records`
against a **single** record each (one call, `records: [...]` with one element) and
confirm the payload actually reaches Knack the same way the legacy single-record tools
did — only the response shape should differ (batch `results` array instead of a bare
`status`/`body` pair).

**T3** — Trigger one error deliberately (e.g. an unknown `recordId`) on both servers and
confirm v2 returns `{ ok: false, tool, error }` with `isError: true`, and the `error`
text matches legacy's message text (error _shape_ changed; error _wording_ should not
have).

## Tier 2 — New behaviour verification (AI-drivable, live, no destructive writes)

These check the bug fixes and efficiency changes made during the rewrite that have no
equivalent in legacy to diff against. Each row names the fix, a concrete way to trigger
it, and what "correct" looks like.

| ID  | What's being checked                                                                 | How to trigger it                                                                                                                                                                                                                                                            | Expected                                                                                                                                                                                                     |
| --- | ------------------------------------------------------------------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| T4  | Connection cardinality (`relationship.has`)                                          | `knack_get_object_connections` on the object from setup 1.1, on both ends of the many/one field                                                                                                                                                                              | The `many` side reports plural/multiple; the `one` side does not — check against what the Knack builder shows for that field                                                                                 |
| T5  | Aggregate paging past 1,000 records                                                  | `knack_aggregate_records` with `maxRecords` > 1,000 against the 1,200+-record object; also with `app.dataAccess.maxRecordsPerQuery` raised if the default clamps it                                                                                                          | `scanned` reaches the requested `maxRecords` (or the true record count, if lower) rather than stopping at 1,000; counts/sums match a manual total for a small filtered slice                                 |
| T6  | CSV formula-injection escaping                                                       | `knack_generate_seed_csvs` against the object with a `=`/`+`/`-`/`@`-leading value                                                                                                                                                                                           | The cell in the generated CSV is prefixed with a leading `'` (or otherwise neutralised) rather than left as a live formula — open the CSV in a spreadsheet app and confirm it renders as text, not a formula |
| T7  | Case-insensitive-then-exact field/alias lookup                                       | `knack_resolve` and `knack_validate_field_mapping` with the two case-variant aliases from setup 1.1                                                                                                                                                                          | An exact-case match wins when both exist; a case-insensitive match resolves only when no exact match does — try both orderings                                                                               |
| T8  | Details/list view field extraction (nested `columns[].groups[].columns[][]`)         | `knack_get_view` (`detail: "fields"`) on a **details** or **list** view                                                                                                                                                                                                      | Every field shown in the Knack builder for that view appears in the response — this shape was previously under-extracted                                                                                     |
| T9  | In-flight runtime-metadata memoization                                               | `knack_cache` with `refresh: true` (no `warm`) to clear caches, then fire two read tools that both need a cold-cache app (e.g. two different `knack_get_object` calls) back to back without waiting; with `DEBUG=1` set on the server process, check the server's stderr log | Only one `runtime_metadata_attempt` log line for the app, not two, even though two tool calls needed it                                                                                                      |
| T10 | Parallel related-record / seed-lookup fetches produce the same content as before     | `knack_get_related_records` on a record with 10+ related records in both directions; `knack_generate_seed_csvs` against an object with 3+ external connection targets                                                                                                        | Same records/values as a manual walk, same order as returned by a single serial fetch (order-preserving) — no duplicates, nothing dropped                                                                    |
| T11 | `knack_verify_record_field_shapes` / `knack_generate_seed_csvs` respect `dataAccess` | Call each against an object **outside** `allowedObjectKeys`, then against fields inside `redactedFieldKeys` that also sit in `allowedFieldKeys` (the overlap fixture from setup 1.1)                                                                                         | Object outside policy is refused; overlapping fields are silently excluded rather than making the whole read fail (this is the exact bug fixed pre-merge)                                                    |
| T12 | `knack_snapshot_app` / `knack_get_view_payload_template` classified `read`           | Call both under `--readonly` / `KNACK_MCP_READONLY=1`                                                                                                                                                                                                                        | Both succeed even in enforced read-only mode (neither sends anything to Knack)                                                                                                                               |

## Tier 3 — Permission model matrix (AI-drivable, mostly non-destructive)

Cross-check every access level against every relevant `app.json` flag. A cell is a
single tool call; expected result is either "allowed" or the specific refusal string.

| Access level  | Flag it needs                                   | Also gated by `dataAccess`? | Refusal when the flag is off            |
| ------------- | ----------------------------------------------- | --------------------------- | --------------------------------------- |
| `read`        | none                                            | yes (record tools only)     | n/a                                     |
| `write`       | `readonly: false`                               | yes                         | matches legacy's read-only refusal text |
| `delete`      | `readonly: false` + `allowDelete: true`         | yes                         | matches legacy                          |
| `view`        | `allowViewMutation: true`                       | no                          | matches legacy                          |
| `view-delete` | `allowViewMutation: true` + `allowDelete: true` | no                          | matches legacy                          |
| `diagnostic`  | `allowDiagnostics: true`                        | no                          | matches legacy                          |

**T13** — For each row, flip the flag off in `app.json`, call a representative tool at
that access level, confirm the refusal, flip it back on, confirm success. Do this for
at least one tool per level (`knack_create_records` for `write`, `knack_delete_records`
for `delete`, `knack_create_view` for `view`, `knack_delete_view` for `view-delete`,
`knack_get_object` with `detail: "raw"` for `diagnostic`).

**T14** — With `KNACK_MCP_READONLY=1` (or `--readonly`) set, confirm **every** app
reports `readonly: true` in `knack_list_apps` regardless of its own `app.json`, and that
a write tool refuses even for an app whose `app.json` says `readonly: false`.

**T15** — Record-read policy: with `dataAccess.allowedObjectKeys` restricting to one
object, confirm `knack_find_records` against a different object refuses, and that
returned records never include a `redactedFieldKeys` field even when explicitly
requested by key.

## Tier 4 — View-mutation safety: only what changed in the rewrite

The guard itself (`src/lib/view-safety.ts`) is unchanged, so every finding recorded in
`knack-mcp/TESTED.md` and every open case in `knack-mcp/TESTING.md` still applies as-is
— run that plan's outstanding cases (P10, N1/N2, V3/V4/V5/V6/V7, etc.) against
**either** server; the guard's behaviour cannot differ between them. What's new here is
the code _around_ the guard.

| ID  | What's being checked                                                     | How to trigger it                                                                                                                                                                            | Expected                                                                                                                                                                                              |
| --- | ------------------------------------------------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| T16 | API key checked before any I/O in view mutations (fixed port regression) | Remove the app's key from the secrets file, then call each of `knack_create_view`, `knack_update_view_order`, `knack_update_view`, `knack_copy_view`, `knack_move_view`, `knack_delete_view` | Each refuses with the missing-API-key message **before** a snapshot is written or a human is prompted — check no new file appears under `schema/snapshots/`                                           |
| T17 | `knack_copy_view` merges both legacy copy tools correctly                | `knack_copy_view` with `sharePages: false` (Knack's own copy) and again with `sharePages: true` (create-from-source, shares child pages)                                                     | `sharePages: false` behaves exactly like legacy `knack_copy_view`; `sharePages: true` exactly like legacy `knack_copy_view_sharing_pages` — same page-sharing behaviour recorded in `TESTED.md` §9/P5 |
| T18 | `knack_get_view_payload_template` merged with the `_from_view` variant   | Call with a bare `viewType` (fresh template) and again with `viewType` + `fromViewKey` set (clone)                                                                                           | Fresh template matches legacy's plain template tool; clone matches legacy's `_from_view` tool, identifiers stripped                                                                                   |

## Tier 5 — Needs a human at the keyboard

Everything below either raises a confirmation prompt an AI cannot observe, or destroys
pages. An AI may prepare the call and describe what it expects; a human must be present
to answer any elicitation and to verify in the Knack builder.

- One full pass of Tier 3/4 with the **elicitation-capable** client, one with the
  **non-elicitation** client (setup 1.3) — confirm the non-elicitation client is refused
  on every cascade-risking call, never silently allowed through.
- Re-run the legacy plan's still-open human-required cases (`knack-mcp/TESTING.md` §4,
  §6, §8 — C6, C8, A3, A4 live, A5, and the recovery drill) against v2, since T16–T18
  above only checked the code path leading up to the guard, not the guard's own
  decisions.
- **Recovery drill** (repeat once per release, same as legacy §8): `knack_snapshot_app`,
  accept one cascade delete, rebuild from the snapshot alone. If anything needed isn't
  in the snapshot, that's a finding.

## Operational checks

- **T19** — `npm run catalogue -w knack-mcp-v2` against the real app in both modes;
  confirm the numbers are in the ballpark of `MIGRATION.md`'s stub-app measurement (they
  won't match exactly — a real app's tool descriptions are fixed, but per-app
  conditionally-advertised tools will vary with what that app's `app.json` opts into).
- **T20** — Override each env var in the table in `README.md` away from its default
  (`KNACK_CACHE_TTL_MS`, `KNACK_MCP_MAX_TOOL_TEXT_BYTES`, `KNACK_MCP_BATCH_CONCURRENCY`,
  etc.) one at a time and confirm the server actually honours it — e.g. a very low
  `KNACK_MCP_MAX_TOOL_TEXT_BYTES` should make a large response come back as the
  structured overflow summary, not a truncated JSON string.
- **T21** — Batch write concurrency: `knack_create_records` with 15+ records and
  `KNACK_MCP_BATCH_CONCURRENCY` set low (e.g. `2`) vs high (e.g. `10`); confirm all
  records are created exactly once either way (no duplicates from a retry racing a
  concurrent request) and that a 429 mid-batch is retried rather than failing the whole
  batch.

## Sign-off checklist

Before pointing anyone else at `knack-mcp-v2`:

- [ ] Tier 1 (differential smoke test) run clean, or every difference explained
- [ ] Tier 2 (new-behaviour verification) run clean
- [ ] Tier 3 (permission matrix) run clean in both client modes
- [ ] Tier 4 run clean; legacy's still-open guard cases re-run against v2 with no new
      divergence
- [ ] Tier 5 human-required cases run, including one recovery drill
- [ ] Operational checks run
- [ ] `npm run build && npm test && npm run lint && npm run format:check` green at the
      repo root

## Results log

One row per run. Settled findings move into `MIGRATION.md` or get fixed and re-tested;
this table keeps the chronology.

| Date | Commit tested | App | Client(s) | Tiers run | Pass / findings |
| ---- | ------------- | --- | --------- | --------- | --------------- |
|      |               |     |           |           |                 |
