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
- Any connection field, so a field key typed with the wrong case (`Field_12`) can be
  resolved (needed for T7). Aliases need no fixture: the generated field map is
  lower-case and alias lookup is exact on both servers.
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
| `knack_resolve_field_alias` / `knack_get_field_type`                                                    | `knack_resolve`                                 | v2 lower-cases a field key before matching (`Field_12` resolves) — see T7   |
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
| T7  | Field keys matched case-insensitively, aliases exactly                               | `knack_resolve` and `knack_validate_field_mapping` with a field key in the wrong case (`Field_12`) and an alias in the wrong case (`object_2.Name`)                                                                                                                          | The field key resolves to `field_12` (legacy reports it not found); the alias is not found on either server, because the generated field map is lower-case and alias lookup is exact                         |
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

**T14** — With `KNACK_MCP_READONLY=1` (or `--readonly`) set, confirm `knack_list_apps`
reports `serverBuild.mode: "readonly"` and that no write, delete, view or diagnostic tool
is advertised (32 tools), so a write attempt against an app whose `app.json` says
`readonly: false` fails as an unknown tool. The per-app `readonly` flag keeps reporting
what `app.json` says; it describes the app's own policy, not the server mode.

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

## Tier 6 — Where a transferred page lands, with more than one candidate

**Run twice on 7 September — see the run notes below.** Both candidate-order hypotheses
were eliminated and the destination is still not predictable; step 9 is what remains. The
procedure is kept here unchanged because the follow-up run uses it.

**Run this with the app in front of you.** It is one measurement, and a fair amount
rests on it: the cascade prompt currently lists every candidate referrer without saying
which wins, `knack_list_page_referrers` says the destination "has not been measured",
and any future "re-parent this child to that view" feature is blocked behind the answer.

**What is already known.** A transfer is measured live twice (`knack-mcp/TESTED.md`
§1): cutting one of two links re-parents the page rather than destroying it. Both times
there was exactly **one** referrer left, so the destination was never in doubt. Nothing
has ever been observed with two.

### Fixture

Build it with the MCP tools, not the builder, so the shapes are ours:

1. A page **P** with a table view **V1** carrying a link column that owns child page
   **C**. (`knack_create_view` with a `{name, parent, views}` page specification — the
   shape that creates a page on a create.)
2. Two further views **V2** and **V3**, on any pages, each with a link column pointing
   at **C**'s slug. Put them on **different** pages, and note which page each is on:
   if Knack's tiebreak turns out to be positional, page order is a candidate rule.
3. Confirm the fixture before touching anything: `knack_list_page_referrers` on **C**
   must report **3** referrers — V1, V2, V3 — and a `consequence` saying the
   destination is unmeasured. If it reports fewer, the fixture is wrong and the
   measurement is worthless.

### The measurement

4. `knack_snapshot_app` on P/V1 first.
5. `knack_update_view` on **V1**, re-sending its columns **without** the link to **C**.
6. **A prompt will fire.** Read it before answering — it should name C as transferred
   rather than doomed, and list both V2 and V3. Record its exact wording. Accept it.
7. `knack_cache refresh: true`, then `knack_list_scenes` and
   `knack_list_page_referrers` on **C**.

### What to write down

- **C's new `parentRef`** — this is the answer. Which of V2/V3's pages did it land on?
- Whether **C survived at all** (it must; if it was destroyed, that is a far bigger
  finding and the transfer rule is wrong).
- The prompt's exact text, so the wording can be checked against what happened.
- Anything that distinguishes V2 from V3 and might be the rule: page order in
  `knack_list_scenes`, view key order, creation order, position within the page.

### Then repeat it once, reversed

8. Rebuild the fixture with **V2 and V3 created in the opposite order** (or on pages in
   the opposite order) and run it again.

One run tells you where it went. **Two runs tell you whether that was the rule or the
coincidence** — and without the second, a "we know where it goes" claim is one
observation dressed up as a law, which is the mistake this plan exists to avoid.

### 9. The follow-up run

Two runs over the **same pair** of candidate pages cannot separate scene order from
lowest scene key. Before building anything, read the scene list in a `manual-app-*`
snapshot you already have and check whether the returned order is simply key order.

- **Not key order** → pick the pair whose key order and list order disagree, run the
  fixture across it once, and the two hypotheses separate in a single run.
- **Key order** → the two may be inseparable through this API. Record that and stop
  trying to predict the destination.

Use a **different** pair of candidate pages either way: re-running over `scene_61` and
`scene_62` adds an observation without adding information.

### What each outcome means

| Outcome                                                                              | What follows                                                                                                                                                                                                               |
| ------------------------------------------------------------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Deterministic, and the rule is legible (lowest view key, first page, creation order) | The prompt can name the destination outright, and `knack_list_page_referrers` can predict it. No new mutation feature needed                                                                                               |
| Deterministic but the rule is not obvious from two runs                              | Keep saying "unmeasured", and reach a chosen destination by sequencing instead: drop the links you do not want it under first, so exactly one referrer remains when the owning link goes — the case already measured twice |
| Not deterministic                                                                    | Sequencing is the only safe route, and the prompt should say so rather than listing candidates as though one were predictable                                                                                              |

**Do not** answer the confirmation prompt on the model's behalf, and stop if C is
destroyed rather than transferred — that would contradict `TESTED.md` §1 and needs
looking at before anything else is run.

### Run notes — 7 September, Tier 6

Two three-referrer transfers, run live with the operator answering both prompts. Both
children **survived**, so the transfer rule the guard depends on holds with more than one
candidate — that was the stop condition and it did not trigger.

| Run | Owner view / page      | Child      | Route created 1st       | Route created 2nd       | Landed on      |
| --- | ---------------------- | ---------- | ----------------------- | ----------------------- | -------------- |
| 1   | `view_60` / `scene_51` | `scene_78` | `view_61` on `scene_62` | `view_62` on `scene_61` | **`scene_61`** |
| 2   | `view_63` / `scene_60` | `scene_79` | `view_64` on `scene_61` | `view_65` on `scene_62` | **`scene_61`** |

**Two hypotheses are eliminated, and the reversal is what did it.**

- **Link creation order.** The winning route was created _second_ in run 1 and _first_ in
  run 2. Neither "first link wins" nor "last link wins" survives.
- **View key order.** The winning view was the _higher_ key in run 1 (`view_62` over
  `view_61`) and the _lower_ in run 2 (`view_64` over `view_65`). Neither "lowest view
  key" nor "highest" survives. This one was not called out in the run report; it falls
  out of the same reversal.

**What is still standing, and why two runs cannot separate it.** Both runs used the
**same pair of candidate pages**, `scene_61` and `scene_62`, in the same returned order.
So scene order, lowest scene key, and "`scene_61` in particular" are indistinguishable
here. Two observations agreeing is not a rule; it is two observations agreeing.

**The cheap next step, and it needs no new fixture to decide.** Read the scene list in
one of the `manual-app-*` snapshots already taken and ask one question: **is the returned
order simply key order?**

- **If it is not**, the app already contains a pair whose key order and list order
  disagree. Run the fixture once more across that pair and the two hypotheses separate in
  a single run.
- **If it is**, scene order and lowest scene key may be indistinguishable through this
  API at all — record that, stop trying to predict the destination, and treat sequencing
  as the answer.

**Not checked, and why.** The instructions asked for the fixture to be verified through
`knack_list_page_referrers`; the compiled server came from `318723a`, which predates that
tool. The substitute — reading the creation responses — was adequate, and the prompts
corroborated it independently by naming **both** remaining routes, which is only possible
with three referrers. **Rebuild the dist from this branch before the next run** so the
fixture check is one call.

Prompt visibility and exact wording remain human-observable only.

**Left on the app:** `view_60`, `view_61`, `view_62`, `scene_78`, `view_63`, `view_64`,
`view_65`, `scene_79`, and six snapshots across the two runs. Left standing deliberately
— a fixture still in place is one the next run can check against.

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

| Date  | Commit tested                                         | App                                                 | Client(s)                                                                                                                                                         | Tiers run                                                                                                                                                                        | Pass / findings                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| ----- | ----------------------------------------------------- | --------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 6 Sep | `f4a0c5b` on `main`; both `dist` builds made from it  | the disposable test app (same as legacy 5 Sep rows) | live: no elicitation (`local-agent-mode-knack`); differential and flag/env cases through a stdio harness that spawned `knack-mcp` and `knack-mcp-v2` side by side | Tier 1 (T1–T3); Tier 2 T4, T6–T9, T12 with T10/T11 partial and T5 not run; Tier 3 T13–T16 through scratch copies of the app folder; Tier 4 T16–T18; operational T19, T20 in part | **Tier 1 clean:** 33 of 45 rows byte-identical, the other 12 explained (tool merges, `serverBuild`, the T7/T8 fixes). **Findings, none blocking:** `KNACK_MCP_READONLY=1` withholds every write tool but does not force per-app `readonly: true` in `knack_list_apps` (T14, legacy identical); unknown-object wording changed from `schema.json` to `schema`; `returnedMatches` added to the record-rule listing; seed CSV connection cells carried record ids while the note said identifier (both servers; fixed the same day, see T6). **T4 is a real fix:** legacy wrote `allowsMultiple: true` for twelve `has: one` fields, v2 writes `false`. Details in the run notes below |
| 6 Sep | `318723a` on `main`                                   | the same disposable test app                        | live: **elicitation-capable** (VS Code 1.136.1), operator at the keyboard answering every prompt                                                                  | Tier 5 cascade cases end to end: two declines, one accepted cascade, a policy refusal, an unanswered prompt, and a rebuild from the snapshot                                     | **The gate works.** Every decline and the accepted cascade behaved as specified, and D1's split wording was confirmed live. **One finding:** an unanswered prompt was refused as `HUMAN_CONFIRMATION_UNAVAILABLE` — "this MCP client cannot prompt a human" — which is false; fixed below. **One friction:** the rebuild needed a key renamed by hand. **Not run:** the non-elicitation client pass                                                                                                                                                                                                                                                                                 |
| 7 Sep | `318723a` dist (predates `knack_list_page_referrers`) | the same disposable test app                        | live: elicitation-capable, operator answering both prompts                                                                                                        | Tier 6: two three-referrer transfers, alternate-route creation order reversed between them                                                                                       | **Both children survived** — the transfer rule holds with more than one candidate, and both landed on `scene_61`. **Eliminated:** link creation order and view key order — the reversal broke both symmetrically. **Still standing:** scene order, lowest scene key, or that page specifically; both runs shared their candidate pair, so two observations cannot separate them. Follow-up is Tier 6 step 9                                                                                                                                                                                                                                                                         |

### Run notes — 6 September

Evidence for the row above. Every app artefact is named by key only. The disposable app
has no `dataAccess` block, `allowDelete: false`, `allowDiagnostics` unset, four objects
and two records per object, so the flag matrix ran against **scratch copies** of the app
folder (a temp directory holding a copy of `schema/app.json` with one flag changed and a
stub second app that opts into everything, so no tool was withheld for want of an
opt-in). The real `app.json` was not edited. The no-key case used an empty secrets file;
no credential was copied anywhere.

**Harness.** A 40-line MCP stdio client spawned each `dist` with `KNACK_APPS_DIR` and
`KNACK_MCP_SECRETS_PATH` set explicitly. First run wasted: the shell already carried both
variables pointing at an old OneDrive checkout, and a `??` default let them win, so both
servers reported the app unknown. Set them unconditionally.

| Case             | Result                   | What was seen                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| ---------------- | ------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| T1               | pass                     | 45 read-only rows, same args both servers. Identical: cache status, object summary/fields/types, `knack_resolve_any` ↔ `knack_resolve`, `get_field`, `find_records`, `get_record`, related records both directions, aggregate, scenes, views, view context, table and form view fields, context bundle, overview, analyze, deep dive, field references, KTL, emails, repoint plan, connections, field shape, snapshot structure, duplicate usage, both templates, seed CSVs, unknown record                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| T1               | explained                | `knack_list_apps` equal once `serverBuild` is stripped. `raw`/`attributes` details: legacy withholds the tool, v2 advertises it and refuses with legacy's `allowDiagnostics` text. `knack_resolve` returns `knack_resolve_any`'s richer shape for alias and field-key inputs (legacy's two narrow tools had their own shapes and wording). `includeSchema` puts `body` beside `schema` instead of under `recordsResponse`; records and fields equal                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| T1               | finding                  | `knack_list_field_references` with `classification: "viewRecordRule", groupByView: true` adds `returnedMatches` — additive, not in the expected-difference list. `knack_get_object` on an unknown object: legacy `Object not found in schema.json: object_99`, v2 `Object not found in schema: object_99` plus `availableObjectKeys`; both soft (`ok: false`, no `isError`)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| T2               | pass                     | One record created and then updated on `object_3` through each server; both `POST`/`PUT` reached Knack (status 200, value read back). Only the envelope differs (`results[]` vs bare `status`/`body`). Delete refused on both by `allowDelete: false` (bogus ids; nothing deleted). Records left for the operator to remove: `6a9d665c621d8756366e7741` (v2), `6a9d66c0f2e2335fa51dd805` (legacy)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| T3               | pass                     | Unknown `appKey` and every permission refusal: v2 `{ ok: false, tool, error }` with `isError`, error text equal to legacy's bare string                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| T4               | pass                     | `knack_get_field` shows `relationship.has: "one"` on `field_38` and on `field_14`. Both servers warmed with `persistFiles` into separate scratch folders: v2 wrote `allowsMultiple: false` for all thirteen connection fields; legacy wrote `true` for the twelve `has: one, belongs_to: many` system fields. The real folder's `schema.json` was rewritten by v2 during this run, so it now carries the corrected values                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| T5               | not run                  | No object with 1,200+ records; generating them needs the operator's go-ahead because deletes are off and cleanup is in the builder                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| T6               | pass, after a fix        | First run: with `useExistingConnectionValues` the connection cells carried record ids on both servers, because a live record has no top-level `identifier` and the lookup fell through to `id`, while the note said "(`identifier`)". Fixed the same day: the parent object's display field (`identifier` in the object metadata) is read from each record first. Re-run through the worktree build: the `object_3` cells came back as '=1+1 … and '+1 …, escaped, and the note names `field_23`. A regression test fails against the pre-fix code                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| T7               | pass; plan reworded      | Aliases are matched exactly on both servers; the generated field map is lower-case so no case-variant alias pair can exist. What changed is the field key: `Field_38` resolves on v2 (`knack_resolve` and `knack_validate_field_mapping`) and fails on legacy. Setup 1.1, the Tier 1 row and T7 now say so                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| T8               | pass                     | Details view `view_8`: v2 lists `field_23` and `field_30` with `sourcePath` under `columns[0].groups[0].columns[0][]`; legacy lists none. Matches the builder's two fields plus a page link                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| T9               | pass                     | `DEBUG=1`, `knack_cache refresh: true`, then two `knack_get_object` calls fired together: one `runtime_metadata_attempt` line, both calls 673 ms                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| T10              | partial                  | One related record in each direction, equal on both servers and in the same order; no fixture with 10+                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| T11              | pass                     | Scratch `dataAccess` allowing `object_4` only, with `field_38` in both `allowedFieldKeys` and `redactedFieldKeys`. v2: `object_3` reads refused; `object_4` records return only `field_31`/`field_32`; `knack_verify_record_field_shapes` refuses `object_3` and checks two fields on `object_4`; seed CSV reports `policyBlockedConnectionTargets` for `object_1` and `object_3`. Legacy: every `object_4` read failed on the overlap, verify checked all eight fields, no blocked-target report                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| T12              | pass                     | `KNACK_MCP_READONLY=1` and `--readonly`: `knack_snapshot_app` and `knack_get_view_payload_template` both succeed; 32 tools advertised, 15 withheld                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| T13              | pass                     | `readonly: true` → both record tools refused with legacy's text. `allowViewMutation: false` → `knack_create_view` and `knack_update_view_order` refused. `allowDelete: false` → record and view deletes refused; `true` → a bogus id reached Knack (404) and a bogus view was refused with `COULD_NOT_VERIFY_VIEW`. `allowDiagnostics: true` → `raw`, `rawMetadata`, `attributes` and `attributes` + `includeRaw` equal to legacy's four tools                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| T14              | pass; plan reworded      | With `KNACK_MCP_READONLY=1` the six writable apps still report `readonly: false`; `serverBuild.mode` is `readonly` and the write tools are absent (calling one gives "Tool not found"). Legacy behaves the same. T14 now describes the mode and the withheld tools rather than the per-app flag                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| T15              | pass                     | Covered by T11: reads outside `allowedObjectKeys` refused; a redacted field never returned, and asking for it by key (`knack_get_related_records` through it, `knack_aggregate_records` grouping on it) is refused rather than silently dropped                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| T16              | pass                     | Empty secrets file: `create_view`, `update_view_order`, `update_view`, `copy_view` both modes, `move_view` refused with the missing-key text on both servers; `delete_view` refused earlier by `allowDelete`. No `snapshots` directory appeared                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| T17              | pass                     | Live. `sharePages: false` on `view_51` → `view_53` on `scene_63`, `pagesCreated` `scene_73` and `scene_74` (Knack duplicated both owned pages, as `TESTED.md` §9 records). `sharePages: true` → `view_54` on `scene_64`, `sharedPages` `scene_71`/`scene_72`, `performedAs: "create_view"`, `sharedPagesVerified: true`, no scenes inserted                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| T18              | pass                     | Fresh and `fromViewKey` templates byte-identical to legacy's two tools. The clone strips `_id` and `key`; its link columns keep the source page slugs                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| T19              | pass                     | Real app: full 46 tools 25.6 KB ~6,555 tokens (one diagnostic tool withheld), read-only 32 tools 17.7 KB ~4,527; stub script 47 / 26.7 KB / 6,685 and 32 / 18.1 KB / 4,527 — matches `MIGRATION.md`. Legacy real app: 60 tools 39.2 KB ~10,033                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| T20              | partial                  | Honoured: `KNACK_CACHE_TTL_MS` (reported back), `KNACK_MCP_MAX_TOOL_TEXT_BYTES` (17.6 KB response became the `truncated` structural summary), `KNACK_MCP_MAX_INLINE_DETAIL_BYTES` (`bodyIncluded`/`attributesIncluded` false with a summary), `KNACK_MCP_PRETTY_TOOL_JSON`, `KNACK_MAX_RESPONSE_BYTES` (413 `response_too_large` envelope), `DEBUG`. Not run: `KNACK_MCP_MAX_EXTRACTED_TEXT_BYTES` (no file field), `KNACK_MCP_BATCH_CONCURRENCY` (needs T21's writes)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| T21              | not run                  | Needs 30 records created and later removed in the builder; waiting on the operator                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| Recovery drill   | **finding**              | Snapshot `manual-app-3` taken, then the operator deleted `scene_63` in the builder: a page created by today's copy, holding `view_53` (table) and its two owned child pages `scene_73` and `scene_74` (a three-page subtree, all fixtures). Rebuild from the snapshot alone is not possible. An app-level or scene-level snapshot (`action manual`) carries, per page, only `sceneKey`, `sceneName`, `sceneSlug`, `parentRef` and each view's key, name and type; the view body (source object, columns, links) is only present in a mutation snapshot for the view being mutated (`action update_view`) or a manual snapshot taken with `viewKey`. `knack_copy_view` and `knack_create_view` write no snapshot, so a view that only ever existed through a create or copy has no definition on disk anywhere. The tree can be named from the snapshot but not rebuilt: a table needs its source object and columns. Nothing was recreated. Fixed the same day, per the operator's choice: `knack_create_view` and `knack_copy_view` now snapshot the view they made after Knack answers (`snapshotPath` in the response, `snapshotNote` when a Knack copy is not yet readable). Verified live through the worktree build: a create wrote `create_view-view_56-1.json` holding the view body and 28 pages; a Knack copy of it first filed **no** snapshot, because Knack lists the inserted view as `{ view: {…} }` and the first cut read only a bare key or `{ key }`; fixed, re-run, and the copy's snapshot then held the copied view. Four tests fail against the pre-fix code |
| C6 (legacy plan) | pass                     | First live execution of the cut. `knack_update_view` on `view_54` (the sharing copy on `scene_64`) re-sent its columns without the link to `scene_72`, a page owned by `view_51` on `scene_69`. No prompt was needed and none could have been shown (this client has no elicitation): the guard classified the link as external, wrote snapshot `update_view-view_54-4`, sent the `PUT` (status 200) and reported `linksRemovedPagesKept` naming `scene_72` with `parentSceneKey: scene_69`. Knack's `changes.updates.scenes` listed `scene_72`, nothing deleted, and the page is still in the scene list afterwards                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| A4 (legacy plan) | pass                     | First live run. Fixture: `knack_create_view` on `scene_61` made `view_55`, a table whose one link column carried a well-formed page specification; Knack created `scene_75` and rewrote the column to its slug, so that page has exactly one link. The A4 payload re-sent the columns without that link and put the same page reference inside `rules.submits[].scene`. Refused with `HUMAN_CONFIRMATION_UNAVAILABLE`: "destroys 1 page(s)", `childPages` naming `scene_75`, `unresolvedLinkCount: 0`. The rule redirect did not count as a retained link. Nothing was sent                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| A3 (legacy plan) | pass; plan wording wrong | Precondition: an update added a third link column pointing at a slug no page has; the guard let it through with no warning and Knack stored it (`changes: {}`). A3 then re-sent the columns with that slug swapped for a second slug no page has, the real link kept. **Refused**, not written: "removes 1 link(s) whose target page this server could not identify", `unresolvedLinkCount: 1`. The legacy plan calls this a known weak spot where "the tally nets zero and nothing is asked"; live, the removed unreadable link is counted and a new unreadable link is not treated as its replacement. Safer than documented. `view_55` keeps the first dangling link. **Finding, fixed the same day at the operator's request:** adding that dangling link drew no warning; every view mutation now returns `danglingLinks` and a `warning` when the sent body names a page that does not exist (guard unchanged, write still allowed)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| A5 (legacy plan) | pass                     | The operator changed the title of `view_55` in the builder and said so; a `knack_update_view` setting a different title followed within about a minute. The pre-mutation snapshot (`update_view-view_55-7`) shows the guard's fresh read already carried the builder's title, so the merge saw the edit; it was replaced only because the update named the same field, which is the intent. The builder then showed the MCP title. No other property changed. The remaining window is the one the plan describes: between the fresh read and the `PUT`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |

**Input shapes that bit before the guard, then fixed.** Record payloads were JSON strings
only on both servers (`records: ["{...}"]`, `records: [{ recordId, data: "{...}" }]`),
and `knack_update_view_order` required `pageGroups`; an object or a missing key failed
MCP schema validation before the handler ran, which is not evidence about the guard, and
two matrix rows had to be re-sent. Fixed the same day on v2 at the operator's request:
records and `data` accept an object or its JSON, `order` accepts an array or its JSON,
and a missing `pageGroups` becomes one full-width row per view in the order given. A
schema error is still what legacy returns for these shapes.

**Left on the app:** two records on `object_3` (ids above), `view_53` on `scene_63` with
`scene_73`/`scene_74`, `view_54` on `scene_64`; snapshots `manual-app-1`, `manual-app-2`
and two `manual-scene_69` files under `schema/snapshots`. `scene_63`, `view_53`, `scene_73` and
`scene_74` were deleted by the operator for the recovery drill. Tier 5 ran later the same
day; see the notes below it.

### Run notes — 6 September, Tier 5

The first pass with a client that can actually prompt, and the first time the cascade
gate has been exercised end to end by a person rather than by a spy. The operator
confirmed afterwards that **every elicitation was rendered and visible** — which is what
turns the timeout case below from a pass into a finding. Artefacts are named by key only.

| Case                    | Result                | What was seen                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| ----------------------- | --------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Capability banner       | pass                  | `humanConfirmation.available: true` for the elicitation-capable client, against the expected build                                                                                                                                                                                                                                                                                                                                                                                    |
| Named-page decline      | pass                  | `update_view` dropping a link column that owned one page: prompted, the page named, declined, nothing sent                                                                                                                                                                                                                                                                                                                                                                            |
| Unresolved-only decline | **pass, closes D1**   | `update_view` dropping only the link whose target no page has: prompted with `unresolvedLinkCount: 1` and the unresolved wording, not `destroys 0 page(s)`. This is `describeRefusedStakes` measured live — D1 is closed                                                                                                                                                                                                                                                              |
| Accepted cascade        | pass, and a new shape | `move_view` between two pages, accepted. Knack **deleted** the owned child page and created a fresh one under the target rather than carrying the original across — new keys on both sides. A move is a destroy-and-recreate for owned pages, not a re-parent                                                                                                                                                                                                                         |
| Delete refused          | pass                  | `delete_view` refused on `allowDelete: false` **before** any prompt. Policy first, human second, which is the right order — a person is never asked to approve something the app forbids outright                                                                                                                                                                                                                                                                                     |
| Unanswered prompt       | **finding, fixed**    | The prompt was left to run past `CASCADE_CONFIRMATION_TIMEOUT_MS`. It failed closed with nothing sent — but as `HUMAN_CONFIRMATION_UNAVAILABLE`, whose text is _"this MCP client cannot prompt a human to confirm it"_, followed by the go-to-the-builder hint. See below                                                                                                                                                                                                             |
| Recovery drill          | pass, with friction   | The view was rebuilt from the move snapshot alone, child page included. It needed the snapshot's `groups` passed as the create tool's `pageGroups` — a rename a person had to spot. New keys throughout, which is unavoidable rather than a shortfall — Knack assigns them and no create can request one. So the drill shows an equivalent view can be rebuilt, which is what it asks for; what it does not yet show is whether anything else in the app still points at the old keys |

**The finding, and why it is the same defect as D1.** A timeout made `elicitInput`
reject, and the catch turned every rejection into `supported: false` — the bucket meaning
_this client has no elicitation capability_. Three distinct states were being reported as
two:

| What happened                        | Reported as                      | True?                            |
| ------------------------------------ | -------------------------------- | -------------------------------- |
| The client cannot prompt at all      | `HUMAN_CONFIRMATION_UNAVAILABLE` | yes                              |
| A human was asked and said no        | `HUMAN_CONFIRMATION_DECLINED`    | yes                              |
| A human was asked and did not answer | `HUMAN_CONFIRMATION_UNAVAILABLE` | **no — the client had prompted** |

That is D1's failure mode surviving in the capability clause after being fixed in the
stakes clause: a refusal that misstates its own reason. It matters beyond wording,
because the advice attached to it is wrong — the message sends the operator to the Knack
builder when the remedy is the prompt still on their screen. The `outcome` union already
carried a `'timeout'` member that nothing in either server ever produced, so this was an
oversight rather than a decision.

**Fixed on both servers**, since the two run side by side for the differential pass and a
divergence here would show up as a false T1 difference:

- A rejection carrying `ErrorCode.RequestTimeout` now returns
  `{ supported: true, accepted: false, outcome: 'timeout' }`. Anything else that throws
  is still a real failure and still `supported: false`. The code is matched, not the
  message text, so an SDK rewording cannot silently undo this.
- That outcome refuses with a new `HUMAN_CONFIRMATION_TIMED_OUT`, saying a human was
  asked and the prompt went unanswered, that **nobody declined it**, and to retry with
  someone at the keyboard. No builder hint.
- The difference from a decline is deliberate and is the point of the split: a decline is
  a decision, so its refusal still says _do not retry without being asked to_; a timeout
  is the absence of one, so retrying is the remedy.

Both servers keep failing closed on every path. No route added here can return an
acceptance, and a test asserts that directly for both the timeout and the failure case.

**Fixed since, from reading the pass back:**

- **The move prompt said "delete" and stopped there.** An accepted move destroys the
  owned child page and Knack makes a new one under the target — so a prompt that only
  names a deletion lets someone approve it believing the page travels with the view.
  The prompt now says a move is not a re-parent, and that the replacement carries a new
  key, so every reference to the old one is about to point at nothing. Move-only: the
  sentence is scoped to `move_view` rather than added to every cascade prompt.
- **An auto-accepted write looked exactly like an approved one.** A mutation that
  destroys nothing is allowed with nobody asked, which is right — but the result said
  nothing about which of the two paths it took. That ambiguity is the whole mechanism
  behind the 4 September report, where a quiet `ok` did the writing and two loud
  refusals took the blame. Every mutation response now carries
  `humanConfirmation: 'not-required' | 'accepted'`.

**Still open after this pass:**

- **What the replacement page a move makes actually contains.** The move was measured
  as destroy-and-recreate, but not what lands in the new page. The prompt now warns
  that a move is not a re-parent and that the replacement carries a new key; it
  deliberately says nothing about the contents, because nothing has been measured. One
  accepted move with a populated child page settles it.
- **Where a transferred page lands with two candidates.** Tier 6 below; blocks both an
  accurate prompt and any re-parent feature. `knack_list_page_referrers` now reports the
  candidates and says plainly that the winner is unmeasured.
- **The non-elicitation client pass.** Tier 5's first bullet wants both profiles, and
  only the elicitation-capable one ran. The refusal path for a client that genuinely
  cannot prompt is covered by tests but has not been run live since the split above.
- **The snapshot-to-create key mismatch** (`groups` vs `pageGroups`). Recovery should not
  need a human to translate. Worth deciding whether the create tool accepts `groups` as
  an alias or the snapshot writes both.
- **What a rebuild leaves dangling.** Knack assigns `view_N` and `scene_N` itself: every
  create, copy and move measured here came back with fresh keys, and the payload template
  strips the identifiers rather than offering them (T18). So a rebuilt view and page
  **always** carry new keys, and the drill should not be read as aiming at key-identical
  restoration — that is not something on offer. Equivalence is the bar, which is what the drill
  already asks for. The open question is the consequence: anything elsewhere in the app
  that referenced the old keys — another view's link column, a rule redirect, a menu
  link — still points at a key that no longer exists. That is the same dangling-link
  shape this plan chases everywhere else, and it is created by the recovery itself. A
  drill that ends at "an equivalent view exists" has not yet checked it. Worth adding a
  step that lists referrers to the old keys before the delete and re-checks them after
  the rebuild.
