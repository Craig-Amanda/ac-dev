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

**Settled on 7 September, in three runs — see the run notes below.** A transferred page
lands on whichever surviving referrer comes first in the order Knack returns its pages
in. Run 3 was pre-registered across a pair where page order and key order disagree, and
eliminated key order. The procedure is kept here for re-testing after any change to how
the scene tree is read.

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

### Run notes — 7 September, Tier 6 run 3 (the pre-registered one)

**Predictions written down before the mutation**, which is what makes this run worth
more than the two before it. The rebuilt `a1fe01d` server was verified first:
`knack_list_page_referrers` matched both standing fixtures (`scene_78` → `view_62`,
`view_61`; `scene_79` → `view_64`, `view_65`) and both controls (`scene_51` zero,
`scene_76` one, `view_55`), with the right consequence sentence in each case. 47 tools
advertised.

**The disagreeing pair.** Returned scene order is **not** numeric key order: `scene_7`
precedes `scene_6`. So the two surviving hypotheses predicted different pages, recorded
before the run — page order → `scene_7`; lowest numeric key → `scene_6`.

**Result.** Owner `view_66` on `scene_51` created child `scene_80`, linked from `view_67`
on `scene_7` and `view_68` on `scene_6`. All three referrers confirmed by the tool.
Owner link removed, human confirmed, child survived with
`parentRef: "thank-you"` — the slug for **`scene_7`**.

| Hypothesis               | Run 1 | Run 2 | Run 3 | Verdict                                    |
| ------------------------ | ----- | ----- | ----- | ------------------------------------------ |
| Returned page order      | ✔     | ✔     | ✔     | **3/3 — stands**                           |
| Lowest numeric scene key | ✔     | ✔     | ✘     | **eliminated** on the run built to test it |
| Link creation order      | —     | ✘     | —     | eliminated by run 2's reversal             |
| View key order           | —     | ✘     | —     | eliminated by run 2's reversal             |

**The rule, as far as it goes:** a transferred page lands on whichever **surviving
referrer comes first in the order Knack returns its pages in**. Both servers now name
that page in the confirmation prompt, and `knack_list_page_referrers` names it in
`consequence`.

**Why it is still hedged in both places.** Three observations, and the rule keys off an
order that is not ours — a page moved in the builder can change it, which would change
the prediction without changing anything in this repository. So the prompt says
"expected to land under X (measured, not guaranteed)" and the tool says "a prediction,
not a promise", and both still point at sequencing for certainty: remove the links you do
not want it under first, so exactly one remains when the owning link goes.

**Not captured:** the confirmation prompt's exact wording, again. It stays
human-observable; the agent cannot see it.

**Left on the app:** `view_60`–`view_68`, `scene_78`, `scene_79`, `scene_80`, and the
snapshots from all three runs. Left standing deliberately.

## Tier 7 — Who can reach a page, and what a parent change does to that

The operator raised the consequence this whole area was missing: in Knack a page's login
and permitted roles follow its **parentage**, so anything that changes a page's parent —
a transfer, a move — can change **who can reach it**. A page can be tidied into a
different part of the tree and quietly leave the audience that used it.

Two gaps, both real, both now closed in code and confirmed live (T23):

- **The prompt could not warn about it.** It asked (`CHECK THE AUDIENCE`) rather than
  answered, because nothing read the permissions. It now names the audience on both sides
  and says whether it changes; it still asks, in the old words, for anything it cannot
  resolve.
- **The snapshot did not capture it.** `parseRuntimeScenes` kept key, name, slug, parent
  and views and nothing else, so a page rebuilt from a snapshot came back **without its
  access control**. Snapshots are now version 3 and carry it.

### T22 — establish the shape first, before any tool is designed — **done, 7 Sep**

One unauthenticated read of the application payload plus `knack_list_scenes`, no
mutation. The first pass found the app had no `login` view at all; the operator added one
in the builder to the page tree under `scene_59`, and the second pass measured the
result. Keys only, as the rule requires.

**What adding a login did.** Knack did not mark `scene_59`. It inserted a new scene
above it — `scene_81`, `type: "authentication"`, `object: null`, `parent: null` — holding
a single view `view_69` of type `login`, and re-parented `scene_59` under it (`parent`
became the new scene's slug). The protected page's key did not change; its parent did.

| Page             | Key                    | Permission-related fields present                                                                |
| ---------------- | ---------------------- | ------------------------------------------------------------------------------------------------ |
| L (login holder) | `scene_81`             | `type: "authentication"`, `object: null`, `parent: null`. No `authenticated`, no role fields     |
| the login view   | `view_69`              | `allowed_profiles: ["profile_2"]`, `limit_profile_access: true`, `registration_type: "closed"`   |
| C (direct child) | `scene_59`             | `type: "page"`, `authenticated: false`, `login_vars: null`, `parent: <L's slug>`. No role fields |
| C (deeper)       | `scene_60`, `scene_65` | only `_id key slug uuid groups parent` (+ `object` on the form page). Every access field absent  |
| P (public)       | `scene_3`              | `type: "page"`, `authenticated: false`, `login_vars: null`, `parent: null`                       |

Not present anywhere in the payload: `authentication_profiles`, `profile_keys`.

The five questions:

1. **Does a protected scene carry its own permission fields?** No. `scene_81` carries
   `type: "authentication"` and nothing else. The roles live on its login view only.
2. **Does the child carry them?** No. `scene_59` says `authenticated: false` while
   sitting directly under the login, so that field is not a protection signal. Deeper
   children carry no access field at all. **This is the "only the login ancestor" shape:
   the answer for a page is an upward walk.**
3. **Does the login view hold the roles rather than the scene?** Yes, exclusively.
   `view_69` is the only view in the app carrying `allowed_profiles` or
   `limit_profile_access`.
4. **How is a role identified?** By profile key (`profile_2`). The only mapping to
   something a person recognises is through objects: each user object carries
   `profile_key` (here `object_1 → all_users`, `object_2 → profile_2`), so the object's
   name is the label. The application's own `users.profiles` list was empty.
5. **How does the public page differ?** It does not, on its own fields: `scene_3` and the
   protected `scene_59` have the same `type`, `authenticated: false`, `login_vars: null`.
   Deeper protected pages have the fields absent. Public and protected are
   indistinguishable from a scene's own fields; only ancestry separates them.

Two things the plan had not anticipated: `parent` is a **slug**, not a key, so the walk
resolves through both; and adding a login **inserts an ancestor** rather than marking the
page, so a snapshot that stores only parents would record the re-parenting but not why.

### What was built from it

- `lib/page-access.ts` — `resolvePageAccess` walks `parentRef` upward to the nearest
  `type: "authentication"` scene (or any scene holding a `login` view) and reads the roles
  there. Public when a top-level page is reached with no login; unknown — never public —
  on an unresolvable parent, a loop, or a login without role fields. Roles are mapped to
  objects through `profile_key`.
- `knack_get_page_access` (read) exposes it: status, ancestry, login scene and view, the
  roles with their object, and the same audience sentence the prompt uses.
- The cascade prompt resolves both sides on a move (each doomed page now, versus the
  target scene its replacement lands under) and on a transfer (the page now, versus its
  expected destination), and heads the paragraph `AUDIENCE CHANGES`, `Audience
unchanged`, or `CHECK THE AUDIENCE` when a side is unknown.
- Snapshots are version 3: scenes keep `sceneType`, `authenticated`, and a login view's
  `allowedProfiles` / `limitProfileAccess`; the file carries a `profiles` map.

### T23 — the walk against the live app (read-only first, then one prompt) — **closed, 7 Sep**

**All nine steps run 7 Sep, all as expected — see the results log.**

**Read-only, no human needed:**

1. `knack_get_page_access` on `scene_65` → expect `protected`, `loginSceneKey: scene_81`,
   `loginViewKey: view_69`, one role whose `objectKey` is `object_2`, ancestry
   `scene_65 → scene_60 → scene_59 → scene_81`. **Ran via VS Code client: `protected` /
   `scene_81` / one role, as expected.**
2. On `scene_59` → the same login and role, despite its `authenticated: false`.
3. On `scene_3` → `public`, ancestry of one.
4. On `scene_81` itself → `protected`, reason says it holds the login.
5. `knack_snapshot_app` → open the file: `snapshotVersion: 3`; the `scene_81` entry has
   `sceneType: "authentication"` and its view carries `allowedProfiles`; `profiles` maps
   `profile_2` to `object_2`.
6. In the builder, change the login's roles (add a second role, or untick "limit to
   roles"), then re-run 1 — the tool reads fresh, so the answer must follow the builder
   without a cache refresh. Revert. **Ran: unticked "limit to roles" in the builder; the
   tool's answer followed with no cache refresh called; reverted.**

**Needs a human at an elicitation-capable client (Tier 5 conditions):**

7. Create a table view on `scene_3` (public) with one link column owning a new child
   page. `knack_move_view` it to `scene_60` (protected). Expected prompt: the
   `AUDIENCE CHANGES` headline, a line `<child key>: now anyone (no login above it); its
replacement under scene_60: only <object_2's name> [profile_2] (login on scene_81) →
CHANGES`. **Decline.** Nothing sent. **Ran: prompt shown, headline `AUDIENCE CHANGES`,
   role shown as the object's name with `[profile_2]` alongside — not the bare key.
   Declined; the child page was confirmed still under `scene_3` afterward.**
8. The same move to another public top-level page: expected `Audience unchanged`.
   **Decline.** **Ran: prompt shown, headline `Audience unchanged`. Declined.**
9. Record the exact prompt text (keys only) in the results log, and whether the role
   label was the object's name or fell back to the bare profile key. **Done — see 7
   above and the results log row below.** Not separately exercised: the
   "client cannot prompt" fallback path (`HUMAN_CONFIRMATION_UNAVAILABLE`); an
   elicitation-capable client was available throughout, so this branch of the code went
   untested here — it is covered by unit tests, not by this live run.

What T23 could not settle: whether Knack's replacement page after a move actually
inherits the target's login at runtime. Both prompts in steps 7–8 were declined, as the
plan requires, so nothing was sent to Knack and the runtime outcome is still unmeasured.
If that matters later, it needs its own run with an accepted move.

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

| Date  | Commit tested                                         | App                                                 | Client(s)                                                                                                                                                         | Tiers run                                                                                                                                                                        | Pass / findings                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| ----- | ----------------------------------------------------- | --------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 6 Sep | `f4a0c5b` on `main`; both `dist` builds made from it  | the disposable test app (same as legacy 5 Sep rows) | live: no elicitation (`local-agent-mode-knack`); differential and flag/env cases through a stdio harness that spawned `knack-mcp` and `knack-mcp-v2` side by side | Tier 1 (T1–T3); Tier 2 T4, T6–T9, T12 with T10/T11 partial and T5 not run; Tier 3 T13–T16 through scratch copies of the app folder; Tier 4 T16–T18; operational T19, T20 in part | **Tier 1 clean:** 33 of 45 rows byte-identical, the other 12 explained (tool merges, `serverBuild`, the T7/T8 fixes). **Findings, none blocking:** `KNACK_MCP_READONLY=1` withholds every write tool but does not force per-app `readonly: true` in `knack_list_apps` (T14, legacy identical); unknown-object wording changed from `schema.json` to `schema`; `returnedMatches` added to the record-rule listing; seed CSV connection cells carried record ids while the note said identifier (both servers; fixed the same day, see T6). **T4 is a real fix:** legacy wrote `allowsMultiple: true` for twelve `has: one` fields, v2 writes `false`. Details in the run notes below                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| 6 Sep | `318723a` on `main`                                   | the same disposable test app                        | live: **elicitation-capable** (VS Code 1.136.1), operator at the keyboard answering every prompt                                                                  | Tier 5 cascade cases end to end: two declines, one accepted cascade, a policy refusal, an unanswered prompt, and a rebuild from the snapshot                                     | **The gate works.** Every decline and the accepted cascade behaved as specified, and D1's split wording was confirmed live. **One finding:** an unanswered prompt was refused as `HUMAN_CONFIRMATION_UNAVAILABLE` — "this MCP client cannot prompt a human" — which is false; fixed below. **One friction:** the rebuild needed a key renamed by hand. **Not run:** the non-elicitation client pass                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| 7 Sep | `318723a` dist (predates `knack_list_page_referrers`) | the same disposable test app                        | live: elicitation-capable, operator answering both prompts                                                                                                        | Tier 6: two three-referrer transfers, alternate-route creation order reversed between them                                                                                       | **Both children survived** — the transfer rule holds with more than one candidate, and both landed on `scene_61`. **Eliminated:** link creation order and view key order — the reversal broke both symmetrically. **Still standing:** scene order, lowest scene key, or that page specifically; both runs shared their candidate pair, so two observations cannot separate them. Follow-up is Tier 6 step 9                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| 7 Sep | `a1fe01d` rebuilt from the branch                     | the same disposable test app                        | live: elicitation-capable, operator confirming                                                                                                                    | Tier 6 run 3, pre-registered: referrer tool verified against two standing fixtures and two controls, then one transfer across a pair where page order and key order disagree     | **Settled.** Predicted before the run: page order → `scene_7`, lowest key → `scene_6`. Landed on `scene_7`. Page order 3/3; **lowest numeric scene key eliminated**. The rule — first surviving referrer in the app's returned page order — is now named in the confirmation prompt on both servers and in `knack_list_page_referrers`, hedged because it rests on an order a builder edit can change                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| 7 Sep | `517987e` on `main` (T22); this branch for the build  | the same disposable test app                        | `knack_list_scenes` through the v2 server, plus one unauthenticated GET of the application payload                                                                | Tier 7 T22 — read-only, two passes                                                                                                                                               | **First pass: no `login` view in the app; stopped as the plan requires.** Operator added one in the builder. **Second pass:** roles live on the login view only (`allowed_profiles`, `limit_profile_access`); the authentication scene and every page beneath carry no role fields; the page directly under the login has `authenticated: false`, same as a public page; `parent` is a slug; adding a login inserted `scene_81` above `scene_59` rather than marking it. Design settled: upward walk. Built `knack_get_page_access`, the audience lines in the cascade prompt, and snapshot version 3 — 29 unit tests. **T23 steps 1–5 then run live** through this branch's `dist` over a stdio client: `scene_65`, `scene_59` protected via `scene_81`/`view_69` with one role mapped to `object_2`, ancestry as predicted; `scene_3` public; `scene_81` protected by its own login; a missing key refused as `SCENE_NOT_FOUND`; the snapshot written as version 3 with the login view's `allowedProfiles`/`limitProfileAccess` and a two-entry `profiles` map. Steps 6–9 not run (6 needs a builder edit, 7–9 an elicitation-capable client) |
| 7 Sep | `5630bf8` on `main` (merged PR #49)                   | the same disposable test app                        | live: VS Code 1.136.1, elicitation-capable, operator answering both prompts                                                                                       | Tier 7 T23 — the full walk, including the two moves                                                                                                                              | **All nine steps as predicted, T23 closed.** Step 0: new server confirmed live. Step 1: `scene_65` → `protected` / `scene_81` / one role. Step 6: unticked "limit to roles" in the builder; the tool's answer followed with no cache refresh called; reverted. Step 7: moving a public page's view into the protected tree showed `AUDIENCE CHANGES`, the role labelled by its object's name with `[profile_2]` alongside — declined, child confirmed still under its original parent. Step 8: the same move to another public page showed `Audience unchanged` — declined. Step 9: prompt text and role-label choice recorded above; the no-elicitation fallback path was not separately exercised live (an elicitation-capable client was available throughout) and remains covered by unit tests only. Both prompts were declined per the plan, so whether a moved page's replacement actually inherits the target's login at runtime is still unmeasured                                                                                                                                                                                    |

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

---

## Tier 8 — A page's layout, and why a moved view renders nowhere

**Measured on 10 September 2026** against `Knack MCP Test`, server `knack-mcp 2.0.0`
compiled from `main @ 2b05891`, with `humanConfirmation.available: false` — the same
client condition as the Noah's Place incident. Every figure below is an observation,
not an inference. Pinned by `src/lib/incident-noahs-place.test.ts`.

### The read path

A page's layout lives in `scene.groups` and is returned by
`GET https://api.knack.com/v1/applications/{appId}` — unauthenticated, the same payload
the front end renders from, and the only place `groups` appears at all. It also carries
`parent` on every scene, which is what `knack_get_page_access` resolves a login from.
It is visible nowhere else:

| Source                               | `views` | `groups`              |
| ------------------------------------ | ------- | --------------------- |
| `knack_list_scenes`                  | yes     | **no**                |
| `knack_snapshot_app` (restore point) | yes     | **no**                |
| `schema/viewMap.json`                | yes     | view-internal only    |
| `knack_get_view_payload_template`    | n/a     | synthesised, not read |
| `GET /v1/applications/{appId}`       | yes     | **yes**               |

So the server can write a page layout and has no way to read one back — including in
the snapshot it takes to make a mutation recoverable.

### What `groups` does

Two states, both measured by loading the live page and reading the rendered view IDs
out of the DOM:

- **`groups: []`** — every view in `views` renders. `scene_62` held `view_61` and
  `view_65`, and both rendered. This is the default for pages built in the builder:
  19 of the app's 34 pages were in this state.
- **`groups` populated** — only the view keys the layout names render. `scene_64` held
  five views and its layout named three; the DOM contained exactly `view_54`,
  `view_57`, `view_58`. `view_55` and `view_56` existed on the page and rendered
  nowhere.

**A page is therefore fail-safe until something writes a layout to it, and fail-silent
afterwards.** Writing `pageGroups` is the step that arms the hazard.

### The move measurement

`view_56` — a `rich_text` view with `links: []`, `columns: []` and no scene reference
anywhere in its definition, so zero cascade risk — moved from `scene_61` (`groups: []`)
into `scene_64` (`groups` naming three keys):

```
knack_move_view scene_61 -> scene_64, view_56
  => ok, humanConfirmation: "not-required", no warning
```

| Page       | `views` before | `views` after          | `groups` before | `groups` after             |
| ---------- | -------------- | ---------------------- | --------------- | -------------------------- |
| `scene_64` | 54, 57, 58, 55 | 54, 57, 58, 55, **56** | 54, 57, 58      | 54, 57, 58 — **unchanged** |

The move appended the view to `views` and did not touch `groups`. Confirmed against the
front end: `view_56` did not render. This is the reported Noah's Place symptom —
present in the page's view list, reachable by its builder URL, visible on neither the
front end nor the back end — reproduced on demand.

The move envelope carries no layout field at all
(`knack-mcp/TESTED.md`: `{action, target_scene_key, view_key, completeViewSchema}`), and
nothing reconciles the target page afterwards.

`view_56` was moved back to `scene_61` and both pages were re-read to confirm the app
was left as found.

### The pre-existing orphan

`scene_64` already held one before the experiment: `views` ended with `view_55` while
`groups` named only the first three keys — the signature of an append that never
updated the layout. Across the app, 30 views on 19 pages sit outside a populated
layout; all but one of those pages have `groups: []`, so only `view_55` was actually
unrendered.

### Still unmeasured

- Whether a **copy** onto a page with a genuine multi-column row flattens it. The copy
  path can only emit one full-width row per view (`buildStarterPageGroups`, pinned by
  test), and `pageGroups` replaces rather than merges, so flattening follows — but it
  has not been run against a real two-column page.
- Whether a layout, once written, can be cleared back to `[]`. No tool sets an empty
  layout, so the write appears to be one-way.

### The repair, measured before it was written

The fix in `ensureMovedViewIsRendered` was run by hand first, on the same page:

1. `knack_move_view` scene_61 → scene_64, `view_56`. `groups` unchanged, view invisible.
2. `knack_update_view_order` on scene_64, `order` = all five keys, `pageGroups` = the
   stored row **plus** one new full-width row for `view_56`.
3. Front end after a hard reload: `view_54, view_57, view_58, view_56` rendered —
   three probe strings where there had been two. `view_55`, deliberately not added,
   stayed invisible.
4. Layout restored, `view_56` moved back, both pages re-read against the baseline.

Two things this settles. Appending a row is enough — the view renders and the rest of
the layout survives, so the repair does not need to rebuild anything. And it must
start from the **stored** `groups`, not from `SceneInfo.layoutViewKeys`: the flattened
list cannot round-trip a multi-column row, so rebuilding from it would restack a page.

### What the builder does, for comparison

**Measured 10 September, the app owner moving `view_56` onto `scene_64` in the Knack
builder** while that page's layout named only `view_54`, `view_57`, `view_58`:

| Page                | `views`          | `groups`                                                |
| ------------------- | ---------------- | ------------------------------------------------------- |
| `scene_61` (source) | lost `view_56`   | `[]` before and after — **untouched**                   |
| `scene_64` (target) | gained `view_56` | gained `{"columns":[{"keys":["view_56"],"width":100}]}` |

Three findings, and they settle the fix:

- **The builder writes the layout.** So `knack_move_view` was missing a step Knack's
  own client performs, not diverging from Knack's model. The endpoint does not do it;
  the caller must.
- **It appends one full-width row** — byte-identical to what
  `ensureMovedViewIsRendered` sends, arrived at independently and pinned by
  "writes exactly what the Knack builder writes".
- **It appends rather than rebuilding.** `view_55` was stranded on that page before
  the builder move and stayed stranded after, so the builder does not reconcile a
  layout it finds incomplete. Neither does the repair.

The source page's `groups` stayed `[]`, so nothing writes a layout to a page that has
none — also matching.

**Knack's front end caches app metadata.** Immediately after step 2 the page still
rendered the old three views; only a full reload showed the fourth. A page checked too
soon after a layout change will look unfixed. Worth knowing before concluding a write
did not land — the metadata endpoint is authoritative and updates immediately.

## Tier 9 - What `keywordEdits` does to the text around a keyword

`knack_update_view` takes a `keywordEdits` map so a caller can change one KTL keyword's
value without retyping every sibling. Two claims about it were made during the Noah's
Place rebuild and both needed testing rather than asserting. One turned out to be wrong.

### Fixture

`view_56` on `scene_61` in **Knack MCP Test**, description empty at the start.

### The measurement

1. Set the description directly, not through `keywordEdits`, to establish a baseline with
   a **newline** between two keywords:

    ```
    _cls=[probe-a]\n_notes= baseline BEFORE
    ```

2. Re-read from `GET https://api.knack.com/v1/applications/{appId}` - not from the tool
   response - and confirm the newline is in the app. It was.

3. Change one keyword through `keywordEdits`:

    ```json
    { "description": { "_notes": " changed AFTER via keywordEdits" } }
    ```

4. Re-read from the metadata endpoint again.

### Result

| Question                                      | Answer  |
| --------------------------------------------- | ------- |
| Did the new keyword value persist?            | **Yes** |
| Did the newline between the keywords survive? | **No**  |

Live value after step 3:

```
_cls=[probe-a] _notes= changed AFTER via keywordEdits
```

**Two corrections came out of this, both to things stated earlier as fact.**

- **`keywordEdits` does persist.** It had been called non-persistent on the strength of a
  `"changes":{}` field in the tool response. That field reflects how the response is
  assembled, not what reached the app. The app had the new value.
- **`update_view` merging into a stale baseline was never substantiated.** The property
  reverts attributed to it are better explained by Knack auto-wiring a default
  `child_page` submit rule at the moment a child page is created - the view changed
  between two reads of ours, not inside a write of ours.

The real defect is narrower than either claim and had gone unnoticed: the **separator**
between keywords is not preserved. `serializeKtlKeywordCluster` joined with a single space
regardless of what the parser had read, and the parser `.trim()`ed each segment, throwing
the separator away before serialization could have honoured it. Every multi-line
description this server touched came back on one line.

Nothing breaks - KTL parses either form - but the description is no longer the one the
person wrote, and a diff against a snapshot shows every keyword as changed.

### The fix, and what it must not do

`KtlKeywordEntry` gained an optional `separator`, recorded by the parser and honoured by
the serializer. Optional deliberately: a hand-built entry has no separator to preserve and
falls back to a space, so existing callers are unaffected.

Three cases had to be right, and each is pinned by a test in
`src/lib/ktl-keywords.test.ts`:

| Case                                        | Required behaviour                                  |
| ------------------------------------------- | --------------------------------------------------- |
| Update in place                             | Keep the separator already in front of that keyword |
| Append to a newline cluster                 | Use a newline, matching the cluster                 |
| Append after a keyword that starts the text | Use a space, **not** that keyword's empty separator |

The third is the one worth stating. A keyword at position 0 has separator `""`. Inheriting
it would emit `_ktlHide_notes=Craig`, which KTL reads as a single unknown keyword - a
silent functional break, worse than the cosmetic one being fixed. The inheritance rule is
therefore the last **non-empty** separator in the cluster, or a space if there is none.

### Not yet re-measured live

The fix is in the worktree only. The running server is the compiled `dist` from
`main`, so a live re-run of the four steps above should follow the build and restart, and
should show the newline surviving step 4.

## Tier 10 - Replaying the destruction on a disposable app

Tiers 8 and 9 measured single behaviours. This one rebuilds the whole sequence that
destroyed pages in production and runs it against the **old build**, to answer one
question: does it still break the app?

It does. It also turned up a condition nobody had stated, which changed how the fix
should be judged.

### Fixture - two identical chains

Built on `scene_69` ("Test Create Table with Child Pages") in **Knack MCP Test**, both
chains three levels deep and structurally identical:

|                                         | Chain A                       | Chain B                       |
| --------------------------------------- | ----------------------------- | ----------------------------- |
| Root form on `scene_69`, owns level 2   | `view_71`                     | `view_72`                     |
| Independent link column into level 2    | `view_78`                     | `view_84`                     |
| Level 2 page                            | `scene_85` `chain-a-level-2`  | `scene_86` `chain-b-level-2`  |
| Level 2 views (table, rich text, form)  | `view_73` `view_74` `view_75` | `view_79` `view_80` `view_81` |
| Level 3 page, owned by the level 2 form | `scene_87` `chain-a-level-3`  | `scene_88` `chain-b-level-3`  |
| Level 3 views (table, rich text)        | `view_76` `view_77`           | `view_82` `view_83`           |

Baseline: **39 scenes, 52 views**, every view present in its page's `groups`.

Chain B exists as the control. Chain A is spent; chain B is left untouched so the same
calls can be replayed after the fix without rebuilding anything.

Every level-2 and level-3 page was created by the **object form** of a `child_page` rule,
not by hand in the builder - which is itself the measurement that retired the earlier
claim that only the builder can create a page.

### What the guard said before anything was touched

`knack_list_page_referrers` on `scene_85`, with descendants:

```
referrerCount: 1
referrers: [{ sceneKey: scene_69, viewKey: view_78 }]
```

**`view_71` is absent.** The `child_page` rule that owns the page is not counted as a
referrer at all, in a read-only tool, on the old build. That is defect 6 visible without
writing anything.

### Run 1 - strip the rule from `view_71` (level 2, which has a second referrer)

```json
{ "rules": { "submits": [ { "key": "submit_1", "action": "message", ... } ] } }
```

|                              |                                      |
| ---------------------------- | ------------------------------------ |
| `humanConfirmation`          | `not-required` - no prompt           |
| Result                       | `ok`, executed                       |
| Pages deleted                | **none**                             |
| `scene_85` after             | **byte-identical**, parent unchanged |
| `scene_87` after             | **byte-identical**                   |
| Only change in the whole app | the rule itself gone from `view_71`  |

### Run 2 - strip the rule from `view_75` (level 3, which has no other referrer)

Same patch shape, one level deeper.

|                     |                                          |
| ------------------- | ---------------------------------------- |
| `humanConfirmation` | `not-required` - no prompt               |
| Result              | `ok`, executed                           |
| Response            | `pagesKnackReportsDeleted: ["scene_87"]` |
| Pages destroyed     | `scene_87`                               |
| Views destroyed     | `view_76`, `view_77`                     |
| App after           | 38 scenes, 50 views                      |
| Chain B             | untouched                                |

**The production failure, reproduced.** No prompt, no refusal, a page and its views gone,
and the server learning of it only from Knack's own response.

### The condition nobody had stated

The two runs differ in exactly one thing: whether anything else linked to the child page.

> **A `child_page` rule deletes its page only when the rule is that page's last inbound
> reference.** With a second referrer, Knack re-parents the page onto that referrer and
> deletes nothing.

This was the guard's stated reasoning for the `transferred` class all along - and it had
never been measured. It now is, in both directions on one fixture.

It also settles whether the fix is over-broad. `collectChildPageSubmitRefs` puts the owned
page into the at-risk set, and then classification decides:

| Case            | Referrers | Class         | Fix does    | Measured         |
| --------------- | --------- | ------------- | ----------- | ---------------- |
| `view_71` strip | `view_78` | `transferred` | allows      | deleted nothing  |
| `view_75` strip | none      | `owned`       | **refuses** | deleted the page |

Both halves match. Killing the exemption for `move_view` alone, rather than everywhere,
is what makes the fix accurate instead of merely cautious - and had it been killed
everywhere, run 1 would now be refused for no reason.

### Pinned

`src/lib/incident-noahs-place.test.ts`, suite "live replay on the test app: the chain
fixture", asserts each run's measured outcome against the fixed code: run 1 writes and
succeeds; run 2 refuses with `HUMAN_CONFIRMATION_UNAVAILABLE`, names `scene_87` by key
and slug, and writes nothing.

### Still to do live

The running server loads its compiled `dist` from the **main** checkout, so these fixes
are not what answered the calls above. Replaying run 2 against chain B (`view_81`, owner
of `scene_88`, which has no other referrer) after a build and restart is the end-to-end
check. Expected: refused, `scene_88` named, `view_82` and `view_83` still present.

Chain B is deliberately left intact for exactly that.

## Tier 11 - Copy a view, then move the copy onto the original's own page

The sequence the app owner identified as the one that broke views, and the one Tier 10
did not cover: Tier 10 replayed a `child_page` rule being stripped, and every move tested
before it went to a _different_ page. This moves a copy onto the page its original still
sits on.

### Running the worktree build for real

The desktop client's server loads its compiled `dist` from the **main** checkout, so it
can never answer with worktree code - which is why Tier 10's runs were all answered by
the old build. The fix is not to merge first: spawn the worktree's own `dist/index.js`
and speak JSON-RPC to it over stdio.

The harness deliberately advertises **no elicitation capability**, matching the real
client, so `humanConfirmation.available` is false and a mutation the guard judges
destructive must be refused rather than prompted. `knack_list_apps` confirms which build
answered - check `serverBuild.moduleDir` and `git.branch` rather than assuming.

It needs two variables the desktop client also sets: `KNACK_APPS_DIR`, and
`KNACK_MCP_SECRETS_PATH` - the secrets are **not** at the home-directory default.

Both builds are therefore available at once: the MCP tools reach the old build, the
harness reaches the fixed one. Every row below says which answered.

### Fixture

`view_3`, a table on `scene_3` ("items"), already had the exact shape: it is the **sole**
referrer to two child pages of its own page, one of which parents a chain three deep.

```
scene_3  items
  view_3  table
    -> view-table-1-details  scene_13  (view_8)
         -> view-table-1-details2  scene_15  (view_10)
              -> view-table-1-details3  scene_16  (view_11)
    -> edit-table-1           scene_14  (view_9)
```

### Step 1 - a plain copy duplicates the whole subtree

`knack_copy_view view_3 scene_3 -> scene_61`, fixed build.

Created `view_85` and **four new pages** with four new views: `item-details`,
`tage--faade`, `final-child`, `item-edit`. The duplication follows the chain all the way
down, not just the directly linked pages. The copy's link columns point at the
duplicates, so original and copy share nothing.

### Step 2 - moving that copy onto `scene_3`

`knack_move_view view_85 scene_61 -> scene_3`, fixed build.

**Refused.** `HUMAN_CONFIRMATION_UNAVAILABLE`, "destroys 4 page(s)", each named by key,
name, slug and depth (0, 0, 1, 2), plus both link columns with their JSON paths.

The old build would have refused this too: the duplicates have exactly one referrer, the
view being moved, so they classify `owned` and were never spared. **No divergence here** -
which is worth stating, because it means this variant was never the dangerous one.

### Step 3 - the variant that diverges

The divergence needs the child page to have a _second_ referrer, which is what Noah's
Place actually had: the copied table and the original both pointed at the same pages.

`knack_copy_view` with `sharePages: true` produces exactly that - `view_90` on `scene_61`,
its link columns pointing at `scene_13` and `scene_14`, **the originals**, nothing
duplicated.

Moving `view_90` onto `scene_3`, fixed build: **refused**, naming `scene_13`, `scene_14`,
`scene_15`, `scene_16` - the live pages `view_3` still uses.

### Step 4 - the same shape on the old build

Re-staged against the disposable duplicates rather than those originals: a second view
(`view_91`) was given link columns into `item-details` and `item-edit`, so both classify
`transferred`. Then `knack_move_view view_85 scene_61 -> scene_3` through the **old
build**.

|                     |                                                                                                           |
| ------------------- | --------------------------------------------------------------------------------------------------------- |
| `humanConfirmation` | `not-required` - no prompt                                                                                |
| Result              | `ok`, executed                                                                                            |
| Pages deleted       | `scene_89` `item-details`, `scene_90` `tage--faade`, `scene_91` `final-child`                             |
| Pages created       | `scene_93` `item-details2`, `scene_94` `tage--faade2`, `scene_95` `final-child2`, `scene_96` `item-edit2` |
| Views deleted       | `view_86`, `view_87`, `view_88`                                                                           |
| Views created       | `view_92`, `view_93`, `view_94`, `view_95`                                                                |
| Links broken        | `view_91` -> `item-details`                                                                               |

**The incident, reproduced.** Knack deleted the subtree and rebuilt it under new keys and
new slugs beneath the target page. Nothing was lost in content; everything was lost in
identity, which is what breaks every reference pointing at the old slug.

### The guard also predicted the wrong thing

The old build's own response said:

```
pagesMovedToAnotherLink: [
  { sceneKey: scene_89, ..., nowReachedFrom: [{ sceneKey: scene_69, viewKey: view_91 }] },
  { sceneKey: scene_92, ..., nowReachedFrom: [{ sceneKey: scene_69, viewKey: view_91 }] }
]
```

It predicted `scene_89` would simply be re-parented onto `view_91` and survive.
`scene_89` was **deleted**, rebuilt as `scene_93` under a new slug, and `view_91`'s link
to it is now broken - the precise opposite of what the caller was told.

So the `transferred` class was not merely too generous on a move; the sentence it
produced was actively false. On the fixed build the report cannot appear for a move at
all: `sparedByClassification` returns false, those pages land in the doomed set, and
`transferredPages` is filtered against it - which the step 3 refusal confirms, its four
pages all in `childPages` with no `pagesMovedToAnotherLink` at all.

### Where `transferred` does still hold

Tier 10 measured it holding for an **update**: a `child_page` rule dropped from a page
with a second referrer deleted nothing. Both measurements together are the whole rule:

| Action                     | Second referrer exists | Knack                                     | Fix     |
| -------------------------- | ---------------------- | ----------------------------------------- | ------- |
| `update_view` drops a link | yes                    | re-parents, keeps the page                | allows  |
| `update_view` drops a link | no                     | deletes the page                          | refuses |
| `move_view`                | yes                    | **deletes and rebuilds under a new slug** | refuses |
| `move_view`                | no                     | deletes and rebuilds                      | refuses |

A move is not a link removal. That is the whole of defect 1, and it took a copy, a
share-copy and two moves on a disposable app to state it in one line.

### Fixture left behind

The test app is now carrying the wreckage on purpose: `scene_93`-`scene_96` with
`view_92`-`view_95`, the dangling `view_91` -> `item-details` column, `view_90` on
`scene_61` sharing the originals, and chain B from Tier 10. Worth clearing before the
next tier run, and worth keeping until this one is reviewed.

## Tier 12 - What the relink actually breaks

Tier 11 chased the _links_ on a copied view, on the reported symptom that a copy "tried
to relink and something in the relink caused the issue". Two hypotheses about the links
were tested and both are wrong. The real defect is in the layout.

### Rejected: referrer count changes how a copy treats a link

`view_3` was plain-copied twice, same source, same target, the only difference being how
many views referenced its child pages.

| Referrers on `scene_13` / `scene_14` | Result                                  |
| ------------------------------------ | --------------------------------------- |
| 1 (only `view_3`)                    | all four pages duplicated, links intact |
| 2 (`view_3` and `view_90`)           | all four pages duplicated, links intact |

No link was cleared either time, and the copy's link columns pointed at the duplicates in
both runs. **Referrer count does not affect the relink.** A page with more referrers is
not shared instead of duplicated either.

### Measured: a plain copy is added to every row of the target page's layout

Isolated down to the smallest case that still shows it - a **link-free `rich_text` view**,
so no child pages, no link columns, nothing but the copy itself - onto `scene_69`, which
had a five-row layout:

```
before: [[view_71], [view_72], [view_78], [view_84], [view_91]]
after:  [[view_71, view_101], [view_72, view_101], [view_78, view_101],
         [view_84, view_101], [view_91, view_101]]
```

One copied view, present in all five rows, rendering **five times**.

This server sends no layout on a plain copy. The whole request body is:

```json
{
    "action": "copy",
    "target_scene_key": "...",
    "view_key": "...",
    "completeViewSchema": false
}
```

So the injection is Knack's `copyview` endpoint, and no caller can ask for it not to
happen.

**It only bites a page that already has an explicit layout.** `groups: []` means "render
every view", Knack writes nothing, and there is nothing to corrupt - which is why the
first copy measured in Tier 11 looked clean (`scene_61` had `groups: []` at the time) and
a later one on the same page did not. The `sharePages` copy in between is what gave that
page an explicit layout.

The mirror image of defect 3. A move writes **no** layout, so the view renders nowhere; a
copy writes it into **every row**, so it renders everywhere. Both stayed invisible for as
long as nothing read `scene.groups`.

### The repair

`ensureCopiedViewRendersOnce`, alongside `ensureMovedViewIsRendered`, with
`buildRepairedCopyLayout` as the pure part.

Adding the key to each row is the _only_ change the endpoint makes to the layout, so
removing every occurrence reconstructs the page's pre-copy layout exactly. That is what
makes the second step defensible rather than a preference: this is not rearranging
someone's page, it is undoing an injection and then doing what a move does - appending
one full-width row, byte-identical to what the move repair appends.

Three decisions worth stating, because each had a plausible alternative:

- **Not "keep the first occurrence".** Knack put the key in every row, so the first
  carries no intent - it is an artefact of iteration order. Keeping it would dress an
  arbitrary pick up as a decision.
- **A row that empties out is kept, not dropped.** Every row Knack injected into already
  held something, so a row can only empty if it was already empty before the copy.
  Dropping it would delete a row someone arranged, to fix a problem they did not cause.
- **An occurrence surviving the strip declines the repair entirely.** It means the layout
  holds a shape the walk did not handle; appending on top would leave the view rendering
  twice, which is the bug being fixed. Better to report it and name the manual fix.

### The verb was wrong, in two places, and tests did not catch it

The first live run returned:

```
layoutRepair: failed
layoutNote: ... layout could not be corrected (status 400) ...
```

Both layout repairs used `PUT`. `/scenes/{key}/views/sort` answers `PUT` with a **400**;
`knack_update_view_order` - the only caller that had ever written a layout for real - had
always used `POST`.

The move repair carried the same wrong verb and **had never been executed live**. Every
move run against the fixed build in Tiers 10 and 11 was refused by the guard before
reaching it, and its unit tests use a fake context that accepts any method. So defect 3's
fix was never actually exercised end-to-end, and looked green throughout.

Worth generalising: a fake context that accepts any verb, any path and any body will
confirm whatever the code does. It tests the shape of a call, never its correctness. Only
the live run distinguished them.

### Verified live, after the fix

Same copy, rebuilt server:

```
layoutRepair: deduplicated
scene_69 groups: [[view_71, view_101, view_102], ..., [view_103]]
```

`view_103` stripped from all five rows and appended once. The `view_101` / `view_102`
corruption from the copies made _before_ the fix is untouched, correctly - the repair
owns only its own copy - and was then cleared with `knack_update_view_order`.

### Cost

The copy path now reads metadata a third time, to see whether the endpoint injected the
key. `view-mutations.test.ts` asserts the exact count rather than "at least two", so a
fourth read cannot appear unnoticed.

## Tier 13 - The move back, and the trap the old guard built

The full sequence as the app owner described it: a copy that **relinks** rather than
duplicates, moved away, then moved **back**. Tier 11 covered a copy moved once. This runs
all three legs and measures each.

### Fixture

`view_96`, a table on `scene_61`, sole owner of `item-details` (`scene_97`, parenting a
chain three deep) and `item-edit3` (`scene_100`). `view_91` on `scene_69` also links to
`item-details`, so that page starts with **two** referrers.

Leg 1: `knack_copy_view` with `sharePages: true`, source and target both `scene_61` - the
copy lands on the **original's own page**, which is what happened in production. It
created `view_104`, sharing `scene_97` and `scene_100`, duplicating nothing.

`scene_97` now has three referrers: `view_96` (the original), `view_91`, and `view_104`
(the copy).

### Leg 2 - move the copy away, old build

`knack_move_view view_104 scene_61 -> scene_67`.

What the response **said**:

```
pagesMovedToAnotherLink: [
  { sceneKey: scene_97, nowReachedFrom: [view_96 (scene_61), view_91 (scene_69)] },
  { sceneKey: scene_100, nowReachedFrom: [view_96 (scene_61)] }
]
```

Two surviving referrers named for `scene_97`. What it **did**, in the same response:

```
pagesKnackReportsDeleted: ["scene_97", "scene_98", "scene_99"]
```

|                                         |                                                                                     |
| --------------------------------------- | ----------------------------------------------------------------------------------- |
| `humanConfirmation`                     | `not-required` - no prompt                                                          |
| Pages deleted                           | `scene_97`, `scene_98`, `scene_99`                                                  |
| Views deleted                           | `view_97`, `view_98`, `view_99`                                                     |
| Pages created                           | `item-details3`, `tage--faade3`, `final-child3`, `item-edit4`, under `menu-scene-5` |
| **`view_96` (the original, untouched)** | link to `item-details` **BROKEN**                                                   |
| **`view_91`**                           | link to `item-details` **BROKEN**                                                   |

**Two other referrers did not save the page.** Tier 11 showed one referrer failing to save
it on a move; this shows two failing. The `transferred` rationale does not degrade
gracefully with referrer count - on a move it is simply wrong.

And the harm lands on a view nobody asked to change. `view_96` was never named in the
call. An operation on the _copy_ broke the _original_. That is the production symptom
exactly.

### Leg 3 - move it back, old build

`knack_move_view view_104 scene_67 -> scene_61`.

**Refused.** `HUMAN_CONFIRMATION_UNAVAILABLE`, four pages named.

### The trap

| Leg     | Referrers on the child page           | Old build                                            |
| ------- | ------------------------------------- | ---------------------------------------------------- |
| 2, away | three (original, `view_91`, the copy) | **allowed** - destroyed three pages, broke two links |
| 3, back | one (only the copy)                   | **refused**                                          |

The old guard had it exactly backwards. It permitted the move that did the damage and
blocked the one that would have undone it - and it blocked the return **because** the
forward move had already stripped the page of every other referrer.

So the operator is left stranded: the destructive leg passes silently, and the tool then
refuses to let them move it back. That is why the incident felt like it broke on the move
back. The move back is where the wall is; the damage was already done on the way out.

### Both legs on the fixed build

| Leg     | Fixed build | Pages named                                                         |
| ------- | ----------- | ------------------------------------------------------------------- |
| 2, away | **refused** | `scene_97` `scene_100` `scene_98` `scene_99` (depths 0, 0, 1, 2)    |
| 3, back | **refused** | `scene_101` `scene_104` `scene_102` `scene_103` (depths 0, 0, 1, 2) |

Neither emitted `pagesMovedToAnotherLink`. The app was **byte-identical** before and after
both refusals, and `view_104` was still on `scene_67` with its subtree intact.

Symmetrical, which is the point. A move is a move whichever direction it runs, and the
fix does not care how many other views share the page.

### What the whole investigation reduces to

| Action                     | Other referrers | Knack                  | Old build  | Fix     |
| -------------------------- | --------------- | ---------------------- | ---------- | ------- |
| `update_view` drops a link | yes             | re-parents, keeps page | allows     | allows  |
| `update_view` drops a link | no              | deletes page           | **allows** | refuses |
| `move_view`                | none            | deletes, rebuilds      | refuses    | refuses |
| `move_view`                | one             | deletes, rebuilds      | **allows** | refuses |
| `move_view`                | two             | deletes, rebuilds      | **allows** | refuses |

Three rows were wrong, all in the same direction: the guard was most permissive exactly
where Knack was most destructive.

## Tier 14 - Can the MCP move views at all, and are these tests worth anything

Two questions from the app owner, both fair, both answered by measurement rather than
argument.

### Is `external` safe on a move? No.

The one classification never measured on a move. A page classified `external` is parented
under a **different page entirely**, so the reasoning was that moving a view that merely
links to it cannot disturb it.

Fixture: `view_109` on `scene_69`, one link column, pointing at `item-edit`
(`scene_92`) - which is parented under `scene_61`, not under `scene_69`. Moved to
`scene_55` on the **old build**.

The response contradicts itself in the same object:

```
linksRemovedPagesKept:    [{ sceneKey: scene_92, sceneSlug: item-edit,
                             parentSceneKey: scene_61 }]
pagesKnackReportsDeleted: ["scene_92"]
pagesCreated:             [{ sceneKey: scene_105, sceneSlug: item-edit5,
                             parentRef: test-move-table-3 }]
```

`scene_92` deleted, `view_89` on it deleted, rebuilt as `scene_105` under the **move's
target**. And three link columns on views nobody named in the call - `view_96` and
`view_91` - were left dangling.

So Knack re-parents a linked page onto the move's destination **regardless of where that
page currently lives**.

### All four classifications, measured

| Classification | Condition               | Knack on a move      | Tier |
| -------------- | ----------------------- | -------------------- | ---- |
| `owned`        | no other referrer       | deletes and rebuilds | 11   |
| `transferred`  | one other referrer      | deletes and rebuilds | 11   |
| `transferred`  | two other referrers     | deletes and rebuilds | 13   |
| `external`     | parented somewhere else | deletes and rebuilds | 14   |

Four for four. **There is no classification under which a move spares a page**, so the
blanket refusal for `move_view` is not caution - it is the only correct answer.

### So can the MCP still move views?

Counted across the production app, 675 views:

|                          | Views | Share   | Through the MCP      |
| ------------------------ | ----- | ------- | -------------------- |
| No page reference at all | 566   | **84%** | move normally        |
| Carries a page reference | 109   | 16%     | refused, no override |

Of the 109: 84 reference a page under their own page, 18 reference one elsewhere, 16
carry a reference this server cannot resolve.

The refusal is scoped to the 16% Knack would rebuild. It is also, on this client,
absolute: `humanConfirmation.available` is false, so there is no prompt to answer and no
override. For those views the Knack builder is the only safe route, and `previewOnly`
exists so the consequences can be read without accepting them.

That is a real cost, and it is worth being plain about: the fix makes a class of move
impossible through this server. The alternative is the behaviour measured above - a page
deleted, rebuilt under a new slug, and links broken on views the caller never mentioned.

### "If we tell the tests what to expect, how can they be any good?"

The objection is right about a class of test here, and the wrong-verb bug is the proof.
Both layout repairs used `PUT`, `/views/sort` answers `PUT` with a 400, and **786 tests
passed**. The fake context falls back to matching a canned response by path when no
`METHOD /path` key matches, so the wrong verb was indistinguishable from the right one.
The tests asserted the path and the body. Neither asserted the method.

What the example-based suites do and do not establish:

- **Do**: the guard's logic given a scene graph, and regression cover - reverting the
  `move_view` line fails 8 of them.
- **Do**: encode _observations_. The inputs are recorded payloads from the incident; the
  expectations are what the live app actually did, measured before the test was written.
  "Refuses the call that deleted `scene_87`" is not a preference.
- **Do not**: establish that the graph fed in matches what Knack returns, that the HTTP
  call is right, or that the tool is wired up. A fake that answers any verb, any path and
  any body confirms whatever the code does.

Three changes came out of it:

1. **The method is now asserted** on both layout repairs. Reverting `POST` to `PUT` fails
   those two tests, where before it failed none.
2. **A property suite** that asserts no specific value: it enumerates 4 parent shapes x 4
   referrer counts and claims one thing over all 16 - _a move never writes_. It also
   asserts the case count, so a generator that quietly stops producing cases cannot pass
   by testing nothing, and asserts the space spans at least three classifications, so the
   property cannot be vacuous.
3. **The live harness** (Tier 11) is now the thing that settles behaviour. Every claim in
   Tiers 10-14 about what Knack does was measured through it or through the metadata
   endpoint, not asserted in a unit test.

Both new safeguards were checked for teeth by breaking the code on purpose:

| Reverted                                    | Tests failing before | Tests failing after |
| ------------------------------------------- | -------------------- | ------------------- |
| `if (action === 'move_view') return false;` | 0                    | **8**               |
| `POST` back to `PUT`                        | 0                    | **2**               |

## Tier 15 - `remote`, and why the builder's moves are safe

The answer, and it invalidates the reasoning behind Tiers 11-14 while leaving their
measurements intact. Established with the app owner driving the builder and capturing its
request payloads.

### The owner's demonstration

Two tables on one page, `view_120` and `view_123`, pointing at the **same two child
pages**. Moving `view_123` in the builder, three times, both directions:

|                                         |                                                               |
| --------------------------------------- | ------------------------------------------------------------- |
| View key                                | kept                                                          |
| `scene_109` / `scene_110`               | present, same slugs, **still parented under the source page** |
| `view_121` / `view_122` (their content) | present, same keys                                            |
| Pages and views created or deleted      | none                                                          |
| All four links, on both tables          | resolving                                                     |
| App totals                              | 52 pages / 81 views before and after                          |

So a move does **not** inherently destroy linked pages. The earlier claim to the contrary
was wrong, and is retracted.

### The two request payloads, side by side

The owner captured both from the builder. The difference is one property:

```
view_62  -> scene_3     {"type":"link","scene":"tier-6-r1-child","header":"Child"}
  response: deletes.scenes [tier-6-r1-child]  inserts.scenes [tier-6-r1-child2, parent items]

view_123 -> scene_108   {"type":"link","scene":"table-1-details3","remote":true,...}
  response: deletes.scenes []                 inserts.scenes []
```

The builder destroyed a page too - on the view whose link column had no `remote` flag.

### The rule

> A link column's `remote` property records whether the view **owns** the page it points
> at, and it is the only thing that decides what a move does to that page.
>
> - **absent or false** - the view owns it. A move takes it along, which Knack implements
>   as delete-and-rebuild under the new parent: new key, new slug. Every reference to the
>   old slug then dangles, including from views nobody touched.
> - **`remote: true`** - the view merely links to it. A move leaves it alone.

Confirmed against the stored definitions: `view_123`'s columns carry `remote: true`;
`view_120`'s, pointing at those same two pages, do not. Every view destroyed in Tiers
11-14 had no `remote` flag.

### Confirmed as a lever, not just a signal

The measurement that makes this actionable:

```
STEP 1  PUT the view back with remote:true on its link columns          -> 200
STEP 2  POST scenes/{page}/copyview action:move  (identical to before)  -> 200
        deletes.scenes: []   inserts.scenes: []
        page present, same slug, same parent, view moved
```

The identical call that had destroyed the page on every previous attempt became
non-destructive. Nothing else changed.

### Four hypotheses that were wrong

All read out of the builder's own shipped bundle (`app.aa695976.js`,
`chunk-vendors.f14382f4.js`) and each tested against the live API:

| Hypothesis                                                 | Test                     | Result                  |
| ---------------------------------------------------------- | ------------------------ | ----------------------- |
| A dedicated `/scenes/{s}/views/{v}/move` route             | POST and PUT             | **404**, does not exist |
| `completeViewSchema` is the view definition, not a boolean | sent the full definition | **still destroyed**     |
| The `x-knack-new-builder` header the builder sends         | added it                 | **still destroyed**     |
| The builder's `/v1/account/{acct}/application/{app}/` base | POST                     | **404** to a REST key   |

The builder's API client, verbatim:

```js
async moveView(e, t, n, r) {
  const o = { action: "move", target_scene_key: t, view_key: n, completeViewSchema: r },
        s = { url: `scenes/${e}/copyview`, method: "POST", data: o };
  return this.axios(s)
}
```

**The same endpoint with the same body.** Its moves are safe because of what its stored
view definitions contain, not because of how it calls.

### What this changes in the guard

`sparedByClassification` for `move_view` no longer returns false outright. It spares a
page when **every** link carrying that reference is `remote: true`. One non-remote link is
an ownership claim and one is enough to rebuild the page, so one is enough to refuse. Menu
links are never spared: they carry no `remote` property, so there is no evidence to spare
them on.

Verified end-to-end through the worktree build against the live app:

| Case                       | Result                                                                                                   |
| -------------------------- | -------------------------------------------------------------------------------------------------------- |
| `view_130`, `remote: true` | **allowed**, page present with the same slug and parent, other view's link intact, `layoutRepair: added` |
| `view_129`, no `remote`    | **refused**, `scene_117 ab-child` named                                                                  |

### What survives from Tiers 11-14, and what does not

**Survives** - every measurement. The REST move really did delete and rebuild in all
those runs, really did break links on views nobody named, and the old build really did
report `pagesMovedToAnotherLink` for a page it then deleted. Those views all had no
`remote` flag, which is now the explanation rather than a puzzle.

**Does not survive** - the generalisation. "No version of a move leaves linked pages
alone" was false, and "referrer count is irrelevant so nothing can be spared" was the
right conclusion from the wrong axis. Referrer count _is_ irrelevant to a move. The axis
that decides it is ownership, and ownership is written down in the view.

### Still open

Whether the server should be able to _set_ `remote: true` on a caller's behalf before a
move - a "move the link, not the page" option. It is measured to work, and it is what the
app owner described wanting from the start. It also silently changes ownership: the page
stays under its old parent, which may no longer have anything linking to it, so it can end
up present but unreachable. Worth reporting rather than doing quietly, and worth the app
owner's decision rather than this file's.

## How to find out what the builder actually does

Written down because it took most of a day to work out, produced the single most
important finding in this file, and will be needed again. Four wrong hypotheses were
killed by it in about twenty minutes each once the method was in place.

### 1. Read the builder's own source first

It is unminified enough to grep and it does not require anyone's cooperation.

```
GET https://builder.knack.com/<account>/<app>/pages/<sceneKey>   (in a browser)
then, from that page:  [...document.querySelectorAll('script[src]')].map(s => s.src)
```

The two that matter are `assets.public.knack.com/production/js/app.<hash>.js` (the Vue
app - components, flows, what it computes before it calls) and
`.../chunk-vendors.<hash>.js` (the API client - actual URLs, methods and bodies). Both
fetch with plain `curl`, no auth. Grep the vendor bundle for the operation name:

```
grep -o 'async moveView[^}]*}' chunk-vendors.js
```

That is how the exact request was found:

```js
async moveView(e, t, n, r) {
  const o = { action: "move", target_scene_key: t, view_key: n, completeViewSchema: r },
        s = { url: `scenes/${e}/copyview`, method: "POST", data: o };
  return this.axios(s)
}
```

Grep the app bundle for the caller (`copyView` found `submitMoveCopy`), which shows what
the builder computes _before_ the call - role transfers, invalid-target checks, and the
separate `updateLayout` dispatch afterwards.

### 2. Take a baseline from the metadata endpoint, not the tool

```
GET https://api.knack.com/v1/applications/<appId>
```

Unauthenticated, immediate, and the only place `scene.groups` is visible. Snapshot before
and after and diff pages, views, slugs and parents. **This is the authority.** A tool
response describing what it did is not evidence; twice today a response named a page as
kept in the same object that reported it deleted.

### 3. Have the app owner drive the builder

They do the action; the diff in step 2 catches it whether or not anything else works. Ask
them to say which view and which direction, and take the baseline **before** they start.

### 4. Capturing the request itself - what works and what does not

- **`read_network_requests` on the browser tool: only shows preflights.** Three moves were
  performed and it returned nothing but `OPTIONS` and telemetry.
- **Wrapping `fetch` and `XMLHttpRequest` from the page console: works, but only in a tab
  you control.** The owner was working in their own tab, so it caught nothing but
  LogRocket traffic. Worth installing anyway; it is observation only and disappears on
  reload.
- **What actually worked: ask the owner to copy the request and response out of their own
  DevTools Network panel.** Two payloads pasted into the conversation settled in one line
  what four experiments could not.

Ask for that first next time.

### 5. Then replicate through the API and compare

Build a disposable fixture with the same shape, call the same endpoint with a REST key,
and diff. If the outcomes differ, the difference is in the request or the stored data -
bisect it one property at a time. Every hypothesis here was killed in a single call
because the fixture was disposable and step 2 answers instantly.

## The ownership model, as measured

One property decides everything, and it is readable:

> A link column's **`remote`** flag records whether the view owns the page it points at.

| Action              | link with no `remote` (owned)                                    | link with `remote: true`                    |
| ------------------- | ---------------------------------------------------------------- | ------------------------------------------- |
| **move**            | page **deleted and rebuilt** under the target: new key, new slug | page **untouched**                          |
| **copy**            | page **duplicated**, the copy repointed at the duplicate         | page **shared**, both point at the same one |
| **remove the link** | not isolated - see below                                         | not isolated - see below                    |

The copy row was measured on one table with two link columns pointing at **sibling child
pages of the same parent**, differing only in the flag: the owned one produced a new page
under the copy's target and the copy was repointed at it, while the remote one was shared
with no page created. So the flag governs copy as well as move.

Also measured: **setting the flag is a lever.** `PUT` the view back with `remote: true` on
its link columns, then re-run a move that had destroyed the page every previous time, and
the page survives untouched. That is "move the link, not the page", available through the
plain REST API.

### Not measured, and stated rather than assumed

**What removing a `remote` link does when it is the page's only referrer.** Two attempts
were confounded: the first page had a second link column, and on the second the slug had
been reused so a form's `child_page` rule also pointed at it. Isolating it needs a page
whose sole reference is one remote link column - which means creating the page _not_ via a
form rule, since that rule is itself a permanent second referrer.

Until then the guard's existing behaviour on link removal is unchanged: parentage plus
referrer count, which Tier 10 measured correct for `child_page` rules. It errs toward
refusing, so the cost of the gap is over-caution rather than damage.

## Before merging - what is worth doing and what is not

**Worth doing, cheap:**

- Report which links will duplicate and which will be shared on a copy, in the copy
  tool's own response. The data is already collected; only the wording is missing.
- Isolate the link-removal case above. One clean fixture answers it.

**Worth doing, not cheap:** exercising every view type and every route through the live
app and checking each shape against this server's model. Only tables, forms, details and
rich text were touched today; calendars, maps, reports, charts and menus were not. The
method above makes each one tractable, but it is a tier of its own, not a pre-merge task.

**Not worth blocking the merge:** the fixes in this branch are each measured, each pinned
by a test, and each strictly safer than what is on `main`. The remaining unknowns are
about being _less_ cautious than necessary, not about damage.

## Tier 16 - live acceptance of `abd638d`, and two things the commit message got wrong

Run 2026-09-11 against the disposable test app, which the owner had just cleared of most
fixtures, through the MCP client rather than a stdio harness. `knack_list_apps` reported
`main @ abd638d`, compiled, `sourceNewerThanBuild: false` - so this exercised the shipped
build and not a worktree copy, which is the check [[knack-mcp-server-dist-path]] exists
to force.

### What held

| Claim                                           | Fixture                                             | Result                                                    |
| ----------------------------------------------- | --------------------------------------------------- | --------------------------------------------------------- |
| #53: marking a sole claim `remote` is refused   | `view_3`'s Edit column, only referrer of `scene_14` | refused, naming `scene_14` **and** `scene_124` at depth 1 |
| Defect 1: no `transferred` exemption on a move  | `scene_13` given a second referrer                  | at risk under `move_view`, spared under `update_view`     |
| Defect 2: audience reported on a quiet mutation | the same transfer, executed                         | `audienceChanges` + `pagesMovedToAnotherLink`, no prompt  |
| Defect 3: a moved view renders                  | link-free `view_139`, `scene_7` -> `scene_9`        | `layoutRepair: "not-needed"`, nothing stranded            |
| Defect 4: stranded views are visible            | layout written without `view_138`                   | `pagesWithUnrenderedViews`, then repaired                 |
| Defect 6: `child_page` rules count              | `view_140` -> `scene_125`, rule its only claim      | counted by `knack_list_page_referrers`; strip refused     |
| Defect 7: separators survive `keywordEdits`     | `_cls=[probe-a]\n<br />_notes=`                     | separator byte-identical, sibling untouched               |

Defect 1 is the one worth keeping. The same page, losing the same link, is at risk under
a move and spared under an update - and the spared answer names `view_138` as the view
that receives it. One fixture, both directions, no appeal to reasoning.

Defect 6 is the one that mattered most. `view_140` carries `columns: []` and `links: []`;
its only reference to `scene_125` is the submit rule. The refusal therefore cannot be
coming from anywhere else, which is what the production failure needed and did not get.

The retraction in Tier 9 is confirmed from the other side: `keywordEdits` persisted, read
back after a genuine cache refresh. `"changes": {}` does describe the response.

### What the commit message overstated

PR #52 says `previewOnly` "returns the full classification - pages at risk, audience
changes, the effective body". It returns the first. It returns neither of the others.

`audienceChanges` was assembled only after the write, on the executed path, while a
preview returns down the refusal path well before it - and `view-safety.ts` has no
concept of audience at all, so the guard could not have supplied it. Measured as a
matched pair: the identical transfer reported an `audienceChanges` row when executed and
nothing at all when previewed.

That inverts the point of defect 2. A re-parent that destroys nothing is the case where a
page silently changes who can reach it, and preview is the safe way to look at a mutation
before accepting it. The one route that could not see it was the careful one.

Fixed by routing both paths through one `readAudienceChanges`, so the preview and the
executed answer cannot drift apart again. The guard is untouched: this is assembled above
it, from the refusal's own `details`.

**The "effective body" half is withdrawn rather than built.** Returning it needs the
guard to surface its merged body through a refusal, and it is the least useful third of
the claim - the caller already knows what it sent, and the merge only differs on
`update_view`. Stated here so the sentence is not left standing.

### A tool name that outlived its tool

Six user-facing strings told callers to run `knack_refresh_cache`. v2 consolidated that
into `knack_cache` with `refresh: true`, which `MIGRATION.md` documents correctly - the
strings were simply missed. An agent that follows them calls a tool that does not exist,
and during this run that misdirection produced a silently ignored argument and a stale
read that looked, for a moment, like `keywordEdits` failing to persist.

Corrected in `lib/field-payload.ts`, `tools/schema.ts` and `tools/views.ts`. The comment
in `tools/context.ts` keeps the old name because it is describing the legacy behaviour.

### Left open

`knack_cache` with `refresh: true` and no `appKey` defaults to `target: "all"` and
re-persists metadata for every configured app, read-only production ones included.
Passing `appKey` scopes it. Documented, not changed - the all-apps warm may well be
someone's deliberate use.

## Tier 17 - details and list views, and what a copy really does to their pages

The tier Tier 16 left as "worth doing, not cheap": exercise view types beyond tables and
forms and check each shape against this server's model. Run 11 September against the test
app on `main @ abd638d`.

### Two of the four could not be built at all

`knack_get_view_payload_template` accepts `grid`, `table`, `form`, `details` and `list`.
**Calendar and search are refused at schema validation** — a clean refusal naming the
supported set, not a silent failure, but it leaves those types with no create path
through this server. They were left unmeasured rather than hand-built: a payload written
from a guess tests the guess, not Knack's shape, which is the one thing this tier is for.

### Details and list carry their links four levels down, and the guard sees them

Both keep page links at `columns[].groups[].columns[][]` as `type: "scene_link"`. Built
one of each owning a page, and the guard handled the depth without trouble:

- `knack_list_page_referrers` counted both nested links, `referrerCount: 1` each
- `move_view` refused both, naming `$.columns[0].groups[0].columns[0][1]` and
  `linkType: "scene_link"`

`MAX_WALK_DEPTH` is 24 and this nesting reaches about 8, so there is room to spare.

### The finding: a copy reported "duplicated" for pages Knack had shared

One operation, three views, flag absent in every case:

| View    | Link node            | Knack did                       | `onCopy` said |         |
| ------- | -------------------- | ------------------------------- | ------------- | ------- |
| table   | `type: "link"`       | duplicated, `scene_128` created | `duplicated`  | correct |
| details | `type: "scene_link"` | **shared**, no scene created    | `duplicated`  | wrong   |
| list    | `type: "scene_link"` | **shared**, no scene created    | `duplicated`  | wrong   |

Proof it shared: both child pages went from one referrer to two — original and copy
pointing at the same slug — and the app's scene count did not move.

Every clause of the note was false for those two: _"a new page with a new slug; the copy
points at it, the original still points at the old one."_ A caller acting on it would
believe the copy independent when the two views had just been left sharing a page.

The cause is a flag borrowed for the wrong question. `remote` answers "does this view
claim the page", which is the right input for the cascade guard; what decides whether
Knack clones the page is the link's node type. Same flag, two questions, one of them
wrong — and nothing in the response had been consulted, though the answer was sitting in
it. `changes.inserts.scenes` was present for the table copy and absent for both others,
and the server's own `pagesCreated` field got it right in all three.

`onCopy` is now read from those reported inserts, which is what the `sharePages` path had
been doing all along with `sharedPagesVerified: true`. `owned` still reports the flag: it
remains a true fact about the link, and the cascade guard still needs it.

The `sharePages: true` route was measured in the same run and was correct throughout —
`sharedPages`, `sharedPagesVerified: true`, no scene inserted, the page gaining a second
referrer.

### Knack put one copy into every row of the target layout

`layoutRepair: "deduplicated"` fired on the details copy: Knack's copyview endpoint had
put the new key into all four rows of the target page's layout, so it would have rendered
four times. Repaired automatically, and reported.

### Tests are not typechecked

`tsconfig.json` carries `"exclude": ["src/**/*.test.ts"]`, and the runner is `tsx`, which
strips types without checking them. Changing `summariseCopyLinkOwnership` to take a second
argument left six call sites passing one — and `tsc --noEmit` exited 0. The breakage
showed up only when the suite ran.

Not changed here; it is a decision about the project's build, not about this defect. Worth
knowing that a green typecheck says nothing about the tests.

### A search view breaks the confound, and the link-type rule survives it

The three cases above were consistent with two different explanations, and could not tell
them apart: every table carried `type: "link"` and every details or list view carried
`type: "scene_link"`, so "the link's node type decides" and "the view's type decides"
predicted the same thing every time.

A search view separates them. Built in the builder on the Items page — the MCP cannot
create one — it is a fourth view type, and it keeps its page links somewhere new again:
not in `columns`, which is empty, but nested in **`results.columns[]`**, carrying
`type: "link"` like a table's.

Measured 11 September, same plain copy as the others:

| View       | Link node            | Where                            | Knack did      |
| ---------- | -------------------- | -------------------------------- | -------------- |
| table      | `type: "link"`       | `columns[]`                      | duplicated     |
| **search** | `type: "link"`       | **`results.columns[]`**          | **duplicated** |
| details    | `type: "scene_link"` | `columns[].groups[].columns[][]` | shared         |
| list       | `type: "scene_link"` | same                             | shared         |

Search duplicated both its pages — `scene_131` and `scene_132`, each under the copy's own
target — while the originals kept theirs: `scene_129` still reports one referrer and it is
still the original view. So a view type that is neither table nor details still duplicates,
provided its links are `type: "link"`. The node type is what decides; the view type only
correlated with it.

The guard read the new location without help. `knack_list_page_referrers` counted both
links, and the move refusal named `$.results.columns[2]` and `$.results.columns[3]`. The
walk is generic over the attributes object rather than a list of known shapes, which is
why a sub-object nobody had measured cost nothing.

`layoutRepair: "deduplicated"` fired here too — Knack had put the copy into all five rows
of the target layout. That is now three of three plain copies where it did so, on three
different view types; it looks like what Knack's copyview endpoint always does to a
non-empty layout rather than an edge case.
