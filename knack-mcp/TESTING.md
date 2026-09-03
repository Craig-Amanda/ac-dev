# Manual test plan — view-mutation safety guard

This is the acceptance and regression checklist for the view-safety guard
(`src/view-safety.ts` and the six view tools in `src/server.ts`). It covers what the
automated suite cannot: real Knack behaviour, builder round-trips, elicitation UX, and
recovery. Run it against a **disposable test app only** — several steps deliberately
destroy pages. Never point any of this at a production app.

Each case has an ID (`C1`, `A3`, …). File one issue per finding and reference the ID.
Record every finding with: the exact tool call and payload, the full response, and a
builder screenshot before/after.

Append a row to the [results log](#results-log) at the end of every run.

## Scope of this file

**This file holds only what is still open.** Everything established — the cascade rule,
the guard's live and automated behaviour, the view-source shapes, and the shape audit —
is recorded in [`TESTED.md`](./TESTED.md) with its evidence.

Read `TESTED.md` first. Re-deriving something settled there is the main way a run wastes
its time, and the second way is treating a partial result as a full one, which is what
the markings below exist to prevent.

| Marking       | What it means                                                                                |
| ------------- | -------------------------------------------------------------------------------------------- |
| **live**      | A real request against a real app, with a human answering any confirmation                   |
| **export**    | Read from a schema export — strong about shape, silent about what Knack does with a request  |
| **automated** | Pinned by the test suite; sufficient alone only where the path refuses before reaching Knack |
| **partly**    | Some arm of the case is settled and some is not. The row says which                          |
| **—**         | Not established                                                                              |

**You cannot report whether a confirmation prompt appeared.** Elicitation goes to the
client and never returns to the calling model. Report the response; leave the prompt to
the human. Three earlier runs were invalidated by an agent reporting "no prompt was
raised" when one had been.

## 1. Test app setup

Build a small app containing one of each shape the guard reasons about:

| #   | Shape                                                                                                                           | Why it matters                                                                        |
| --- | ------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------- |
| S1  | A table with two link columns ("View details" / "Edit"), where the detail page has its own child pages **2–3 levels deep**      | Descendant expansion; grandchildren were once silently dropped from the prompt        |
| S2  | A **details** view and a **calendar** view with child-page links                                                                | These are `type: "scene_link"` internally and were once invisible to the guard        |
| S3  | A **search** view with a link column in its results                                                                             | Link in a non-obvious container (`results.columns[]`)                                 |
| S4  | A **form** with a Link/URL field, and a submit rule redirecting to a page                                                       | Both are decoys: `type: "link"` with no `scene`, and a `scene` that is not navigation |
| S5  | A **menu** view with: links to its own child pages, a link to a top-level page that exists elsewhere, and an external URL entry | The original incident surface; owned vs external vs url in one view                   |
| S6  | One child page linked from **two different views**                                                                              | The `transferred` class (page re-parents instead of dying)                            |
| S7  | Accented page names (é, à, ç) and one page **renamed after creation**                                                           | Slug vs name drift; slug matching is case/trim-sensitive code                         |

Configure `app.json` with `allowViewMutation: true` and `allowDelete: true`.

## 2. Client capability matrix

| ID  | Case                                                        | Expected                                                                                                                  | Proven                                                                                                                                            |
| --- | ----------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------- |
| M1  | `knack_list_apps` from a client that advertises elicitation | Banner reports the client name and `cascadeDeleteBehaviour: prompts-human`                                                | **partly** — prompts were raised on all four live runs, so the capability was advertised and acted on; the banner's own wording was not read back |
| M2  | `knack_list_apps` from a client that does not               | `refuses`, and **every** destructive case below is refused with `HUMAN_CONFIRMATION_UNAVAILABLE` — never silently allowed | **automated** — no live run against a non-elicitation client                                                                                      |

Run the destructive sections (3, 4) once in each mode.

## 3. Correctness — does it do what it claims

| ID  | Case                                                                                                                                                     | Expected                                                                                                                                                                                                             | Proven                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| --- | -------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| C1  | Title/description edit on **every** link-bearing view type (S1–S5)                                                                                       | Goes through with **no prompt**; page links intact after                                                                                                                                                             | **live: table, menu** — real `PUT` on both, title changed and every link intact afterwards; the menu (`view_22`) was checked link-by-link. **live-sim: all types** — title-only edit simulated on all 402 link-bearing views, zero prompts. Open: a real `PUT` on details, calendar, search, form                                                                                                                                             |
| C2  | After each C1 edit, open the view in the **builder** and check every setting survived: filters, sorts, column rules, form rules, display/design settings | Nothing silently reset. ⚠️ The rebuild-from-metadata was verified complete for **tables**; details, calendar and form are unverified, and on a menu only the links are — this is the highest-value check in the plan | **live: tables in full; menu partly** — two tables, configured differently, diffed against the builder's own save request; agreed on every key but `design`, which was `{}` on both. On the menu, the title edit preserved every link, but its **non-link** settings (`format: "tabs"`, label, display options) were never read back from the builder — that sliver, plus details, calendar and form in full, is the plan's highest-value gap |
| C6  | Cut the link to an **external** page (S5's link to the elsewhere page)                                                                                   | No pages destroyed; response names the severed link; page survives in the builder                                                                                                                                    | **automated** only. The live menu run **re-sent** its external entry rather than cutting it, so the cut has never been executed. 211 refs classified `external` in live-sim                                                                                                                                                                                                                                                                   |
| C8  | `delete_view` and `move_view` on a link-bearing view; `move_view` on a menu                                                                              | Every link counts as dropped; full doomed set prompted; decline leaves everything intact                                                                                                                             | **live-sim** for `delete_view` — would prompt on 207 views, worst case 11 doomed pages. **—** for `move_view`: never executed and re-parenting on move is unmeasured, which is why the guard treats every link as dropped there                                                                                                                                                                                                               |
| C9  | `copy_view` of a link-bearing view                                                                                                                       | Allowed without prompt (source untouched); verify source pages intact after                                                                                                                                          | **—**                                                                                                                                                                                                                                                                                                                                                                                                                                         |

## 4. Adversarial — try to get around the guard

| ID  | Case                                                                                                                     | Expected                                                                                                                           | Proven                                                                                                                                                                                                                                                                                                                            |
| --- | ------------------------------------------------------------------------------------------------------------------------ | ---------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| A1  | `links` array nested deep inside another property of an update payload                                                   | Treated as navigation; retention arithmetic still applies                                                                          | **—** for this exact shape. The rule is that the innermost _named_ array must be `links` or `columns`, so a nested `links[]` should still count, but no test drives it from an arbitrary wrapper property                                                                                                                         |
| A2  | Scene ref written as `{key: "..."}` object instead of a string; slug in different case; slug with surrounding whitespace | All resolve; no page silently reclassified                                                                                         | **automated** for the object form. **—** for case and whitespace drift: the code trims and lowercases, and nothing asserts it. **live-sim** confirms slug-and-key resolution on real metadata (the `view_109` case that once lost two grandchildren)                                                                              |
| A3  | Replace one _broken_ (unresolvable) link with a **different** broken link in the same payload                            | Known weak spot: unreadable links are counted by tally, not identity. Document what happens                                        | **known limit, now documented in code.** The tally nets zero drops and nothing is asked. Accepted because naming the swap needs an identity the link does not have, and the alternative — any change to an unreadable link asks — is the permanent refusal this replaced. Still worth executing once to record the real behaviour |
| A4  | Put a `scene` ref inside a form **submit rule** in the payload while dropping the real navigation link in the same PUT   | Must still prompt — a rule redirect is not a retained link (regression: this exact hole was fixed)                                 | **automated** — two tests, including one at the guard boundary; reverting the fix fails both. Never run live, and it is the highest-value adversarial case for that reason                                                                                                                                                        |
| A5  | Race: edit the view in the **builder**, then immediately send a title edit via MCP                                       | Known weak spot: the merged body is built from a metadata read and may overwrite the builder change. Document exactly what is lost | **—**. Narrowed but not closed: the guard now takes **one** metadata read per mutation, so the view, scene tree, referrer graph and snapshot cannot straddle someone else's edit _within_ a run. The window between that read and the `PUT` remains                                                                               |
| A10 | `create_view` with a payload carrying links to existing pages                                                            | Allowed (nothing replaced); verify the linked pages are unaffected                                                                 | **—**. `create_view` is deliberately exempt from the cascade check, on the grounds that a create replaces nothing. That reasoning is untested against Knack                                                                                                                                                                       |

## 5. Recovery drill (run once per release)

1. Take a `knack_snapshot_app`.
2. Deliberately accept a cascade delete of a 3-page subtree (S1's detail branch).
3. Rebuild those pages **using only the snapshot file**.

If the rebuild needs information the snapshot does not hold, that is a finding — the
snapshot's entire purpose is this drill.

**Proven: —.** Snapshots have been written on every guarded mutation and their contents
inspected (C10), but no page tree has ever been rebuilt from one. That makes this the
only case in the plan where the _whole_ recoverability claim is untested rather than
partly covered, and the two things most likely to surface here are already known: the
object schema is a `schemaPath` pointer rather than embedded data, and snapshots are
never pruned. Neither is a finding; needing anything _else_ is.

## 6. View sources — connections and filters

A separate surface from the cascade guard: not "what does a mutation destroy" but "does
the payload we build describe the records the user asked for". It matters most when
**copying or moving a view and then repointing it**, where a wrong source returns
plausible rows through the wrong relationship and looks like success.

Most of this was settled on 2 September from a production app export of 738 views, and
is recorded in `KNACK_VIEW_SOURCE_SHAPE` in `src/server.ts` with its counts and gaps.
The cases below are what the export could **not** settle.

| ID  | Case                                                                                                                                               | Expected                                                                                    | Proven                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| --- | -------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| V1  | Create a connection-scoped view from `knack_get_view_payload_template`, then read the stored view back and diff its `source` against what was sent | Knack stores what we posted, or the diff names exactly what it rewrote                      | **live, in part** — created at commit `26983b0`: `object`, `connection_key` and `relationship_type` all came back identical to what was posted, on two unrelated objects. **Three keys, not the whole block.** `sort` is separately ruled out as a silent rewrite by 192 stored counter-examples in the export, but the remaining keys are unexamined, so a read-back is still the honest check for a shape not seen before                                                                                                                                                                                               |
| V3  | Two objects joined by **more than one** connection; scope a view through a named one                                                               | Only records reached by _that_ connection appear                                            | **partly — scoping passes, naming blocked.** `scene_28`/`view_276` returned **1 of 1**: the single Booking belonging to the page's Client, from a set where at least two Clients hold Bookings. An unscoped source would have shown more, so the connection does scope. **Naming itself stays untested and blocked**: only one Booking-to-Client connection exists in this app, so nothing here separates "scoped through the field I named" from "scoped through the only field there is"                                                                                                                                |
| V4  | Repoint a **copied** view at a different connection, recomputing `relationship_type`                                                               | Rows follow the new connection; `relationship_type` matches which object owns the new field | **live, inconclusive — and the shape of the doubt matters.** Both arms stored on `scene_83` as separate views: `view_277` correct with `local`, `view_278` deliberately wrong with `foreign`. **Knack accepted the mismatch without error.** `view_278` returned **5 of 5 — every Client**, so unscoped rather than mis-scoped. Two readings fit: the wrong `relationship_type` voids the scope, or that page supplies no bindable parent record and any scope there returns everything. Only `view_277`'s count on the same page separates them, and that is the view a stale `existingViewKeys` dropped from the layout |
| V5  | A table on a details page scoped to the **page's** record rather than the logged-in account                                                        | Correct related records for a chosen parent; parent page and siblings untouched             | **live, page-record scoping confirmed** — `parent_source` stored as posted, and Knack's own builder describes the view as "records connected to the same Client connected to this page's Risk Summary", which is the case's question answered in its own words. Returned 1 row with no page moved or lost. **Still open:** whether those are the _right_ related records for a chosen parent — a count is not correctness                                                                                                                                                                                                 |
| V6  | Filter semantics — one top-level rule plus one group, sent once as `match: "all"` and once as `"any"`                                              | With `all`, groups are OR; with `any`, groups are AND                                       | **export: strongly evidenced** — one real view carried `match: "all"` with a group of five equality tests on a single field, which only parses as OR. Open as a deliberate A/B run                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| V7  | `operator: "user"` as a filter rule on a connection field, with no `authenticated_user` on the source                                              | Scopes to the logged-in account by the second, independent mechanism                        | **export: 60 occurrences** — the mechanism exists and is separate from the source flag. Its behaviour relative to `authenticated_user` is unmeasured                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| V8  | A multi-hop source whose `parent_source.connection` differs from `connection_key`                                                                  | Rows resolve through the parent hop, not through `connection_key` twice                     | **—**. 5 of 8 observed `parent_source` blocks named a different hop, so the case is real; no run has confirmed what it returns                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |

### What the export already settled

Recorded so a run does not re-derive it. Full detail, with counts, lives in
`KNACK_VIEW_SOURCE_SHAPE`.

- **Four source patterns**, distinguished only by which keys are present: plain,
  connection-scoped, logged-in-user, multi-hop.
- **`relationship_type` follows ownership of the connection field** — `foreign` when it
  sits on the view's own object, `local` when it sits on the other object and points
  back. Clean across all 102 connected sources, no exceptions.
- **`connection_key` and `relationship_type` never appear apart.**
- **`authenticated_user` is only ever `true`**, and appears with or without a
  connection — a view scoped to the user's own record carries it alone.
- **`criteria` is an object**, `{ match, rules, groups }`, and `groups` is an array of
  rule arrays. The first block is `rules`, not group zero: a real view populated both.
- **`value_field` never appears in a source** — 0 of 738 views. All 55 occurrences are
  in view _rule_ criteria (records, emails, submits) with `value_type: "custom"`.
  Sorting is `source.sort`, a separate array. Do not look for a sort field inside a
  filter rule.

## Results log

Runs against this plan. Settled outcomes move to [`TESTED.md`](./TESTED.md); this table
keeps the chronology so a later run can be compared against an earlier one.

One row per full run. The two rows below are the evidence behind the **Proven**
column — neither was a full pass of this plan, and both are recorded so a later run
can be compared against them rather than starting from nothing.

| Date  | Commit tested          | App                                           | Client(s)                  | Sections run                                                                                       | Pass / findings                                                                                                                                                                                                                                                                            |
| ----- | ---------------------- | --------------------------------------------- | -------------------------- | -------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| 1 Sep | `8c28bb5` (pre-review) | `NP Place Playground` (purpose-built fixture) | elicitation-capable        | C1, C3–C5, C7 — tables `view_239`/`view_267` and menu `view_22`, four real `PUT`s                  | Pass. Reversed the PR's founding premise: a re-sent link destroys nothing, and a page dies only with its **last** link                                                                                                                                                                     |
| 2 Sep | `1c52a34`              | production app, 963 scenes / 1,889 views      | n/a (read-only simulation) | C1, C8 (`delete_view`), A2, A7 — guard functions over live metadata                                | Pass. 402 views title-edited with zero prompts; 315 owned / 211 external / 41 transferred / **1 unknown**; `delete_view` worst case 11 doomed pages                                                                                                                                        |
| 2 Sep | `9ce6f57` (analysis)   | production app export, 738 views              | n/a (offline export)       | Section 7 — source and criteria shapes read from a schema export                                   | Pass. Four source patterns catalogued; `relationship_type` ownership rule clean across 102 sources; `value_field` absent from all 738 sources                                                                                                                                              |
| 3 Sep | `26983b0`              | `NP Place Playground`                         | elicitation-capable        | V1–V5 of the view-source section — five views created, each read back                              | Knack stored the three compared keys as posted, on two objects. **Finding:** it accepted a `relationship_type` contradicting connection ownership without error. V3/V4/V5 row correctness outstanding; V6–V8 not run                                                                       |
| 3 Sep | `37cd3f2`              | `NP Place Playground`                         | elicitation-capable        | V3a re-run on a data-bearing fixture; V4 arms split into two views; row counts read in the builder | V3 passes: 1 of 1, correctly scoped. V4 inconclusive — the wrong arm returned all 5 records and the correct arm's count is still missing. **Two tool findings:** explicit `fieldKeys` suppressed real column headers, and a stale `existingViewKeys` silently dropped a view from its page |
|       |                        |                                               |                            |                                                                                                    |                                                                                                                                                                                                                                                                                            |
