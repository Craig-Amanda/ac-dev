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

## Reading a row count

A count only means something next to a control. Round 3 read four counts in a context
that bound no logged-in account, and two of them were zeroes that could not be told apart
from a working filter returning nothing.

So for any case whose result depends on context — `authenticated_user`, `operator:
"user"`, `parent_source`, a connection scope on a details page:

- Put an **unscoped view of the same object on the same page**. Its count is the
  denominator, and without it a scoped count has nothing to be smaller than.
- Say **how the page was opened** — builder preview, or the live app signed in as a named
  test account. They bind different things, and a preview binds no account at all.
- Do not assume context binds page-wide from one result. In round 3 a page record bound
  on `scene_75` while no account bound anywhere in the same session.

## Start here — what is left, in the order worth doing it

Four live runs against the playground app are behind this file, plus two read-only
passes — one over a production app's metadata, one over its schema export. What remains
splits into work an agent can drive alone and work that needs a human at the keyboard,
and that split matters more than the case numbering.

**Can be driven by an agent, highest value first:**

| Order | Case         | Why it is first                                                                              |
| ----- | ------------ | -------------------------------------------------------------------------------------------- |
| 1     | N1           | The one behaviour shipped without a live check — `no_data_text` is automated-only            |
| 2     | V4           | Re-run on a page where record binding is proven, with a control view. One number decides it  |
| 3     | V6           | Work the three cheap explanations below the table before entertaining a new filter semantics |
| 4     | V7           | Needs a signed-in read, not a different payload                                              |
| 5     | C9, A10      | Non-destructive and never run                                                                |
| 6     | A1, A2 drift | Constructed payloads; no app state at risk                                                   |

**Needs a human present** — these raise a confirmation prompt or destroy pages, and an
agent must not answer a prompt or start one of these unattended: **C6**, **C8**
(`move_view`), **A3**, **A4** live, **A5**, and the recovery drill in section 6.

**Blocked, not open.** Do not spend a run on these:

- **M2** needs a client that does not advertise elicitation. None is available.
- **V3's naming half** needs a second connection between the same object pair. The
  playground has only one, so nothing there can separate "scoped through the field I
  named" from "scoped through the only field there is".
- **V5's correctness half** needs a hand-verified expected record set for a chosen
  parent, not a row count.

## 1. Test app setup

The shapes the guard reasons about, and what each one is there to catch. The playground
app already covers most of this — see the note under the table before building anything:

| #   | Shape                                                                                                                           | Why it matters                                                                        |
| --- | ------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------- |
| S1  | A table with two link columns ("View details" / "Edit"), where the detail page has its own child pages **2–3 levels deep**      | Descendant expansion; grandchildren were once silently dropped from the prompt        |
| S2  | A **details** view and a **calendar** view with child-page links                                                                | These are `type: "scene_link"` internally and were once invisible to the guard        |
| S3  | A **search** view with a link column in its results                                                                             | Link in a non-obvious container (`results.columns[]`)                                 |
| S4  | A **form** with a Link/URL field, and a submit rule redirecting to a page                                                       | Both are decoys: `type: "link"` with no `scene`, and a `scene` that is not navigation |
| S5  | A **menu** view with: links to its own child pages, a link to a top-level page that exists elsewhere, and an external URL entry | The original incident surface; owned vs external vs url in one view                   |
| S6  | One child page linked from **two different views**                                                                              | The `transferred` class (page re-parents instead of dying)                            |
| S7  | Accented page names (é, à, ç) and one page **renamed after creation**                                                           | Slug vs name drift; slug matching is case/trim-sensitive code                         |

**The playground app already carries one view of each type**, from the four runs behind
this file: tables, a menu (`view_22`), details (`view_193`), a form (`view_21`), search
(`view_285`) and a calendar (`view_288`). Do not rebuild it. What is _not_ established is
whether its link topology covers S1–S7 — the 2–3-level descendant chain, the
twice-linked child page, and the accented and renamed pages in particular. Check those
against the list above before treating a fixture as present.

`app.json` needs `allowViewMutation: true` and `allowDelete: true`. **A test agent must
never edit it** — that is the human operator's one-time setup, and a tool refusing
because a flag is off is a result to report, not an obstacle to route around.

## 2. Client capability matrix

| ID  | Case                                                        | Expected                                                                                                                  | Proven                                                                                                                                            |
| --- | ----------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------- |
| M1  | `knack_list_apps` from a client that advertises elicitation | Banner reports the client name and `cascadeDeleteBehaviour: prompts-human`                                                | **partly** — prompts were raised on all four live runs, so the capability was advertised and acted on; the banner's own wording was not read back |
| M2  | `knack_list_apps` from a client that does not               | `refuses`, and **every** destructive case below is refused with `HUMAN_CONFIRMATION_UNAVAILABLE` — never silently allowed | **automated** — no live run against a non-elicitation client                                                                                      |

Run the destructive sections (3, 4) once in each mode.

## 3. Correctness — does it do what it claims

**C1 and C2 are closed** for every link-bearing view type — table, menu, details, form,
search and calendar — and moved to `TESTED.md` with their per-type diffs. The one defect
they found (`no_data_text` never written) is fixed.

| ID  | Case                                                                        | Expected                                                                                 | Proven                                                                                                                                                                                                                          |
| --- | --------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| C6  | Cut the link to an **external** page (S5's link to the elsewhere page)      | No pages destroyed; response names the severed link; page survives in the builder        | **automated** only. The live menu run **re-sent** its external entry rather than cutting it, so the cut has never been executed. 211 refs classified `external` in live-sim                                                     |
| C8  | `delete_view` and `move_view` on a link-bearing view; `move_view` on a menu | Every link counts as dropped; full doomed set prompted; decline leaves everything intact | **live-sim** for `delete_view` — would prompt on 207 views, worst case 11 doomed pages. **—** for `move_view`: never executed and re-parenting on move is unmeasured, which is why the guard treats every link as dropped there |
| C9  | `copy_view` of a link-bearing view                                          | Allowed without prompt (source untouched); verify source pages intact after              | **—**                                                                                                                                                                                                                           |

## 4. Adversarial — try to get around the guard

| ID  | Case                                                                                                                     | Expected                                                                                                                           | Proven                                                                                                                                                                                                                                                                                                                            |
| --- | ------------------------------------------------------------------------------------------------------------------------ | ---------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| A1  | `links` array nested deep inside another property of an update payload                                                   | Treated as navigation; retention arithmetic still applies                                                                          | **—** for this exact shape. The rule is that the innermost _named_ array must be `links` or `columns`, so a nested `links[]` should still count, but no test drives it from an arbitrary wrapper property                                                                                                                         |
| A2  | Scene ref written as `{key: "..."}` object instead of a string; slug in different case; slug with surrounding whitespace | All resolve; no page silently reclassified                                                                                         | **automated** for the object form. **—** for case and whitespace drift: the code trims and lowercases, and nothing asserts it. **live-sim** confirms slug-and-key resolution on real metadata (the `view_109` case that once lost two grandchildren)                                                                              |
| A3  | Replace one _broken_ (unresolvable) link with a **different** broken link in the same payload                            | Known weak spot: unreadable links are counted by tally, not identity. Document what happens                                        | **known limit, now documented in code.** The tally nets zero drops and nothing is asked. Accepted because naming the swap needs an identity the link does not have, and the alternative — any change to an unreadable link asks — is the permanent refusal this replaced. Still worth executing once to record the real behaviour |
| A4  | Put a `scene` ref inside a form **submit rule** in the payload while dropping the real navigation link in the same PUT   | Must still prompt — a rule redirect is not a retained link (regression: this exact hole was fixed)                                 | **automated** — two tests, including one at the guard boundary; reverting the fix fails both. Never run live, and it is the highest-value adversarial case for that reason                                                                                                                                                        |
| A5  | Race: edit the view in the **builder**, then immediately send a title edit via MCP                                       | Known weak spot: the merged body is built from a metadata read and may overwrite the builder change. Document exactly what is lost | **—**. Narrowed but not closed: the guard now takes **one** metadata read per mutation, so the view, scene tree, referrer graph and snapshot cannot straddle someone else's edit _within_ a run. The window between that read and the `PUT` remains                                                                               |
| A10 | `create_view` with a payload carrying links to existing pages                                                            | Allowed (nothing replaced); verify the linked pages are unaffected                                                                 | **—**. `create_view` is deliberately exempt from the cascade check, on the grounds that a create replaces nothing. That reasoning is untested against Knack                                                                                                                                                                       |

## 5. New behaviour not yet run live

| ID  | Case                                                                                                                                      | Expected                                                                                              | Proven                                                                                                                                                                                                                                                                         |
| --- | ----------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| N1  | Create a table and a list from `knack_get_view_payload_template`, read both back, and open a page with no matching records in the builder | `no_data_text` comes back as posted, and the view renders that line rather than Knack's stock message | **automated** at commit `1a837c9`, and reverting any of the three rules fails the suite. **Never run live.** The export says the key is stored on 223 of 223 views that carry it, but nothing has confirmed Knack accepts a value we post or that the view actually renders it |
| N2  | Convert a details view to a list with `knack_get_view_payload_template_from_view`, passing no `noDataText`                                | The clone gains a derived line, since a details source carries no such key                            | **automated** only. The conversion path itself has never been run against Knack                                                                                                                                                                                                |

Both are cheap, need no confirmation prompt, and destroy nothing — which is why they are
first in the run order above. N1 is the only behaviour in the server shipped without a
live check.

## 6. Recovery drill (run once per release)

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

## 7. View sources — connections and filters

A separate surface from the cascade guard: not "what does a mutation destroy" but "does
the payload we build describe the records the user asked for". It matters most when
**copying or moving a view and then repointing it**, where a wrong source returns
plausible rows through the wrong relationship and looks like success.

Most of this was settled on 2 September from a production app export of 738 views, and
is recorded in `KNACK_VIEW_SOURCE_SHAPE` in `src/server.ts` with its counts and gaps.
The cases below are what the export could **not** settle.

| ID  | Case                                                                                                                                               | Expected                                                                                    | Proven                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| --- | -------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| V1  | Create a connection-scoped view from `knack_get_view_payload_template`, then read the stored view back and diff its `source` against what was sent | Knack stores what we posted, or the diff names exactly what it rewrote                      | **live, in part** — created at commit `26983b0`: `object`, `connection_key` and `relationship_type` all came back identical to what was posted, on two unrelated objects. **Three keys, not the whole block.** `sort` is separately ruled out as a silent rewrite by 192 stored counter-examples in the export, but the remaining keys are unexamined, so a read-back is still the honest check for a shape not seen before                                                                                                                                                                                                                                                                            |
| V3  | Two objects joined by **more than one** connection; scope a view through a named one                                                               | Only records reached by _that_ connection appear                                            | **partly — scoping passes, naming blocked.** `scene_28`/`view_276` returned **1 of 1**: the single child record belonging to the page's parent record, from a set where at least two parents hold children. An unscoped source would have shown more, so the connection does scope. **Naming itself stays untested and blocked**: only one connection joins that object pair in this app, so nothing here separates "scoped through the field I named" from "scoped through the only field there is"                                                                                                                                                                                                   |
| V4  | Repoint a **copied** view at a different connection, recomputing `relationship_type`                                                               | Rows follow the new connection; `relationship_type` matches which object owns the new field | **live, still inconclusive — but the doubt has moved.** `view_277` was restored to `scene_83` and returned **5**, the same as the deliberately-wrong `view_278`. That selects "the page supplies no bindable parent record" over "the wrong `relationship_type` voids the scope" — **except** that the same session's two user-scoped views also returned 0, so the read context bound no account, and what `scene_83` binds is itself unmeasured. Knack accepting the mismatch without error still stands. **What settles it:** run both arms on `scene_75`, where the multi-hop case returned a non-zero count and a page record is therefore known to bind, with an unscoped control view alongside |
| V5  | A table on a details page scoped to the **page's** record rather than the logged-in account                                                        | Correct related records for a chosen parent; parent page and siblings untouched             | **live, page-record scoping confirmed** — `parent_source` stored as posted, and Knack's own builder described the view in prose as records connected to the same intermediate record connected to this page's record, which is the case's question answered in its own words. Returned 1 row with no page moved or lost. **Still open:** whether those are the _right_ related records for a chosen parent — a count is not correctness                                                                                                                                                                                                                                                                |
| V6  | Filter semantics — one top-level rule plus one group, sent once as `match: "all"` and once as `"any"`                                              | With `all`, groups are OR; with `any`, groups are AND                                       | **export: strongly evidenced. The live A/B run does not reconcile.** `view_279` (`all`) returned **2** and `view_280` (`any`) returned **12**, with the stored criteria matching what was posted. Neither number matches any of the three readings the fixture was built to separate — inversion predicted (4, 11), flattened (1, 46), groups-ignored (5, 5) — and both sit strictly _between_ inversion and flattened. Do not read a fourth semantics out of that yet: three cheaper explanations come first, listed below the table                                                                                                                                                                  |
| V7  | `operator: "user"` as a filter rule on a connection field, with no `authenticated_user` on the source                                              | Scopes to the logged-in account by the second, independent mechanism                        | **export: 60 occurrences. The live run is void — no account was bound.** `view_283` (`operator: "user"`) and `view_282` (`authenticated_user: true`) both returned **0**, against a predicted 11 connected and 55 unscoped. Two zeroes are equally consistent with the mechanisms agreeing and with neither being exercised, so this settles nothing either way. **What settles it:** read the counts from the live app while signed in as a test account that holds connected records — not from a builder preview — with an unscoped control view on the same page                                                                                                                                   |

### V6 — rule out the cheap explanations before mapping a new semantics

`(2, 12)` fitting none of the three readings is more likely to be a measurement problem
than a fourth filter semantics. Work these in order; each is cheaper than the sweep that
would follow.

1. **Enumerate the fixture from the data, not by hand.** Pull the records with
   `knack_find_records` and print each one's A/B/C truth values, then recompute all five
   counts from that listing. The same run's other predictions (11 connected, 55 unscoped)
   were also wrong against reality, which is the first sign the arithmetic — not Knack —
   is the thing that does not match.
2. **Diff the stored criteria for keys Knack _added_,** per rule, not just for the keys
   that match what was posted. Knack filter rules can carry their own join key, and a
   default written on save would make the effective expression neither inversion nor
   flattened. "Persisted sources matched the posted criteria" does not rule this out —
   nobody looked for additions.
3. **Read `source.limit` and the rendered page size.** A truncated count reads exactly
   like a narrower filter.

Only if all three come back clean is `(2, 12)` a genuine unmapped semantics. The
follow-up then is a sweep that varies one thing at a time — one rule alone, two rules
alone, one group of one rule, one group of two — rather than another two-view A/B.

### Creating views on one page: sequentially, and re-derived

Recorded here because it cost a view in round 3 and will do it again. `pageGroups`
replaces a page's whole layout, and the layout a template derives is a read taken once.
Two creates issued in parallel against the same page therefore both derive the layout as
it stood before either ran, and the second one's write removes the first one's view from
the page — it still exists, and nothing renders it. `view_281` was lost this way; the
sequential retry, re-deriving between creates, kept every view.

The stale-`existingViewKeys` warning does not catch this: it fires when a caller passes a
list that is missing a view, not when two derivations race.

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

| Date  | Commit tested          | App                                           | Client(s)                  | Sections run                                                                                       | Pass / findings                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| ----- | ---------------------- | --------------------------------------------- | -------------------------- | -------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 1 Sep | `8c28bb5` (pre-review) | `NP Place Playground` (purpose-built fixture) | elicitation-capable        | C1, C3–C5, C7 — tables `view_239`/`view_267` and menu `view_22`, four real `PUT`s                  | Pass. Reversed the PR's founding premise: a re-sent link destroys nothing, and a page dies only with its **last** link                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| 2 Sep | `1c52a34`              | production app, 963 scenes / 1,889 views      | n/a (read-only simulation) | C1, C8 (`delete_view`), A2, A7 — guard functions over live metadata                                | Pass. 402 views title-edited with zero prompts; 315 owned / 211 external / 41 transferred / **1 unknown**; `delete_view` worst case 11 doomed pages                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| 2 Sep | `9ce6f57` (analysis)   | production app export, 738 views              | n/a (offline export)       | Section 7 — source and criteria shapes read from a schema export                                   | Pass. Four source patterns catalogued; `relationship_type` ownership rule clean across 102 sources; `value_field` absent from all 738 sources                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| 3 Sep | `26983b0`              | `NP Place Playground`                         | elicitation-capable        | V1–V5 of the view-source section — five views created, each read back                              | Knack stored the three compared keys as posted, on two objects. **Finding:** it accepted a `relationship_type` contradicting connection ownership without error. V3/V4/V5 row correctness outstanding; V6–V8 not run                                                                                                                                                                                                                                                                                                                                                                                                       |
| 3 Sep | `37cd3f2`              | `NP Place Playground`                         | elicitation-capable        | V3a re-run on a data-bearing fixture; V4 arms split into two views; row counts read in the builder | V3 passes: 1 of 1, correctly scoped. V4 inconclusive — the wrong arm returned all 5 records and the correct arm's count is still missing. **Two tool findings:** explicit `fieldKeys` suppressed real column headers, and a stale `existingViewKeys` silently dropped a view from its page                                                                                                                                                                                                                                                                                                                                 |
| 4 Sep | `ed68454`              | `NP Place Playground`                         | elicitation-capable        | R1/R1b, V6, V7, V8, and C1/C2 on details, form, search, calendar                                   | C1/C2 closed for all four remaining view types, each diffed against the builder's own save request. V8 passes: a multi-hop source resolved through the parent hop (3 rows vs 0 predicted for the alternative). R1b passes live. **Defect found:** no generated table or list carried `no_data_text`, so Knack stored `""` and every view fell back to its stock empty-state line. V4 still inconclusive, V6 does not reconcile with any predicted reading, V7 void — no account was bound at read time. **Operational finding:** two parallel creates on one page raced their derived `pageGroups` and detached `view_281` |
|       |                        |                                               |                            |                                                                                                    |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
