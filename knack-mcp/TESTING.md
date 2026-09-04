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

| Order | Case         | Why it is there                                                                                  |
| ----- | ------------ | ------------------------------------------------------------------------------------------------ |
| 1     | P10          | A menu key this server does not preserve — the same shape of loss as no_data_text                |
| 2     | N1, N2       | The only behaviour in the server shipped without a live check — `no_data_text` is automated-only |
| 3     | V3           | Newly unblockable, and it is the case the source-building tools rest on                          |
| 4     | V4           | One row count decides it, given a page whose binding a control view confirms                     |
| 5     | V6           | Build S12 to the truth table below, then read one pair of numbers                                |
| 6     | V7           | Needs a signed-in read as one of S11's accounts, not a different payload                         |
| 7     | V5           | Correctness, not a count — write the expected rows down first                                    |
| 8     | C9, A10, P4  | Non-destructive and never run                                                                    |
| 9     | A1, A2 drift | Constructed payloads; no app state at risk                                                       |

P2 and P3 follow P1 — both are cheap once its answer is known, and neither makes sense
before it.

**Needs a human present** — these raise a confirmation prompt or destroy pages, and an
agent must not answer a prompt or start one of these unattended: **C6**, **C8**
(`move_view`), **A3**, **A4** live, **A5**, and the recovery drill in section 7.

**Blocked on the client, not the app.** **M2** needs an MCP client that does not
advertise elicitation. No app fixture changes that, so do not spend a run on it.

**Was blocked, unblocked by a fresh app.** Both failed on the last playground for want of
a fixture, and both are buildable now — see S8 and the data requirements in section 1:

- **V3's naming half** needs two connections between the same object pair (S8). With one
  connection, nothing separates "scoped through the field I named" from "scoped through
  the only field there is".
- **V5's correctness half** needs a hand-verified expected record set for a chosen parent
  rather than a row count. Write the expected rows down before creating the view.

Build for these while the app is being built. Retrofitting a second connection onto a
populated app is harder than putting one there at the start.

## 1. Test app setup

Build a small app containing one of each shape below. Every row names the open cases it
serves, so a shape with no case left against it can be skipped — and a case whose shape
is missing cannot be run, however tempting the payload looks.

**Structural shapes** — what the cascade guard reasons about:

| #   | Shape                                                                                                                           | Why it matters                                                                        | Serves                 |
| --- | ------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------- | ---------------------- |
| S1  | A table with two link columns ("View details" / "Edit"), where the detail page has its own child pages **2–3 levels deep**      | Descendant expansion; grandchildren were once silently dropped from the prompt        | C8, C9, A10, section 7 |
| S2  | A **details** view and a **calendar** view with child-page links                                                                | These are `type: "scene_link"` internally and were once invisible to the guard        | C9, N2                 |
| S3  | A **search** view with a link column in its results                                                                             | Link in a non-obvious container (`results.columns[]`)                                 | C9                     |
| S4  | A **form** with a Link/URL field, and a submit rule redirecting to a page                                                       | Both are decoys: `type: "link"` with no `scene`, and a `scene` that is not navigation | A4                     |
| S5  | A **menu** view with: links to its own child pages, a link to a top-level page that exists elsewhere, and an external URL entry | The original incident surface; owned vs external vs url in one view                   | C6, C8                 |
| S6  | One child page linked from **two different views**                                                                              | The `transferred` class (page re-parents instead of dying)                            | C8                     |
| S7  | Accented page names (é, à, ç) and one page **renamed after creation**                                                           | Slug vs name drift; slug matching is case/trim-sensitive code                         | A2 drift               |

**Source and filter shapes** — what the view-source cases need. These are the ones the
last playground could not supply, and building them deliberately is the whole reason a
fresh app is worth the effort:

| #   | Shape                                                                                                                                                 | Serves |
| --- | ----------------------------------------------------------------------------------------------------------------------------------------------------- | ------ |
| S8  | **Two different connection fields joining the same pair of objects**                                                                                  | V3, V4 |
| S9  | Two objects joined so that the connection field lives on the **other** object and points back, as well as one where it lives on the view's own object | V4     |
| S10 | A **three-object chain** A → B → C, where the two hops are different connection fields                                                                | V1, V4 |
| S11 | An accounts object with **two login-capable test accounts**, each holding connected records, plus records connected to neither                        | V7, M1 |
| S12 | An object holding records that cover **all five** A/B/C truth combinations for the V6 fixture — see V6's own section for the exact list               | V6     |
| S13 | A page whose source **matches no records**, and a view whose linked target is **unresolvable** (a deleted page, or a slug that resolves to nothing)   | N1, A3 |

### Data requirements, not just shapes

A shape with no records in it cannot answer a counting question — an earlier run returned
"No Data" and could not tell correct scoping from an empty pairing. So:

- Every connected object pair needs **at least two parents that hold children**, or a
  scoped count of 1 proves nothing about the scope.
- S11's accounts must be **usable to sign in to the live app**. V7 needs a signed-in read;
  a builder preview binds no account and its zero means nothing.
- Every scoped view gets an **unscoped control view of the same object on the same page**.
  That is the standing rule above, and it is a fixture requirement, not a run-time one.
- Keep the record counts small enough to verify by hand. The expected row set for each
  case should be something a person can write down before the view is created.

### One-time setup

`app.json` needs `allowViewMutation: true` and `allowDelete: true`. **A test agent must
never edit it** — that is the human operator's setup, and a tool refusing because a flag
is off is a result to report, not an obstacle to route around.

Take a `knack_snapshot_app` before the first destructive case. Section 6 depends on one
existing, and it is the only thing that can rebuild a cascade-deleted page tree.

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

| ID  | Case                                                                        | Expected                                                                                 | Proven                                                                                                                                                                                                                                                                                                                                                                                                         |
| --- | --------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| C6  | Cut the link to an **external** page (S5's link to the elsewhere page)      | No pages destroyed; response names the severed link; page survives in the builder        | **automated** only. The live menu run **re-sent** its external entry rather than cutting it, so the cut has never been executed. 211 refs classified `external` in live-sim                                                                                                                                                                                                                                    |
| C8  | `delete_view` and `move_view` on a link-bearing view; `move_view` on a menu | Every link counts as dropped; full doomed set prompted; decline leaves everything intact | **live-sim** for `delete_view` — would prompt on 207 views, worst case 11 doomed pages. **—** for `move_view`: never executed and re-parenting on move is unmeasured, which is why the guard treats every link as dropped there                                                                                                                                                                                |
| C9  | `copy_view` of a link-bearing view                                          | Allowed without prompt (source untouched); verify source pages intact after              | **partly — the request shape is confirmed.** Two real builder copy requests matched the envelope `knack_copy_view` already sends, `{action, target_scene_key, view_key, completeViewSchema}`, byte-for-byte in shape. Both carried scene links, so a link-bearing copy is what the builder itself does. **Still open:** executing one through the server and confirming the source view's linked pages survive |

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

## 6. Repointing a copied view

Opened by two real builder `copy` requests on 4 September, which showed that a view's
`source` block holds a minority of its connection references — 2 of 10 in the sample.
`TESTED.md` §7 has the full map and the reasoning; these are the questions it could not
answer.

| ID  | Case                                                                                                                                                                                              | Expected                                                                                 | Proven                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| --- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| P1  | **In the builder, change a view's source connection on a view with connection-reached columns, then capture the save request.** Does the builder rewrite `columns[].connection.key`, or leave it? | Unknown when written                                                                     | **CLOSED — it leaves them, and that is correct.** A rescope adding `connection_key`, `relationship_type`, `authenticated_user` and `parent_source` to a source that had none left both column connections untouched; they were already set while the source had no connection at all. A column connection is a display path out of the view's own object, independent of scoping. This **corrected** the claim shipped hours earlier that they must be repointed alongside the source. See `TESTED.md` §7 |
| P2  | Same question for `columns[].edit_rules[].connection`, the dotted `object_N.field_N` form                                                                                                         | Follows P1, or does not                                                                  | **partly** — reclassified as a display connection by P1's answer, so a rescope should leave it alone too. Not separately observed: no captured rescope touched a view carrying edit rules. Low priority now that the model is right                                                                                                                                                                                                                                                                       |
| P3  | Repoint a view whose columns reach through the old connection, leave the column keys stale, and read the rendered rows                                                                            | Columns render values from the wrong relationship                                        | **VOID — the premise was wrong.** P1 shows a rescope does not make a display connection stale, so there is nothing to leave stale. Replaced by **P6**                                                                                                                                                                                                                                                                                                                                                     |
| P4  | `knack_plan_view_repoint` against a real view with connection columns                                                                                                                             | Reports the scope list and the display list separately, and says which a rescope touches | **automated** against a reduction of a real copy request; **—** live                                                                                                                                                                                                                                                                                                                                                                                                                                      |

| P5 | **Does copying a view duplicate the child pages it owns?** | Per container, as it turns out | **CLOSED, and narrower than first recorded.** A copied **link column**'s owned child page IS duplicated (next-numbered slug), operator-confirmed. A copied **menu**'s links are NOT — two menus, replicated twice, came back on the original slugs. Both copy requests look identical, so the request cannot predict the outcome. The shared-page case gives the page a second referrer, which is the `transferred` class the guard already models. See `TESTED.md` §9 |
| P6 | **Retarget** — change `source.object` on an existing view | Unclear that this is even reachable. The builder binds a view to its object when the view is added, and every column, filter, sort and rule is a field on it; no capture has ever shown `object` changing, and all three rescopes left it alone | **REFRAMED, not a builder case.** Treat this as **API-only**: our tools post whatever JSON they are handed, so `update_view` with a different `source.object` is reachable through this server even if the builder offers no way to do it. That makes it our hazard rather than a Knack behaviour to discover, and the useful work is a guard rail — refuse or loudly warn when an update changes `source.object` — not an expedition to find out what Knack stores. Logged as P9 |

| P7 | `copy_view` and `create_view` should report the pages Knack created alongside the view | A response that names only the new view under-reports what the mutation did | **—**. Not a safety hole — nothing is destroyed — but an operator reading the response cannot tell that pages appeared. Snapshot the scene list before and after to get the exact set |
| P8 | A menu **create** whose `links[].scene` is a page specification (`{name, parent, views}`) | The pages are created; the guard reports them as new pages rather than as unreadable links | **partly** — the shape is captured, confirmed live by the operator, and `isScenePageSpecification` is **automated**. The guard's own arithmetic is safe already (a specification counts toward retention, so it nets zero drops), but no live run has driven this shape through `update_view` rather than a create |

| P9 | Guard rail: should `update_view` refuse a payload that changes `source.object`? | A retarget invalidates every column, display connection, filter, sort and rule at once, and no builder path produces one — so a payload carrying it is far more likely a mistake than an intention | **—**, and it is a **design decision rather than a test**. Refusing outright is the safe default and costs a legitimate caller nothing today, since nothing here needs to retarget. Worth deciding before anything is built on the assumption that a retarget is supported |
| P10 | A title edit through `knack_update_view` on a menu whose `auto_link` is `true` | `auto_link` survives | **—**. `auto_link` was unknown until a menu save request showed it, so nothing in this server writes or preserves it. A merged body that dropped it would silently turn it off and report nothing — the same class of loss as `no_data_text`, which is why this one is worth running |

**What to capture, and how.** These want builder save/copy requests rather than MCP
calls — the request **body only, never the headers**, which carry a live builder session
cookie. A body is far more informative than a description of one: the two that opened
this section corrected two shipped claims and revealed three reference sites in a single
reading.

Four captures are in and recorded: the rescope pair (P1), a `move` request, a details
view and a form. `TESTED.md` §7–§9 hold what they settled.

Eight captures are in and recorded: the rescope trio, a `copy` pair, a `move`, a details
view, a form, and a menu create plus its move. `TESTED.md` §7–§10 hold what they settled,
and between them they closed P1 and P5 and corrected three claims.

Eleven captures are in. Between them they closed P1 and P5, reframed P6, closed C2's menu
arm, and corrected five claims — two of which this work had shipped hours earlier.

Most useful next, in order: **P10**, a title edit on an `auto_link` menu, because a key
this server does not preserve is exactly how `no_data_text` was lost; then **P9**, which
is a decision to make rather than a test to run; then **P7**, the exact set of pages a
copy creates, which needs a scene list either side rather than a request body.

**P6 is no longer the headline.** It was written as "the edit nobody has run", on the
assumption that a retarget was a builder workflow. It probably is not one at all — see its
row — and the honest consequence is that the remaining open work is confirmation and
guard-rail decisions rather than anything likely to overturn the model.

## 7. Recovery drill (run once per release)

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

## 8. View sources — connections and filters

A separate surface from the cascade guard: not "what does a mutation destroy" but "does
the payload we build describe the records the user asked for". It matters most when
**copying or moving a view and then repointing it**, where a wrong source returns
plausible rows through the wrong relationship and looks like success.

Most of this was settled on 2 September from a production app export of 738 views, and
is recorded in `KNACK_VIEW_SOURCE_SHAPE` in `src/server.ts` with its counts and gaps.
The cases below are what the export could **not** settle.

| ID  | Case                                                                                                                                               | Expected                                                                                    | Proven                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| --- | -------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| V1  | Create a connection-scoped view from `knack_get_view_payload_template`, then read the stored view back and diff its `source` against what was sent | Knack stores what we posted, or the diff names exactly what it rewrote                      | **live, in part** — created at commit `26983b0`: `object`, `connection_key` and `relationship_type` all came back identical to what was posted, on two unrelated objects. **Three keys, not the whole block.** `sort` is separately ruled out as a silent rewrite by 192 stored counter-examples in the export, but the remaining keys are unexamined, so a read-back is still the honest check for a shape not seen before                                                                                                                                                                                                                                                                                                                                    |
| V3  | Two objects joined by **more than one** connection; scope a view through a named one                                                               | Only records reached by _that_ connection appear                                            | **partly — scoping passes, naming blocked.** `scene_28`/`view_276` returned **1 of 1**: the single child record belonging to the page's parent record, from a set where at least two parents hold children. An unscoped source would have shown more, so the connection does scope. **Naming itself stays untested and blocked**: only one connection joins that object pair in this app, so nothing here separates "scoped through the field I named" from "scoped through the only field there is"                                                                                                                                                                                                                                                           |
| V4  | Repoint a **copied** view at a different connection, recomputing `relationship_type`                                                               | Rows follow the new connection; `relationship_type` matches which object owns the new field | **live, still inconclusive — but the doubt has moved.** `view_277` was restored to `scene_83` and returned **5**, the same as the deliberately-wrong `view_278`. That selects "the page supplies no bindable parent record" over "the wrong `relationship_type` voids the scope" — **except** that the same session's two user-scoped views also returned 0, so the read context bound no account, and what `scene_83` binds is itself unmeasured. Knack accepting the mismatch without error still stands. **What settles it:** run the two arms as separate views on a page whose record binding an unscoped control view on that same page confirms — the previous run's page bound nothing measurable, which is the whole reason the result was unreadable |
| V5  | A table on a details page scoped to the **page's** record rather than the logged-in account                                                        | Correct related records for a chosen parent; parent page and siblings untouched             | **live, page-record scoping confirmed** — `parent_source` stored as posted, and Knack's own builder described the view in prose as records connected to the same intermediate record connected to this page's record, which is the case's question answered in its own words. Returned 1 row with no page moved or lost. **Still open:** whether those are the _right_ related records for a chosen parent — a count is not correctness                                                                                                                                                                                                                                                                                                                        |
| V6  | Filter semantics — one top-level rule plus one group, sent once as `match: "all"` and once as `"any"`                                              | With `all`, groups are OR; with `any`, groups are AND                                       | **export: strongly evidenced. The live A/B run does not reconcile.** `view_279` (`all`) returned **2** and `view_280` (`any`) returned **12**, with the stored criteria matching what was posted. Neither number matches any of the three readings the fixture was built to separate — inversion predicted (4, 11), flattened (1, 46), groups-ignored (5, 5) — and both sit strictly _between_ inversion and flattened. Do not read a fourth semantics out of that yet: three cheaper explanations come first, listed below the table                                                                                                                                                                                                                          |
| V7  | `operator: "user"` as a filter rule on a connection field, with no `authenticated_user` on the source                                              | Scopes to the logged-in account by the second, independent mechanism                        | **export: 60 occurrences. The live run is void — no account was bound.** `view_283` (`operator: "user"`) and `view_282` (`authenticated_user: true`) both returned **0**, against a predicted 11 connected and 55 unscoped. Two zeroes are equally consistent with the mechanisms agreeing and with neither being exercised, so this settles nothing either way. **What settles it:** read the counts from the live app while signed in as a test account that holds connected records — not from a builder preview — with an unscoped control view on the same page                                                                                                                                                                                           |

### V6 — the fixture, then the cheap explanations

**Three readings, not two.** Two of them are easy to conflate: making a group's operator
match the top level _is_ removing its parentheses, for any number of groups. The fixture
has to separate all three.

| Reading                                      | `match: "all"` means | `match: "any"` means |
| -------------------------------------------- | -------------------- | -------------------- |
| **Inversion** — what the code claims         | `A AND (B OR C)`     | `A OR (B AND C)`     |
| **Flattened** — groups take the top operator | `A AND B AND C`      | `A OR B OR C`        |
| **Ignored** — groups dropped entirely        | `A`                  | `A`                  |

**S12's exact requirement.** Pick three predicates A, B and C where **B and C sit on
different fields** — two values of one field makes `B AND C` unsatisfiable and collapses
two readings into one. Then hold records covering all five of:

| #   | Combination   |
| --- | ------------- |
| 1   | `A ∧ B ∧ C`   |
| 2   | `A ∧ B ∧ ¬C`  |
| 3   | `A ∧ ¬B ∧ ¬C` |
| 4   | `¬A ∧ B ∧ C`  |
| 5   | `¬A ∧ B ∧ ¬C` |

With exactly those five and nothing else in the object, the three readings give distinct
counts, so a single pair of numbers identifies which is true:

| Arm            | Inversion | Flattened | Ignored |
| -------------- | --------- | --------- | ------- |
| `match: "all"` | **2**     | **1**     | **3**   |
| `match: "any"` | **4**     | **5**     | **3**   |

Send the two arms as **two views on two separate pages** — writing both to one view
overwrote the first arm on an earlier run and made the result unattributable. If the
object holds more than the five records, derive the predicted counts from what is
actually there rather than copying the table.

**Then the cheap explanations.** `(2, 12)` from the last run fits none of the three
readings, and that is more likely a measurement problem than a fourth semantics. Work
these in order; each is cheaper than the sweep that would follow.

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

| Date  | Commit tested          | App                                           | Client(s)                     | Sections run                                                                                       | Pass / findings                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| ----- | ---------------------- | --------------------------------------------- | ----------------------------- | -------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 1 Sep | `8c28bb5` (pre-review) | `NP Place Playground` (purpose-built fixture) | elicitation-capable           | C1, C3–C5, C7 — tables `view_239`/`view_267` and menu `view_22`, four real `PUT`s                  | Pass. Reversed the PR's founding premise: a re-sent link destroys nothing, and a page dies only with its **last** link                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| 2 Sep | `1c52a34`              | production app, 963 scenes / 1,889 views      | n/a (read-only simulation)    | C1, C8 (`delete_view`), A2, A7 — guard functions over live metadata                                | Pass. 402 views title-edited with zero prompts; 315 owned / 211 external / 41 transferred / **1 unknown**; `delete_view` worst case 11 doomed pages                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| 2 Sep | `9ce6f57` (analysis)   | production app export, 738 views              | n/a (offline export)          | Section 7 — source and criteria shapes read from a schema export                                   | Pass. Four source patterns catalogued; `relationship_type` ownership rule clean across 102 sources; `value_field` absent from all 738 sources                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| 3 Sep | `26983b0`              | `NP Place Playground`                         | elicitation-capable           | V1–V5 of the view-source section — five views created, each read back                              | Knack stored the three compared keys as posted, on two objects. **Finding:** it accepted a `relationship_type` contradicting connection ownership without error. V3/V4/V5 row correctness outstanding; V6–V8 not run                                                                                                                                                                                                                                                                                                                                                                                                       |
| 3 Sep | `37cd3f2`              | `NP Place Playground`                         | elicitation-capable           | V3a re-run on a data-bearing fixture; V4 arms split into two views; row counts read in the builder | V3 passes: 1 of 1, correctly scoped. V4 inconclusive — the wrong arm returned all 5 records and the correct arm's count is still missing. **Two tool findings:** explicit `fieldKeys` suppressed real column headers, and a stale `existingViewKeys` silently dropped a view from its page                                                                                                                                                                                                                                                                                                                                 |
| 4 Sep | `ed68454`              | `NP Place Playground`                         | elicitation-capable           | R1/R1b, V6, V7, V8, and C1/C2 on details, form, search, calendar                                   | C1/C2 closed for all four remaining view types, each diffed against the builder's own save request. V8 passes: a multi-hop source resolved through the parent hop (3 rows vs 0 predicted for the alternative). R1b passes live. **Defect found:** no generated table or list carried `no_data_text`, so Knack stored `""` and every view fell back to its stock empty-state line. V4 still inconclusive, V6 does not reconcile with any predicted reading, V7 void — no account was bound at read time. **Operational finding:** two parallel creates on one page raced their derived `pageGroups` and detached `view_281` |
| 4 Sep | `99422f5` (analysis)   | production app, two builder copy requests     | n/a (captured request bodies) | Section 6 — a repointed copy request read for its reference sites                                  | **Two shipped claims corrected:** the four source patterns are not a closed set (one payload carried all four scoping keys at once), and `no_data_text` is not always two words. **Finding:** a view's source holds a minority of its connection references — 2 of 10, the other 8 in `columns[].connection.key` and `columns[].edit_rules[].connection`, across three distinct connection fields. `knack_copy_view`'s envelope confirmed correct                                                                                                                                                                          |
|       |                        |                                               |                               |                                                                                                    |                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
