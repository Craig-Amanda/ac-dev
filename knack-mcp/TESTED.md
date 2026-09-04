# What is settled — the view-mutation safety guard

The companion to `TESTING.md`, which holds only what is still open. This file is the
record of what has been established and how, so nothing here is re-derived and nothing
here is re-argued from memory.

Every entry names its evidence. Where a claim was later narrowed or overturned, that is
recorded rather than edited away — the corrections are the most useful part of the file,
because each one marks a place where a confident reading turned out to be wrong.

## How to read the evidence column

| Marking             | What it means                                                                                                                                                                   |
| ------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **live**            | A real request against a real Knack app, and where a confirmation was involved, a human answered it                                                                             |
| **export**          | Read from a schema export: 1,911 fields and 738 views of a second app, 31 August, analysed 2–3 September. Strong about **shape**; silent about what Knack _does_ with a request |
| **automated**       | Pinned by the test suite, and reverting the rule fails a test. Sufficient on its own for a path that refuses **before** anything reaches Knack                                  |
| **by construction** | Follows from the code's structure rather than from a test — the branch that would do otherwise does not exist                                                                   |

A `live` marking means the behaviour held once, on one app, at one commit. It is not a
guarantee about another Knack plan or region. What it does mean is that a failure there
now is a regression rather than a discovery.

---

## 1. The cascade rule

The four claims every view-safety rule originally rested on. Three were false.

| Claim                                                       | Verdict                                    | Evidence                                                                                                                                                                                                                                     |
| ----------------------------------------------------------- | ------------------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Replacing `columns` destroys the pages behind re-sent links | **false**                                  | **live**, 1 Sep. A table with five child pages, four sole-referenced, verified by a 154-view scan first. Every link re-sent byte-for-byte: 0 pages destroyed                                                                                 |
| A page dies when _a_ link to it goes                        | **false** — it dies with its **last** link | **live**, 1 Sep. One link column omitted destroyed exactly that page; a two-referrer page's link omitted destroyed nothing                                                                                                                   |
| A menu's `links` is more dangerous than `columns`           | **false** — same rule, different array     | **live**, 1 Sep. A seven-link menu, one entry dropped: 3 pages died (the entry's page and its two descendants), the six re-sent links all survived, and three of the survivors were sole-referenced so a second referrer cannot explain them |
| The public payload is the whole view definition             | **true**, for tables                       | **live**, 1 Sep. Two tables diffed against the Knack builder's own save request; agreed on every key but `design`, which was `{}` on both — including one with table design fully on, whose settings live in `table_design`                  |

**The second run is what rules out the alternative reading.** It took the view from 16
columns to 15, so Knack did not merge or ignore the array.

**Why every earlier cascade fitted the false premise.** Both production incidents were
pages whose link had genuinely stopped being sent. Neither separated the two
explanations, which is how the premise survived unmeasured for so long.

## 2. Guard behaviour, live

| Behaviour                                                                        | Evidence                                                                                                                        |
| -------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------- |
| A title edit on a link-bearing view goes through with no prompt and no link lost | **live** — a table and the seven-link menu, 1 Sep, checked link by link                                                         |
| The prompt names exactly the right pages, grandchildren included                 | **live** — the menu run named the dropped entry's page _and its two descendants_                                                |
| An accepted cascade matches the prediction                                       | **live** — `pagesKnackReportsDeleted` equalled the guard's prediction exactly                                                   |
| A declined prompt changes nothing                                                | **live** — 25 of 25 pages preserved                                                                                             |
| Cutting one of two links transfers the page rather than destroying it            | **live, twice** — the page re-parented onto the view that still linked to it                                                    |
| A snapshot is written before every guarded mutation                              | **live** — present, timestamped, `snapshotVersion: 2`, carrying the scene tree, the view definition, and a `schemaPath` pointer |

| The stale-`existingViewKeys` warning fires on a real page | **live**, 4 Sep — an `existingViewKeys` list naming one of a page's several views returned the warning; the same call with the argument omitted derived the full layout and warned about nothing |

**Fleet scale, from a 963-scene production app** (**live**, read-only): 402 link-bearing
views title-edited with zero prompts; 315 owned / 211 external / 41 transferred / **1
unknown** across 568 references; `delete_view` would prompt on 207 views with a worst
case of 11 doomed pages.

That single `unknown` is the number that matters. `unknown` is the fail-safe bucket, so
a high count would mean prompting constantly on shapes the guard does not understand —
and operators learning to click through prompts without reading them.

## 3. Guard behaviour, automated

Each refuses **before** anything reaches Knack, so a live run could observe no more than
the refusal the suite already asserts. Every test asserts no mutation followed.

| Path                                                  | Code                                              |
| ----------------------------------------------------- | ------------------------------------------------- |
| Payload nested past the walk depth                    | `STRUCTURE_TOO_DEEP`                              |
| Unparseable payload                                   | `INVALID_UPDATES_JSON`                            |
| Payload with nothing in it                            | `EMPTY_UPDATE_PAYLOAD`                            |
| Legacy `confirmDestructive` on a cascade-risky update | `CONFIRMATION_UPGRADE_REQUIRED`                   |
| Unreadable view or scene tree                         | `COULD_NOT_VERIFY_VIEW`, `SCENE_TREE_UNAVAILABLE` |
| Snapshot could not be written                         | `SNAPSHOT_FAILED`                                 |
| No human available to ask                             | `HUMAN_CONFIRMATION_UNAVAILABLE`                  |
| Human declined                                        | `HUMAN_CONFIRMATION_DECLINED`                     |

Also automated: a page cycle terminates rather than hanging; a stale scene reference
counts as at-risk rather than safe; a scene reference given as `{key: "..."}` resolves.

**An unanswered prompt is handled by construction, not by a test.** The guard proceeds
only on `accepted: true` and reports the outcome (`timeout`, `cancel`, `error`) in the
refusal, so there is no branch in which a timeout proceeds. What remains unmeasured is
whether a given client returns a timeout at all rather than hanging.

## 4. View source shapes

**export**, unless noted. Full detail with counts lives in `KNACK_VIEW_SOURCE_SHAPE` in
`src/server.ts`.

- **Four patterns**, distinguished only by which keys are present: plain (325 views),
  connection-scoped (57), logged-in user (16), multi-hop (6).
- **`relationship_type` follows ownership of the connection field** — `foreign` (84)
  where it sits on the view's own object, `local` (18) where it sits on the other object
  and points back. Clean across all 102 connected sources.
- **`connection_key` and `relationship_type` never appear apart.**
- **`authenticated_user` is only ever `true`** (28 occurrences), and appears with or
  without a connection — a view scoped to the user's own record carries it alone.
- **`criteria` is an object**, `{match, rules, groups}`; `groups` is an array of rule
  arrays; the first block is `rules` and not group zero, proven by a view populating
  both. A group carries no `match` of its own.
- **A group's operator is the inverse of the top-level `match`** — strongly evidenced
  rather than run: one view carries `match: "all"` with a group of five equality tests on
  a single field, which AND-ed would match nothing.
- **`operator: "user"` is a second, independent way to scope to the logged-in account**
  (60 occurrences) — a filter rule, not a source flag.
- **`sort` is not forced.** 36 stored views carry `sort: []` and 156 carry no `sort` key
  at all, so a builder panel showing a sort field is displaying a default rather than
  something Knack wrote.

## 5. View source round-trips

**live**, 3 Sep, on a playground app at commit `26983b0`.

| Established                                                            | Detail                                                                                                                                                                                                                                      |
| ---------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Knack stores a hand-built source rather than rewriting it**          | Two connection-scoped tables on unrelated objects. `object`, `connection_key` and `relationship_type` came back identical in both. Corroborated independently by the `sort` finding above                                                   |
| **Connection scoping works**                                           | A view returned 1 of 1 — the single record belonging to the page's connected parent, from a set where at least two parents hold records. Unscoped would have shown more                                                                     |
| **A multi-hop `parent_source` is understood as intended**              | Knack's own builder rendered it in prose as records connected to the same intermediate record connected to this page's record — it parsed a payload assembled from a schema export and described the two hops correctly, in the right order |
| **Knack does not validate `relationship_type` against the connection** | A view repointed at a connection whose field lives on the other object was stored with `"foreign"` where ownership makes it `"local"`, accepted without error and reported nowhere                                                          |

**live**, 4 Sep, same app, at commit `ed68454`:

| Established                                                                        | Detail                                                                                                                                                                                                                                                           |
| ---------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **A multi-hop source resolves through the parent hop, not `connection_key` twice** | `view_284` on `scene_75` stored `connection_key: field_363` with `parent_source: {object: object_4, connection: field_355}` and `relationship_type: foreign`. It returned **3** rows, the parent-hop prediction; applying `connection_key` twice predicted **0** |
| **A page record context does bind, on a page that has one**                        | The same run's non-zero count is the proof. It is worth stating separately because two user-scoped views in the same session returned 0 — page-record context was bound, account context was not                                                                 |

**Scope of the round-trip claim.** Three keys were compared, not the whole `source`
block. The `sort` question — the most likely candidate for a silent rewrite — is ruled
out separately by 192 stored counter-examples. Other keys remain unexamined, which is
why the constant still recommends a read-back for a shape it has not seen.

**Why the validation finding matters.** It makes `buildViewSource`'s refusal of a
connection without a relationship type **the only guard**, rather than a
belt-and-braces check on top of Knack's own validation.

## 6. A title edit preserves the rest of the view, per type

**live**, 4 Sep, at commit `ed68454`. For each type: the full raw view was read, only the
title was changed through `knack_update_view`, the view was read back and diffed key by
key, and the builder's own save request for the same view was diffed against that.

| Type     | View       | Diff after the edit | Keys present only in the builder's save request |
| -------- | ---------- | ------------------- | ----------------------------------------------- |
| details  | `view_193` | `title` only        | `design` (empty)                                |
| form     | `view_21`  | `title` only        | `design`, two empty nested `format` objects     |
| search   | `view_285` | `title` only        | `design`, `no_data_text`                        |
| calendar | `view_288` | `title` only        | `design`, one empty nested `format` object      |

With the two tables and the menu already settled, C1 and C2 are now closed for every
link-bearing view type the plan names.

**The builder's empty keys are not uniformly harmless.** `design: {}` and empty nested
`format` objects are inert scaffolding — the API does not require them and their absence
changes nothing. `no_data_text` is the exception: absent, Knack stores `""` and the view
renders its stock empty-state line instead of anything the author chose. That one key
was a real defect, found only because this diff listed it, and it is fixed at
`1a837c9`. The lesson is that "the builder adds empty keys the API does not need" is a
generalisation worth checking one key at a time.

## 7. Shape claims audited

Every documented shape and payload assertion in `src/server.ts`, checked against the
export from an app other than the one they were written from.

**Held up:**

| Claim                                            | Result                                                    |
| ------------------------------------------------ | --------------------------------------------------------- |
| `connection` definition shape                    | `relationship: {object, has, belongs_to}` — 541/541 exact |
| `equation` definition shape                      | 15 keys documented, 15 observed, no drift either way      |
| Field-type coverage                              | 23 types in use, all 23 documented                        |
| `values[]` record and value forms                | Both match the observed key sets                          |
| Source key goes in `values[].input`, not `value` | 117 of 126 used `input` with an empty `value`             |
| Equation tokens use the field-key form           | 185 tokens, none name-based                               |
| Equations cannot cross many-to-many connections  | 39 crossed, none many-to-many                             |

**Corrected — true of the original sample, false in general:**

- **`criteria[].value_field`** was recorded as the object's auto_increment key "in every
  working example observed", purpose unclear. It is **200 of 223**, and `value_type`
  decides: with `"field"` it names the comparison target and was never auto_increment;
  with `"custom"` the literal in `value` is used and `value_field` is inert.
- **A rule's `key`** was described as a string. 17 of 236 rules carry none at all.
- **`values[]`** was documented as a closed set. One observation carries an extra
  `action` key.
- **`value_field` is not a source key at all** — 0 of 738 sources. A verbal report that
  it was the default sort field did not survive the export.

**Measured since the audit:**

- **`no_data_text` belongs to two view types only.** Across the same 738 views it appears
  on `table` (217 of 224) and `list` (6 of 6), and on none of the 74 details, 152 form,
  48 menu, 2 calendar, 38 report, 55 login, 120 registration or 19 rich_text views. A
  `search` view carries it too, seen in a builder save request rather than the export.
- **Nobody leaves it blank on purpose.** All 223 stored values are non-empty, and every
  one is exactly two words. None contains a template token, so the string cannot vary at
  render time — a value derived from the object at build time is the whole of what Knack
  allows.

**Not audited, and not claimed:** `formattedShape` and `rawShape`, 32 of each. They
describe record _values_, and a schema export holds no records.
`knack_verify_record_field_shapes` is the way to check those.

## 8. Defects found by testing, and fixed

Each of these was found by running the thing rather than reading it.

- **No generated table or list carried an empty-state line.** `no_data_text` was set
  nowhere in the server, so Knack filled the key with `""` and every generated view fell
  back to its stock message. Found by the per-type diff above, where it showed up as a
  key present only in the builder's save request. Templates now derive
  `No <object name> Records`, or a bare `No records` with no schema loaded.

- **A referrer-index fail-open** counted a form's submit-rule redirect as a link, making
  a page look multi-referenced so its last real link could be cut with no prompt.
  Reproduced end to end before fixing. `collectNavigationRefs` now takes only nodes whose
  innermost named array is `columns` or `links`.
- **The same inversion on the retention side.** `payloadRetainsSceneRef` walked the whole
  outgoing body, so a submit-rule redirect read as a retained link while the real sole
  link was being cut in that very `PUT`. Found by a review pass after the first fix.
- **Generated views showed raw field keys as column headers.** Explicit `fieldKeys` took
  a branch passing an empty schema, so name and type could only fall back to the key —
  and the same branch skipped scene-derived layout keys.
- **A stale `existingViewKeys` silently orphaned a view.** `pageGroups` is the page's
  whole layout, so two creates with the same key list rebuilt it without the first, which
  stayed live and URL-reachable with nowhere to appear.
- **Two diagnostics that lied.** `serverBuild.commit` named the checkout rather than the
  build, so a test ran against code three commits behind what it reported; and
  `knack_refresh_cache` echoed `persistFiles: true` beside `warm: false`, claiming writes
  it never made.

## 9. Two findings about testing agents

Worth keeping because they change how a run is designed, not just what it finds.

- **A calling agent is not a witness to whether a prompt appeared.** Elicitation goes to
  the client and never returns to the model. On three runs an agent reported "no
  confirmation prompt was raised" when one had been. It is the one thing such an agent can
  never truthfully report, and it invalidated three runs before being understood.
- **A calling agent can refuse before the human is asked.** One declined to submit a run
  on its own policy grounds, before any prompt existed. `cascadeDeleteBehaviour:
prompts-human` describes the transport's capability, not the agent's willingness.

## 10. Test-design errors made along the way

Recorded because each one produced a run that proved nothing, and the pattern is worth
recognising.

- **A fixture where nothing was sole-referenced.** The first live cascade test used two
  views that both linked to the same page, so no page could lose its last link. Voided.
- **Both arms of a comparison applied to one view.** The second wrote the correct and
  deliberately wrong variants to the same view, so the first was overwritten and the
  result could not be attributed to either.
- **A discriminator that could not discriminate.** A filter test asked for three
  predicted row sets where two were the same expression: making a group's operator match
  the top level _is_ removing its parentheses, for any number of groups. The genuine third
  reading is groups ignored entirely.
- **A test whose fixture had no data.** A scoping test returned "No Data", which cannot
  distinguish correct scoping from an empty pairing.
- **A fix tested below its own seam.** Three times now: a test of the helper passed
  whichever way the call site was wired, so the fix was unpinned until the decision was
  extracted and tested directly. The third instance — `no_data_text` — is why the four
  template payload branches were pulled out into `buildViewTemplatePayload`, so what a
  caller receives can be asserted rather than only the helpers feeding it.
- **A row count read without establishing what the page bound.** Round 3 read counts for
  a user-scoped and a parent-scoped view in a context that bound no account: both
  returned 0, which is equally consistent with the two mechanisms agreeing and with
  neither being exercised. A count is interpretable only alongside a control on the same
  page — an unscoped view of the same object, whose count is the denominator — and a
  record of how the page was opened, since a builder preview and a signed-in app session
  bind different things. The same run's multi-hop case returned a non-zero count on a
  different page, so context binding is per-page and cannot be assumed from one result.
- **Two creates issued in parallel against one page.** Each derived the page layout as it
  stood before either ran, and the second create's `pageGroups` overwrote the first's, so
  `view_281` existed but nothing rendered it. `pageGroups` replaces the layout rather
  than adding to it, and derivation is a read taken once. Creates on one page have to be
  sequential, re-deriving between them — which is what the retry did, and it kept every
  view on the page.
