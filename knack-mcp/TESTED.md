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

| Path                                                                   | Code                                              |
| ---------------------------------------------------------------------- | ------------------------------------------------- |
| Payload nested past the walk depth                                     | `STRUCTURE_TOO_DEEP`                              |
| Unparseable payload                                                    | `INVALID_UPDATES_JSON`                            |
| Payload with nothing in it                                             | `EMPTY_UPDATE_PAYLOAD`                            |
| New-page specification with no `views`, or on a link with no `type`    | `MALFORMED_PAGE_SPECIFICATION`                    |
| Live view carrying a stored specification object, re-sent by the merge | `STORED_PAGE_SPECIFICATION`                       |
| Legacy `confirmDestructive` on a cascade-risky update                  | `CONFIRMATION_UPGRADE_REQUIRED`                   |
| Unreadable view or scene tree                                          | `COULD_NOT_VERIFY_VIEW`, `SCENE_TREE_UNAVAILABLE` |
| Snapshot could not be written                                          | `SNAPSHOT_FAILED`                                 |
| No human available to ask                                              | `HUMAN_CONFIRMATION_UNAVAILABLE`                  |
| Human declined                                                         | `HUMAN_CONFIRMATION_DECLINED`                     |

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

## 7. Where a connection reference hides in a view

**export + two real builder copy requests**, 4 September. Two sibling views of one
object from a second app, captured as the builder's own `copy` requests with the source
already repointed. The envelope was `{action: "copy", target_scene_key, view_key,
completeViewSchema}` — which is byte-for-byte the shape `knack_copy_view` already sends.

**A view's `source` block is a minority of its connection references.** In one sampled
table, 10 references pointed at a connection and only 2 were in `source`:

| Where                               | Count | Note                                                    |
| ----------------------------------- | ----- | ------------------------------------------------------- |
| `source.connection_key`             | 1     | the scope everyone thinks of                            |
| `source.parent_source.connection`   | 1     | a different field from `connection_key` in both samples |
| `columns[].connection.key`          | 6     | columns showing a field on the connected record         |
| `columns[].edit_rules[].connection` | 2     | dotted `object_N.field_N`, a form seen nowhere else     |

Three **distinct** connection fields across them, so "repoint the connection" is
ambiguous until the list exists.

### Corrected the same day: scope and display are different references

The paragraph that stood here said the eight references outside `source` "do not move
when the source does", implying they should be repointed alongside it. **That is
backwards**, and a builder before-and-after pair settled it a few hours later.

The capture: a view whose `source` had **no connection at all** (`{object, criteria,
limit, sort}`) was rescoped in the builder to add `connection_key: field_786`,
`relationship_type: foreign`, `authenticated_user: true` and a `parent_source`. Two
columns carried `connection: {key: field_786}` **before and after, unchanged.**

They were already set while the source had no connection, which is the proof:

| Kind                   | Where                                                                                    | What it decides                                                | On a rescope        |
| ---------------------- | ---------------------------------------------------------------------------------------- | -------------------------------------------------------------- | ------------------- |
| **scope-connection**   | `source.connection_key`, `source.parent_source.connection`                               | which records the view lists                                   | **change these**    |
| **display-connection** | `columns[].connection.key`, `edit_rules[].connection`, form input `source.connections[]` | where a shown value is read from, out of the view's own object | **leave untouched** |

A display connection is a path from the view's **own object** to a connected record. It
has nothing to do with how the view's records are scoped, so a rescope leaves it correct
and rewriting it would be the bug. What does invalidate every one of them is changing
**`source.object`** — then every field, display connection, filter, sort and rule names a
field on an object the view no longer lists.

So there are two edits both called "repointing", and they share almost nothing:
**rescope** touches the scope list only; **retarget** puts everything in doubt.
`knack_plan_view_repoint` now reports the two lists separately and says which is which.

Two further reference sites are easy to miss:

- **`columns[].source.filters`** is a **flat** array of `{field, value, operator}`,
  constraining which connected records a cell editor offers. That makes four structures
  behind one word: view source criteria is an object `{match, rules, groups}`; field
  conditional rules are flat; a column's source filters are flat; edit-rule criteria are
  flat.
- **`description` embeds bare keys** in KTL directives —
  `_bulk_actions=[label, field_1029], [Assign Work, view_2685]`. Nothing else in this
  server reads a description, so a copy carries them verbatim and they keep naming the
  original's fields and **views**. That `view_2685` is a cross-view reference invisible to
  every other check here, the cascade guard included.

`collectViewReferences` and `planViewRepoint` exist for this, and
`knack_plan_view_repoint` surfaces it read-only. The scan walks the schema **generically**
rather than reading a list of known paths — the same lesson the cascade guard learned
about `links` and `columns`, and the reason it found the dotted edit-rule form and the
description tokens without being told they existed.

## 8. `remote` is not the owned/external classifier

Worth recording because it looked like a shortcut and is not one. A link column can carry
`remote: true`, and in two captures every link to a page elsewhere had it while the link
to the view's own child page did not. That suggested Knack ships the owned-vs-external
signal the cascade guard currently derives from the scene tree.

Measured across all 457 scene references in the 738-view export, against each target's
actual `parent`:

| Flag           | Target is a child of the view's page | Target is not | Unresolved |
| -------------- | ------------------------------------ | ------------- | ---------- |
| `remote: true` | **31**                               | 94            | 0          |
| absent         | 307                                  | 14            | 11         |

So **absence is a strong hint of ownership** — 307 of 321 resolved, 95.6% — but
**presence is only 75% external**, and 31 links marked `remote: true` point at the view's
own child page. It is not a classifier, and swapping the scene-tree derivation for it
would have introduced a 25% error rate into the cascade guard's most important decision.
`remote` is never written `false` (125 true, 0 false, 332 absent), so it follows the same
presence-is-the-meaning pattern as `authenticated_user`; what it actually controls is not
established, and nothing here needs it.

The scene tree remains the authority. The hypothesis was reasonable from two payloads and
wrong at fleet scale, which is the argument for measuring rather than generalising — the
same mistake this file has now recorded four times.

## 9. What a copy does to linked pages depends on the container

**Confirmed by the operator, 4 September**, after being inferred from two captures.

**A copied link column's owned child page is duplicated.** A copy request posted a link
to one page; the copy's own schema names the next-numbered slug. Every other link in it —
all flagged `remote: true` — was unchanged. The operator confirmed: _"yes it does and
did."_

**A copied menu's links are not.** Operator-confirmed on both sides: _"If you copy a
table that will copy the child pages too so this is different behaviour."_ Two menu
copies, each replicated twice, came back pointing at the **original** slugs — a two-link menu kept both, a
ten-link menu kept all ten. No page duplicated, no slug incremented.

So the behaviour is **per container, not per view**: a link column's owned child page gets
duplicated, a menu's linked pages get shared.

**The menu half, measured directly on 5 September.** `knack_copy_view` of a two-link menu
onto another page: the copy's links name the same two slugs as the source, Knack's
response inserted no scenes, and a snapshot shows both pages still with their one original
parent. Reported on 4 September; measured now.

**A move, measured the same day, on both containers** (C8 in `TESTING.md`). The MCP's
own `move_view` was refused each time — no human to confirm — so both moves were made in
the builder, with a snapshot taken either side. **They differ, the same way copies do.**

_A menu_ (one link, one child page): the menu arrived on the new page with its link
intact, and the child page survived with its **original parent unchanged** — it now sits in
the page tree under the page the menu left, linked from a page it is not a descendant of.
Nothing dropped, nothing died. The move turned an **owned** link into an **external** one,
which the cascade check already handles.

_A table_ (two link columns; five owned pages in a three-level tree, all named by the
refusal). Knack **rebuilt the tree under the new page and rewrote the link columns to the
copies**: five new pages under new slugs, the table's two `scene` values now naming them. Of the five originals, **three were deleted and two
survived orphaned** — still under their original parents, their own views intact, and no
link anywhere pointing at them:

| Original     | Depth | Kind    | Own children | After the move     |
| ------------ | ----- | ------- | ------------ | ------------------ |
| edit page    | 0     | form    | none         | **deleted**        |
| details page | 0     | details | one          | survived, orphaned |
| edit page    | 1     | form    | none         | **deleted**        |
| details page | 1     | details | one          | survived, orphaned |
| edit page    | 2     | form    | none         | **deleted**        |

Every deleted page was a leaf holding a form; every survivor had a child of its own and
held a details view. One measurement could not say which of those two properties decides
it, so a second table was built to separate them: a form page **with** a child, and a
details page **without** one, then moved the same way.

| Original       | Kind    | Own children | After the move              |
| -------------- | ------- | ------------ | --------------------------- |
| edit page      | form    | one          | **deleted, with its child** |
| its child page | (empty) | none         | **deleted**                 |
| details page   | details | none         | survived, orphaned          |

**It is the kind of page, not the shape of the tree.** Across both runs, eight original
pages: every page holding a **form** was deleted, whatever its depth and whether or not it
had children — and its descendants went with it. Every page holding a **details** view
survived, orphaned, whether or not it had children. Both times the whole tree was rebuilt
under the new page first, with new slugs, and the table's link columns rewritten to the
copies.

**What it means for the guard.** Three things, none of them the menu's answer. A table
move **does** destroy pages, so "every link counts as dropped" is right to ask — and its
count over-predicts (five named, three died), which is the safe side. A move also
**creates** pages, a whole tree of them, which nothing in the move tool's report says;
`pagesCreated` now reads Knack's `changes.inserts`, so a move through this server would
list them. And a move can leave **orphans**: live pages, reachable by URL, that no link
reaches — a state the guard has no class for. Whether Knack's own builder move is a copy
then a partial delete is a guess consistent with the copy finding; the endpoint the
server posts to is `copyview` with `action: "move"`, which is suggestive and no more.

So the rule stays as it is: on a menu it over-warns, on a table it is right to warn and
over-counts — and the over-count is now explained: the details pages it names survive as
orphans. Refining the prompt to say which named pages die and which are orphaned would be
the next step, and it rests on two runs and two page kinds. A page holding both a form and
a details view, or a list, calendar or search view, has not been moved.

⚠️ **And both copy requests look identical.** Each posts the source view's own slugs
verbatim; the difference appears only in what Knack stores afterwards. A request body
cannot be used to predict which outcome you get — the only ways to know are to read the
copy back or to know this rule.

**What it means for the guard.** A shared page gains a second referrer, which is exactly
the `transferred` class the cascade check already models: a later link-drop on either menu
re-parents the page rather than destroying it, because that drop is not the last
reference. So the menu case is the _safer_ of the two and the existing arithmetic covers
it — provided the referrer index is rebuilt after a copy, since the copy is what created
the second referrer.

Neither case changes the guard's exemption. `copy_view` is exempt from the cascade check
because a copy **destroys** nothing, which holds both ways. What the duplicating case adds
is that a copy can also **create**, and nothing in this server says so: the response
reports the view it made, not the pages Knack made alongside it. A10's premise — "a create
replaces nothing" — survives; its wording, "verify the linked pages are unaffected", is
too narrow for a link column and about right for a menu.

**A menu create can create pages directly.** A real menu create posted its links as:

```json
{
    "name": "New Page 1",
    "type": "scene",
    "scene": { "name": "New Page 1", "parent": "developer", "views": [] }
}
```

`links[].scene` is an **object** here — a page specification, not a reference. Both pages
were created, empty, as `views: []` says.

Measured against the export, the stored form is never an object: **457 of 459 `scene`
properties are slug strings and 2 are null**. So Knack resolves the specification on save,
and this shape reaches the guard only from a caller-supplied payload — never from stored
metadata.

**The guard is safe here in the sense that matters — it destroys nothing.**
`readSceneProperty` cannot read a reference out of the object, so it counts as an
unreadable link; and because an unreadable link in the outgoing body counts toward
_retention_, a specification nets zero drops and asks nothing.
`isScenePageSpecification` names the shape so an operator is not told "unreadable link"
about a page they are deliberately adding.

⚠️ **Corrected 4 September by a live report, and corrected again 5 September by running
it.** The reasoning here once continued "a page being created is not a page that could
break". A tester then reported posting this shape through `knack_update_view` and reading
back slugs — `menu-child`, `shared-page` — pointing at pages that did not exist, which was
filed as D2: the shape creates on a create and not on an update.

Driven live on 5 September, on a fresh menu built for it (`view_13` on `scene_10`), the
update path **does** create the page. Every row below is one `PUT` through the guard, and
the "created" column is Knack's own `changes.inserts.scenes`, not this server's prediction:

| `links[]` entry posted in an update                         | Stored as      | Created                                      |
| ----------------------------------------------------------- | -------------- | -------------------------------------------- |
| `type: "scene"`, `scene: {name, parent: <slug>, views: []}` | slug           | yes — `scene_17`, parent as given            |
| same, `parent` given as a scene **key**                     | slug           | yes — `scene_18`, parent stored as the key   |
| same, no `parent`                                           | slug           | yes — `scene_19`, no parent                  |
| same, `parent` naming no page                               | slug           | yes — `scene_20`, parent stored as given     |
| `type: "scene"`, spec with **no `views`**                   | **the object** | **no**                                       |
| spec with `views: []` but **no `type`** on the link         | **the object** | yes — `scene_21`; **again** on the next save |

Three things follow. **First,** a well-formed specification behaves the same on an update
as on a create, so D2 as filed is not reproduced and P8's update half is answered
positively. **Second,** the export's "never an object" (457 slugs, 2 nulls) was a fact
about that app, not about Knack: two of the six rows left an object in `scene`, and both
are live on `view_13` now. **Third,** the `type`-less row is a hazard of a kind the guard
does not model. The object survives the save, so the merged body re-sends it on every
later edit — a byte-identical re-send made `scene_22` under a second slug — and the
builder, which also saves the whole view, presumably does the same. `pagesCreated` was read
from the request rather than from Knack's response, so it named the `views`-less spec as
created when nothing was.

**All three were fixed the same afternoon and run live on the new build.**
`MALFORMED_PAGE_SPECIFICATION` refuses both malformed shapes — on the payload alone for a
create, on the merged body for an update; `STORED_PAGE_SPECIFICATION` refuses a copy or
move of a view holding a kept object, and an update that re-sends one, naming each
object's shape and the repair that fits it; and the response now reports
`pagesRequested` from the payload and `pagesCreated` from Knack's `changes.inserts`, with
key, slug and parent, plus `pagesRequestedButNotCreated` when the two disagree.

A review of the first cut moved three things. The `type` rule is judged on menu links
only, where it was measured — a table or details link column is `type: "link"` or
`"scene_link"` by design. A kept object is matched on name, parent and whether it carries
`views`, not on the link's `type`: adding `type: "scene"` to a kept factory object would
make its page again and is refused, while adding `views` to a kept dangling object is the
repair that creates its page and goes through. And `copy_view` now reads its source, so a
copy of a view holding a kept object is refused rather than duplicating it. Live: a
title change on a factory view refused; the two malformed shapes refused; each read back
unchanged; the well-formed shape allowed twice, each time creating its page and coming back
as a slug, with `pagesRequested` and `pagesCreated` naming the same page.

Knack's metadata was not lagging. A re-send under a minute after the write, with the new
slug swapped back for its spec, was refused with the page **named** as doomed and
`unresolved: 0`. So the tester's `unresolved: 2` means those two pages were absent from
fresh metadata at the time — a different payload shape, or pages created and then removed
between their calls. Which is the tester's to say, and the two links on `view_12` stay
live until they have.

**A copy that shares a table's pages, measured 5 September.** Knack's copy duplicates
them, so the question was whether a plain create would. A table was created on one page
with two link columns carrying well-formed page specifications — the first specification
ever posted in a _column_ rather than a menu link — and Knack made both pages and rewrote
the columns to their slugs, exactly as on a menu. A second table was then created on
another page with link columns naming those two slugs. Knack kept the slugs as posted,
`changes.inserts.scenes` was empty, and a snapshot shows both pages still with their one
original parent and the second page with no children. So a copy that shares pages is a
create built from the source's definition, and `knack_copy_view_sharing_pages` does
exactly that, checking Knack's response for those two facts before reporting the copy as
shared. Whether the shared page opens correctly when reached from the second table is a
builder-side check.

## 10. A menu's non-link settings, read back at last

The last sliver of C2. A menu title edit was known to preserve every link, but the menu's
own settings had never been read out of the builder. A menu **save** request supplies
them:

| Key                        | Value in the capture | Note                                                |
| -------------------------- | -------------------- | --------------------------------------------------- |
| `format`                   | `"tabs"`             | also seen as `"none"` on a freshly created menu     |
| `label`                    | `"Menu"`             | distinct from both `name` and `title`               |
| `title`                    | `""`                 | present and empty on the save, absent on the create |
| `auto_link`                | `true`               | not previously recorded anywhere                    |
| `menu_links_design_active` | `false`              | the design toggle, same shape as a column's         |

So a menu carries `name`, `label` **and** `title` as three separate strings, and
`auto_link` is a menu key this server has never written.

**C2's menu arm is now fully closed.** A before-and-after pair of the builder's own title
change settles it: both bodies carry 14 keys, **none added, none removed, and exactly one
value changed** — `title` from `""` to `"Testing"`. Every link came through untouched, and
so did `auto_link`, `label`, `format` and `menu_links_design_active`.

**And the `auto_link` worry was misplaced, for a reason worth keeping.** It could not have
been lost by an update: `buildEffectiveUpdateBody` is `{...storedAttributes, ...patch}`, so
any key the patch does not name survives by construction. The two failure modes are
different, and only one of them loses keys:

| Path         | How the body is made        | Can it lose a key it has never heard of? |
| ------------ | --------------------------- | ---------------------------------------- |
| **update**   | merged onto the stored body | **No** — unnamed keys pass through       |
| **template** | constructed from nothing    | **Yes** — every key must be written      |

`no_data_text` went missing from a **template**. `auto_link` cannot go missing from an
**update**. So the risk class is "keys a template must write", not "keys an update must
preserve" — and the place to look for the next `no_data_text` is
`buildViewTemplatePayload`, not the merge.

Pinned against the capture itself: real before + real patch is asserted to equal real
after, key for key. The merge strips `key` and `_id` and nothing else — deliberate, since
the endpoint is already addressed by key, and four live PUTs went through without them.
Reverting the merge to ignore the stored body fails 23 tests.

## 11. A declined confirmation does not write — reconstructed under doubt

Worth recording because it was reported as the opposite, and because the reconstruction
is the useful part.

A tester reported that `knack_update_view` returned `HUMAN_CONFIRMATION_DECLINED` twice
and that a live read-back nonetheless showed the update applied, with two links pointing
at pages that were never created. Read at face value, that is the guard's central promise
failing.

It is not what happened. Run against the guard with a spy transport, on the reported
payload and app shape:

| Call | Links stored | Prompted?              | PUT sent | Result                        |
| ---- | ------------ | ---------------------- | -------- | ----------------------------- |
| 1    | 0            | **no — auto-accepted** | **yes**  | `ok`                          |
| 2    | 4            | yes, `unresolved: 2`   | no       | `HUMAN_CONFIRMATION_DECLINED` |

Call 1 destroyed nothing, so `destroysNothing` was true and the guard auto-accepted
without asking — correct behaviour, and the call that wrote. Calls 2 and 3 were then
refused _because of_ call 1: the two slugs it wrote resolve to no scene, so
`expansion.unresolvedRefs` is 2, `destroysNothing` goes false, and a decline refuses. The
write and the declines are different calls in that order, and the code path agrees — a
decline returns before the mutation function is ever called.

Two details settle it beyond the reconstruction. `menu-child` is Knack's slugification of
"Menu Child", and this server slugifies nothing but builder URLs, so the read-back cannot
be a local echo. And "no dialog was visible" on the first call is correct rather than
suspicious: none was requested.

**What the report did find** is that the refusal on call 2 reads _"destroys 0 page(s) and
was not confirmed"_, because both refusal strings interpolate `requiredKeys.length` and
never mention the unresolved count that triggered them. The prompt itself was not at
fault — its headline already branches on the unresolved-only case and words it correctly —
but the refusal is what a caller reads back, and one that says nothing was at stake on a
call refused over two unresolved links understates its own reason. That is a defect in the
thing this work exists to provide: D1 in `TESTING.md`, fixed 5 September. Both refusals
now go through `describeRefusedStakes` and name pages, unresolved links, or both.

**The lesson for reading a report like this.** Every individual observation in it was
accurate. The inference joining them — that the declines caused the write — was the only
wrong part, and it was wrong because a silent successful call is invisible in a transcript
while a refusal is loud. Reconstruct the sequence before accepting the conclusion, and
reconstruct it in code rather than by argument.

## 12. A retarget is refused outright

**Decided by the operator, 4 September**, with the reasoning that settles it: _updating a
view is destructive._

Knack's view PUT replaces rather than patches. So changing `source.object` is not a
mis-scope that returns the wrong rows — every column, display connection, filter, sort
and rule in the view names a field on the object being replaced, and all of them are
written in the same request. The view does not move to a new object; it is overwritten
with one whose configuration refers to nothing.

No builder path produces it either. A view is bound to its object when it is added, and
across **eleven captured builder requests** `object` never changed once — three of those
were rescopes, which changed filter rules and source scoping keys and left the object
alone.

So `update_view` and `move_view` now refuse a payload whose `source.object` differs from
the stored one, with `SOURCE_OBJECT_CHANGE_REFUSED`, naming both objects and what to do
instead. **automated**, and reverting the comparison fails 2 tests.

Deliberately narrow, because the false-positive traps here are well documented:

- Both sides must be readable. Refusing on an unreadable stored object would make a view
  whose metadata omits its source permanently un-editable — the same trap unreadable links
  and url links already sprang.
- A payload with no `source`, or a `source` naming no object, is not a retarget. The
  overwhelming majority of updates are the former, and this never fires for them.
- A rescope that keeps the same object is allowed, which is the workflow the captures
  actually showed.

This was previously written up as a case to go and test. That framing was wrong: the
edit is not reachable in the builder, so there was no Knack behaviour to discover — only a
decision about what this server should permit. Recorded as much because mistaking a design
decision for an experiment is a way to spend a run and learn nothing.

## 13. Two guesses the export settled

Both were assertions I had written into code or a test comment without evidence, and an
adversarial review pass flagged them. Neither needed asking — the export answered.

**A sort entry always carries an `order`.** A test comment claimed Knack "defaults it",
and the runtime check accepted an entry without one while the TypeScript type required
it. Measured across the export: **428 stored sort entries, 0 omitting `order`** — 340
`asc`, 88 `desc`. So the type was right and the runtime was the laxer of the two.
`buildViewSource` now requires it, and requires the field to be a real field key: **241
of 241** `connection.key` values in the export are `field_N`, so a label like
`"Contact"` is not a shape Knack stores either.

**The hyphenated pair is `<connection>-<target field>`, connection first.** A record
rule's `connection_field` takes the form `field_784-field_74`. I had reported the pair
whole and defended it in a test comment; a review said it should not be reported as a
field key, and neither of us knew which half was which.

The field schema settles it. All 30 `connection_field` values in the export are
hyphenated — no dotted or plain form exists — and in 4 of 4 checked against the 1,911
field definitions:

| Pair                  | First half                         | Second half            | Second lives on the object the first points at |
| --------------------- | ---------------------------------- | ---------------------- | ---------------------------------------------- |
| `field_218-field_217` | `connection` on object_16 → obj_18 | `connection` on obj_18 | yes                                            |
| `field_199-field_57`  | `connection` on object_16 → obj_4  | `name` on obj_4        | yes                                            |
| `field_297-field_296` | `connection` on object_16 → obj_24 | `number` on obj_24     | yes                                            |
| `field_233-field_57`  | `connection` on object_19 → obj_4  | `name` on obj_4        | yes                                            |

Two of them share a second half (`field_57`) through different connections, which is
what "reach the same field by two routes" looks like. So the first half is always the
connection, and `distinctDisplayKeys` reports that rather than the pair.

One caveat kept deliberately: the pair was captured from a second app, while the schema
that decodes it comes from the export. The structural rule held 4 of 4 and the semantics
are a reporting choice rather than a mutation, so the risk of being wrong is low — but it
is a cross-app generalisation and worth revisiting if a capture ever contradicts it.

## 14. Shape claims audited

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

**The move envelope, and how it differs from copy:**

`{action: "move", target_scene_key, view_key, completeViewSchema}` — the same envelope as
copy with a different action. One difference in the body: a **copy** sets
`key: "new"` while a **move** keeps the view's real key (and both keep `_id`). So the
key is how Knack tells "make me a new one" from "relocate this one", and
`knack_move_view` should carry the existing key rather than blanking it.

**More reference sites, from a details view and a form:**

- A **details** view nests `connection: {key}` on field items several levels deep, inside
  `columns[].groups[].columns[][]`. The generic walk finds them; an enumerated path list
  written for tables would not have.
- A **form input** carries `source.connections[]` —
  `{field: {key}, source: {type: "input", field: {key}}}` — filtering the options of one
  connection by the value chosen in another input. A fifth filter-shaped structure.
- `connection_field: "field_784-field_74"` is a **hyphenated field pair**, a third
  reference format after the bare key and the dotted `object_N.field_N`.
- A form input can carry `view: "view_2835"`, naming another view for inline
  option-inserts. Another cross-view reference no other check reads.
- Record IDs appear as literal criteria values (`value: ["69c65fc34474ae2b2f53d409"]`) and
  inside connection defaults with an HTML `identifier`. Copying a view carries those
  record IDs verbatim, which is fine within an app and meaningless across one.

**How the builder actually scopes to an account, 4 September:**

Three captures of one view rescoped three ways settle which mechanism each builder
control writes — and none of them added `connection_key`:

| Builder action                 | What it wrote                                              |
| ------------------------------ | ---------------------------------------------------------- |
| baseline                       | `criteria.rules: []`                                       |
| scope to the logged-in account | `rules: [{field, operator: "user", value: ""}]`            |
| scope to one specific record   | `rules: [{field, operator: "is", value: ["<record id>"]}]` |

So `operator: "user"` — the 60-occurrence mechanism in the export — is what the **filter**
UI writes for "the logged-in account", while `source.authenticated_user` comes from
configuring the **source** itself. Two controls, two mechanisms, and the filter route
leaves the source block untouched. That also means V7's two arms are reached by different
builder paths rather than being alternatives for the same one.

All three captures left `columns[].connection.key` unchanged, which is the scope/display
split holding for a third time.

**`limit` has four forms, not one:** `""` (472), `null` (85), absent (28) and an actual
number (2 — a real row cap). `buildViewSource` writes `""`, the most common; the constant
should not read as though it were the only form.

**Corrected by the copy requests, 4 September:**

- **The "four source patterns" were a closed taxonomy, and are not.** Both payloads
  carried `connection_key` + `relationship_type` + `authenticated_user` +
  `parent_source` in one block — a combination none of the four documented patterns
  shows. The keys are independent switches that compose freely; the four were simply the
  combinations the first export happened to contain. `buildViewSource` already built
  additively, so only the documentation was wrong — but a reader would have judged the
  real combination unobserved.
- **`limit` is not always present.** The builder omits the key entirely where
  `buildViewSource` always writes `limit: ''`. Knack accepted our explicit empty string
  in a round-trip, so both work; it is not mandatory.
- **`sort` was unreachable through the templates.** `buildViewSource` hardcoded
  `sort: []` with no option to pass one. Both payloads carried a real sort and they
  differed (`desc` on one field, `asc` on another), so a rebuild through the template
  silently reordered the view. Now an option, with an entry missing a field refused
  outright — a sort with no field is stored and orders nothing, which reads as a working
  sort in the builder.

**Measured since the audit:**

- **`no_data_text` belongs to two view types only.** Across the same 738 views it appears
  on `table` (217 of 224) and `list` (6 of 6), and on none of the 74 details, 152 form,
  48 menu, 2 calendar, 38 report, 55 login, 120 registration or 19 rich_text views. A
  `search` view carries it too, seen in a builder save request rather than the export.
- **Nobody leaves it blank on purpose.** All 223 stored values are non-empty. None
  contains a template token, so the string cannot vary at render time — a value derived
  from the object at build time is the whole of what Knack allows.
- **Corrected 4 Sep:** this said every value was "exactly two words". True of all 223 in
  that app, false in general — two builder copy requests from a second app carried
  three- and four-word values, neither using the word "Records". Non-empty is the rule;
  length is not. The derived default is a floor, and `noDataText` matches a differing
  house style.

**Not audited, and not claimed:** `formattedShape` and `rawShape`, 32 of each. They
describe record _values_, and a schema export holds no records.
`knack_verify_record_field_shapes` is the way to check those.

## 15. Defects found by testing, and fixed

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

- **Two refusals that understated their reason.** `HUMAN_CONFIRMATION_DECLINED` and
  `HUMAN_CONFIRMATION_UNAVAILABLE` interpolated only the count of named pages, so a
  refusal caused entirely by unresolved links read _"destroys 0 page(s)"_. Found by the
  live report in §11. The prompt headline already had its own wording for that case; the
  refusals now share the same split through `describeRefusedStakes`, and the decline's
  details carry the unresolved count.

- **Two page-specification shapes Knack mishandles, and the guard let through.** A
  specification with no `views` array is stored as the raw object and creates no page; a
  link with a specification but no `type: "scene"` creates the page and keeps the object,
  so every later save creates it again. Found by driving the D2 report live (§9).
  `MALFORMED_PAGE_SPECIFICATION` now refuses both, and `STORED_PAGE_SPECIFICATION` refuses
  a copy, move or re-sending update of a view already carrying a kept object, naming which
  shape each object is and the repair that fits it. Both run live on the new build: three
  refusals, three clean read-backs, and the well-formed shape still creating its page.

- **`pagesCreated` reported the request, not the result.** It was read from the payload,
  so a `views`-less specification was reported as created while Knack made nothing. The
  response now carries `pagesRequested` from the payload and `pagesCreated` from Knack's
  `changes.inserts`, with key and slug, and names anything requested that did not arrive.

## 16. Two findings about testing agents

Worth keeping because they change how a run is designed, not just what it finds.

- **A calling agent is not a witness to whether a prompt appeared.** Elicitation goes to
  the client and never returns to the model. On three runs an agent reported "no
  confirmation prompt was raised" when one had been. It is the one thing such an agent can
  never truthfully report, and it invalidated three runs before being understood.
- **A calling agent can refuse before the human is asked.** One declined to submit a run
  on its own policy grounds, before any prompt existed. `cascadeDeleteBehaviour:
prompts-human` describes the transport's capability, not the agent's willingness.

## 17. Test-design errors made along the way

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
- **A fix threaded most of the way.** `createsPages` was computed in the guard, added to
  its returned decision, and then dropped by the tool wrapper that builds the response —
  so the claim that a caller is told which pages a copy creates was unmet twice, once
  when the predicate was wired nowhere and again when the value stopped one layer short.
  Both times a review found it, not a test. The lesson is that "I added the field" is not
  the same as "the field reaches the caller", and only a test at the outermost boundary
  can tell them apart.
- **Mutation testing proves a rule fires, not that it is aimed correctly.** Every rule in
  this work was verified to fail when reverted, and that caught nothing in this round: the
  six findings were a rule pointed at the wrong path shape, a boundary assumed to be
  validated because its TypeScript type looked right, a reduction applied to the wrong
  half of a compound value, and a value dropped between two layers. All four are invisible
  to "revert it and watch a test fail", because the test agrees with the rule. Reading the
  diff adversarially found them; running it did not.
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
