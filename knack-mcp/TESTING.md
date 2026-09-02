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

---

## 1. Test app setup

Build a small app containing one of each shape the guard reasons about:

| # | Shape | Why it matters |
|---|-------|----------------|
| S1 | A table with two link columns ("View details" / "Edit"), where the detail page has its own child pages **2–3 levels deep** | Descendant expansion; grandchildren were once silently dropped from the prompt |
| S2 | A **details** view and a **calendar** view with child-page links | These are `type: "scene_link"` internally and were once invisible to the guard |
| S3 | A **search** view with a link column in its results | Link in a non-obvious container (`results.columns[]`) |
| S4 | A **form** with a Link/URL field, and a submit rule redirecting to a page | Both are decoys: `type: "link"` with no `scene`, and a `scene` that is not navigation |
| S5 | A **menu** view with: links to its own child pages, a link to a top-level page that exists elsewhere, and an external URL entry | The original incident surface; owned vs external vs url in one view |
| S6 | One child page linked from **two different views** | The `transferred` class (page re-parents instead of dying) |
| S7 | Accented page names (é, à, ç) and one page **renamed after creation** | Slug vs name drift; slug matching is case/trim-sensitive code |

Configure `app.json` with `allowViewMutation: true` and `allowDelete: true`.

## 2. Client capability matrix

| ID | Case | Expected |
|----|------|----------|
| M1 | `knack_list_apps` from a client that advertises elicitation | Banner reports the client name and `cascadeDeleteBehaviour: prompts-human` |
| M2 | `knack_list_apps` from a client that does not | `refuses`, and **every** destructive case below is refused with `HUMAN_CONFIRMATION_UNAVAILABLE` — never silently allowed |

Run the destructive sections (3, 4) once in each mode.

## 3. Correctness — does it do what it claims

| ID | Case | Expected |
|----|------|----------|
| C1 | Title/description edit on **every** link-bearing view type (S1–S5) | Goes through with **no prompt**; page links intact after |
| C2 | After each C1 edit, open the view in the **builder** and check every setting survived: filters, sorts, column rules, form rules, display/design settings | Nothing silently reset. ⚠️ The rebuild-from-metadata was verified complete for **tables only**; details, form, calendar and menu are unverified — this is the highest-value check in the plan |
| C3 | Remove one link column (S1) / one menu entry (S5) | Prompt lists **exactly** the right pages, including grandchildren, with names |
| C4 | Accept a C3 prompt | `pagesKnackReportsDeleted` in the response matches the prediction exactly; builder confirms |
| C5 | Decline a C3 prompt | Zero mutation — view and pages untouched, response says `HUMAN_CONFIRMATION_DECLINED` |
| C6 | Cut the link to an **external** page (S5's link to the elsewhere page) | No pages destroyed; response names the severed link; page survives in the builder |
| C7 | Cut one of the two links to the **transferred** page (S6) | Page survives and re-appears in the builder under the view that still links to it |
| C8 | `delete_view` and `move_view` on a link-bearing view; `move_view` on a menu | Every link counts as dropped; full doomed set prompted; decline leaves everything intact |
| C9 | `copy_view` of a link-bearing view | Allowed without prompt (source untouched); verify source pages intact after |
| C10 | A mutation's snapshot file | Present, timestamped, contains the full scene tree and the view definition |

## 4. Adversarial — try to get around the guard

| ID | Case | Expected |
|----|------|----------|
| A1 | `links` array nested deep inside another property of an update payload | Treated as navigation; retention arithmetic still applies |
| A2 | Scene ref written as `{key: "..."}` object instead of a string; slug in different case; slug with surrounding whitespace | All resolve; no page silently reclassified |
| A3 | Replace one *broken* (unresolvable) link with a **different** broken link in the same payload | Known weak spot: unreadable links are counted by tally, not identity. Document what happens |
| A4 | Put a `scene` ref inside a form **submit rule** in the payload while dropping the real navigation link in the same PUT | Must still prompt — a rule redirect is not a retained link (regression: this exact hole was fixed) |
| A5 | Race: edit the view in the **builder**, then immediately send a title edit via MCP | Known weak spot: the merged body is built from a metadata read and may overwrite the builder change. Document exactly what is lost |
| A6 | Page cycle: page A links to B, B links back to A; delete the view holding A's link | No hang, no infinite doomed list; walk terminates |
| A7 | A link whose slug belongs to a page that was deleted in the builder (stale ref) | Counted as **unknown/at-risk**, prompt warns "more may be destroyed than listed" — never treated as safe |
| A8 | Payload nested ~25 levels deep; invalid JSON; empty `{}` | `STRUCTURE_TOO_DEEP` / `INVALID_UPDATES_JSON` / `EMPTY_UPDATE_PAYLOAD` — all before any request reaches Knack |
| A9 | Legacy `confirmDestructive: true` on a cascade-risky update | `CONFIRMATION_UPGRADE_REQUIRED`; the flag never authorizes anything |
| A10 | `create_view` with a payload carrying links to existing pages | Allowed (nothing replaced); verify the linked pages are unaffected |

## 5. Fail-closed paths

| ID | Case | Expected |
|----|------|----------|
| F1 | Unreachable metadata (temporarily wrong `appId`) | `COULD_NOT_VERIFY_VIEW` / `SCENE_TREE_UNAVAILABLE`; no PUT sent |
| F2 | Snapshots directory read-only or missing | `SNAPSHOT_FAILED`; mutation refused |
| F3 | Elicitation prompt left unanswered past the timeout | Treated as declined; no mutation |

## 6. Recovery drill (run once per release)

1. Take a `knack_snapshot_app`.
2. Deliberately accept a cascade delete of a 3-page subtree (S1's detail branch).
3. Rebuild those pages **using only the snapshot file**.

If the rebuild needs information the snapshot does not hold, that is a finding — the
snapshot's entire purpose is this drill.

## Results log

One row per full run. Baseline first.

| Date | Commit tested | App | Client(s) | Sections run | Pass / findings |
|------|--------------|-----|-----------|--------------|-----------------|
| | | | | | |
