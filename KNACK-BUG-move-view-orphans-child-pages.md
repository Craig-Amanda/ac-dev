# Moving a view orphans all but one of its child pages

**Area:** Builder — page/view management
**Severity:** Data integrity — unreachable pages are left in the app, and nothing reports them
**Reproducibility:** 2/2 identical. **Reproduces in the current builder and in Next Gen.**
**Measured:** 11 September 2026, app `6aa3c4cfb11f1a7bf738ed11`

## Summary

Moving a view that owns child pages does not re-parent those pages. Knack **rebuilds** them under the destination with new keys and new slugs, then deletes the originals — but **only the first original is deleted**. The rest survive, still parented to the source page, linked by no view.

Total page count **rises** on an operation expected to be neutral: 244 → 247 in both runs.

## Steps to reproduce

1. On page A, add a details view with four `scene_link` columns, each creating a child page (the view owns all four; none is `remote`).
2. Confirm each child page has exactly one referrer — the view.
3. Move the view from page A to page B, confirming the child-page prompt.
4. Inspect the page list. `GET /v1/applications/{appId}` shows keys, slugs and parents — how the data below was captured.

## Expected

Either the four child pages are re-parented to page B keeping their keys and slugs, or they are rebuilt under page B and **all four** originals are removed. Either way the page count is unchanged and nothing unreachable remains.

## Actual

- Four **new** pages created under page B, each with a new key and a slug suffixed `2`.
- **One** original deleted — the one belonging to the *first* `scene_link` in column order.
- The other **three** originals remain under page A, referenced by nothing.

## Run 1 — original slugs

`view_1822` moved from `scene_528` (`client-summary`) to `scene_592` (`test-assess-parts-copy`). 244 → 247 pages.

| | Key | Slug |
|---|---|---|
| Created under destination | `scene_597`–`scene_600` | `edit-client2`, `view-client-details3`, `edit-client-22`, `view-client-details-22` |
| Deleted | `scene_593` only | `edit-client` |
| **Orphaned** | `scene_594`, `scene_595`, `scene_596` | `view-client-details2`, `edit-client-2`, `view-client-details-2` |

## Run 2 — child pages renamed first, to rule out a slug collision

All four child pages were given fresh names and slugs colliding with nothing in the app: `remove-the-client`, `view-the-client`, `update-the-client`, `double-view-the-client`. `view_1822` was then moved back to `scene_528`. 244 → 247 pages.

| | Key | Slug |
|---|---|---|
| Created under destination | `scene_601`–`scene_604` | `remove-the-client2`, `view-the-client2`, `update-the-client2`, `double-view-the-client2` |
| Deleted | `scene_597` only | `remove-the-client` |
| **Orphaned** | `scene_598`, `scene_599`, `scene_600` | `view-the-client`, `update-the-client`, `double-view-the-client` |

**The outcome is identical with non-colliding slugs.** The `2` suffix is therefore self-inflicted: each new page is created while the original it replaces still exists, so the slug it wants is always taken — by itself. Renaming cannot avoid it.

In both runs the single deleted page was the **first** `scene_link` in the view's column order, which suggests the deletion loop terminates after one iteration.

## Impact

1. **Silent growth.** Each move of a view owning *n* child pages leaves *n − 1* unreachable pages. Repeated moves compound it.
2. **Invisible to a referrer count.** The builder's warning before destroying a child page rests on that page having a referrer. An orphan has **zero**, not one, so "would removing this link destroy the page?" answers *no* — there is no link left to remove. Orphans evade exactly the check most likely to find them.
3. **No builder surface reports them.** They appear in no navigation, and nothing flags a page that no view links.
4. **Slug churn breaks external references.** Rebuilt pages have new slugs, so any bookmarked URL, embedded link, or address held outside the app stops working — as does any reference from a view nobody touched.

## Secondary issue, same operation

The moved view's row is removed from the source page's layout, but the row is left in place, empty — `{"columns":[{"keys":[],"width":100}]}` persists in that page's `groups`. It reproduces after every move and accumulates; the two moves above left one empty row on each page involved. It renders as a gap.

## Suggested fixes

1. Ensure the deletion of originals iterates over **all** rebuilt child pages, not only the first.
2. Better: re-parent the child pages instead of rebuilding them, preserving keys and slugs so external references survive.
3. If rebuilding must remain, delete each original **before** creating its replacement, so the replacement can keep the slug.
4. Remove the emptied layout row from the source page.

Full before/after `GET /v1/applications/{appId}` payloads for both runs, and the builder's `POST /copyview` request body, available on request.
