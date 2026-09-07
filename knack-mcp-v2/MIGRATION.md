# Migrating from knack-mcp

Same capabilities, fewer tools. Point your client at `knack-mcp-v2/dist/index.js`
instead of `knack-mcp/dist/server.js`; `server-readonly.js` becomes the `--readonly`
flag. The apps folder, `app.json` permissions, secrets file and cache files are unchanged.

## Catalogue size

Measured with `npm run catalogue` against a stub app that opts into everything, tokens
at four characters each:

| Mode      | knack-mcp                         | knack-mcp-v2                     | Change |
| --------- | --------------------------------- | -------------------------------- | ------ |
| full      | 64 tools, 42.2 KB, ~10,600 tokens | 49 tools, 28.1 KB, ~7,000 tokens | −33%   |
| read-only | 39 tools, 21.6 KB, ~5,400 tokens  | 34 tools, 19.0 KB, ~4,800 tokens | −12%   |

`knack_list_page_referrers` and `knack_get_page_access` are the two tools here with no
legacy counterpart. The first costs about 235 tokens of the budget above — measured, not
estimated; a per-tool average would have said 140, and this one carries more schema and
a longer description than average because both halves of what it reports need saying.
The second costs about 115 tokens (one required argument, one sentence).

`knack_snapshot_app` and `knack_get_view_payload_template` never send a request to
Knack — one writes to the local app folder, the other only builds a payload — so both
are `read` access and always advertised, including under `--readonly`. An earlier draft
of this rewrite gated them behind `allowViewMutation` like the tools that actually
mutate a view; that withheld the one tool that helps build a payload for those gated
tools, and took away the snapshot this tool exists for on exactly the apps where a
manual builder change is riskiest. Fixed before merge.

## Tool mapping

Every legacy tool is reachable. Where a row shows a parameter, the old behaviour is
that parameter's value on the new tool.

| Legacy tool                                                                                                 | New tool                                                                                                     |
| ----------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------ |
| `knack_list_apps`                                                                                           | `knack_list_apps`                                                                                            |
| `knack_set_context`                                                                                         | `knack_set_context`                                                                                          |
| `knack_cache_status`                                                                                        | `knack_cache`                                                                                                |
| `knack_refresh_cache`                                                                                       | `knack_cache` with `refresh: true` (`warm`, `persistFiles` unchanged)                                        |
| `knack_list_objects`                                                                                        | `knack_list_objects`                                                                                         |
| `knack_get_object`                                                                                          | `knack_get_object` with `detail: "summary"`                                                                  |
| `knack_get_object_fields`, `knack_list_fields`                                                              | `knack_get_object` with `detail: "fields"` (the default)                                                     |
| `knack_list_field_types`                                                                                    | `knack_get_object` with `detail: "types"`                                                                    |
| `knack_get_raw_object`                                                                                      | `knack_get_object` with `detail: "raw"` (still needs `allowDiagnostics`)                                     |
| `knack_get_raw_object_metadata`                                                                             | `knack_get_object` with `detail: "rawMetadata"` (still needs `allowDiagnostics`)                             |
| `knack_get_field`                                                                                           | `knack_get_field`                                                                                            |
| `knack_resolve_field_alias`, `knack_resolve_any`, `knack_get_field_type`                                    | `knack_resolve` with `identifier`                                                                            |
| `knack_get_object_connections`                                                                              | `knack_get_object_connections`                                                                               |
| `knack_describe_field_shape`                                                                                | `knack_describe_field_shape`                                                                                 |
| `validateFieldMapping`                                                                                      | `knack_validate_field_mapping`                                                                               |
| `generateSnapshotStructure`                                                                                 | `knack_generate_snapshot_structure`                                                                          |
| `checkForDuplicateFieldUsage`                                                                               | `knack_check_duplicate_field_usage`                                                                          |
| `knack_get_record`                                                                                          | `knack_get_record`                                                                                           |
| `knack_find_records`                                                                                        | `knack_find_records`                                                                                         |
| `knack_get_object_records_with_schema`                                                                      | `knack_find_records` with `includeSchema: true`                                                              |
| `knack_get_related_records`                                                                                 | `knack_get_related_records`                                                                                  |
| `knack_aggregate_records`                                                                                   | `knack_aggregate_records`                                                                                    |
| `knack_verify_record_field_shapes`                                                                          | `knack_verify_record_field_shapes`                                                                           |
| `knack_create_record`, `knack_batch_create_records`                                                         | `knack_create_records` (`records: [...]`, one element for a single create)                                   |
| `knack_update_record`, `knack_batch_update_records`                                                         | `knack_update_records`                                                                                       |
| `knack_delete_record`, `knack_batch_delete_records`                                                         | `knack_delete_records` (`recordIds: [...]`, `confirm: true`)                                                 |
| `knack_upload_asset`                                                                                        | `knack_upload_asset`                                                                                         |
| `knack_download_file`, `knack_read_file`                                                                    | unchanged                                                                                                    |
| `knack_list_scenes`, `knack_list_views`                                                                     | unchanged                                                                                                    |
| `knack_get_view_context`                                                                                    | `knack_get_view` with `detail: "context"` (the default)                                                      |
| `knack_list_view_fields`                                                                                    | `knack_get_view` with `detail: "fields"`                                                                     |
| `knack_get_view_attributes` (`includeRawAttributes`)                                                        | `knack_get_view` with `detail: "attributes"` (param renamed to `includeRaw`; still needs `allowDiagnostics`) |
| `knack_plan_view_repoint`                                                                                   | `knack_plan_view_repoint`                                                                                    |
| `knack_get_view_payload_template`                                                                           | `knack_get_view_payload_template`                                                                            |
| `knack_get_view_payload_template_from_view` (`targetViewType`)                                              | `knack_get_view_payload_template` with `fromViewKey` (param renamed to `viewType`)                           |
| `knack_snapshot_app`                                                                                        | `knack_snapshot_app` (snapshot version 3: scenes carry their access fields, plus a `profiles` map)           |
| _(no legacy tool)_                                                                                          | `knack_list_page_referrers`, `knack_get_page_access`                                                         |
| `knack_create_view`, `knack_update_view`, `knack_update_view_order`, `knack_move_view`, `knack_delete_view` | unchanged                                                                                                    |
| `knack_copy_view`                                                                                           | `knack_copy_view` (`sharePages: false`, the default)                                                         |
| `knack_copy_view_sharing_pages` (`sourceViewKey`)                                                           | `knack_copy_view` with `sharePages: true` (param renamed to `viewKey`)                                       |
| `knack_get_context_bundle`, `knack_get_app_overview`, `knack_analyze_data_model`, `knack_app_deep_dive`     | unchanged                                                                                                    |
| `knack_list_field_references`                                                                               | `knack_list_field_references`                                                                                |
| `knack_find_views_with_record_rule_field`                                                                   | `knack_list_field_references` with `classification: "viewRecordRule", groupByView: true`                     |
| `knack_search_ktl_keywords`, `knack_search_emails`, `knack_generate_seed_csvs`                              | unchanged                                                                                                    |
| `knack_create_field`, `knack_update_field`, `knack_delete_field`, `knack_duplicate_field`                   | unchanged                                                                                                    |

## Response shape changes

- A single record create, update or delete now returns the batch shape: per-record
  results under `results`, not a single `status`/`body` pair.
- `knack_find_records` with `includeSchema` adds the schema fields beside the records
  response instead of wrapping the records under `recordsResponse`.
- Errors are returned as `{ "ok": false, "tool": "...", "error": "..." }` with the MCP
  `isError` flag, instead of a bare error string.
- `KNACK_MCP_COMPACT_TOOL_METADATA` is gone; descriptions are short at source.
- Two tools now enforce an app's `dataAccess` policy that previously read past it
  (legacy did the same; carried over rather than introduced here, and closed before
  merge): `knack_verify_record_field_shapes` excludes redacted and non-allowed fields
  from its preview and refuses an object outside `allowedObjectKeys`;
  `knack_generate_seed_csvs` no longer reads a connected parent object outside
  `allowedObjectKeys` to borrow its display values, and reports any it skipped under
  `policyBlockedConnectionTargets`.
- `knack_generate_seed_csvs` with `useExistingConnectionValues` now fills connection
  cells with each parent record's **display value** (the field the object's metadata
  names as `identifier`), which Knack's importer matches to a record. Legacy fell
  through to the record id, because a live record carries no top-level `identifier`
  key; the per-field note now names the field actually read. Measured 6 September on
  the disposable test app.
- `knack_create_view` and `knack_copy_view` (both modes) now return a `snapshotPath`
  too. The guard still writes nothing _before_ them, since they destroy nothing; the
  snapshot is taken _after_ Knack answers and holds the view that was made plus the
  scene tree as it stands with any pages the create added. Legacy wrote no snapshot for
  a create or copy, so a view that only ever existed through one had no definition on
  disk — the 6 September recovery drill could not rebuild a copied table deleted in the
  builder. For Knack's own copy the new view is read back from fresh metadata; when it
  is not there yet the response carries a `snapshotNote` and the snapshot holds the
  tree without the view.
- A view mutation whose sent body carries a page link that names no page (neither a
  scene key nor a slug in the tree) now returns `danglingLinks` (each ref with where it
  sits) and a `warning`. The guard is unchanged and still lets it through: adding such
  a link destroys nothing, so nothing is asked. Knack stores it and it opens nothing —
  that is how the two dangling links on the 4 September menu came to be, and on 6
  September another was stored in silence while setting up A3. Menu entries that point
  outside the app are not counted.
- `knack_create_records` and `knack_update_records` take each record's values as an
  object or as a JSON string; `knack_update_view_order` takes `order` as an array or
  its JSON and no longer requires `pageGroups` (omitted, it becomes one full-width row
  per view in the order given). Legacy accepted JSON strings only and required the
  layout, so the natural shape failed MCP input validation before the handler ran — a
  schema error that never reached the permission checks or the guard, and that spoiled
  two rows of the 6 September permission matrix until the payloads were re-sent.
- A `dataAccess.allowedFieldKeys` entry that overlaps `redactedFieldKeys`, or that
  names a field the schema no longer has, is now silently excluded from what a record
  read returns — it used to make every read of that object fail.

- Snapshots are `snapshotVersion: 3`. `parseRuntimeScenes` now keeps a scene's `type`
  (as `sceneType`), `authenticated`, and `allowed_profiles` / `limit_profile_access`
  (as `allowedProfiles` / `limitProfileAccess`) on the scene and on its views, so a
  `login` view's roles survive into the snapshot; the file also carries `profiles`,
  mapping each profile key to the user object that defines it. Nothing else in the
  snapshot moved. `knack_list_scenes` with `includeViews` shows the same three fields on
  a login view and nowhere else, so a Tier 1 differential row differs from legacy only
  on an app that has one.
- The cascade prompt's `CHECK THE AUDIENCE` paragraph, added 7 September as a question,
  is now an answer where it can be: on a move or a transfer it names who reaches each
  re-parented page before and after and says `CHANGES`, `unchanged` or `UNKNOWN`. The
  question is kept, in the same words, for whatever it cannot resolve.

## What did not change

The safety guard (`lib/view-safety.ts`) is byte-for-byte the legacy module, the
per-app permission model and its error messages are the same, the snapshot format is
`snapshotVersion: 2`, and every note string a tool returned (`cacheNote`, `mergeNote`,
the read-policy refusals) is preserved.
