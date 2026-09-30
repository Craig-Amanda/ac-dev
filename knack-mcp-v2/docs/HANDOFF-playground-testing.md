# Handoff: test the Knack MCP changes on the playground

You are a local agent with the `knack-mcp-v2` server connected and access to the Noah's
Place playground app. Your job is to run the tests below against the **playground only**,
record exactly what happens, and report back. You are testing; you are not fixing. If
something fails, record it and carry on. Do not edit code, do not commit, do not push.

## What changed (read this first)

Branch `ccr-ac9968fd-i54dc0`, pull request #84 in `Craig-Amanda/ac-dev`. What is new:

- **API usage from Knack's rate-limit headers.** `knack_list_apps` now returns `apiUsage`
  for each app. Requests wait for the burst window instead of drawing 429 errors. A 429 once
  the daily allowance is spent is not retried. Record batches the daily reading says cannot
  fit are refused. See "API usage" in `knack-mcp-v2/README.md`.
- **Table descriptions and field stamps.** A table's description lives on its
  auto-increment (AI) field. `knack_create_object` now requires `description` and
  `notedBy`. `knack_create_field` now requires `notedBy` on every field. See "Table
  descriptions" and "Field description notes" in the same README.
- **Descriptions in one note.** Every field description is now stored as
  `_notes=[<words> | <name> on <date>]`, and `knack_create_field` also requires
  `description`. The auto-increment field the server adds is named `AI`.
- **Automatic cache clearing.** After any successful change the server clears that app's
  cached metadata, so a read straight afterwards is fresh. There is no need to run
  `knack_cache` by hand between steps.
- **Table keywords.** A keyword in the description of a table's auto-increment field applies
  to every field on that table. `dataAccess.objectKeywords` in `app.json` is retired, and an
  app that still sets it is refused. See "Keywords for a whole table" in the README.

The code is covered by unit tests against a fake Knack. Nothing has run against a real app.
The real behaviour of Knack is what you are here to find out.

## Safety rules

- Use only the playground app. If `knack_list_apps` shows several apps, pick the one named
  for Noah's Place / the playground. If you are not sure which, stop and ask the human.
- Create only objects named `MCP probe ...`. Delete every one you create before you finish
  (step 13). Never delete or edit anything else.
- The REST API key is a secret. Never print it, log it or paste it into your report. For
  the `curl` steps, read it from the secrets file into a shell variable and use it there.
- If the playground's `app.json` has `readonly: true` or `allowDelete: false`, tell the
  human. Do not edit the secrets file.

## Setup

```bash
git fetch origin
git checkout ccr-ac9968fd-i54dc0 && git pull
npm ci            # from the repository root, never inside knack-mcp-v2
npm run build
```

Restart the Claude session so the new tool schemas load, then confirm with
`knack_list_apps`: each app must have an `apiUsage` object. If it does not, the old build
is still running; stop and say so.

Then refresh the caches for the playground once, so the first reads start clean:
`knack_cache { appKey, refresh: true, warm: true }`. After that you should not need to
again: a successful change clears that app's cache by itself (its `cacheNote` says so).
Step 9 checks this, so do not refresh by hand before the read-backs it asks for.

## Tests

For every step record: the exact call, the response (trimmed to the relevant keys), and
**pass**, **fail** or **unexpected** against the stated expectation. Quote errors verbatim.

### Part A: API usage

1. **Before and after.** Call `knack_list_apps`. Note `apiUsage` for the playground (it may
   be `null` if no call has been made yet). Make any read call, for example
   `knack_list_objects`, then `knack_list_apps` again.
    - Expect: `apiUsage.plan` filled in (`limit`, `remaining`, `used`, `percentUsed`,
      `resetsAt` at exactly 00:00:00 UTC, to the second), `burstLimit: 10`, `callsThisSession`
      above 0 and `readAt` set. There is no `burst` block any more: the window lasts about a
      second, so only its size is reported. `knack_list_objects` alone must leave `plan` as
      `null` (it reads metadata, not the REST API).
2. **Burst pacing.** Pick any object with records. Fire 15 `knack_find_records` calls at
   once (`rowsPerPage: 1`), in parallel if you can.
    - Expect: all 15 succeed; none reports a 429. Record how long the batch took, and
      `apiUsage.callsThisSession` and `plan.remaining` afterwards.
3. **Headers by `curl`.** Tool responses do not show headers, so use `curl -i` with the
   app id and REST key from the secrets and apps files (keep the key in a variable):
   `X-Knack-Application-Id`, `X-Knack-REST-API-Key`, base `https://api.knack.com/v1`.
   Run: (a) `GET /objects`, (b) a `GET` for a record that does not exist
   (`/objects/<key>/records/000000000000000000000000`), (c) a write, once the probe table
   exists (step 7): `POST /objects/<probe key>/records` with body `{}`.
   For each, paste **only** the `x-planlimit-*` and `x-ratelimit-*` header lines.
    - Expect: the same six headers on all three. Say if any is missing.
4. **Provoking a 429.** Fire 15 `curl` reads at once, straight at the API (not through the
   server, which paces itself). Say whether any returned 429, and paste its
   `x-ratelimit-*` and `retry-after` headers if it did.
5. **Two apps, one account.** Apps share the daily reading when they share a
   `builderAccountSlug` in `app.json` (case ignored) or, if that is not set, the account slug
   in their loaded metadata. Take the playground and another app on the same Knack account
   (for example Noah): run `knack_list_objects` on both so their metadata loads, make a REST
   read on the playground (`knack_find_records`), then call `knack_list_apps`.
    - Expect: the other app's `apiUsage.plan.remaining` matches the playground's, even
      though it has made no REST call. Also try an app on a different account: it must not
      match. If you cannot find two apps on one account, write "not applicable".

### Part B: table descriptions and field stamps

Use `notedBy: "Test agent"` throughout.

6. **Dry run.** `knack_create_object { name: "MCP probe 1", description: "Temporary probe
table. Safe to delete.", notedBy, dryRun: true }`.
    - Expect: `create_object_dry_run` with `wouldWriteDescription`; **no** new table in
      `knack_list_objects`.
7. **Create (answers D1).** Same call with `dryRun: false`.
    - Record the whole `objectDescription` block and the new `objectKey`.
    - The key question: is `addedAutoIncrementField` present? If **absent**, Knack made its
      own AI field when the table was created through the API. If **present**, Knack did
      not, and the server added one named `AI`.
    - Expect `objectDescription.ok: true` and `verified: true`, and no `warning`.
8. **Read it back.** `knack_get_object { objectKey, detail: "fields" }`.
    - Record: every field's key, name and type; `objectDescription` (`fieldKey`, `text`,
      `autoIncrementKeys`). Expect `text` to be the description without the stamp.
    - Also check `knack_list_objects` and `knack_get_app_overview`: the probe table must
      show its description, cut to 160 characters, with no `_notes` stamp.
    - Call `knack_get_field` on the AI field and paste its raw `description` and
      `meta.description`. Expect exactly `_notes=[Temporary probe table. Safe to delete. | Test agent on <today>]`:
      the words and who and when together inside one note.
9. **Length limit, and the cache.** With `knack_update_object { objectKey, description }`:
   (a) 1,500 characters that include a line break; (b) 5,000 characters; (c) 20,000
   characters. After each, read the field back with `knack_get_field`, **and** call
   `knack_get_object` straight away without refreshing the cache: its `objectDescription`
   must already show the new words. That proves the automatic cache clearing; say if it is
   stale.
    - Record for each: accepted, truncated (say to how many characters) or refused (quote the
      error). If (c) is refused or cut, find the real limit to within about 500 characters.
    - Finish by setting it back to `"Temporary probe table. Safe to delete."`.
10. **Stamp survives an edit.** After step 9, `knack_get_field` on the AI field.
    - Expect exactly one `_notes=[...]`, still ending `| Test agent on <the day of step 7>]`
      (a content edit must not re-stamp), with the current words before the `|`.
11. **Refusals.** Each of these must return a preflight error and change nothing:
    (a) `knack_create_object` with `description: "   "`; (b) with `notedBy: " "`;
    (c) `knack_update_object` with `description: ""`; (d) `knack_create_field` with
    `notedBy` omitted (this may be rejected by the schema before the tool runs; quote it);
    (e) `knack_create_field` with `description` omitted or `"  "`: expect a preflight error
    saying a description is required.
12. **Field descriptions and nudges.**
    - (a) `knack_create_field` on the probe table: `name: "Updated on"`, `type: "short_text"`,
      `description: "Updated on"`, `notedBy`. Then `knack_get_field`: expect the description
      to be exactly `_notes=[Updated on | Test agent on <today>]`, and no `descriptionWarning`.
    - (b) `knack_create_field` with `dryRun: true`: `type: "equation"`,
      `format: {"equation":"1+1"}` (as a JSON string), `description: "Total"`. Expect a
      `descriptionWarning` (an equation described in fewer than four words). Repeat with
      `description: "Adds one and one, for the probe"`: expect none.
    - (c) A second AI field (answers D5): `knack_create_field { type: "auto_increment",
name: "Second ID", description: "Second counter", notedBy }`. Record whether Knack
      allows it. If it does, `knack_get_object` must show two keys in `autoIncrementKeys`,
      with the described one first.
    - (d) Existing table with no AI field: only if the playground has one
      (`knack_get_object` shows no `auto_increment` field). `knack_update_object
{ description, notedBy }` on it must refuse with "has no auto-increment field" and
      change nothing. Otherwise write "not applicable".
13. **Delete the description holder, then clean up.**
    - (a) `knack_delete_field` on the probe table's description-holding AI field.
      Expect a `lostObjectDescription` and a `warning`.
    - (b) `knack_update_object { objectKey, description }` on the probe table: expect the
      "no auto-increment field" refusal.
    - (c) `knack_create_field { type: "auto_increment", ... }` to give it one back, then
      `knack_update_object` with a description again: expect success.
    - (d) `knack_delete_object` on every `MCP probe` table, preview first, then
      `confirm: true`. Finish with `knack_list_objects` to prove none is left.

## Things only a person can check (list them for the human, do not guess)

- **Table keywords (new).** In the Builder, add `_mcp_nodata` after the `_notes` in a
  probe table's AI field description. Then through the server: another field on that table
  must read as `"[redacted]"` and refuse writes (`knack_find_records`, `knack_create_records`),
  a field on a different table must be unaffected, and `knack_update_object` with a new
  description must keep the keyword. Then try `_mcp_schemalock` in place of it: deleting any
  other field on the table must be refused, naming the AI field. Remove the keyword after.
- **Retired setting (new).** In a copy of the playground's `app.json`, add
  `"dataAccess": { "objectKeywords": { "object_1": ["_mcp_nodata"] } }`: every tool that
  selects the app must refuse with a message naming the table, and `knack_list_apps` must show
  a `configProblem`. Restore the file afterwards.
- **Builder view.** Does the description written in step 7 read well in the Builder's field
  list? Is it the same text you read back through the API?
- **Usage screen.** Do the Builder's API usage figures agree with `apiUsage.plan` (used,
  limit, reset at 00:00 UTC)?
- **Email work (not built yet).** Sending test emails from a form rule and a task, and
  finding where emails sit inside an action link and an inline edit table. The steps are in
  section 6 of `SCOPE-api-budget-and-email-design.md`.

## Report

Write your results to `knack-mcp-v2/docs/PLAYGROUND-RESULTS.md` and leave it uncommitted;
the human will send it back. Use this shape, one row per numbered step:

| Step | Result (pass / fail / unexpected / not applicable) | Evidence (trimmed, secrets removed) |
| ---- | -------------------------------------------------- | ----------------------------------- |

Finish with a short **Answers** list, one line each, stating plainly:

- **D1:** did the API-created table already have an AI field? Its name and key.
- **D3:** the description length limit, and whether over-length input is refused or cut.
- **D5:** can a table have two AI fields?
- **Headers:** are all six present on a read, a 404 and a write? Did a 429 happen, and what
  did it carry?
- **Burst window:** did 15 parallel reads all succeed, and how long did they take?
- **Same account:** did two apps share one daily reading? (or "not applicable")
- **Anything unexpected**, however small, especially any response that differs from what the
  README says.

Finally, list what you left behind. It should be nothing: every `MCP probe` table deleted.
