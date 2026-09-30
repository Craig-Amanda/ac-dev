# Handoff: test the Knack MCP changes on the playground

You are a local agent with the `knack-mcp-v2` server connected and access to the Noah's
Place playground app. Your job is to run the tests below against the **playground only**,
record exactly what happens, and report back. You are testing; you are not fixing. If
something fails, record it and carry on. Do not edit code, do not commit, do not push.

## What changed (read this first)

Branch `ccr-ac9968fd-i54dc0`, pull request #84 in `Craig-Amanda/ac-dev`. Two features:

- **API usage from Knack's rate-limit headers.** `knack_list_apps` now returns `apiUsage`
  for each app. Requests wait for the burst window instead of drawing 429 errors. A 429 once
  the daily allowance is spent is not retried. Record batches the daily reading says cannot
  fit are refused. See "API usage" in `knack-mcp-v2/README.md`.
- **Table descriptions and field stamps.** A table's description lives on its
  auto-increment (AI) field. `knack_create_object` now requires `description` and
  `notedBy`. `knack_create_field` now requires `notedBy` on every field. See "Table
  descriptions" and "Field description notes" in the same README.

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

Then refresh the caches for the playground so reads see current schema:
`knack_cache { appKey, refresh: true, warm: true }`. Repeat this after any step that
creates or changes a table or field (responses carry a `cacheNote` saying so).

## Tests

For every step record: the exact call, the response (trimmed to the relevant keys), and
**pass**, **fail** or **unexpected** against the stated expectation. Quote errors verbatim.

### Part A: API usage

1. **Before and after.** Call `knack_list_apps`. Note `apiUsage` for the playground (it may
   be `null` if no call has been made yet). Make any read call, for example
   `knack_list_objects`, then `knack_list_apps` again.
    - Expect: `apiUsage.plan` and `apiUsage.burst` filled in, `callsThisSession` above 0,
      `readAt` set, `plan.resetsAt` at 00:00 UTC.
2. **Burst pacing.** Pick any object with records. Fire 15 `knack_find_records` calls at
   once (`rowsPerPage: 1`), in parallel if you can.
    - Expect: all 15 succeed; none reports a 429. Record how long the batch took and the
      `apiUsage.burst` values afterwards.
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
5. **Two apps, one account.** Only if `knack_list_apps` shows another app whose
   `builderAccountSlug` is the same (case ignored) as the playground's: make a read on the
   playground, then call `knack_list_apps`.
    - Expect: the other app's `apiUsage.plan.remaining` matches the playground's, even
      though it has made no call. If there is no such app, write "not applicable".

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
      not, and the server added one named `Record Number`.
    - Expect `objectDescription.ok: true` and `verified: true`, and no `warning`.
8. **Read it back.** Refresh caches, then `knack_get_object { objectKey, detail: "fields" }`.
    - Record: every field's key, name and type; `objectDescription` (`fieldKey`, `text`,
      `autoIncrementKeys`). Expect `text` to equal the description without a stamp.
    - Also check `knack_list_objects` and `knack_get_app_overview`: the probe table must
      show its description, cut to 160 characters, with no `_notes` stamp.
    - Call `knack_get_field` on the AI field and paste its raw `description` and
      `meta.description`. Expect the words, a space, then `_notes=[Test agent on <today>]`.
9. **Length limit (answers D3).** With `knack_update_object { objectKey, description }`:
   (a) 1,500 characters that include a line break; (b) 5,000 characters; (c) 20,000
   characters. After each, read the field back with `knack_get_field`.
    - Record for each: accepted, truncated (say to how many characters) or refused (quote the
      error). If (c) is refused or cut, find the real limit to within about 500 characters.
    - Finish by setting it back to `"Temporary probe table. Safe to delete."`.
10. **Stamp survives an edit.** After step 9, `knack_get_field` on the AI field.
    - Expect exactly one `_notes=[...]` and it still reads `Test agent on <the day of
step 7>` (a content edit must not re-stamp).
11. **Refusals.** Each of these must return a preflight error and change nothing:
    (a) `knack_create_object` with `description: "   "`; (b) with `notedBy: " "`;
    (c) `knack_update_object` with `description: ""`; (d) `knack_create_field` with
    `notedBy` omitted (this may be rejected by the schema before the tool runs; quote it).
12. **Field stamps and nudges.**
    - (a) `knack_create_field` on the probe table: `name: "Updated on"`, `type:
"short_text"`, `notedBy`, **no** description. Then `knack_get_field`: expect the
      description to be only `_notes=[Test agent on <today>]`. Expect no
      `descriptionWarning`.
    - (b) `knack_create_field` with `dryRun: true`: `type: "equation"`, `format:
{"equation":"1+1"}` (as a JSON string), no description. Expect `descriptionWarning`.
      Repeat with a description: expect none.
    - (c) A second AI field (answers D5): `knack_create_field { type: "auto_increment", name:
"Second ID", notedBy }`. Record whether Knack allows it. If it does, `knack_get_object`
      must show two keys in `autoIncrementKeys`, with the described one first.
    - (d) Existing table with no AI field: only if the playground has one (`knack_get_object`
      shows no `auto_increment` field). `knack_update_object { description, notedBy }` on it
      must refuse with "has no auto-increment field" and change nothing. Otherwise write "not
      applicable".
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

- **D2.** In the Builder, open a probe table's AI field: can its description be edited? Can
  the field be deleted, retyped or moved? Is it hidden or locked?
- **D4.** Set a normal field's description to only `_notes=[Test agent on 2026-09-30]` in
  the Builder: is it accepted and shown, and does KTL treat it as a keyword?
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
