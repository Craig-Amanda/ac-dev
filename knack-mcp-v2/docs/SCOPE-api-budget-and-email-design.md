# Scope: API call awareness, email design, and object and field descriptions

Status: scoping only. No code has changed. Nothing here has been run against a live
Knack app. Everything marked **(unverified)** depends on a Builder check in the last
section.

## 1. API call awareness (per app, per Knack day)

**Status: built** (see the "API usage" section of `README.md`). It differs from the
design below in four ways:

- Usage is shown by `knack_list_apps` (an `apiUsage` block per app), not `knack_cache`
  or a new tool, so the catalogue is unchanged at 71 and 37 tools.
- A response note reports the exact number of calls that request made, not the
  before-and-after drop in Knack's `remaining`, because other clients' calls in the same
  moment would make the drop misleading.
- Only the batch record tools check the budget up front. Analysis and reference scans
  page through an unknown number of records, so they are not estimated.
- There is no fallback counter and no `app.json` block; the headers are reliable.
- The daily reading is shared between apps on the same account (the app's
  `builderAccountSlug`, or the account slug in loaded runtime metadata), since the
  allowance is per account. The burst limit stays per app, because it is not known
  whether Knack applies it per account.

### Goal

- Know how many Knack API calls the server has made for an app since that app's last
  reset, and how many are left against the account's daily allowance (the allowance is per Knack
  account, not per app).
- Warn before a bulk operation would eat the rest of the day's budget.

### What the code gives us

- Every authenticated call goes through `KnackContext.request` (`src/context.ts`,
  around line 297). `requestWithRetry` calls `request` again on each retry, so retries
  are counted for free.
- Two places bypass it and build the auth headers themselves: `src/tools/records.ts`
  (around line 1440) and possibly `src/attachments.ts`. They must be routed through
  the counter or the number will be too low.
- `AppConfig` (`src/config.ts`) is where per-app settings live, so the allowance and
  reset time belong in `app.json`.

### Design (revised: headers first)

You confirmed that Knack returns the limit, the calls remaining and the reset time in the
response headers. That makes Knack's own figure the source of truth, and it already
includes calls from the front end, Make and other integrations, which a local counter can
never see. The local counter becomes a fallback only.

- **Capture the headers.** `knackFetchJson` (`src/http.ts`) currently drops response
  headers. Add an optional `rateLimit` field to `KnackApiResult` with two readings,
  parsed defensively (a missing or malformed header gives `undefined`, never an error).
  Your sample response shows Knack sends **two separate limits**:

    | Headers                                                           | Sample                 | Meaning                                                                                                                                 |
    | ----------------------------------------------------------------- | ---------------------- | --------------------------------------------------------------------------------------------------------------------------------------- |
    | `x-planlimit-limit`, `x-planlimit-remaining`, `x-planlimit-reset` | 75000, 37501, 57413141 | The account's daily API allowance. Reset is **milliseconds until reset** (about 15.9 hours, so 00:00 UTC).                              |
    | `x-ratelimit-limit`, `x-ratelimit-remaining`, `x-ratelimit-reset` | 10, 8, 1790755388      | A short burst limit of 10 requests per window. Reset is **epoch seconds** (08:03:08 UTC, within a minute of when you sent the request). |

    The plan reset unit and time come from the numbers, not from Knack's documentation,
    so they are checked by B2 (below). The parser converts both resets to an absolute
    time (`resetsAt`), so nothing downstream cares about the unit.

- **Record the latest reading per app** on `KnackContext` from every response that
  carries the headers (one place: `request`). Keep `{ limit, remaining, resetsAt,
readAt }`. Every call refreshes it, so it costs no extra calls.
- **No reading yet.** Until the first authenticated call returns, usage is unknown. The
  context tool can take one cheap call (a one-record read) to prime it, and says so.
- **Staleness.** Other clients spend from the same allowance between our calls, so a
  reading is a snapshot. Reports include `readAt`. After `resetsAt` passes, the reading
  is discarded rather than shown as still valid.
- **Fallback counter.** Only if the headers turn out to be missing on some endpoints
  (check B2) or on some plans: count this server's calls per app and window them by an
  optional `app.json` `apiCalls: { dailyLimit, resetTime, resetTimezone }` block. If the
  headers are reliable, this block and its persistence file are dropped, which removes
  most of the earlier design.
- **Per-request count.** The registry (`src/registry.ts`) wraps every tool call, so it
  records `remaining` before and after. The drop is how many calls that request cost,
  including other clients' calls in the same moment, so it is labelled an approximation.
  A separate exact count of this server's own calls per tool call is cheap to keep and is
  reported alongside it. Either goes into the response only above a threshold (default
  25 calls) or when a warning line is crossed (80 percent used, 95 percent used).
- **Surface it without growing the catalogue.** Add an `apiUsage` object (`used`,
  `limit`, `remaining`, `resetsAt`, `readAt`) to the existing context tool
  (`src/tools/context.ts`, to confirm). No new tool, so the three-place documentation
  update and the token budget are untouched. Fallback if you want a dedicated call: a
  read-only `knack_api_usage`, which brings the README, FEATURES.html and count updates
  in `CLAUDE.md` (71 to 72 full, 37 to 38 read-only).
- **Preflight for bulk tools.** `knack_create_records`, the analysis scans and
  `knack_find_orphaned_field_refs` estimate their call count up front. If it exceeds
  `remaining` (when known), they refuse and say what it would cost, unless the caller
  passes an override. When `remaining` is unknown they proceed and warn.
- **Burst limit and concurrency.** Ten requests per window is tight against
  `BATCH_CONCURRENCY` (default 5, up to 10; `src/config.ts`). Five requests in flight,
  each finishing in well under a second, can pass ten in a window and draw 429s. Use the
  burst `remaining` and `resetsAt` to pace: when `remaining` reaches 0, wait until
  `resetsAt` before the next request. This lives in `request`, so every tool gets it, and
  it is the more likely thing to hit in practice than the daily plan limit.
- **429 handling.** `requestWithRetry` currently backs off a fixed 500 ms. A 429 from the
  burst limit should wait for its `resetsAt` (capped at a few seconds). A 429 or
  `remaining: 0` on the daily plan limit should fail fast with the reset time, not retry
  at all, since retrying cannot help until 00:00 UTC.
- **Which number the user sees.** `apiUsage` reports the daily plan figures
  (`used = limit - remaining`, 37,499 of 75,000 in your sample) and the burst figures
  separately. The 80 and 95 percent warnings apply to the plan limit only.

### Files

- New: `src/lib/rate-limit.ts` (header parsing and usage formatting), plus its test.
- Changed: `src/http.ts`, `src/context.ts`, `src/registry.ts`, `src/tools/records.ts`
  (the bypass), `src/tools/context.ts`, bulk tools, `README.md`.
- Tests: header parsing for each reset format and for missing or garbage values, the
  reading being discarded after reset, per-request delta, the bypass path, a bulk
  refusal, and 429 waiting on the reset. Window and persistence tests only if the
  fallback counter is built.

### Open decisions

- Whether to build the fallback counter at all. Recommendation: not until check B2 shows
  a gap in the headers.
- Whether to block or only warn when over budget. Recommendation: block bulk writes,
  warn on everything else.

## 2. Email design: forms, tasks, inline edit rules, action rules

### Reading of the request (confirmed 30 September)

The HTML body of the emails Knack sends from:

- **Form views:** email rules (`rules.emails`, edited by `knack_edit_view_rules`).
- **Scheduled tasks:** the `email` object on an `email` action.
- **Action rules:** these belong to an **action link**, which can sit on a list, a
  details view or a table. In the view JSON an action link is a column with
  `action_rules[]`, each holding `record_rules` and `submit_rules`
  (`knack_add_action_link` already builds them).
- **Inline edit rules:** inline editing is a setting on **tables**.

Where an email sits inside an action rule, and what rules an inline edit carries, is not
visible from the code. Run sheet step 6 finds out by creating one of each in the
playground and searching for it.

### What the code gives us

- Email content already flows through the server. Tasks carry `email: { subject,
message, recipients }`. Views carry email rules that `knack_search_emails` can find.
- The guards already refuse a `{field_N}` that reads a no-data field inside any email
  (`ruleFieldRefusal` in `ARCHITECTURE.md`). Any generated template must go through
  the same check, since it will quote fields.
- Nothing builds or validates email HTML today, and nothing restyles an existing one.

### Design

- **Template builder (pure).** `src/lib/email-template.ts` builds table-based HTML with
  inline CSS only, which is what email clients render reliably. Inputs: preset
  (`notification`, `confirmation`, `reminder`, `digest`), heading, intro, a list of
  label and `{field_N}` rows, an optional button (label and link), brand colour, and
  footer text. Output: subject suggestion, HTML, and a plain-text version.
- **Lint.** A checker for any email message the server writes: no script or external
  stylesheet, no style block if check B1 shows Knack strips it, size limit, balanced
  tags, every `{field_N}` exists, contrast on the brand colour, a visible plain-text
  fallback.
- **Wiring, without new tools where possible.**
    - Add an optional `emailTemplate` argument to `knack_create_task` and
      `knack_update_task` that fills `email.message` from a preset.
    - Same for email rules through `knack_add_view_rules` and `knack_edit_view_rules`.
    - One read-only `knack_preview_email` so you can see the HTML for a preset with sample
      values before anything is written. This is the only new tool, and it needs the
      three-place documentation update.
- **Retrofit.** A dry-run pass that lists every existing email rule and task email with
  a lint result, so restyling is a reviewed batch, not a blind rewrite. It reuses
  `knack_search_emails` output.
- **Safety.** All writes keep the existing preview, read-back and snapshot behaviour.
  Restyling changes only `message` (and optionally `subject`), never recipients or
  criteria.

### Files

- New: `src/lib/email-template.ts` and test, `src/lib/email-lint.ts` and test.
- Changed: `src/tools/tasks.ts`, `src/tools/view-mutations.ts`, `src/lib/rule-edits.ts`,
  `src/tools/analysis.ts`, then `README.md`, `docs/FEATURES.html`, and `MIGRATION.md`
  only if a legacy mapping changes.
- Catalogue: one new tool, so run `npm run catalogue` and keep the description under
  120 characters.

### Brand per app (decided: per app, set up with the user)

Each app carries an `emailBrand` block in `app.json` (primary colour, logo URL, footer
text, optional from name). A brand is never guessed. The first time an email is built for
an app with no brand, the server says so and the model sets one up with you:

- New tool `knack_set_email_brand`: with the client's form prompt available it asks for
  the colour, logo URL and footer in one form; otherwise the model asks in chat and passes
  the answers. It previews a sample email, and writes `app.json` only after you confirm.
  It changes local config, not Knack. Being a new tool it needs the three-place
  documentation update and takes the catalogue to 72 in full mode and 38 in read-only.
- Until a brand is set, the neutral default is used and every response says so.

### Open decisions

- Should the plain-text fallback be written at all if Knack cannot send multipart
  (check B3)?

## 3. Object and field descriptions

**Update, 30 September, after the playground run.** Answers to D2 and D4 changed the
design:

- **D4:** the Builder does not accept a description that is only a stamp. So every field
  now needs a description (a few words if it is obvious), and the words and who and when
  live together inside one note: `_notes=[<words> | <name> on <date>]`. `description` is
  now required on `knack_create_field`. Older descriptions, with the words outside the
  note, are read correctly and rewritten into this form when next written; there is no
  bulk rewrite.
- **D2:** the auto-increment field can be edited, deleted, retyped and moved in the
  Builder, so a table's description can be lost there; through this server,
  `knack_delete_field` returns the words. Nothing guards a retype, which the Builder
  allows.
- The auto-increment field the server adds is named `AI`.
- After any successful change, the server now clears that app's cached metadata itself, so
  the next read is fresh. The "run `knack_cache`" note is gone.

**Status: built.** See "Table descriptions" and "Field description notes" in `README.md`.
It differs from the design below in these ways:

- Only new tables get an auto-increment field added. `knack_update_object` on an existing
  table with none refuses and says how to add one (you said "going forward only").
- The catalogue grew by 378 bytes (about 95 tokens) in full mode from the new required
  inputs; read-only is unchanged.
- Deleting a description-holding field returns the words in `lostObjectDescription`
  rather than refusing.
- Reads show the description on `knack_list_objects`, `knack_get_app_overview` and
  `knack_get_object`.
- Verified on the playground on 30 September (results in the pull request): an
  API-created table has **no** auto-increment field (Knack adds `Name`, a text field
  called `Record ID` and three owner connections), so the server always adds one; it is
  named `AI` because Knack renamed a second `Record ID` to `Record ID Copy`.
  A description of 20,000 characters is accepted whole. Knack allows two auto-increment
  fields on one table. Still for a person: D2 and D4 in the Builder.

### Goal

- Every field created through the server carries who and when, even when the field is
  obvious and needs no explanation.
- Every object carries a description an AI can read. Knack has no object description, so
  it is held on the object's auto-increment (AI) field, which you say Knack adds to every
  new object.

### What the code gives us

- `knack_create_object` sends `fields: []` (`src/tools/objects.ts`, around line 96), takes
  no description, and says "no custom fields yet". So the object description has nowhere
  to go today.
- `knack_create_field` already stamps `_notes=[<name> on <date>]`, but only when a
  non-empty description is supplied (`src/tools/fields.ts`, around line 256). A field with
  no description gets no stamp, which is the gap you describe.
- `appendKtlNote` and the update path already keep the stamp and the trailing keyword
  cluster intact on later edits. The object description reuses them; nothing new is
  needed for stamping.
- The server already knows the `auto_increment` field type (`src/lib/field-shapes.ts`).
- `app.json` `dataAccess.objectKeywords` exists only because "Knack objects have no
  description to carry them" (`src/config.ts`). This feature is the natural home for
  that later, but moving the limits is a security-model change and is **out of scope**
  here.

### Design

- **Stamp every field.** `knack_create_field` requires `notedBy` always. With no
  description it writes a bare `_notes=[Craig on 2026-09-29]`; with one it writes the
  text then the stamp, as now. Updates keep the current rules (existing stamp preserved,
  `restampNote` to re-attribute).
- **Which fields deserve text.** Decided: this is going-forward only, with no backfill.
  An obvious field gets the stamp and, at most, a short label such as "Updated on" or
  "Updated by", or "Person" on a client object. Whether a field is obvious is a judgement,
  so when unsure the model asks you rather than guessing; the tool descriptions and README
  say so, and the server's soft warning names any computed field or connection created
  with no text. Obvious fields (First name, Email, Created date) get the
  stamp only. Fields whose meaning is not clear from the name and type get a sentence:
  formula, equation and concatenation fields, connections, fields with conditional rules,
  multiple choice or yes/no fields whose values mean something, and any field with a
  business rule behind it. This is a judgement, so it lives in the tool description and
  the README, plus a soft warning (never a refusal) when one of those types is created
  with no text. The warning names the field and the type.
- **Object description on create.** `knack_create_object` gains `description` and
  `notedBy`, both required outside `dryRun`, so no object is created without one. After the
  POST it finds the auto-increment field (in the POST response, or by re-reading the
  object if the response omits fields), writes `description` plus the stamp to it through
  the same code path `knack_update_field` uses, then reads it back to verify. The
  dry run shows both the object and the description it would write.
- **Partial failure.** If the object is created but the description write fails, the
  response says so plainly, names the object and the field, and says how to retry. It
  never rolls the object back on its own.
- **Object description on update.** `knack_update_object` gains an optional
  `description`. It edits the AI field's text and keeps the original stamp, exactly as a
  field edit does. `notedBy` is needed only to set the first description on an older
  object that has none.
- **Reading it.** The schema and object overview tools return `description` for each
  object, read from its AI field. In a list it is cut to a short length; the
  single-object view returns it in full. Where an object has no AI field, has more than
  one, or has an empty description, the response says which, rather than guessing.
- **Protecting it.** The AI field's description holds the object's meaning, so the
  existing guards apply: `looseningKeywords` and the KTL-keyword-drop guard already stop an
  edit removing a keyword, and deleting or retyping an object's AI field should warn that
  it also removes the object description.
- **Backfill.** Dropped: you decided this applies going forward only.
- **Catalogue.** No new tool, so no count changes. The README rows for
  `knack_create_object`, `knack_update_object` and `knack_create_field`, the "Field
  description notes" section, and the matching FEATURES.html rows change in prose, and
  `docs-drift.test.ts` will not catch a stale description, so check them by hand.

### Files

- Changed: `src/tools/objects.ts`, `src/tools/fields.ts`, `src/tools/schema.ts`,
  `src/lib/field-payload.ts` (if the stamp helper needs a description-less path),
  `README.md`, `docs/FEATURES.html`.
- Tests: object create writes the AI field description and verifies it; create with the
  AI field missing from the POST response; partial failure; `dryRun` output; bare stamp on
  a field with no description; warning for a formula with no text; update keeps the stamp;
  an object with no or several AI fields.

### Open decisions

- Decided: `notedBy` becomes required on every `knack_create_field` call. It changes an
  existing tool's contract, so any saved prompt that omits it will start to fail.
- Where to put the object description in the AI field's text, given it can also carry
  `_mcp_*` keywords. Recommendation: description first, keywords next, `_notes` last,
  which is the layout the README already requires.

## 4. Suggested order

1. Builder checks below (about 45 minutes).
2. Object and field descriptions: smallest and self-contained once checks D1 and D2 pass.
3. API usage from response headers: header parsing, latest reading per app, context output.
4. Bulk preflight and 429 handling.
5. Email template and lint, then wiring into tasks and rules, then `knack_preview_email`.
6. Retrofit dry runs (descriptions and emails), then a reviewed batch on your real apps.

Each step ships as its own pull request with `npm run typecheck`, `npm run test` and
`npm run build` green.

## 5. Builder checks for the morning

Use the disposable test app, not production. Record what you see, ideally with a
screenshot or the raw text. Answers change the design, so the ones marked **blocks** need
doing first.

### A. Scope questions (answered 30 September)

- **A4:** a brand per app, set up with the user (see section 2).
- **A5:** none for now.
- **A6:** action rules are on action links (lists, details, tables); inline edit is on
  tables (see section 2).
- **A7:** yes, and ask when unsure (see section 3).
- **A8:** yes, `notedBy` is required on every field create.
- **A9:** none; going forward only.

The original questions follow for reference.

- **A1.** What does "API call awareness in 24 hours" mean to you: this server's own
  calls, or everything hitting the app? (The server can only see its own.)
- **A2.** Should a bulk write be blocked or just warned when it would exceed the budget?
- **A3.** Superseded: the limit comes from the response headers, so no per-app setting is needed.
- **A4.** One house email style, or a brand per app?
- **A5.** Which existing emails matter most (top three by volume)? I will restyle those
  first.
- **A6.** **Blocks email work.** What do you mean by "inline edit rules" and "action
  rules"? Name where they appear in the Builder.

- **A7.** Confirm my reading of "obvious fields need no description apart from who and
  when": obvious fields get a bare `_notes=[name on date]` and nothing else. Is that right?
- **A8.** Should `notedBy` be required on every `knack_create_field` call?
- **A9.** Which objects should be backfilled first?

### B. Knack behaviour I cannot verify from here

- **B1. (blocks email work) HTML support.** In a form email rule and in a task email
  action, send a test with: an inline-styled table, a `<style>` block, a button made as a
  link with a background, and an image from a public URL. Note which of these survive in
  the received email in Gmail and Outlook, and on a phone. Does the Builder's message box
  show a rich text editor or raw HTML?
- **B2. Header confirmation (units confirmed, two checks left).** Your second sample
  settled the units. `x-planlimit-reset` fell from 57,413,141 to 56,899,839, a drop of
  513,302, and `x-ratelimit-reset` rose from 1790755388 to 1790755901, a gain of 513
  seconds. So the plan reset is milliseconds until reset, and the burst reset is epoch
  seconds. Both readings sit about a second after the call that produced them, which
  suggests the burst window is one second (10 requests per second); that is inferred, not
  confirmed. `x-planlimit-remaining` fell from 37,501 to 36,948, so **553 calls in about
  8.5 minutes** were spent by something other than this server, which was not running. That
  is the case for reading Knack's figure, not counting our own. Still to check: (a) the
  Builder usage screen agrees with the plan figures and the 00:00 UTC reset; (b) whether a
  write, a 404 and a 429 carry the same headers (to provoke a 429, fire 15 quick reads in
  one second).
- **B3. Plain text.** In the received test email, view the source. Is there a plain-text
  part, or HTML only? Does Knack add its own header, footer or "sent by Knack" branding
  you cannot remove?
- **B4. Field rendering.** In one test email, include `{field_N}` for: a date, a
  multi-line text, a connection to several records, a currency, a yes/no, and an empty
  value. Note how each renders and whether empty values leave a blank gap.
- **B5. Limits.** Paste a very long message (about 20,000 characters). Does Knack accept
  it, truncate it, or refuse it? What is the subject length limit?
- **B6. Sender.** Can you set a from name or reply-to on a form rule and on a task? Are
  they the same options?
- **B7. Tasks versus rules.** Do task emails and form email rules accept exactly the same
  fields (recipients, cc, bcc)? The server currently assumes `email: { subject, message,
recipients }` for tasks.
- **B8. Counting.** Note the app's API count, run three known calls (one read, one
  create, one paginated read of 2 pages), and check the count again. Does a read that
  returns 1,000 records cost 1 call or more? Does a failed call (400, 429) count?
- **B9. Does the front end spend the allowance?** Load a page with a table view and note
  whether the count moves. This decides how misleading the local number is.

### D. Object descriptions (block the descriptions work)

- **D1. (blocks) The AI field.** Create an object through the API, not the Builder (the
  `knack_create_object` tool with `dryRun` off, on the test app). Does it get an
  auto-increment field automatically? Note its name, its key, and whether the create
  response lists it or you only see it on re-reading the object.
- **D2. (blocks) Can it hold a description?** Open that field in the Builder. Can you edit
  its description? Can you delete it, retype it or move it? Is it ever hidden or locked?
- **D3. Length and layout.** Paste a 1,500 character description with a line break into
  it. Is it accepted, truncated or refused? How does the Builder show it in the fields
  list? What is the real limit?
- **D4. Bare stamp.** On a normal field, set the description to only
  `_notes=[Craig on 2026-09-29]`. Does the Builder accept and display it, and does KTL,
  if you use it, treat it as a keyword and not visible text?
- **D5. More than one.** Can an object have two auto-increment fields? If so, which should
  count as the object's description holder?
- **D6. Older objects.** Do your existing objects each have exactly one AI field? Name two
  that do not.

### E. After I build (a short list to confirm in the Builder)

- Send the generated `notification` preset from a form rule and from a task; compare the
  received email against `knack_preview_email` output.
- Restyle one real email through the dry run, then open the rule in the Builder and check
  the message box shows the HTML intact and the rule keys and criteria are unchanged.
- Create a new object with the tool, open its AI field in the Builder and check the
  description and stamp are there and read well. Then create one obvious field and one
  formula field with no text, and check the stamp and the warning.
- Trigger the bulk preflight on purpose with a low `dailyLimit` and confirm the refusal
  message is clear.

## 6. Run sheet for the playground

Test app: the Noah's Place playground (you wrote "NP Place Playground"; say if that is a
different app). Run this in your local Claude session, where the Knack tools are connected,
with `allowViewMutation` and `allowDelete` on for the playground only. Nothing here should
touch another app.

1. **D1.** Call `knack_create_object` with name `MCP probe 1`, `dryRun` false. Report
   whether the response lists any field. Then list the object's fields (for example
   `knack_get_app_overview`) and report every `auto_increment` field with its key and name.
2. **D2 and D3.** Call `knack_update_field` on that field with a 1,500 character
   description that includes a line break, and `notedBy`. Report whether it was accepted,
   truncated or refused, and read the field back. Then in the Builder try to delete,
   retype and move that field, and say what happens.
3. **D5.** Call `knack_create_field` with type `auto_increment` on the probe object.
   Report whether a second one is allowed.
4. **D4 (Builder).** On any normal field, set the description to only
   `_notes=[Craig on 2026-09-30]` and say whether the Builder accepts and shows it.
5. **API checks.** Call `knack_list_apps` and note `apiUsage`. Fire 15 `knack_find_records`
   calls with limit 1 at once, and report any 429 and the `apiUsage` afterwards. Tool
   responses do not show headers, so for B2(b) use `curl -i` on a write, a request for a
   record that does not exist, and (if you can provoke one) a 429, and paste the
   `x-planlimit-*` and `x-ratelimit-*` lines from each.
6. **A6 follow-up.** In the Builder, add a rule that sends an email to (a) an action link
   on a table, (b) an action link on a details view, and (c) an inline-edit table, if it
   allows one. Then run `knack_search_emails {}` and paste the `path` for each hit, and
   run `knack_get_view` on the three views and paste the action link column and the
   table's inline edit settings. This shows where the emails and rules live.
7. **B1, B3 to B7 (Builder and inbox).** Send the test emails described above from a
   form rule and from a task, to a Gmail and an Outlook address, and open them on a phone.
8. **Clean up.** Delete the `MCP probe 1` object.
