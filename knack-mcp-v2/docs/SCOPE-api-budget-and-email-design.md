# Scope: API call awareness and email design

Status: scoping only. No code has changed. Nothing here has been run against a live
Knack app. Everything marked **(unverified)** depends on a Builder check in the last
section.

## 1. API call awareness (per app, per Knack day)

### Goal

- Know how many Knack API calls the server has made for an app since that app's last
  reset, and how many are left against its daily allowance.
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
  headers. Add an optional `rateLimit` field to `KnackApiResult`: `limit`, `remaining`
  and `resetsAt`, parsed defensively (missing or malformed headers give `undefined`,
  never an error). Header names are **(unverified)**: I expect `X-RateLimit-Limit`,
  `X-RateLimit-Remaining` and `X-RateLimit-Reset`, but the reset value could be epoch
  seconds, epoch milliseconds or seconds until reset. The parser will accept all three
  by magnitude, and check B2 confirms the real names and format.
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
- **429 handling.** `requestWithRetry` currently backs off a fixed 500 ms. When a 429
  carries a reset time, wait for it (capped) or fail fast with the reset time, instead of
  burning retries against an exhausted allowance.

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

### Reading of the request

I have taken this to mean the HTML body of the emails Knack sends from:

- **Form views:** email rules (`rules.emails`, edited by `knack_edit_view_rules`).
- **Scheduled tasks:** the `email` object on an `email` action (`knack_create_task`,
  `knack_update_task`).
- **Inline edit rules and action rules:** I am not sure which Knack feature you mean
  for each. My guess is action rules are the record rules on table views and inline
  edit is the table's inline-editing setting, and that you want the emails they can
  trigger to match. **Please confirm what each one is** (check A6).

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

### Open decisions

- Do you want one house style or a per-app brand block in `app.json`? Recommendation:
  a per-app `emailBrand` block (colour, logo URL, footer), with a neutral default.
- Should the plain-text fallback be written at all if Knack cannot send multipart (check B3)?

## 3. Suggested order

1. Builder checks below (about 30 minutes).
2. API counter: choke point, window logic, persistence, context output. Smaller and lower risk.
3. Bulk preflight.
4. Email template and lint, then wiring into tasks and rules, then `knack_preview_email`.
5. Retrofit dry run, then a reviewed restyle of your real emails.

Each step ships as its own pull request with `npm run typecheck`, `npm run test` and
`npm run build` green.

## 4. Builder checks for the morning

Use the disposable test app, not production. Record what you see, ideally with a
screenshot or the raw text. Answers change the design, so the ones marked **blocks** need
doing first.

### A. Scope questions (no app needed)

- **A1.** What does "API call awareness in 24 hours" mean to you: this server's own
  calls, or everything hitting the app? (The server can only see its own.)
- **A2.** Should a bulk write be blocked or just warned when it would exceed the budget?
- **A3.** Superseded: the limit comes from the response headers, so no per-app setting is needed.
- **A4.** One house email style, or a brand per app?
- **A5.** Which existing emails matter most (top three by volume)? I will restyle those
  first.
- **A6.** **Blocks email work.** What do you mean by "inline edit rules" and "action
  rules"? Name where they appear in the Builder.

### B. Knack behaviour I cannot verify from here

- **B1. (blocks email work) HTML support.** In a form email rule and in a task email
  action, send a test with: an inline-styled table, a `<style>` block, a button made as a
  link with a background, and an image from a public URL. Note which of these survive in
  the received email in Gmail and Outlook, and on a phone. Does the Builder's message box
  show a rich text editor or raw HTML?
- **B2. (blocks API work) Header names and format.** Make one API call with the REST key
  (for example `curl -i` on a one-record read) and paste every response header, or at
  least the rate limit ones. I need the exact names, whether the reset value is epoch
  seconds, epoch milliseconds or seconds remaining, and its timezone if it is a date.
  Also check whether a write, a 404 and a 429 carry the same headers, and whether the
  numbers match the usage screen in the Builder.
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

### C. After I build (a short list to confirm in the Builder)

- Send the generated `notification` preset from a form rule and from a task; compare the
  received email against `knack_preview_email` output.
- Restyle one real email through the dry run, then open the rule in the Builder and check
  the message box shows the HTML intact and the rule keys and criteria are unchanged.
- Trigger the bulk preflight on purpose with a low `dailyLimit` and confirm the refusal
  message is clear.
