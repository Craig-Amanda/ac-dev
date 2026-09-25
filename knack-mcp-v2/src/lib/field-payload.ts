import { asRecord, describeError } from './util.js';
import { type CachedField, type CachedSchema } from '../types.js';

export type FieldPayloadPreflight = {
    payload: Record<string, unknown> | null;
    errors: string[];
};

/**
 * Parse a JSON object supplied to a field mutation tool without allowing arrays or primitives.
 *
 * @param value JSON text supplied by the MCP client.
 * @param label Input name used in validation feedback.
 * @returns The parsed object or a user-actionable validation error.
 */
export function parseJsonObjectInput(
    value: string,
    label: string,
): FieldPayloadPreflight {
    try {
        const payload = asRecord(JSON.parse(value));
        return payload
            ? { payload, errors: [] }
            : { payload: null, errors: [`${label} must be a JSON object.`] };
    } catch (error) {
        return {
            payload: null,
            errors: [`${label} must be valid JSON: ${describeError(error)}`],
        };
    }
}

/**
 * Check the minimum field payload contract locally before making a Builder API request.
 * Advanced Knack format settings remain pass-through so the MCP does not reject valid settings
 * that are not represented in its cached schema.
 *
 * @param payload Candidate field definition.
 * @param requireIdentity Whether both name and type are required, as they are for field creation.
 * @returns Validation errors. An empty array means the payload is safe to send to Knack.
 */
export function validateFieldPayload(
    payload: Record<string, unknown>,
    requireIdentity: boolean,
): string[] {
    const errors: string[] = [];
    const hasName = Object.hasOwn(payload, 'name');
    const hasType = Object.hasOwn(payload, 'type');

    if (requireIdentity && !hasName) errors.push('Field name is required.');
    if (requireIdentity && !hasType) errors.push('Field type is required.');
    if (hasName && (typeof payload.name !== 'string' || !payload.name.trim())) {
        errors.push('Field name must be a non-empty string.');
    }
    if (hasType && (typeof payload.type !== 'string' || !payload.type.trim())) {
        errors.push('Field type must be a non-empty string.');
    }

    for (const property of ['format', 'relationship']) {
        if (
            Object.hasOwn(payload, property) &&
            asRecord(payload[property]) === null
        ) {
            errors.push(`${property} must be a JSON object when supplied.`);
        }
    }

    if (payload.type === 'connection') {
        const format = asRecord(payload.format);
        const relationship = asRecord(payload.relationship);
        const target = format?.object || relationship?.object;
        if (typeof target !== 'string' || !/^object_\d+$/i.test(target)) {
            errors.push(
                'Connection fields require format.object or relationship.object with an object key (for example object_12).',
            );
        }
    }

    return errors;
}

/**
 * KTL keywords only work when the whole keyword cluster trails the description — this is
 * a hard requirement of KTL's own parsing, not a style choice this codebase made up (see
 * the knack-mcp-v2 README's "Field description notes" section). A description can carry
 * several keywords in that trailing cluster (e.g. `_ktlHide`), and `_notes` is not
 * necessarily the last one among them — so this match is bounded to `_notes`'s own known
 * shape (`_notes=<name> on <YYYY-MM-DD>`, non-greedy up to the date) rather than greedy to
 * end-of-string, so any keyword sitting after it in the cluster is left untouched instead
 * of being swallowed into the extracted/stripped tag.
 */
const KTL_NOTES_TAG_PATTERN = /_notes=(?!\[).+? on \d{4}-\d{2}-\d{2}/;
/**
 * A note in KTL's bracket form, `_notes=[any text]`: the only form written, since 25
 * September (asked for then, after a field ended up with a person's bracket note plus a
 * plain stamp beside it). A person's own text is kept, with the MCP's attribution inside
 * the brackets as ` | <name> on <date>`; a note with no text of its own is just
 * `_notes=[<name> on <date>]`. The plain form above is still recognised, so stamps
 * written before then are found, and rewritten in brackets on their next write.
 */
const KTL_BRACKET_NOTE_PATTERN = /_notes=\[[^\]]*\]/;
/** An attribution at the end of a bracket note's text: ` | Amanda on 2026-09-25`. */
const BRACKET_ATTRIBUTION_SUFFIX = / \| ([^|\]]+? on \d{4}-\d{2}-\d{2})$/;
/** A bracket note that is only an attribution: `_notes=[Amanda on 2026-09-25]`. */
const WHOLE_ATTRIBUTION = /^[^|\]]+? on \d{4}-\d{2}-\d{2}$/;
/**
 * Every `_notes` keyword in either form, global so stripping removes all of them, not
 * just the first. A description should only ever carry one (this module always replaces
 * rather than stacks), but a stray extra one — e.g. from a manual edit in the builder
 * before this tool existed — must not survive a strip-then-append: without the `g` flag
 * `.replace()` only touches the first match, leaving old stamps behind as new ones pile
 * up alongside them. The bracket form is tried first, so a plain match never starts
 * inside one.
 */
const KTL_NOTES_TAG_GLOBAL_PATTERN = new RegExp(
    `${KTL_BRACKET_NOTE_PATTERN.source}|${KTL_NOTES_TAG_PATTERN.source}`,
    'g',
);

/**
 * Build the trailing `_notes=[<name> on <YYYY-MM-DD>]` KTL keyword that attributes a
 * description write to whoever instructed it.
 *
 * @param notedBy Human who instructed the change (not the AI).
 * @param when Attribution timestamp; defaults to now.
 */
export function formatKtlNoteTag(
    notedBy: string,
    when: Date = new Date(),
): string {
    return buildNote(null, formatAttribution(notedBy, when));
}

function formatAttribution(notedBy: string, when: Date = new Date()): string {
    return `${notedBy} on ${when.toISOString().slice(0, 10)}`;
}

/** The inside of the description's bracket note, or null when it has none. */
function bracketNoteInner(description: string): string | null {
    const match = description.match(KTL_BRACKET_NOTE_PATTERN);
    return match ? match[0].slice('_notes=['.length, -1).trim() : null;
}

/**
 * The person's own words in a bracket note, without the attribution at its end (''
 * when the note is only an attribution); null when the description has no bracket note.
 */
function bracketNoteText(description: string): string | null {
    const inner = bracketNoteInner(description);
    if (inner === null) return null;
    if (WHOLE_ATTRIBUTION.test(inner)) return '';
    return inner.replace(BRACKET_ATTRIBUTION_SUFFIX, '').trim();
}

/** Who added the note and when (`Amanda on 2026-09-25`), from either form. */
function noteAttribution(description: string): string | null {
    const inner = bracketNoteInner(description);
    if (inner !== null) {
        if (WHOLE_ATTRIBUTION.test(inner)) return inner;
        return inner.match(BRACKET_ATTRIBUTION_SUFFIX)?.[1] ?? null;
    }
    return (
        description.match(KTL_NOTES_TAG_PATTERN)?.[0].slice('_notes='.length) ??
        null
    );
}

/** One note, always in brackets: `_notes=[text | attribution]`, either part optional. */
function buildNote(text: string | null, attribution: string | null): string {
    const inner = [text, attribution].filter(Boolean).join(' | ');
    return `_notes=[${inner}]`;
}

/**
 * The existing `_notes` keyword on a description, if any — bracket form first, then the
 * plain stamp; the tag only, no leading whitespace.
 */
export function extractKtlNoteTag(description: string): string | null {
    const match =
        description.match(KTL_BRACKET_NOTE_PATTERN) ??
        description.match(KTL_NOTES_TAG_PATTERN);
    return match ? match[0] : null;
}

/**
 * A description with its `_notes` keyword (either form) removed, wherever it sits in
 * the trailing keyword cluster. Removing it can leave a gap between neighbouring
 * keywords (e.g. `_ktlHide` on one side, `_notes=...` on the other), so this also
 * collapses any resulting run of spaces rather than just trimming the end.
 */
export function stripKtlNoteTag(description: string): string {
    return description
        .replace(KTL_NOTES_TAG_GLOBAL_PATTERN, '')
        .replace(/ {2,}/g, ' ')
        .trim();
}

/**
 * Attribute a description to whoever instructed it, leaving exactly one `_notes`
 * keyword, always in brackets. With no note of the person's own, that is
 * `_notes=[<name> on <date>]`; when the description carries a bracket note, the
 * attribution goes inside it (`_notes=[their text | <name> on <date>]`), replacing any
 * attribution already there. The note is appended after whatever else is there
 * (including other trailing keywords, e.g. `_ktlHide`), so it joins — rather than
 * displaces — the trailing keyword cluster KTL requires. Use this when a note is being
 * added for the first time, or when the instructor has explicitly asked to re-attribute
 * an existing one — see preserveKtlNote for the default "who added it" behaviour on an
 * ordinary content edit.
 *
 * @param description Human-authored description text (already trimmed, non-empty).
 * @param notedBy Human who instructed the change.
 * @param when Attribution timestamp; defaults to now.
 */
export function appendKtlNote(
    description: string,
    notedBy: string,
    when?: Date,
): string {
    const base = stripKtlNoteTag(description);
    const tag = buildNote(
        bracketNoteText(description),
        formatAttribution(notedBy, when),
    );
    return base ? `${base} ${tag}` : tag;
}

/**
 * Carry an existing `_notes` attribution forward onto new description text. `_notes`
 * records who *added* the note, not who last edited the field, so an ordinary content
 * edit must not change it — only appendKtlNote (an explicit restamp) does that.
 *
 * If the new text brings its own bracket note, its words win; otherwise the stored
 * note's words are kept. Either way the stored attribution stays, and a plain stamp
 * written before brackets were the rule comes back in brackets.
 *
 * @param newBody New description text.
 * @param existingDescription The field's current stored description (source of the note
 *   to preserve).
 * @returns `newBody` with one note, or just `newBody` if there was none.
 */
export function preserveKtlNote(
    newBody: string,
    existingDescription: string,
): string {
    const attribution = noteAttribution(existingDescription);
    const text =
        bracketNoteText(newBody) ?? bracketNoteText(existingDescription);
    const tag =
        text !== null || attribution ? buildNote(text, attribution) : null;
    const body = stripKtlNoteTag(newBody);
    if (!tag) return body;
    return body ? `${body} ${tag}` : tag;
}

/**
 * Mirror a field's description into meta.description before it goes out over the wire.
 *
 * Knack's fields API does not reliably persist a bare top-level `description` on
 * create/update — verified in production use, where a top-level `description` silently
 * failed to stick and had to be resent under `meta.description` to actually take effect.
 * The runtime metadata endpoint (parseRuntimeSchema) already reads description from either
 * location, so writing to both keeps that read-side fallback correct while guaranteeing the
 * value actually persists. Mutates payload in place; a no-op when description isn't a string.
 *
 * @param payload Field create/update payload about to be sent to Knack.
 */
export function normalizeFieldDescriptionForWrite(
    payload: Record<string, unknown>,
): void {
    if (typeof payload.description !== 'string') return;
    const existingMeta = asRecord(payload.meta) || {};
    payload.meta = { ...existingMeta, description: payload.description };
}

/**
 * Reminder attached to schema-mutating tool responses: nothing in this server invalidates
 * the in-memory/on-disk schema cache automatically, so cached-schema tools can silently
 * return pre-mutation data until a refresh is run.
 */
export const SCHEMA_CACHE_STALE_NOTE =
    'Schema cache not auto-invalidated — run knack_cache with appKey set to this app plus refresh:true, warm:true before trusting cached-schema tools. Omit appKey and it refreshes and rewrites metadata for every configured app.';

/**
 * Reminder attached to scene/view-mutating tool responses, for the same reason as
 * SCHEMA_CACHE_STALE_NOTE but for the scene/view cache.
 */
export const VIEW_CACHE_STALE_NOTE =
    'View cache not auto-invalidated — run knack_cache with appKey set to this app plus refresh:true, warm:true before trusting cached-view tools. Omit appKey and it refreshes and rewrites metadata for every configured app.';

/**
 * Reminder attached to knack_update_field responses (dry-run and live) whenever the
 * update touches format/relationship: whether Knack's PUT merges or fully replaces a
 * partial nested object has not been independently verified.
 */
export const NESTED_MERGE_UNCERTAINTY_NOTE =
    "Knack's merge behaviour for partial format/relationship objects is unverified — check knack_get_field afterwards.";

export type FieldWriteMatchCriteria =
    { fieldKey: string } | { name: string; type: string };

/**
 * Locate the field a create/update field request just touched inside Knack's raw write
 * response. Most field writes return a compact `{ field: {...} }` body, but Knack's API
 * returns the full application schema (every object's field list) for connection-field
 * writes, since a connection also updates the cross-object relationship graph — that body
 * can run into tens of thousands of characters. This searches whichever shape the response
 * actually took so the caller can project a huge response down to just the touched field.
 *
 * @param body Raw Knack API response body.
 * @param objectKey Object the field write targeted.
 * @param criteria Match by fieldKey (updates, where the key is already known) or by
 *   name+type (creates, where Knack assigns the key).
 * @returns The matching field record, or undefined if the shape wasn't recognised.
 */
export function findFieldInFieldWriteResponse(
    body: unknown,
    objectKey: string,
    criteria: FieldWriteMatchCriteria,
): Record<string, unknown> | undefined {
    const root = asRecord(body);
    if (!root) return undefined;

    const matchesCriteria = (field: Record<string, unknown>): boolean =>
        'fieldKey' in criteria
            ? field.key === criteria.fieldKey
            : field.name === criteria.name && field.type === criteria.type;

    const directField = asRecord(root.field);
    if (directField && matchesCriteria(directField)) return directField;

    const objectsContainer =
        asRecord(root.application)?.objects ?? root.objects;
    const objects = Array.isArray(objectsContainer) ? objectsContainer : [];
    for (const objEntry of objects) {
        const obj = asRecord(objEntry);
        if (!obj || obj.key !== objectKey) continue;
        const fields = Array.isArray(obj.fields) ? obj.fields : [];
        const matches = fields
            .map((f) => asRecord(f))
            .filter((f): f is Record<string, unknown> =>
                Boolean(f && matchesCriteria(f)),
            );
        // fieldKey is a genuine unique identifier, so a single match is trustworthy
        // (more than one would mean corrupted data, not a realistic case). name+type
        // is not unique within an object (Knack allows duplicate field names) — with
        // more than one match there is no reliable way to tell which entry is the one
        // just created, so return undefined rather than guess in either case.
        if (matches.length === 1) return matches[0];
    }

    return undefined;
}

export type EquationTokenCheck = {
    errors: string[];
    warnings: string[];
};

export const FIELD_KEY_PATTERN = /^field_\d+$/i;
export const FIELD_ALIAS_OBJECT_FIELD_KEY_PATTERN =
    /^(object_\d+)\.(field_\d+)$/i;

/**
 * Validate the {...} reference tokens in an equation string against the cached schema.
 * Knack silently resolves an unmatched token to 0 rather than erroring, so catching bad
 * references here — before the write reaches a live app — is the only safety net available.
 *
 * @param schema Cached schema for the app the field belongs to.
 * @param objectKey Object the equation field lives on.
 * @param equation Raw equation string from format.equation.
 * @returns Errors for tokens that cannot resolve, and warnings for tokens that resolve unreliably.
 */
export function validateEquationTokens(
    schema: CachedSchema,
    objectKey: string,
    equation: string,
): EquationTokenCheck {
    const errors: string[] = [];
    const warnings: string[] = [];

    const object = schema.objects?.find((entry) => entry.key === objectKey);
    if (!object) {
        warnings.push(
            `Could not validate equation tokens: object ${objectKey} was not found in the cached schema, so this write is going out unchecked. Run knack_cache with appKey set to this app plus refresh:true, warm:true and re-check if that is unexpected.`,
        );
        return { errors, warnings };
    }

    const fieldsByKey = new Map(
        (object.fields || []).map((field) => [field.key, field]),
    );
    const objectsByKey = new Map(
        (schema.objects || []).map((entry) => [entry.key, entry]),
    );

    const isCrossableConnection = (field: CachedField): boolean =>
        field.type === 'connection' &&
        Boolean(field.connectedObject) &&
        !field.allowsMultiple;

    const tokens = equation.match(/\{[^{}]+\}/g) || [];
    for (const rawToken of tokens) {
        const token = rawToken.slice(1, -1);
        const parts = token.split('.');

        if (parts.length === 1) {
            const [fieldKey] = parts;
            if (!FIELD_KEY_PATTERN.test(fieldKey)) {
                warnings.push(
                    `Token {${token}} looks name-based rather than a field key. Name-based tokens have been observed to resolve inconsistently (correct on one read, 0 on the next) — prefer {field_key}.`,
                );
                continue;
            }
            if (fieldsByKey.has(fieldKey)) continue;

            let hint = '';
            for (const field of object.fields || []) {
                if (!isCrossableConnection(field) || !field.connectedObject) {
                    continue;
                }
                const connectedObject = objectsByKey.get(field.connectedObject);
                if (
                    connectedObject?.fields?.some(
                        (candidate) => candidate.key === fieldKey,
                    )
                ) {
                    hint = ` It exists on connected object ${field.connectedObject} — did you mean {${field.key}.${fieldKey}}?`;
                    break;
                }
            }
            errors.push(
                `Token {${token}} does not match any field on ${objectKey}.${hint}`,
            );
            continue;
        }

        if (parts.length === 2) {
            const [connectionKey, targetKey] = parts;

            if (/^object_\d+$/i.test(connectionKey)) {
                errors.push(
                    `Token {${token}} qualifies by object key (${connectionKey}), which equations do not accept. Use {connection_field_key.target_field_key} instead — the connection *field* on ${objectKey} that points at ${connectionKey}, not the object key itself.`,
                );
                continue;
            }

            if (
                !FIELD_KEY_PATTERN.test(connectionKey) ||
                !FIELD_KEY_PATTERN.test(targetKey)
            ) {
                warnings.push(
                    `Token {${token}} looks name-based rather than {connection_field_key.target_field_key}. Name-based tokens have been observed to resolve inconsistently — prefer the field-key form.`,
                );
                continue;
            }

            const connectionField = fieldsByKey.get(connectionKey);
            if (!connectionField) {
                errors.push(
                    `Token {${token}}: ${connectionKey} is not a field on ${objectKey}.`,
                );
                continue;
            }
            if (connectionField.type !== 'connection') {
                errors.push(
                    `Token {${token}}: ${connectionKey} is a ${connectionField.type ?? 'non-connection'} field on ${objectKey}, not a connection — only many-to-one / one-to-one connections can be crossed in an equation.`,
                );
                continue;
            }
            if (connectionField.allowsMultiple) {
                errors.push(
                    `Token {${token}}: ${connectionKey} allows multiple connected records (many-to-many or one-to-many) — Knack equations can only cross many-to-one / one-to-one connections.`,
                );
                continue;
            }
            if (!connectionField.connectedObject) {
                warnings.push(
                    `Token {${token}}: could not verify — connection field ${connectionKey} has no resolvable target object in the cached schema.`,
                );
                continue;
            }

            const connectedObject = objectsByKey.get(
                connectionField.connectedObject,
            );
            if (!connectedObject) {
                warnings.push(
                    `Token {${token}}: could not verify — connected object ${connectionField.connectedObject} is not in the cached schema.`,
                );
                continue;
            }

            const hasTarget = (connectedObject.fields || []).some(
                (candidate) => candidate.key === targetKey,
            );
            if (!hasTarget) {
                errors.push(
                    `Token {${token}}: field ${targetKey} does not exist on connected object ${connectionField.connectedObject} (via ${connectionKey}).`,
                );
            }
            continue;
        }

        warnings.push(
            `Token {${token}} has more than one "." and could not be validated.`,
        );
    }

    return { errors, warnings };
}
