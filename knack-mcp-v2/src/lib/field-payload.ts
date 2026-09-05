import { asRecord } from './util.js';
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
            errors: [
                `${label} must be valid JSON: ${error instanceof Error ? error.message : String(error)}`,
            ],
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
    'Schema cache not auto-invalidated — run knack_refresh_cache(warm:true) before trusting cached-schema tools.';

/**
 * Reminder attached to scene/view-mutating tool responses, for the same reason as
 * SCHEMA_CACHE_STALE_NOTE but for the scene/view cache.
 */
export const VIEW_CACHE_STALE_NOTE =
    'View cache not auto-invalidated — run knack_refresh_cache(warm:true) before trusting cached-view tools.';

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
            `Could not validate equation tokens: object ${objectKey} was not found in the cached schema, so this write is going out unchecked. Run knack_refresh_cache and re-check if that is unexpected.`,
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
