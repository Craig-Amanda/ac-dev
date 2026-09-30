/**
 * Which copy of a field's description is the real one.
 *
 * Knack's field carries the description twice: at the top level (`description`) and under
 * `meta.description`. The builder edits `meta.description` only, so after a person changes
 * a description in the builder the top-level copy is left behind. Measured on the
 * playground (30 September): a `_mcp_schemalock` added in the builder showed in
 * `meta.description` while `description` still read without it, and a write that trusted
 * the top-level copy did not see the lock and then wrote a description that left it out.
 *
 * Writes through this server set both copies (see normalizeFieldDescriptionForWrite), so
 * `meta.description` is never behind. It is the one to trust; the top-level copy is used
 * only when there is no `meta.description` at all.
 *
 * Pure logic, no I/O.
 */
import { asRecord } from './util.js';

/** A field's description as the builder shows it, or '' when it has none. */
export function readFieldDescription(field: unknown): string {
    const record = asRecord(field);
    const meta = asRecord(record?.meta)?.description;
    if (typeof meta === 'string') return meta;
    return typeof record?.description === 'string' ? record.description : '';
}
