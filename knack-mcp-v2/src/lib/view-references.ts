/**
 * What a reference found inside a view schema is for.
 *
 * The split that matters is **scope versus display**, and it was measured rather
 * than assumed. A builder before-and-after pair on 4 September added
 * `connection_key`, `relationship_type`, `authenticated_user` and `parent_source`
 * to a source that previously had none — and left every `columns[].connection.key`
 * exactly as it was. Those columns already named a connection field while the source
 * had no connection at all, which is the proof: a display connection is the path
 * from the view's **own object** out to a connected record, and it has nothing to do
 * with how the view's records are scoped.
 *
 * So:
 *
 * - `scope-connection` decides **which records** appear. Change it to rescope.
 * - `display-connection` decides **where a shown value is read from**. A rescope
 *   leaves these correct, and rewriting them alongside one is a bug, not diligence.
 * - What does invalidate them is changing `source.object`: every field, display
 *   connection, filter, sort and rule then names a field on an object the view no
 *   longer lists.
 */
export type ViewReferenceKind =
    | 'scope-connection'
    | 'display-connection'
    | 'scoped-field'
    | 'navigation'
    | 'object'
    | 'other';

export type ViewReference = {
    /** JSON path into the view schema, e.g. `columns[1].connection.key`. */
    path: string;
    value: string;
    kind: ViewReferenceKind;
};

export const KNACK_KEY_PATTERN = /^(?:field|object|view|scene)_\d+$/;
// `object_44.field_1029`, the form edit_rules use for a connection.
export const DOTTED_CONNECTION_PATTERN = /^object_\d+\.field_\d+$/;
// `field_784-field_74`, the pair form a record rule's connection_field uses. Without
// this it fell through to the prose branch and was reported as two loose keys, losing
// the display connection it actually names.
export const HYPHENATED_PAIR_PATTERN = /^field_\d+-field_\d+$/;
// KTL directives embed bare keys in prose: `_bulk_actions=[label, field_1029]`.
export const EMBEDDED_KEY_PATTERN = /\b(?:field|view|scene)_\d+\b/g;

/**
 * Classify a reference by where it sits rather than by what it looks like.
 *
 * Path-based on purpose. `field_1029` means something different in
 * `source.connection_key` than in `source.sort[0].field`, and only the path can
 * tell them apart.
 */
export function classifyViewReference(path: string): ViewReferenceKind {
    // Scope: what decides which records the view lists. `parent_source` is
    // `{object, connection}` and both halves specify one hop, so the object at the far
    // end is part of the scope rather than a bystander — a review caught it sitting in
    // `other`, which left the RESCOPE list missing half the hop. The kind name reads as
    // "part of the scope specification", not "is itself a connection field".
    if (
        /connection_key$/.test(path) ||
        /parent_source\.(?:connection|object)$/.test(path)
    ) {
        return 'scope-connection';
    }

    // Display: what decides where a shown value is read from. Measured to survive
    // a rescope untouched, and to exist independently of one.
    if (
        /\.connection\.key$/.test(path) ||
        /edit_rules\[\d+\]\.connection$/.test(path) ||
        /connection_field$/.test(path) ||
        /source\.connections\[\d+\]/.test(path)
    ) {
        return 'display-connection';
    }

    // A `scene` may be a slug string or a reference object — `{key}`, `{scene}` or
    // `{slug}`, all of which readSceneReference resolves. The nested forms end the path
    // one segment deeper, and matching only `.scene` classified `{key}` as `other` and
    // dropped a `{slug}` value from the scan entirely.
    if (/(?:^|\.)scene(\.(?:key|scene|slug))?$/.test(path)) {
        return 'navigation';
    }

    if (/object$/.test(path)) {
        return 'object';
    }

    // A field named inside any filter, rule, sort or value block. These are the
    // references that keep pointing at the old object after a repoint and still
    // look valid, which is the failure this whole scan exists to surface.
    if (
        /\.field$/.test(path) ||
        /\.field\.key$/.test(path) ||
        // A column's `id` mirrors its field key, so it has to follow the field.
        /\.id$/.test(path) ||
        // Details and list layouts store the field key under a bare `key` on items
        // nested in columns[].groups[].columns[][] — the shape buildViewGroupField
        // itself writes. Without this every field in a details payload landed in
        // `other` and scopedFieldCount read 0. Scoped to a layout container so the
        // view's own top-level `key` is not swept up with them.
        (/\.key$/.test(path) &&
            /(?:columns|groups|inputs)\[\d+\]/.test(path)) ||
        /criteria\[\d+\]/.test(path) ||
        /filters\[\d+\]/.test(path) ||
        /sort\[\d+\]/.test(path) ||
        /rules\[\d+\]/.test(path) ||
        /values\[\d+\]/.test(path)
    ) {
        return 'scoped-field';
    }

    return 'other';
}

/**
 * Every Knack key held anywhere in a view schema, with the path that holds it.
 *
 * Walks the structure generically instead of reading a list of known paths. That
 * is deliberate, and it is the same lesson the cascade guard learned about
 * `links` and `columns`: an enumerated path list only finds the shapes someone
 * thought of, and Knack keeps putting references in new places — a column's own
 * `source.filters`, an `edit_rules` entry's dotted `object_N.field_N`, a KTL
 * directive inside `description`. A generic walk finds those without being told,
 * and covers details, list, form and search layouts for free.
 *
 * Read-only. It reports; it changes nothing.
 *
 * @param schema A complete view schema, as returned by view metadata or posted
 *   to a copy request.
 * @returns Every reference found, in walk order, de-duplicated by path.
 */
export function collectViewReferences(schema: unknown): ViewReference[] {
    const found: ViewReference[] = [];

    const walk = (node: unknown, path: string): void => {
        if (typeof node === 'string') {
            if (KNACK_KEY_PATTERN.test(node)) {
                found.push({
                    path,
                    value: node,
                    kind: classifyViewReference(path),
                });
                return;
            }

            if (
                DOTTED_CONNECTION_PATTERN.test(node) ||
                HYPHENATED_PAIR_PATTERN.test(node)
            ) {
                // Recorded whole rather than split: the stored value is the
                // compound string, so that is what a repoint has to rewrite.
                found.push({
                    path,
                    value: node,
                    kind: classifyViewReference(path),
                });
                return;
            }

            // A slug like `cancel-appointment3` in a `scene` position is a real
            // navigation reference even though it is not a `scene_N` key.
            if (classifyViewReference(path) === 'navigation' && node !== '') {
                found.push({ path, value: node, kind: 'navigation' });
                return;
            }

            // Prose that embeds keys — KTL directives in `description` are the
            // known case, and they are invisible to every other check.
            const embedded = node.match(EMBEDDED_KEY_PATTERN);
            if (embedded) {
                for (const key of new Set(embedded)) {
                    found.push({
                        path: `${path} (embedded)`,
                        value: key,
                        kind: 'other',
                    });
                }
            }

            return;
        }

        if (Array.isArray(node)) {
            node.forEach((item, index) => walk(item, `${path}[${index}]`));
            return;
        }

        if (node && typeof node === 'object') {
            for (const [key, value] of Object.entries(node)) {
                walk(value, path ? `${path}.${key}` : key);
            }
        }
    };

    walk(schema, '');
    return found;
}

/**
 * Group a view's references by what each kind of change would invalidate.
 *
 * Two different edits are called "repointing" and they have almost nothing in
 * common:
 *
 * - **Rescope** — change `connection_key`, `parent_source` or `authenticated_user`.
 *   Only `scopeConnections` are involved. Display connections, fields, filters and
 *   sorts all stay valid, because they name fields on an object that has not
 *   changed. Measured: a builder rescope left all six display connections untouched.
 * - **Retarget** — change `source.object`. Now everything is suspect: every field,
 *   display connection, filter, sort and rule names a field on the old object.
 *
 * @param schema A complete view schema.
 * @returns References split by kind, plus the distinct connection fields in each.
 */
export function planViewRepoint(schema: unknown): {
    scopeConnections: ViewReference[];
    displayConnections: ViewReference[];
    scopedFields: ViewReference[];
    navigation: ViewReference[];
    other: ViewReference[];
    distinctScopeKeys: string[];
    distinctDisplayKeys: string[];
} {
    const references = collectViewReferences(schema);
    const byKind = (kind: ViewReferenceKind) =>
        references.filter((reference) => reference.kind === kind);

    const scopeConnections = byKind('scope-connection');
    const displayConnections = byKind('display-connection');

    /**
     * Reduce each compound reference to the connection field it names.
     *
     * The two compound forms are built differently, so they reduce differently, and a
     * review caught the hyphenated one being reported whole as though it were a field
     * key:
     *
     * - `object_44.field_1029` is `<object>.<connection field>` — the field is last.
     * - `field_784-field_74` is `<connection field>-<field on the far object>` — the
     *   connection is **first**. Measured: all 30 `connection_field` values in the
     *   export are hyphenated, and in 4 of 4 checked against the field schema the
     *   first half is a `connection` whose relationship points at the object the
     *   second half lives on. So the first half is the connection, the second the
     *   target field.
     */
    const fieldsOf = (list: ViewReference[]) => [
        ...new Set(
            list.map((reference) => {
                const value = reference.value;
                if (HYPHENATED_PAIR_PATTERN.test(value)) {
                    return value.split('-')[0];
                }
                return value.split('.').pop() as string;
            }),
        ),
    ];

    return {
        scopeConnections,
        displayConnections,
        scopedFields: byKind('scoped-field'),
        navigation: byKind('navigation'),
        other: [...byKind('object'), ...byKind('other')],
        distinctScopeKeys: fieldsOf(scopeConnections),
        distinctDisplayKeys: fieldsOf(displayConnections),
    };
}
