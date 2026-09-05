import {
    type CachedField,
    NON_FORM_FIELD_TYPES,
    type SceneInfo,
    type TemplateFieldDescriptor,
} from '../types.js';
import { FIELD_KEY_PATTERN } from './field-payload.js';
import { asRecord, getTrimmedString } from './util.js';

/**
 * Warn when an explicit existingViewKeys list is missing views the page already has.
 *
 * `pageGroups` is the page's whole layout, not an addition to it, so creating a view
 * with a list that omits an existing key drops that view out of the layout. It still
 * exists and is still reachable by its builder URL — it simply has nowhere to appear.
 *
 * Measured the hard way on 2026-09-03: two views were created back to back with the
 * same existingViewKeys, captured before either existed. The second create rebuilt the
 * layout without the first, which vanished from the page while remaining a live view.
 * Nothing in the response said so, which is what this note is for.
 *
 * @param explicitKeys The caller's existingViewKeys, as passed.
 * @param sceneViewKeys The keys the scene actually holds right now.
 * @returns A note to add to the response, or null when the list is complete.
 */
export function describeLayoutKeyGap(
    explicitKeys: string[],
    sceneViewKeys: string[],
): string | null {
    if (explicitKeys.length === 0 || sceneViewKeys.length === 0) return null;

    const passed = new Set(explicitKeys);
    const missing = sceneViewKeys.filter((key) => !passed.has(key));
    if (missing.length === 0) return null;

    return `existingViewKeys omits ${missing.length} view(s) the page currently holds (${missing.join(', ')}). pageGroups replaces the page layout rather than adding to it, so creating this view will drop those from the page — they will still exist and stay reachable by their builder URL, but nothing will show them. Omit existingViewKeys to have them derived from ${'the scene'} instead, or include every key above.`;
}

/**
 * Which canonical view types carry `no_data_text`.
 *
 * Measured on 2026-09-04 across a 738-view export: the key appears only on
 * `table` (217 of 224) and `list` (6 of 6). It is absent from every details,
 * form, menu, calendar, report, login, registration and rich_text view. A
 * `search` view carries it too — its builder save request writes one — but
 * search is not a template branch here.
 *
 * Of the 223 views that hold the key, **all 223 are non-empty**: nobody leaves
 * it blank on purpose. Knack stores no template tokens in it either (0 of 223
 * contain a placeholder), so the value cannot be dynamic at render time — the
 * most that can be done is derive it from the object at build time.
 *
 * Corrected 2026-09-04: this once said every stored value was "exactly two
 * words". That was true of all 223 in the first app and false in general — two
 * builder copy requests from a second app carried three- and four-word values,
 * neither containing the word "Records". So the derived default here is a
 * sensible floor rather than a house style, and `noDataText` is the way to match
 * an app whose convention differs. Length is not a rule; non-empty is.
 */
export const NO_DATA_TEXT_VIEW_TYPES = new Set(['table', 'list']);

export function viewTypeCarriesNoDataText(canonicalType: string): boolean {
    return NO_DATA_TEXT_VIEW_TYPES.has(canonicalType);
}

/**
 * The empty-state line for a record-backed view.
 *
 * Left unset, Knack's builder fills the key with an empty string and the view
 * renders its stock message. Naming the object is strictly more useful, so the
 * object's own name is used when the schema was loaded; appending `Records`
 * rather than pluralising the name keeps a singular object name reading
 * correctly ("No Booking Records", not "No Bookings" guessed from "Booking").
 * Without a name — no appKey passed, or the object missing from the schema —
 * it falls back to a bare `No records`.
 */
export function buildNoDataText(objectName?: string | null): string {
    const name = typeof objectName === 'string' ? objectName.trim() : '';
    return name ? `No ${name} Records` : 'No records';
}

export function buildStarterPageGroups(
    existingViewKeys: string[],
): Array<{ columns: Array<{ keys: string[]; width: number }> }> {
    const rows = existingViewKeys.map((viewKey) => ({
        columns: [{ keys: [viewKey], width: 100 }],
    }));
    rows.push({ columns: [{ keys: ['new'], width: 100 }] });
    return rows;
}

export type ViewTemplatePayloadOptions = {
    canonicalType: string;
    displayName: string;
    resolvedTitle: string;
    viewSource: Record<string, unknown>;
    fieldDescriptors: TemplateFieldDescriptor[];
    pageGroups: unknown[];
    noDataText: string;
};

/**
 * The starter payload for each template type.
 *
 * Extracted from the tool handler so the payload a caller actually receives can
 * be asserted, rather than only the helpers that feed it. Two defects in this
 * file were invisible to helper-level tests for exactly that reason: a header
 * fell back to the raw field key because the call site passed an empty field
 * list, and `no_data_text` was never written at all because no branch set it.
 * A test of `buildNoDataText` passes either way; a test of this does not.
 */
export function buildViewTemplatePayload({
    canonicalType,
    displayName,
    resolvedTitle,
    viewSource,
    fieldDescriptors,
    pageGroups,
    noDataText,
}: ViewTemplatePayloadOptions): Record<string, unknown> {
    if (canonicalType === 'table') {
        return {
            name: displayName,
            type: 'table',
            title: resolvedTitle,
            links: [],
            groups: [],
            inputs: [],
            source: viewSource,
            columns: fieldDescriptors.map((field) =>
                buildViewFieldColumn(field),
            ),
            no_data_text: noDataText,
            pageGroups,
        };
    }

    if (canonicalType === 'form') {
        return {
            name: displayName,
            type: 'form',
            title: resolvedTitle,
            action: 'insert',
            links: [],
            groups: [
                {
                    columns: [
                        {
                            width: 100,
                            inputs: fieldDescriptors.map((field) =>
                                buildFormInputField(field),
                            ),
                        },
                    ],
                },
            ],
            rules: {
                emails: [],
                fields: [],
                records: [],
                submits: [
                    {
                        key: 'submit_1',
                        action: 'message',
                        message: '<p>Form successfully submitted.</p>',
                        is_default: true,
                        reload_show: true,
                    },
                ],
            },
            source: viewSource,
            pageGroups,
        };
    }

    if (canonicalType === 'details') {
        return {
            name: displayName,
            type: 'details',
            title: resolvedTitle,
            links: [],
            groups: [],
            inputs: [],
            layout: 'full',
            source: viewSource,
            columns: [
                {
                    width: 100,
                    groups: [
                        {
                            columns: [
                                fieldDescriptors.map((field) =>
                                    buildViewGroupField(field),
                                ),
                            ],
                        },
                    ],
                },
            ],
            pageGroups,
        };
    }

    return {
        name: displayName,
        type: 'list',
        title: resolvedTitle,
        links: [],
        groups: [],
        inputs: [],
        layout: 'full',
        source: viewSource,
        columns: [
            {
                width: 100,
                groups: [
                    {
                        columns: [
                            fieldDescriptors.map((field) =>
                                buildViewGroupField(field),
                            ),
                        ],
                    },
                ],
            },
        ],
        reportType: null,
        allow_limit: true,
        filter_type: 'none',
        hide_fields: false,
        no_data_text: noDataText,
        pageGroups,
    };
}

export function buildTemplateFieldDescriptors(
    fieldKeys: string[],
    objectFields: CachedField[] = [],
    maxFields = 12,
): TemplateFieldDescriptor[] {
    const objectFieldsByKey = new Map(
        objectFields.map((field) => [field.key, field]),
    );
    const selectedFields =
        fieldKeys.length > 0
            ? fieldKeys.map(
                  (fieldKey) =>
                      objectFieldsByKey.get(fieldKey) || { key: fieldKey },
              )
            : objectFields.slice(0, Math.max(maxFields, 1));

    return selectedFields.map((field) => ({
        key: field.key,
        name: field.name || field.key,
        type: field.type || 'text',
    }));
}

export function isEligibleFormField(field: CachedField): boolean {
    const fieldType = (field.type || '').trim().toLowerCase();
    if (!fieldType) return true;
    return !NON_FORM_FIELD_TYPES.has(fieldType);
}

export function getSceneViewKeys(
    scenes: SceneInfo[],
    sceneKey?: string,
): string[] {
    if (!sceneKey) return [];
    return (
        scenes
            .find((scene) => scene.sceneKey === sceneKey)
            ?.views.map((view) => view.viewKey) || []
    );
}

/**
 * One filter rule inside a view's source criteria.
 *
 * Measured across 466 source criteria blocks in a production app export (738 views,
 * 31 August 2026): 339 of 345 rules were exactly `{ field, operator, value }`. Three
 * rarer variants exist and are deliberately not modelled — a rule adding `object_key`
 * to name a field on another object, one adding a display `label`, and a date-range
 * rule shaped `{ type: 'week', field, operator: 'is during the current', value: {
 * date: '', all_day: false } }`. Build those by hand until one is measured on purpose.
 */
export type ViewSourceFilterRule = {
    field: string;
    operator: string;
    value?: unknown;
};

/**
 * A view's source criteria: one first block plus any number of groups.
 *
 * `match` governs the first block, and a group's internal operator is the *inverse* of
 * it. See KNACK_VIEW_SOURCE_SHAPE for the observation that settles that, which is not
 * a reading of documentation but of a real filter that only parses one way.
 */
export type ViewSourceFilters = {
    match?: 'all' | 'any';
    rules?: ViewSourceFilterRule[];
    groups?: ViewSourceFilterRule[][];
};

export type ViewSourceSort = { field: string; order: 'asc' | 'desc' };

export type ViewSourceOptions = {
    objectKey: string;
    connectionKey?: string;
    relationshipType?: 'foreign' | 'local';
    authenticatedUser?: boolean;
    parentSource?: { object: string; connection: string };
    filters?: ViewSourceFilters;
    /**
     * The view's default sort. Omitted means `[]`, which is a real stored state —
     * 36 stored `sort: []` and 156 with no sort key at all in the export. Passing
     * one matters when rebuilding an existing view: two real builder copy requests
     * each carried a sort, and different ones, so a rebuild that hardcodes `[]`
     * silently drops the ordering the view was designed around.
     */
    sort?: ViewSourceSort[];
};

/**
 * Assemble a view's `source` block, including connection scoping and filters.
 *
 * The templates used to emit a flat source — object, empty criteria, empty sort — which
 * is the only shape they could produce, because nothing in this server knew what a
 * connected or filtered source looked like. Both are now measured, so both can be built
 * rather than guessed at by the caller.
 *
 * Two invariants are enforced rather than trusted, because each has a silent failure
 * mode on the far side:
 *
 * `connection_key` and `relationship_type` always travel together — 102 of 102
 * connected sources carried both, none carried one alone. A connection with no
 * relationship type does not describe a direction, and Knack is left to pick one.
 *
 * `authenticated_user` was `true` in all 28 occurrences and never `false`. It reads as
 * a flag whose presence is the meaning, so `false` omits the key rather than writing
 * it — writing `false` would assert something never observed.
 *
 * @param options Source object, optional connection scoping, optional filters.
 * @returns The `source` block, ready to drop into a view payload.
 */
export function buildViewSource(
    options: ViewSourceOptions,
): Record<string, unknown> {
    const {
        objectKey,
        connectionKey,
        relationshipType,
        authenticatedUser,
        parentSource,
        filters,
        sort,
    } = options;

    if (!objectKey) {
        throw new Error('objectKey is required to build a view source.');
    }

    // Validated at runtime, not just in the type. `parseJsonInput` casts, so
    // `ViewSourceSort[]` promises nothing about what a caller actually sent — a review
    // pointed out that `order: "sideways"` and a non-string field both reached Knack.
    if (sort !== undefined && !Array.isArray(sort)) {
        throw new Error('sort must be an array of { field, order } entries.');
    }

    for (const entry of sort ?? []) {
        const record = entry as unknown;
        if (!record || typeof record !== 'object' || Array.isArray(record)) {
            throw new Error(
                'Every sort entry must be an object of the form { field, order }.',
            );
        }

        const { field, order } = record as { field?: unknown; order?: unknown };

        if (typeof field !== 'string' || field.trim() === '') {
            throw new Error(
                'Every sort entry needs a field, as a non-empty string. A sort with no field is stored but orders nothing, which reads as a working sort in the builder.',
            );
        }

        if (!FIELD_KEY_PATTERN.test(field)) {
            throw new Error(
                `Sort field must be a field key like "field_12", not ${JSON.stringify(field)}. A sort naming something that is not a field is stored and orders nothing.`,
            );
        }

        // Required, not optional. Measured across the export: all 428 stored sort
        // entries carry an order — 340 `asc`, 88 `desc`, none omitted. An earlier
        // version accepted an entry without one on the unevidenced grounds that
        // "Knack defaults it". That was a guess, and the type has always said the
        // property is required.
        if (order !== 'asc' && order !== 'desc') {
            throw new Error(
                `Sort order must be "asc" or "desc", not ${JSON.stringify(order)}. All 428 stored sort entries measured carry one of those two, so an entry without an order is not a shape Knack writes.`,
            );
        }
    }

    if (connectionKey && !relationshipType) {
        throw new Error(
            'relationshipType is required alongside connectionKey. Use "foreign" when the connection field lives on the view\'s own object, "local" when it lives on the other object and points back — that split held for all 102 connected sources measured.',
        );
    }

    if (relationshipType && !connectionKey) {
        throw new Error(
            'connectionKey is required alongside relationshipType. The relationship type describes a direction; without the connection field there is nothing for it to describe.',
        );
    }

    if (parentSource && (!parentSource.object || !parentSource.connection)) {
        throw new Error(
            'parentSource needs both object and connection. It names the record context the page supplies, so a half-specified hop cannot be resolved.',
        );
    }

    const criteria = buildViewSourceCriteria(filters);

    const source: Record<string, unknown> = {
        object: objectKey,
        criteria,
        sort: sort ?? [],
        limit: '',
    };

    if (connectionKey) {
        source.connection_key = connectionKey;
        source.relationship_type = relationshipType;
    }

    if (parentSource) {
        source.parent_source = {
            object: parentSource.object,
            connection: parentSource.connection,
        };
    }

    // Presence is the meaning; see the note above on never writing `false`.
    if (authenticatedUser === true) {
        source.authenticated_user = true;
    }

    return source;
}

/**
 * Normalise the criteria half of a source, defaulting an absent filter to match-all.
 *
 * An empty first block with `match: "all"` is what every unfiltered view in the export
 * carried, so it is the right shape for "no filter" rather than omitting `criteria`.
 */
export function buildViewSourceCriteria(
    filters: ViewSourceFilters | undefined,
): Record<string, unknown> {
    const match = filters?.match ?? 'all';

    if (match !== 'all' && match !== 'any') {
        throw new Error(
            `filters.match must be "all" or "any", received ${JSON.stringify(match)}.`,
        );
    }

    const rules = filters?.rules ?? [];
    const groups = filters?.groups ?? [];

    if (!Array.isArray(rules)) {
        throw new Error('filters.rules must be an array of rules.');
    }

    if (!Array.isArray(groups) || groups.some((g) => !Array.isArray(g))) {
        throw new Error(
            'filters.groups must be an array of arrays: each group is a list of rules, and a group carries no match of its own.',
        );
    }

    const check = (rule: unknown, where: string): ViewSourceFilterRule => {
        const record = rule as Partial<ViewSourceFilterRule> | null;
        if (!record || typeof record !== 'object') {
            throw new Error(`${where} must be an object.`);
        }
        if (!record.field || !record.operator) {
            throw new Error(`${where} needs both field and operator.`);
        }
        return {
            field: record.field,
            operator: record.operator,
            value: record.value ?? '',
        };
    };

    return {
        match,
        rules: rules.map((rule, i) => check(rule, `filters.rules[${i}]`)),
        groups: groups.map((group, gi) =>
            group.map((rule, i) => check(rule, `filters.groups[${gi}][${i}]`)),
        ),
    };
}

/**
 * What a view's `source` block looks like, measured rather than inferred.
 *
 * Recorded in the same spirit as KNACK_CONDITIONAL_RULES_SHAPE: the shapes that were
 * actually observed, with the counts behind them and the gaps stated plainly. Read from
 * a production app export of 738 views on 31 August 2026. Keys and values below are
 * placeholders; only the structure is from the export.
 */
export const KNACK_VIEW_SOURCE_SHAPE = {
    summary:
        "A view's source decides which records it shows. The scoping keys are INDEPENDENT and compose freely — do not read the patterns below as a closed set of four. Verified against a production app export (738 views) on 2026-08-31, then corrected on 2026-09-04 by two real builder copy requests from a second app that carried connection_key, relationship_type, authenticated_user AND parent_source in one block, which is none of the named patterns.",
    composition:
        'Treat each key as an independent switch on top of `{ object, criteria, sort }`: connection_key+relationship_type scope through a connection, authenticated_user scopes to the logged-in account, parent_source adds a hop through the record the page supplies. Any combination is legal, including all of them at once. The named patterns below are the four combinations the first export happened to contain, not the four that exist.',
    patterns: {
        plain: '{ "object": "object_1", "criteria": { "match": "all", "rules": [], "groups": [] }, "sort": [{ "field": "field_1", "order": "asc" }], "limit": "" }',
        connectionScoped:
            '{ "object": "object_1", "criteria": { ... }, "sort": [], "limit": "", "connection_key": "field_2", "relationship_type": "foreign" }',
        loggedInUser:
            '{ "object": "object_1", "criteria": { ... }, "sort": [], "limit": "", "connection_key": "field_2", "relationship_type": "foreign", "authenticated_user": true }',
        multiHop:
            '{ "object": "object_1", "criteria": { ... }, "sort": [], "limit": "", "connection_key": "field_2", "relationship_type": "foreign", "parent_source": { "object": "object_2", "connection": "field_3" } }',
    },
    allKeysAtOnce:
        '{ "object": "object_1", "criteria": { ... }, "sort": [{ "field": "field_9", "order": "desc" }], "connection_key": "field_2", "relationship_type": "foreign", "authenticated_user": true, "parent_source": { "object": "object_2", "connection": "field_3" } } — observed twice in a second app, on two sibling views of one object. Note there is no `limit` key at all: the builder omits it where buildViewSource always writes `limit: ""`. Knack accepted our explicit empty string in a round-trip, so both forms work, but do not treat limit as mandatory.',
    counts: 'plain 325 views · connection-scoped 57 · logged-in user 16 · multi-hop 6 · authenticated_user seen 28 times in total across variants. Those counts are one app; a second app supplied the all-keys-at-once combination absent from them.',
    notes: [
        'relationship_type is decided by which object owns the connection field, and the split was clean across all 102 connected sources: "foreign" (84) where the connection field lives on the view\'s own object and points outward, "local" (18) where it lives on the other object and points back. This is the value to recompute when a copied view is repointed at a different connection — carrying the original over is how a copy silently returns the wrong rows.',
        'connection_key and relationship_type always appeared together. Neither was ever present alone.',
        "authenticated_user was true in every occurrence and never false. It also appears without connection_key at all — a form on the logged-in user's own record carried `{ object, sort, authenticated_user: true }` and nothing else — so it is not solely a modifier on a connection.",
        'parent_source names the record context the page supplies, as `{ object, connection }`. In 3 of 8 cases its connection was the same field as connection_key; in the other 5 it named an earlier, different hop, which is the case that cannot be reconstructed from connection_key alone. Every occurrence sat alongside relationship_type "foreign".',
        'source.type is unrelated to the above: "registration" on registration views (120) and "database" on a handful of others (6). It is not a filter or a connection.',
        'Sorting lives in source.sort as `[{ field, order }]` — a separate array from criteria. Notably `value_field` never appeared inside any source in the export (0 of 738 views); all 55 of its occurrences were in view rule criteria (records, emails and submits), paired with `value_type: "custom"` for field-to-field comparison. Do not look for a sort field inside a criteria rule.',
        'Scoping to the logged-in user has a second, separate mechanism: a criteria rule with `operator: "user"` and an empty value, applied to a connection field (60 occurrences). That is a filter rule rather than a source flag, and the two can be used independently.',
        'Knack stores these keys as posted rather than rewriting them. Measured on 2026-09-03 by creating a connection-scoped table on two unrelated objects and reading each back: object, connection_key and relationship_type came back byte-for-byte in both. So a source built here is what the view ends up with — but the read-back is still the honest way to confirm a shape this file has not seen, since only these four patterns have been round-tripped.',
        'Knack does not validate relationship_type against the connection. A view repointed at a connection whose field lives on the other object was stored with "foreign" and accepted without error, where ownership makes it "local". Nothing downstream will tell you; that silence is why buildViewSource refuses a connection with no relationship type rather than guessing one.',
    ],
    criteria: {
        summary:
            'criteria is an object, not an array. `match` governs the first block; each group is an array of rules and carries no match of its own.',
        shape: '{ "match": "all", "rules": [{ "field": "field_1", "operator": "is", "value": "x" }], "groups": [[{ "field": "field_2", "operator": "is", "value": "a" }, { "field": "field_2", "operator": "is", "value": "b" }]] }',
        semantics:
            'A group\'s internal operator is the inverse of match. With match "all": rules AND together and each group is an OR. With match "any": rules OR together and each group is an AND.',
        evidence:
            'Observed rather than documented. One table carried match "all", four AND-ed top-level rules, and a single group of five equality tests on the *same* field with five different values. AND-ing five equality tests on one field matches nothing, so the group can only be an OR — which is the inverse of the enclosing match. The same view also proves the first block is `rules` and not group zero, since it populates both at once.',
        counts: '466 criteria blocks: match "all" 439, "any" 27. 135 carried rules, 30 carried groups. Every one of the 42 groups seen was an array of `{ field, operator, value }`.',
        operators:
            'Observed in source criteria: is, user, is not, contains, does not contain, is blank, is not blank, is after, is before, is after today, is today or after, is during the current, higher than. Not exhaustive — it is what this app happened to use.',
    },
} as const;

/**
 * Decide which fields a view template shows, and what each column is called.
 *
 * Extracted so the call site is testable rather than only the helper beneath it. The
 * bug this replaces lived entirely in the wiring: explicit `fieldKeys` were passed
 * with an empty schema, so `buildTemplateFieldDescriptors` could only fall back to the
 * key and every generated column read as `field_196` in the builder. A test of that
 * helper passed either way, which is why the decision now has a seam of its own.
 *
 * @param options Caller-supplied keys, the object's schema fields, and the view type.
 * @returns The descriptors, whether they were derived, and notes for the response.
 */
export function resolveTemplateFields(options: {
    fieldKeys: string[];
    allObjectFields: CachedField[];
    objectKey: string;
    canonicalType: string;
    maxFields?: number;
}): {
    fieldDescriptors: TemplateFieldDescriptor[];
    derivedFromSchema: boolean;
    notes: string[];
} {
    const {
        fieldKeys,
        allObjectFields,
        objectKey,
        canonicalType,
        maxFields = 12,
    } = options;
    const notes: string[] = [];

    if (fieldKeys.length > 0) {
        // Explicit keys still decide *which* fields appear; the schema only supplies
        // each one's label and type. A key the schema does not know keeps falling back
        // to itself rather than failing the call.
        const fieldDescriptors = buildTemplateFieldDescriptors(
            fieldKeys,
            allObjectFields,
            maxFields,
        );
        const named = fieldDescriptors.filter(
            (field) => field.name !== field.key,
        ).length;

        if (allObjectFields.length === 0) {
            notes.push(
                `No schema fields were available for ${objectKey}, so column headers fall back to field keys. Pass appKey, or expect headers like "field_123" in the builder.`,
            );
        } else if (named < fieldDescriptors.length) {
            notes.push(
                `${fieldDescriptors.length - named} of ${fieldDescriptors.length} field(s) were not found in ${objectKey}'s schema, so those column headers fall back to the field key.`,
            );
        }

        return { fieldDescriptors, derivedFromSchema: false, notes };
    }

    if (allObjectFields.length === 0) {
        notes.push(
            `No schema fields were found for ${objectKey}. Pass fieldKeys explicitly or ensure schema/runtime metadata is available.`,
        );
        return { fieldDescriptors: [], derivedFromSchema: false, notes };
    }

    const candidateFields =
        canonicalType === 'form'
            ? allObjectFields.filter((field) => isEligibleFormField(field))
            : allObjectFields;
    const fieldDescriptors = buildTemplateFieldDescriptors(
        [],
        candidateFields,
        maxFields,
    );
    notes.push(
        `Derived ${fieldDescriptors.length} field(s) from object metadata for ${objectKey}.`,
    );

    if (canonicalType === 'form') {
        const excludedCount = allObjectFields.length - candidateFields.length;
        if (excludedCount > 0) {
            notes.push(
                `Excluded ${excludedCount} non-input field(s) from the derived form template.`,
            );
        }
    }

    return { fieldDescriptors, derivedFromSchema: true, notes };
}

export function buildViewFieldColumn(field: TemplateFieldDescriptor) {
    const column: Record<string, unknown> = {
        id: field.key,
        type: 'field',
        align: 'left',
        field: { key: field.key },
        rules: [],
        width: {
            type: 'default',
            units: 'px',
            amount: '50',
        },
        header: field.name,
        grouping: false,
        conn_link: '',
        link_text: '',
        link_type: 'text',
        group_sort: 'asc',
        link_field: '',
        ignore_edit: false,
        img_gallery: '',
        conn_separator: '',
        ignore_summary: false,
        link_design_active: false,
        icon: {
            icon: '',
            align: 'left',
        },
    };

    // Only present when the column reaches through a connection. A column on the
    // view's own object carries no `connection` key at all, so writing an empty
    // one would invent a shape rather than reproduce the measured two.
    if (field.connectionKey) {
        column.connection = { key: field.connectionKey };
    }

    return column;
}

export function buildViewGroupField(field: TemplateFieldDescriptor) {
    const item: Record<string, unknown> = {
        key: field.key,
        copy: '',
        type: 'field',
        value: '',
        name: field.name,
        show_map: false,
        conn_link: '',
        link_text: '',
        link_type: 'text',
        map_width: 400,
        link_field: '',
        map_height: 300,
        img_gallery: '',
        conn_separator: '',
        link_design_active: false,
        icon: {
            icon: '',
            align: 'left',
        },
        format: {
            styles: [],
            label_custom: true,
            label_format: 'left',
        },
    };

    // A details view carries the same `connection: { key }` a table column does, on
    // field items nested inside columns[].groups[].columns[][] — measured from a real
    // builder copy request. Without this, columnConnections was accepted for a details
    // or list template, silently dropped, and then reported as applied.
    if (field.connectionKey) {
        item.connection = { key: field.connectionKey };
    }

    return item;
}

export function buildFormInputField(field: TemplateFieldDescriptor) {
    return {
        id: field.key,
        key: field.key,
        type: field.type,
        label: field.name,
        instructions: '',
        field: { key: field.key },
    };
}

export function getOptionLabel(value: unknown): string | null {
    const direct = getTrimmedString(value);
    if (direct) return direct;

    const rec = asRecord(value);
    if (!rec) return null;

    const candidates = [
        rec.label,
        rec.name,
        rec.text,
        rec.value,
        rec.identifier,
    ];
    for (const candidate of candidates) {
        const label = getTrimmedString(candidate);
        if (label) return label;
    }

    return null;
}

export function collectOptionLabels(
    value: unknown,
    output: string[],
    seen: Set<string>,
): void {
    if (Array.isArray(value)) {
        for (const item of value) {
            const label = getOptionLabel(item);
            if (label) {
                const dedupeKey = label.toLowerCase();
                if (!seen.has(dedupeKey)) {
                    seen.add(dedupeKey);
                    output.push(label);
                }
                continue;
            }

            const rec = asRecord(item);
            if (!rec) continue;
            for (const nestedKey of ['options', 'choices', 'values']) {
                if (nestedKey in rec) {
                    collectOptionLabels(rec[nestedKey], output, seen);
                }
            }
        }
        return;
    }

    const rec = asRecord(value);
    if (!rec) return;
    for (const nestedKey of ['options', 'choices', 'values']) {
        if (nestedKey in rec) {
            collectOptionLabels(rec[nestedKey], output, seen);
        }
    }
}

export function extractChoiceOptions(...candidates: unknown[]): string[] {
    const output: string[] = [];
    const seen = new Set<string>();
    for (const candidate of candidates) {
        collectOptionLabels(candidate, output, seen);
    }
    return output;
}

export function extractBoolean(...candidates: unknown[]): boolean | undefined {
    for (const candidate of candidates) {
        if (typeof candidate === 'boolean') return candidate;
        if (typeof candidate === 'number') return candidate !== 0;
        if (typeof candidate !== 'string') continue;
        const normalised = candidate.trim().toLowerCase();
        if (['true', 'yes', 'y', '1'].includes(normalised)) return true;
        if (['false', 'no', 'n', '0'].includes(normalised)) return false;
    }
    return undefined;
}
