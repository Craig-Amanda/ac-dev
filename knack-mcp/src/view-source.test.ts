import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import {
    KNACK_CONDITIONAL_RULES_SHAPE,
    KNACK_VIEW_SOURCE_SHAPE,
    buildTemplateFieldDescriptors,
    buildViewSource,
    buildNoDataText,
    buildViewTemplatePayload,
    collectViewReferences,
    planViewRepoint,
    describeLayoutKeyGap,
    resolveTemplateFields,
    viewTypeCarriesNoDataText,
} from './server.js';

describe('buildViewSource', () => {
    /**
     * The source shapes here come from a production app export rather than invention.
     * That distinction is the whole lesson of the view-safety work: a hand-written
     * fixture encodes exactly the assumption the code already makes, so every shape
     * asserted below was observed in a real app first.
     */
    it('builds an unfiltered source as an empty match-all block', () => {
        assert.deepEqual(buildViewSource({ objectKey: 'object_1' }), {
            object: 'object_1',
            criteria: { match: 'all', rules: [], groups: [] },
            sort: [],
            limit: '',
        });
    });

    it('scopes to a connection with its relationship type', () => {
        const source = buildViewSource({
            objectKey: 'object_1',
            connectionKey: 'field_2',
            relationshipType: 'foreign',
        });

        assert.equal(source.connection_key, 'field_2');
        assert.equal(source.relationship_type, 'foreign');
    });

    it('refuses a connection with no relationship type', () => {
        // 102 of 102 connected sources carried both. A connection with no direction
        // leaves Knack to pick one, which is a wrong-rows bug rather than an error.
        assert.throws(
            () =>
                buildViewSource({
                    objectKey: 'object_1',
                    connectionKey: 'field_2',
                }),
            /relationshipType is required/,
        );
    });

    it('refuses a relationship type with no connection', () => {
        assert.throws(
            () =>
                buildViewSource({
                    objectKey: 'object_1',
                    relationshipType: 'local',
                }),
            /connectionKey is required/,
        );
    });

    it('writes authenticated_user only when true', () => {
        // It was true in all 28 occurrences and never false, so false omits the key
        // rather than asserting something never observed.
        const on = buildViewSource({
            objectKey: 'object_1',
            authenticatedUser: true,
        });
        const off = buildViewSource({
            objectKey: 'object_1',
            authenticatedUser: false,
        });

        assert.equal(on.authenticated_user, true);
        assert.equal('authenticated_user' in off, false);
    });

    it('scopes to the logged-in user with no connection at all', () => {
        // A real form on the user's own record carried exactly this and nothing else,
        // so authenticated_user is not solely a modifier on a connection.
        const source = buildViewSource({
            objectKey: 'object_1',
            authenticatedUser: true,
        });

        assert.equal('connection_key' in source, false);
        assert.equal(source.authenticated_user, true);
    });

    it('carries a parent hop that differs from the connection', () => {
        // The case that cannot be reconstructed from connection_key alone: 5 of 8
        // observed parent_source blocks named a different, earlier hop.
        const source = buildViewSource({
            objectKey: 'object_1',
            connectionKey: 'field_2',
            relationshipType: 'foreign',
            parentSource: { object: 'object_2', connection: 'field_3' },
        });

        assert.deepEqual(source.parent_source, {
            object: 'object_2',
            connection: 'field_3',
        });
    });

    it('refuses a half-specified parent hop', () => {
        assert.throws(
            () =>
                buildViewSource({
                    objectKey: 'object_1',
                    connectionKey: 'field_2',
                    relationshipType: 'foreign',
                    parentSource: {
                        object: 'object_2',
                    } as unknown as { object: string; connection: string },
                }),
            /parentSource needs both/,
        );
    });

    it('nests groups as arrays of rules, with no match of their own', () => {
        // The shape question the export settled: groups is rule[][], and the first
        // block is `rules` rather than group zero — a real view populated both.
        const source = buildViewSource({
            objectKey: 'object_1',
            filters: {
                match: 'all',
                rules: [{ field: 'field_1', operator: 'is', value: 'x' }],
                groups: [
                    [
                        { field: 'field_2', operator: 'is', value: 'a' },
                        { field: 'field_2', operator: 'is', value: 'b' },
                    ],
                ],
            },
        });

        const criteria = source.criteria as {
            match: string;
            rules: unknown[];
            groups: unknown[][];
        };
        assert.equal(criteria.match, 'all');
        assert.equal(criteria.rules.length, 1);
        assert.equal(criteria.groups.length, 1);
        assert.equal(criteria.groups[0].length, 2);
        assert.equal('match' in (criteria.groups[0][0] as object), false);
    });

    it('defaults an absent value to an empty string, as the export does', () => {
        const source = buildViewSource({
            objectKey: 'object_1',
            filters: { rules: [{ field: 'field_1', operator: 'is blank' }] },
        });

        const criteria = source.criteria as {
            rules: Array<{ value: unknown }>;
        };
        assert.equal(criteria.rules[0].value, '');
    });

    it('refuses a match that is neither all nor any', () => {
        assert.throws(
            () =>
                buildViewSource({
                    objectKey: 'object_1',
                    filters: {
                        match: 'either' as unknown as 'all',
                    },
                }),
            /must be "all" or "any"/,
        );
    });

    it('refuses a group that is not an array of rules', () => {
        assert.throws(
            () =>
                buildViewSource({
                    objectKey: 'object_1',
                    filters: {
                        groups: [
                            {
                                field: 'field_1',
                                operator: 'is',
                            },
                        ] as unknown as Array<
                            Array<{ field: string; operator: string }>
                        >,
                    },
                }),
            /array of arrays/,
        );
    });

    it('refuses a rule missing its operator', () => {
        assert.throws(
            () =>
                buildViewSource({
                    objectKey: 'object_1',
                    filters: {
                        rules: [
                            { field: 'field_1' } as unknown as {
                                field: string;
                                operator: string;
                            },
                        ],
                    },
                }),
            /needs both field and operator/,
        );
    });

    it('records the measured shape rather than a guess', () => {
        // The point of the constant is provenance. If the summary stops naming what it
        // was verified against, it has become the kind of claim this project removes.
        assert.match(KNACK_VIEW_SOURCE_SHAPE.summary, /Verified against/);
        assert.match(
            KNACK_VIEW_SOURCE_SHAPE.criteria.semantics,
            /inverse of match/,
        );
        assert.equal(
            KNACK_VIEW_SOURCE_SHAPE.notes.some((note) =>
                /never appeared inside any source/.test(note),
            ),
            true,
        );
    });
});

describe('KNACK_CONDITIONAL_RULES_SHAPE', () => {
    /**
     * These notes were checked against a 1,911-field schema export from a second app.
     * Two of them were true of the sample they were written from and false in general,
     * which is the failure mode a dated note exists to make visible. The assertions
     * here pin the corrected reading so a future edit cannot quietly restore it.
     */
    it("no longer calls value_field's purpose unclear", () => {
        const notes = KNACK_CONDITIONAL_RULES_SHAPE.notes.join(' ');
        assert.equal(/purpose is unclear/.test(notes), false);
        assert.match(
            notes,
            /value_type decides whether value_field means anything/,
        );
    });

    it('does not claim the auto_increment target held in every example', () => {
        const notes = KNACK_CONDITIONAL_RULES_SHAPE.notes.join(' ');
        assert.equal(/in every working example/.test(notes), false);
        assert.match(notes, /200 of 223/);
    });

    it('records that a rule key can be absent, not merely non-sequential', () => {
        const notes = KNACK_CONDITIONAL_RULES_SHAPE.notes.join(' ');
        assert.match(notes, /can be absent altogether/);
    });

    it('names both verifications in its summary', () => {
        assert.match(
            KNACK_CONDITIONAL_RULES_SHAPE.summary,
            /2026-08-14.*2026-09-03/s,
        );
    });
});

describe('buildTemplateFieldDescriptors', () => {
    /**
     * Explicit fieldKeys used to bypass the schema entirely, so every generated view
     * carried raw field keys as its column headers — visible in the builder next to a
     * hand-built view showing real labels. The keys still choose which fields appear;
     * the schema only supplies each one's label and type.
     */
    const schemaFields = [
        { key: 'field_1', name: 'Client Name', type: 'short_text' },
        { key: 'field_2', name: 'Booking Date', type: 'date_time' },
    ];

    it('resolves labels and types for explicitly named keys', () => {
        const descriptors = buildTemplateFieldDescriptors(
            ['field_2', 'field_1'],
            schemaFields,
        );

        assert.deepEqual(descriptors, [
            { key: 'field_2', name: 'Booking Date', type: 'date_time' },
            { key: 'field_1', name: 'Client Name', type: 'short_text' },
        ]);
    });

    it('keeps the caller order rather than the schema order', () => {
        const descriptors = buildTemplateFieldDescriptors(
            ['field_2', 'field_1'],
            schemaFields,
        );

        assert.deepEqual(
            descriptors.map((field) => field.key),
            ['field_2', 'field_1'],
        );
    });

    it('falls back to the key when the schema is unavailable', () => {
        // The honest degradation: a header reading "field_9" is worse than a label but
        // better than failing the call, and the tool now says so in its notes.
        const descriptors = buildTemplateFieldDescriptors(['field_9'], []);

        assert.deepEqual(descriptors, [
            { key: 'field_9', name: 'field_9', type: 'text' },
        ]);
    });

    it('falls back only for the keys the schema does not know', () => {
        const descriptors = buildTemplateFieldDescriptors(
            ['field_1', 'field_99'],
            schemaFields,
        );

        assert.equal(descriptors[0].name, 'Client Name');
        assert.equal(descriptors[1].name, 'field_99');
    });
});

describe('resolveTemplateFields', () => {
    /**
     * This is the seam the header bug actually lived in. Testing the helper beneath it
     * passed whichever way the call site was wired, so the decision — whether the
     * schema reaches explicitly-named keys at all — needs a test of its own.
     */
    const schemaFields = [
        { key: 'field_1', name: 'Client Name', type: 'short_text' },
        { key: 'field_2', name: 'Booking Date', type: 'date_time' },
    ];

    it('gives explicitly named keys their real labels', () => {
        const { fieldDescriptors, derivedFromSchema } = resolveTemplateFields({
            fieldKeys: ['field_1'],
            allObjectFields: schemaFields,
            objectKey: 'object_1',
            canonicalType: 'table',
        });

        assert.equal(fieldDescriptors[0].name, 'Client Name');
        assert.notEqual(fieldDescriptors[0].name, 'field_1');
        // The keys were chosen by the caller, so nothing was derived.
        assert.equal(derivedFromSchema, false);
    });

    it('says so in its notes when a header falls back to a key', () => {
        const { notes } = resolveTemplateFields({
            fieldKeys: ['field_1', 'field_99'],
            allObjectFields: schemaFields,
            objectKey: 'object_1',
            canonicalType: 'table',
        });

        assert.equal(
            notes.some((note) => /1 of 2 field\(s\) were not found/.test(note)),
            true,
        );
    });

    it('warns when no schema was available at all', () => {
        const { notes, fieldDescriptors } = resolveTemplateFields({
            fieldKeys: ['field_1'],
            allObjectFields: [],
            objectKey: 'object_1',
            canonicalType: 'table',
        });

        assert.equal(fieldDescriptors[0].name, 'field_1');
        assert.equal(
            notes.some((note) => /headers fall back to field keys/.test(note)),
            true,
        );
    });

    it('derives from the schema when no keys are named', () => {
        const { fieldDescriptors, derivedFromSchema } = resolveTemplateFields({
            fieldKeys: [],
            allObjectFields: schemaFields,
            objectKey: 'object_1',
            canonicalType: 'table',
        });

        assert.equal(derivedFromSchema, true);
        assert.equal(fieldDescriptors.length, 2);
    });
});

describe('describeLayoutKeyGap', () => {
    /**
     * The case that produced this: two views created back to back with the same
     * existingViewKeys, captured before either existed. pageGroups is the page's whole
     * layout, so the second create rebuilt it without the first — which stayed a live
     * view with nowhere to appear, and nothing in the response mentioned it.
     */
    it('names the views an explicit list would drop', () => {
        const gap = describeLayoutKeyGap(
            ['view_1', 'view_2'],
            ['view_1', 'view_2', 'view_3'],
        );

        assert.notEqual(gap, null);
        assert.match(gap as string, /view_3/);
        assert.match(gap as string, /replaces the page layout/);
    });

    it('stays quiet when the list is complete', () => {
        assert.equal(
            describeLayoutKeyGap(['view_1', 'view_2'], ['view_1', 'view_2']),
            null,
        );
    });

    it('stays quiet when the list covers more than the scene reports', () => {
        // A key the caller knows about and the scene does not is their business —
        // only the reverse drops something that exists.
        assert.equal(
            describeLayoutKeyGap(['view_1', 'view_9'], ['view_1']),
            null,
        );
    });

    it('says nothing when no explicit list was passed', () => {
        // Derivation handles that path, and it cannot go stale.
        assert.equal(describeLayoutKeyGap([], ['view_1', 'view_2']), null);
    });

    it('says nothing when the scene reports no views', () => {
        assert.equal(describeLayoutKeyGap(['view_1'], []), null);
    });
});

describe('buildNoDataText', () => {
    /**
     * Measured across a 738-view export: `no_data_text` appears only on table
     * (217 of 224) and list (6 of 6) views, and every one of those 223 values is
     * non-empty. None contains a template token, so the string cannot vary at
     * render time — deriving it from the object at build time is the whole of
     * what is available.
     */
    it('names the object when the schema supplied a name', () => {
        assert.equal(buildNoDataText('Booking'), 'No Booking Records');
    });

    it('appends Records rather than pluralising the name', () => {
        // Guessing a plural from a singular object name is how "No Bookinges"
        // happens. The suffix reads correctly whichever way the name is written.
        assert.equal(
            buildNoDataText('Service Visit'),
            'No Service Visit Records',
        );
    });

    it('falls back to a bare line when no name is available', () => {
        // No appKey passed, or the object missing from the schema.
        assert.equal(buildNoDataText(null), 'No records');
        assert.equal(buildNoDataText(undefined), 'No records');
    });

    it('treats a blank or whitespace name as no name', () => {
        assert.equal(buildNoDataText(''), 'No records');
        assert.equal(buildNoDataText('   '), 'No records');
    });

    it('trims a padded name rather than embedding the padding', () => {
        assert.equal(buildNoDataText('  Booking  '), 'No Booking Records');
    });

    it('never returns an empty string', () => {
        // The defect being fixed: an unset key becomes "" in Knack, and the view
        // then renders its stock line instead of anything the author chose.
        for (const name of ['Booking', '', '   ', null, undefined]) {
            assert.notEqual(buildNoDataText(name).trim(), '');
        }
    });
});

describe('viewTypeCarriesNoDataText', () => {
    it('accepts the two types the export shows carrying the key', () => {
        assert.equal(viewTypeCarriesNoDataText('table'), true);
        assert.equal(viewTypeCarriesNoDataText('list'), true);
    });

    it('rejects every type the export shows without it', () => {
        // 0 occurrences each across 738 views: details (74), form (152),
        // menu (48), calendar (2), report (38), login (55), registration (120),
        // rich_text (19).
        for (const type of [
            'details',
            'form',
            'menu',
            'calendar',
            'report',
            'login',
            'registration',
            'rich_text',
        ]) {
            assert.equal(viewTypeCarriesNoDataText(type), false, type);
        }
    });

    it('does not accept the pre-canonical grid alias', () => {
        // Callers canonicalise `grid` to `table` before asking. If that ever
        // stops happening, a grid template silently loses its empty-state line,
        // so the alias must not quietly pass here.
        assert.equal(viewTypeCarriesNoDataText('grid'), false);
    });
});

describe('buildViewTemplatePayload', () => {
    /**
     * These assert the payload a caller actually receives. The helper tests above
     * pass whichever way this is wired, which is how `no_data_text` came to be
     * missing from every generated view in the first place — the same
     * below-its-own-seam mistake made twice before in this file.
     */
    const base = {
        displayName: 'Bookings',
        resolvedTitle: 'Bookings',
        viewSource: buildViewSource({ objectKey: 'object_1' }),
        fieldDescriptors: buildTemplateFieldDescriptors(
            ['field_1'],
            [{ key: 'field_1', name: 'Reference', type: 'short_text' }],
        ),
        pageGroups: [{ columns: [{ keys: ['new'], width: 100 }] }],
        noDataText: 'No Booking Records',
    };

    it('writes no_data_text into a table payload', () => {
        const payload = buildViewTemplatePayload({
            ...base,
            canonicalType: 'table',
        });
        assert.equal(payload.no_data_text, 'No Booking Records');
    });

    it('writes no_data_text into a list payload', () => {
        const payload = buildViewTemplatePayload({
            ...base,
            canonicalType: 'list',
        });
        assert.equal(payload.no_data_text, 'No Booking Records');
    });

    it('leaves no_data_text off a details payload', () => {
        // 0 of 74 details views in the export carry the key. Writing one would
        // be inventing a shape rather than reproducing a measured one.
        const payload = buildViewTemplatePayload({
            ...base,
            canonicalType: 'details',
        });
        assert.equal('no_data_text' in payload, false);
    });

    it('leaves no_data_text off a form payload', () => {
        const payload = buildViewTemplatePayload({
            ...base,
            canonicalType: 'form',
        });
        assert.equal('no_data_text' in payload, false);
    });

    it('agrees with viewTypeCarriesNoDataText for every template type', () => {
        // The guard against the two drifting apart: one says which types carry
        // the key, the other writes it, and nothing else keeps them in step.
        for (const canonicalType of ['table', 'list', 'details', 'form']) {
            const payload = buildViewTemplatePayload({
                ...base,
                canonicalType,
            });
            assert.equal(
                'no_data_text' in payload,
                viewTypeCarriesNoDataText(canonicalType),
                canonicalType,
            );
        }
    });

    it('carries the derived line end to end for a table', () => {
        // The call site's own decision: an unnamed object must still produce a
        // non-empty line, because an absent key becomes "" in Knack.
        const payload = buildViewTemplatePayload({
            ...base,
            canonicalType: 'table',
            noDataText: buildNoDataText(null),
        });
        assert.equal(payload.no_data_text, 'No records');
    });

    it('still resolves real column headers for a table', () => {
        // Pins the earlier defect at the same seam: an explicit fieldKeys list
        // used to reach the builder with no object fields, so every header read
        // as its raw field key.
        const payload = buildViewTemplatePayload({
            ...base,
            canonicalType: 'table',
        });
        const columns = payload.columns as Array<{ header?: string }>;
        assert.equal(columns[0].header, 'Reference');
    });
});

/**
 * A faithful reduction of a real builder copy request, 4 September: every
 * reference-bearing shape it carried, with the cosmetics dropped. Reduced rather
 * than invented — the point of these tests is that the scanner finds references
 * in the places Knack actually puts them, and a hand-built fixture would only
 * contain the places I already thought of.
 */
const COPY_REQUEST_SCHEMA = {
    no_data_text: 'No Jobs to Assign',
    type: 'table',
    columns: [
        {
            type: 'field',
            field: { key: 'field_1029' },
            rules: [
                {
                    key: '22',
                    actions: [{ action: 'text-style' }],
                    criteria: [
                        {
                            field: 'field_1022',
                            value: '',
                            operator: 'is not blank',
                        },
                    ],
                },
            ],
        },
        {
            id: 'field_1722',
            type: 'field',
            field: { key: 'field_1722' },
            connection: { key: 'field_1029' },
        },
        {
            id: 'field_1838',
            type: 'field',
            field: { key: 'field_1838' },
            connection: { key: 'field_1029' },
        },
        {
            id: 'field_1316',
            type: 'field',
            field: { key: 'field_1316' },
            source: {
                filters: [
                    { field: 'field_63', value: 'active', operator: 'is' },
                ],
            },
            edit_rules: [
                {
                    key: '1',
                    action: 'connection',
                    connection: 'object_44.field_1029',
                    values: [
                        {
                            type: 'value',
                            field: 'field_903',
                            value: 'IN PROGRESS',
                        },
                    ],
                    criteria: [
                        {
                            field: 'field_1316',
                            value: '',
                            operator: 'is not blank',
                        },
                    ],
                },
            ],
        },
        { type: 'link', scene: 'assign-engineer', header: 'Assign Work' },
    ],
    source: {
        object: 'object_54',
        criteria: {
            match: 'all',
            rules: [{ field: 'field_1025', value: 'Booked', operator: 'is' }],
            groups: [],
        },
        authenticated_user: true,
        connection_key: 'field_1513',
        relationship_type: 'foreign',
        parent_source: { connection: 'field_2057', object: 'object_4' },
        sort: [{ field: 'field_1062', order: 'asc' }],
    },
    description: '_bulk_actions=[label, field_1029], [Assign Work, view_2685]',
};

describe('collectViewReferences', () => {
    it('classifies a column connection as display, not scope', () => {
        // Corrected 4 Sep by a builder before-and-after pair. A rescope that added
        // connection_key, relationship_type, authenticated_user and parent_source
        // left every column connection untouched — and they were already set while
        // the source had no connection at all. They are a display path out of the
        // view's own object, so a rescope must NOT rewrite them.
        const refs = collectViewReferences(COPY_REQUEST_SCHEMA);
        const columnConnections = refs.filter((reference) =>
            /^columns\[\d+\]\.connection\.key$/.test(reference.path),
        );
        assert.equal(columnConnections.length, 2);
        for (const reference of columnConnections) {
            assert.equal(reference.value, 'field_1029');
            assert.equal(reference.kind, 'display-connection');
        }
    });

    it('classifies the source connection and parent hop as scope', () => {
        const refs = collectViewReferences(COPY_REQUEST_SCHEMA);
        const byPath = new Map(
            refs.map((reference) => [reference.path, reference.kind]),
        );
        assert.equal(byPath.get('source.connection_key'), 'scope-connection');
        assert.equal(
            byPath.get('source.parent_source.connection'),
            'scope-connection',
        );
    });

    it('finds the dotted object.field form an edit rule uses', () => {
        const refs = collectViewReferences(COPY_REQUEST_SCHEMA);
        const dotted = refs.find(
            (reference) => reference.value === 'object_44.field_1029',
        );
        assert.ok(dotted, 'the dotted edit-rule connection was not found');
        assert.equal(dotted.kind, 'display-connection');
        assert.match(dotted.path, /edit_rules\[0\]\.connection$/);
    });

    it('finds keys embedded in a description’s KTL directives', () => {
        // Nothing else in the server reads these, so a copy carries them verbatim
        // and they keep naming the original's fields and views.
        const refs = collectViewReferences(COPY_REQUEST_SCHEMA);
        const embedded = refs.filter((reference) =>
            reference.path.endsWith('(embedded)'),
        );
        assert.deepEqual(embedded.map((reference) => reference.value).sort(), [
            'field_1029',
            'view_2685',
        ]);
    });

    it('classifies a scene slug as navigation, not as a connection', () => {
        // A slug is not a `scene_N` key, so only its position identifies it.
        const refs = collectViewReferences(COPY_REQUEST_SCHEMA);
        const nav = refs.filter((reference) => reference.kind === 'navigation');
        assert.deepEqual(
            nav.map((reference) => reference.value),
            ['assign-engineer'],
        );
    });

    it('separates the source connection from the parent hop', () => {
        const refs = collectViewReferences(COPY_REQUEST_SCHEMA);
        const byPath = new Map(
            refs.map((reference) => [reference.path, reference.value]),
        );
        assert.equal(byPath.get('source.connection_key'), 'field_1513');
        assert.equal(
            byPath.get('source.parent_source.connection'),
            'field_2057',
        );
    });

    it('walks shapes it was never told about', () => {
        // The whole reason for a generic walk: an enumerated path list only finds
        // what someone anticipated, and Knack keeps adding places.
        const refs = collectViewReferences({
            some_future_block: {
                nested: [{ deeper: { connection: { key: 'field_77' } } }],
            },
        });
        assert.equal(refs.length, 1);
        assert.equal(refs[0].value, 'field_77');
        assert.equal(refs[0].kind, 'display-connection');
    });

    it('returns nothing for a schema with no references', () => {
        assert.deepEqual(
            collectViewReferences({ title: 'Plain', rows: [] }),
            [],
        );
        assert.deepEqual(collectViewReferences(null), []);
    });
});

describe('planViewRepoint', () => {
    it('separates the fields a rescope changes from the ones it must not', () => {
        // The whole point of the correction: a rescope touches the scope list and
        // nothing else. The first version of this conflated the two.
        const plan = planViewRepoint(COPY_REQUEST_SCHEMA);
        assert.deepEqual(plan.distinctScopeKeys.sort(), [
            'field_1513',
            'field_2057',
        ]);
        assert.deepEqual(plan.distinctDisplayKeys, ['field_1029']);
    });

    it('puts every scope reference inside the source block', () => {
        // If a scope reference ever appears outside `source`, the model is wrong
        // and this fails rather than quietly mis-grouping it.
        const plan = planViewRepoint(COPY_REQUEST_SCHEMA);
        assert.equal(plan.scopeConnections.length, 2);
        for (const reference of plan.scopeConnections) {
            assert.ok(
                reference.path.startsWith('source.'),
                `scope reference outside source: ${reference.path}`,
            );
        }
    });

    it('puts every display reference outside the source block', () => {
        const plan = planViewRepoint(COPY_REQUEST_SCHEMA);
        assert.equal(plan.displayConnections.length, 3);
        for (const reference of plan.displayConnections) {
            assert.equal(reference.path.startsWith('source.'), false);
        }
    });

    it('keeps navigation out of the connection list', () => {
        // Copying a view adds a reference to a linked page rather than removing
        // one, so navigation is the cascade guard's business and not a repoint's.
        const plan = planViewRepoint(COPY_REQUEST_SCHEMA);
        assert.equal(plan.navigation.length, 1);
        for (const reference of [
            ...plan.scopeConnections,
            ...plan.displayConnections,
        ]) {
            assert.notEqual(reference.kind, 'navigation');
        }
    });
});

describe('buildViewSource sort', () => {
    it('defaults to an empty sort, which is a real stored state', () => {
        const source = buildViewSource({ objectKey: 'object_1' });
        assert.deepEqual(source.sort, []);
    });

    it('carries a supplied sort through', () => {
        // A rebuild that hardcodes [] looks correct and orders differently. Both
        // sampled copy requests carried a sort, and different ones.
        const source = buildViewSource({
            objectKey: 'object_1',
            sort: [{ field: 'field_9', order: 'desc' }],
        });
        assert.deepEqual(source.sort, [{ field: 'field_9', order: 'desc' }]);
    });

    it('refuses a sort entry with no field', () => {
        assert.throws(
            () =>
                buildViewSource({
                    objectKey: 'object_1',
                    sort: [{ field: '', order: 'asc' }],
                }),
            /Every sort entry needs a field/,
        );
    });

    it('emits every scoping key at once, since they compose', () => {
        // The combination two real copy requests carried, and the one none of the
        // four documented patterns showed.
        const source = buildViewSource({
            objectKey: 'object_54',
            connectionKey: 'field_1513',
            relationshipType: 'foreign',
            authenticatedUser: true,
            parentSource: { object: 'object_4', connection: 'field_2057' },
            sort: [{ field: 'field_1062', order: 'asc' }],
        });
        assert.equal(source.connection_key, 'field_1513');
        assert.equal(source.relationship_type, 'foreign');
        assert.equal(source.authenticated_user, true);
        assert.deepEqual(source.parent_source, {
            object: 'object_4',
            connection: 'field_2057',
        });
        assert.deepEqual(source.sort, [{ field: 'field_1062', order: 'asc' }]);
    });
});

describe('buildViewTemplatePayload column connections', () => {
    const descriptors = [
        { key: 'field_1', name: 'Own Field', type: 'short_text' },
        {
            key: 'field_2',
            name: 'Reached Field',
            type: 'short_text',
            connectionKey: 'field_3',
        },
    ];

    it('emits connection only on the column that reaches through one', () => {
        const payload = buildViewTemplatePayload({
            canonicalType: 'table',
            displayName: 'T',
            resolvedTitle: 'T',
            viewSource: buildViewSource({ objectKey: 'object_1' }),
            fieldDescriptors: descriptors,
            pageGroups: [],
            noDataText: 'No records',
        });
        const columns = payload.columns as Array<Record<string, unknown>>;
        assert.equal('connection' in columns[0], false);
        assert.deepEqual(columns[1].connection, { key: 'field_3' });
    });

    it('is found by the scanner once emitted', () => {
        // End to end: what the template writes is what a later repoint must find.
        const payload = buildViewTemplatePayload({
            canonicalType: 'table',
            displayName: 'T',
            resolvedTitle: 'T',
            viewSource: buildViewSource({ objectKey: 'object_1' }),
            fieldDescriptors: descriptors,
            pageGroups: [],
            noDataText: 'No records',
        });
        const plan = planViewRepoint(payload);
        // A template's column connection is a display path, like the real ones.
        assert.ok(plan.distinctDisplayKeys.includes('field_3'));
    });
});

describe('scanner classifications a review caught', () => {
    /**
     * Three misses found by a Copilot review on PR #42, each verified with a probe
     * before being fixed. Worth keeping as tests rather than just fixing, because the
     * first two are the same mistake in two coats: classifying by the *tail* of a path
     * without accounting for a reference that sits one segment deeper.
     */
    it('treats a nested scene reference as navigation, in all three forms', () => {
        // `readSceneReference` resolves {key}, {scene} and {slug}. Matching only a path
        // ending `.scene` classified {key} as `other` and — worse — dropped a {slug}
        // value from the scan entirely, because its string matches no key pattern.
        for (const scene of [
            { key: 'scene_9' },
            { scene: 'a-page' },
            { slug: 'a-page' },
        ]) {
            const refs = collectViewReferences({
                columns: [{ type: 'link', scene }],
            });
            assert.equal(refs.length, 1, JSON.stringify(scene));
            assert.equal(refs[0].kind, 'navigation', JSON.stringify(scene));
        }
    });

    it('still treats a plain slug string as navigation', () => {
        // The common case, and the one the widened pattern must not break.
        const refs = collectViewReferences({
            columns: [{ type: 'link', scene: 'a-page' }],
        });
        assert.deepEqual(refs, [
            { path: 'columns[0].scene', value: 'a-page', kind: 'navigation' },
        ]);
    });

    it('reads a hyphenated field pair as one display connection', () => {
        // `field_784-field_74` matched no pattern, so it fell through to the prose
        // branch and was reported as two loose keys — losing the display connection it
        // names, and misclassifying both halves as `other`.
        const refs = collectViewReferences({
            columns: [
                {
                    edit_rules: [
                        {
                            values: [
                                { connection_field: 'field_784-field_74' },
                            ],
                        },
                    ],
                },
            ],
        });
        assert.equal(refs.length, 1);
        assert.equal(refs[0].value, 'field_784-field_74');
        assert.equal(refs[0].kind, 'display-connection');
    });

    it('counts a hyphenated pair among the display keys', () => {
        // The consequence of the miss: planViewRepoint omitted it from both lists.
        const plan = planViewRepoint({
            columns: [
                {
                    edit_rules: [
                        {
                            values: [
                                { connection_field: 'field_784-field_74' },
                            ],
                        },
                    ],
                },
            ],
        });
        assert.equal(plan.displayConnections.length, 1);
        // Kept whole. The dotted `object_N.field_N` form reduces to its field because
        // the object prefix is context, but a hyphenated pair names both halves and
        // neither is redundant — reducing it would report a field the value does not
        //name on its own.
        assert.deepEqual(plan.distinctDisplayKeys, ['field_784-field_74']);
    });
});

describe('boundary validation a review caught', () => {
    /**
     * `parseJsonInput` casts rather than checks, so a TypeScript type on a parsed
     * value promises nothing about what a caller actually sent. Two findings from the
     * same root, both verified before fixing.
     */
    it('refuses a sort order Knack does not store', () => {
        assert.throws(
            () =>
                buildViewSource({
                    objectKey: 'object_1',
                    sort: [{ field: 'field_1', order: 'sideways' as 'asc' }],
                }),
            /"asc" or "desc"/,
        );
    });

    it('refuses a sort field that is not a string', () => {
        assert.throws(
            () =>
                buildViewSource({
                    objectKey: 'object_1',
                    sort: [{ field: 42 as unknown as string, order: 'asc' }],
                }),
            /non-empty string/,
        );
    });

    it('refuses a sort entry that is not an object', () => {
        assert.throws(
            () =>
                buildViewSource({
                    objectKey: 'object_1',
                    sort: ['field_1'] as unknown as Array<{
                        field: string;
                        order: 'asc';
                    }>,
                }),
            /must be an object/,
        );
    });

    it('refuses a sort that is not an array', () => {
        assert.throws(
            () =>
                buildViewSource({
                    objectKey: 'object_1',
                    sort: { field: 'field_1' } as unknown as Array<{
                        field: string;
                        order: 'asc';
                    }>,
                }),
            /must be an array/,
        );
    });

    it('still accepts an entry with no order, since Knack defaults it', () => {
        const source = buildViewSource({
            objectKey: 'object_1',
            sort: [{ field: 'field_1' } as { field: string; order: 'asc' }],
        });
        assert.deepEqual(source.sort, [{ field: 'field_1' }]);
    });
});

describe('a details field item can reach through a connection', () => {
    it('emits connection on a details payload, not only a table', () => {
        // The finding: columnConnections was accepted for every template type but
        // consumed only by the table branch, so details and list payloads dropped it
        // while the response reported it as applied. The captured details view proves
        // the shape — `connection: { key }` on field items nested inside
        // columns[].groups[].columns[][].
        const payload = buildViewTemplatePayload({
            canonicalType: 'details',
            displayName: 'D',
            resolvedTitle: 'D',
            viewSource: buildViewSource({ objectKey: 'object_1' }),
            fieldDescriptors: [
                { key: 'field_1', name: 'Own', type: 'short_text' },
                {
                    key: 'field_2',
                    name: 'Reached',
                    type: 'short_text',
                    connectionKey: 'field_3',
                },
            ],
            pageGroups: [],
            noDataText: 'No records',
        });

        const plan = planViewRepoint(payload);
        assert.deepEqual(plan.distinctDisplayKeys, ['field_3']);
    });

    it('leaves it off a details item with no connection', () => {
        const payload = buildViewTemplatePayload({
            canonicalType: 'details',
            displayName: 'D',
            resolvedTitle: 'D',
            viewSource: buildViewSource({ objectKey: 'object_1' }),
            fieldDescriptors: [
                { key: 'field_1', name: 'Own', type: 'short_text' },
            ],
            pageGroups: [],
            noDataText: 'No records',
        });

        assert.deepEqual(planViewRepoint(payload).displayConnections, []);
    });
});
