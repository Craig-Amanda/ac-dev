import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import {
    KNACK_CONDITIONAL_RULES_SHAPE,
    KNACK_VIEW_SOURCE_SHAPE,
    buildTemplateFieldDescriptors,
    buildViewSource,
    describeLayoutKeyGap,
    resolveTemplateFields,
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
