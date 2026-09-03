import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { KNACK_VIEW_SOURCE_SHAPE, buildViewSource } from './server.js';

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
