import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import {
    checkSourceRelationship,
    editRuleKeyRefusal,
    emptyLabelRefusal,
    linkColumnRefusal,
    nestedRuleRefusal,
    normaliseLinkColumns,
    recordRuleValueRefusal,
    sceneKeysToSlugs,
    sourceWarnings,
    submitRuleRefusal,
    TABLE_VIEW_DEFAULTS,
    withLinkColumnDefaults,
    withMenuLinkDefaults,
    withViewDefaults,
} from './view-payload-checks.js';
import {
    recordRuleReferenceRefusal,
    type RuleReferenceContext,
} from './rule-edits.js';
import type { CachedObject } from '../types.js';

describe('checkSourceRelationship', () => {
    const objects: CachedObject[] = [
        {
            key: 'object_6',
            name: 'Guest Act Bookings',
            fields: [
                {
                    key: 'field_750',
                    name: 'Event Programme Conx',
                    type: 'connection',
                    connectedObject: 'object_53',
                },
                { key: 'field_751', type: 'short_text' },
                {
                    key: 'field_752',
                    type: 'connection',
                    connectedObject: 'object_6',
                },
                { key: 'field_753', type: 'connection' },
                {
                    key: 'field_754',
                    type: 'connection',
                    connectedObject: 'object_999',
                },
            ],
        },
        { key: 'object_53', name: 'Event Programmes', fields: [] },
        { key: 'object_99', name: 'Unrelated', fields: [] },
    ];

    for (const [object, supplied, expected] of [
        ['object_6', 'foreign', 'foreign'],
        ['object_6', 'local', 'foreign'],
        ['object_53', 'local', 'local'],
        ['object_53', 'foreign', 'local'],
    ]) {
        it(`${object} with "${supplied}" expects "${expected}"`, () => {
            const result = checkSourceRelationship(
                {
                    object,
                    connection_key: 'field_750',
                    relationship_type: supplied,
                },
                objects,
            );
            assert.equal(result.expectedRelationshipType, expected);
            assert.deepEqual(result.warnings, []);
            if (supplied === expected) assert.equal(result.refusal, null);
            else {
                assert.equal(
                    result.refusal?.error,
                    'RELATIONSHIP_TYPE_MISMATCH',
                );
                assert.match(
                    result.refusal?.message ?? '',
                    /field_750.*Event Programme Conx.*object_6.*Guest Act Bookings/,
                );
                assert.match(
                    result.refusal?.message ?? '',
                    new RegExp(
                        `source.object is ${object}.*must be "${expected}".*Nothing was sent\\.`,
                    ),
                );
            }
        });
    }

    for (const [connection_key, message] of [
        ['field_999', /not found/],
        ['field_751', /not a connection field/],
        ['field_753', /no connected object/],
        ['field_754', /object_999.*not found/],
    ] as const) {
        it(`reports invalid connection ${connection_key}`, () => {
            const result = checkSourceRelationship(
                {
                    object: 'object_6',
                    connection_key,
                    relationship_type: 'foreign',
                },
                objects,
            );
            assert.match(result.refusal?.message ?? '', message);
            assert.match(result.refusal?.message ?? '', /Nothing was sent\./);
        });
    }

    it('refuses an unrelated connection rather than treating it as local', () => {
        const checked = checkSourceRelationship(
            {
                object: 'object_99',
                connection_key: 'field_750',
                relationship_type: 'local',
            },
            objects,
        );
        assert.equal(checked.refusal?.error, 'INVALID_SOURCE_CONNECTION');
        assert.match(
            checked.refusal?.message ?? '',
            /object_6.*object_53.*not source.object object_99/,
        );
    });

    it('reports an unknown source object', () => {
        const checked = checkSourceRelationship(
            {
                object: 'object_999',
                connection_key: 'field_750',
                relationship_type: 'local',
            },
            objects,
        );
        assert.equal(checked.refusal?.error, 'INVALID_SOURCE_CONNECTION');
        assert.match(
            checked.refusal?.message ?? '',
            /source.object "object_999".*not found/,
        );
    });

    it('refuses invalid direction strings', () => {
        const checked = checkSourceRelationship(
            {
                object: 'object_6',
                connection_key: 'field_750',
                relationship_type: 'backwards',
            },
            objects,
        );
        assert.equal(checked.refusal?.error, 'INVALID_RELATIONSHIP_TYPE');
    });

    it('fails explicitly when ownership metadata is unavailable', () => {
        const checked = checkSourceRelationship(
            {
                object: 'object_6',
                connection_key: 'field_750',
                relationship_type: 'foreign',
            },
            null,
        );
        assert.equal(checked.refusal?.error, 'COULD_NOT_VERIFY_SOURCE');
    });

    for (const relationship_type of ['foreign', 'local']) {
        it(`warns without refusing an unverified self-connection with ${relationship_type}`, () => {
            const checked = checkSourceRelationship(
                {
                    object: 'object_6',
                    connection_key: 'field_752',
                    relationship_type,
                },
                objects,
            );
            assert.equal(checked.refusal, null);
            assert.equal(checked.expectedRelationshipType, undefined);
            assert.match(
                checked.warnings[0],
                /self-connection.*not been verified/,
            );
        });
    }

    it('does not derive direction from parent_source or authenticated_user', () => {
        const checked = checkSourceRelationship(
            {
                object: 'object_6',
                connection_key: 'field_750',
                relationship_type: 'foreign',
                parent_source: { object: 'object_99', connection: 'field_999' },
                authenticated_user: true,
            },
            objects,
        );
        assert.equal(checked.refusal, null);
        assert.equal(checked.expectedRelationshipType, 'foreign');
    });

    it('leaves unconnected sources and existing missing-direction warnings alone', () => {
        for (const source of [
            undefined,
            { object: 'object_6' },
            { object: 'object_6', connection_key: 'field_750' },
        ]) {
            assert.deepEqual(checkSourceRelationship(source, null), {
                refusal: null,
                warnings: [],
            });
        }
    });
});

describe('normaliseLinkColumns', () => {
    it('moves link_field into field.key and blanks link_field (the shape that gave HTTP 500)', () => {
        const result = normaliseLinkColumns([
            {
                type: 'link',
                header: 'No. Jobs',
                link_type: 'field',
                link_field: 'field_2269',
            },
        ]);
        assert.deepEqual(result.problems, []);
        assert.equal(result.corrections.length, 1);
        assert.deepEqual(result.columns[0], {
            type: 'link',
            header: 'No. Jobs',
            link_type: 'field',
            link_field: '',
            field: { key: 'field_2269' },
        });
    });

    it('leaves a Builder-shaped field link untouched', () => {
        const column = {
            type: 'link',
            link_type: 'field',
            field: { key: 'field_2269' },
            link_field: '',
        };
        const result = normaliseLinkColumns([column]);
        assert.deepEqual(result.corrections, []);
        assert.equal(result.columns[0], column);
    });

    it('refuses a field link with no field, two different fields, or a non-field key', () => {
        const result = normaliseLinkColumns([
            { type: 'link', link_type: 'field', header: 'A' },
            {
                type: 'link',
                link_type: 'field',
                field: { key: 'field_1' },
                link_field: 'field_2',
            },
            { type: 'link', link_type: 'field', link_field: 'Jobs' },
        ]);
        assert.equal(result.problems.length, 3);
        assert.match(result.problems[0], /no field/);
        assert.match(result.problems[1], /two different fields/);
        assert.match(result.problems[2], /not a field key/);
        assert.equal(
            linkColumnRefusal(result.problems)?.error,
            'INVALID_LINK_COLUMN',
        );
    });

    it('needs link_text on a text link unless it shows an icon', () => {
        const result = normaliseLinkColumns([
            { type: 'link', link_type: 'text', link_text: '' },
            {
                type: 'link',
                link_type: 'text',
                link_text: '',
                icon: { icon: 'fa-pencil', align: 'left' },
            },
            { type: 'link', link_type: 'text', link_text: 'Edit' },
        ]);
        assert.equal(result.problems.length, 1);
        assert.match(result.problems[0], /columns\[0\]/);
    });

    it('ignores columns that are not links', () => {
        const column = { type: 'field', field: { key: 'field_1' } };
        const result = normaliseLinkColumns([column]);
        assert.deepEqual(result.problems, []);
        assert.equal(result.columns[0], column);
    });
});

describe('sceneKeysToSlugs', () => {
    const SCENES = [
        {
            sceneKey: 'scene_1312',
            sceneSlug: 'view-guest-act-event-booking-details',
        },
        { sceneKey: 'scene_7', sceneSlug: 'home' },
        { sceneKey: 'scene_8', sceneSlug: undefined },
    ];

    it('changes a table link column key to the page slug (Spot view_2725)', () => {
        const result = sceneKeysToSlugs(
            {
                columns: [
                    { type: 'link', header: 'Details', scene: 'scene_1312' },
                    { type: 'field', field: { key: 'field_1' } },
                ],
            },
            SCENES,
        );
        assert.deepEqual(result.value.columns[0], {
            type: 'link',
            header: 'Details',
            scene: 'view-guest-act-event-booking-details',
        });
        assert.equal(result.corrections.length, 1);
        assert.match(result.corrections[0], /"Details".*scene_1312.*slug/);
    });

    it('reaches nested details columns and menu links', () => {
        const result = sceneKeysToSlugs(
            {
                groups: [
                    {
                        columns: [
                            {
                                columns: [
                                    { type: 'scene_link', scene: 'scene_1312' },
                                ],
                            },
                        ],
                    },
                ],
                links: [{ name: 'Home', type: 'scene', scene: 'scene_7' }],
            },
            SCENES,
        );
        assert.equal(
            result.value.groups[0].columns[0].columns[0].scene,
            'view-guest-act-event-booking-details',
        );
        assert.equal(result.value.links[0].scene, 'home');
        assert.equal(result.corrections.length, 2);
    });

    it('leaves submit rules, slugs, unknown keys and page specifications alone', () => {
        const payload = {
            rules: { submits: [{ action: 'existing_page', scene: 'scene_7' }] },
            columns: [
                { type: 'link', scene: 'home' },
                { type: 'link', scene: 'scene_999' },
                { type: 'link', scene: 'scene_8' },
                {
                    type: 'link',
                    scene: { name: 'New', parent: 'home', views: [] },
                },
            ],
        };
        const result = sceneKeysToSlugs(payload, SCENES);
        assert.equal(result.value, payload);
        assert.deepEqual(result.corrections, []);
    });
});

describe('withLinkColumnDefaults', () => {
    it('fills the Builder keys and lets the caller win', () => {
        const column = withLinkColumnDefaults({
            type: 'link',
            link_text: 'History',
            scene: 'history',
            width: { type: 'default', units: 'px', amount: '80' },
        });
        assert.equal(column.link_type, 'text');
        assert.equal(column.link_field, '');
        assert.deepEqual(column.rules, []);
        assert.deepEqual(column.width, {
            type: 'default',
            units: 'px',
            amount: '80',
        });
        assert.equal(column.link_text, 'History');
    });
});

describe('submitRuleRefusal', () => {
    it('refuses actions the Builder does not have, and an action with no target', () => {
        const refusal = submitRuleRefusal(
            [
                { action: 'scene', scene: 'x' },
                { action: 'redirect', scene: 'x' },
                { message: 'no action' },
                { action: 'child_page' },
                { action: 'existing_page' },
                { action: 'url', url: ' ' },
            ],
            'submitRules',
        );
        assert.equal(refusal?.error, 'INVALID_SUBMIT_ACTION');
        for (const index of [0, 1, 2, 3, 4, 5])
            assert.match(
                refusal!.message,
                new RegExp(`submitRules\\[${index}\\]`),
            );
        assert.match(refusal!.message, /"action": "scene"/);
        assert.match(refusal!.message, /no "action"/);
        assert.match(refusal!.message, /"child_page" rule with no "scene"/);
        assert.match(
            refusal!.message,
            /"existing_page" rule with no "existing_page"/,
        );
    });

    it('accepts each action as the Builder stores it', () => {
        assert.equal(
            submitRuleRefusal(
                [
                    { key: 'submit_1', action: 'message', message: 'Saved' },
                    {
                        key: 'submit_2',
                        action: 'url',
                        url: 'https://example.com',
                    },
                    {
                        key: 'submit_3',
                        action: 'existing_page',
                        existing_page: 'mcp-test-page-login',
                    },
                    { key: 'submit_4', action: 'parent_page' },
                    {
                        key: 'submit_5',
                        scene: 'new-child-page',
                        action: 'child_page',
                        message: '',
                        is_default: true,
                        reload_show: true,
                    },
                ],
                'x',
            ),
            null,
        );
    });
});

describe('recordRuleValueRefusal', () => {
    it('refuses blank record and connection values', () => {
        const refusal = recordRuleValueRefusal(
            [
                {
                    action: 'record',
                    values: [
                        { field: 'field_1', type: 'record', input: '' },
                        { field: 'field_2', type: 'connection' },
                        {
                            field: 'field_3',
                            type: 'connection',
                            connection_field: 'field_2688-field_88',
                        },
                        { field: 'field_4', type: 'record', input: 'field_9' },
                        { field: 'field_5', type: 'value', value: '' },
                    ],
                },
            ],
            'recordRules',
        );
        assert.equal(refusal?.error, 'INVALID_RULE_VALUE');
        assert.match(
            refusal!.message,
            /values\[0\] \(field_1\) is type "record" with no "input"/,
        );
        assert.match(
            refusal!.message,
            /values\[1\] \(field_2\) is type "connection" with no "connection_field"/,
        );
        assert.doesNotMatch(refusal!.message, /values\[[234]\]/);
    });
});

describe('sourceWarnings', () => {
    it('warns about parent_source with no connection_key', () => {
        const warnings = sourceWarnings({
            object: 'object_113',
            parent_source: { object: 'object_12', connection: 'field_2688' },
        });
        assert.equal(warnings.length, 1);
        assert.match(warnings[0], /parent_source/);
    });

    it('warns about a connection_key with no relationship_type', () => {
        assert.equal(
            sourceWarnings({ object: 'o', connection_key: 'field_1' }).length,
            1,
        );
    });

    it('is quiet for the Builder shape and for sources with neither key', () => {
        assert.deepEqual(
            sourceWarnings({
                object: 'object_113',
                connection_key: 'field_2688',
                relationship_type: 'foreign',
                parent_source: null,
            }),
            [],
        );
        assert.deepEqual(sourceWarnings({ object: 'object_1' }), []);
        assert.deepEqual(sourceWarnings(undefined), []);
    });
});

describe('withViewDefaults for other view types', () => {
    it('fills the list switches, and the details and form keys', () => {
        const list = withViewDefaults({ type: 'list' });
        assert.equal(list.payload.allow_exporting, false);
        assert.equal(list.payload.keyword_search, false);
        assert.equal(list.payload.list_layout, 'one-column');
        const details = withViewDefaults({ type: 'details' });
        assert.equal(details.payload.label_format, 'left');
        assert.equal(details.payload.hide_fields, false);
        const form = withViewDefaults({ type: 'form', alert: 'x' });
        assert.equal(form.payload.alert, 'x');
        assert.equal(form.payload.submit_button_text, 'Submit');
    });

    it('leaves a search view alone', () => {
        const search = { type: 'search' };
        assert.equal(withViewDefaults(search).payload, search);
    });
});

describe('withViewDefaults', () => {
    it("adds the Builder's off switches to a table that lacks them", () => {
        const result = withViewDefaults({ type: 'table', name: 'T' });
        assert.deepEqual(
            result.added.sort(),
            Object.keys(TABLE_VIEW_DEFAULTS).sort(),
        );
        assert.equal(result.payload.keyword_search, false);
        assert.equal(result.payload.allow_exporting, false);
        assert.equal(result.payload.allow_preset_filters, false);
    });

    it('keeps what the caller set, and leaves other view types alone', () => {
        const table = withViewDefaults({
            type: 'table',
            keyword_search: true,
            rows_per_page: '50',
        });
        assert.equal(table.payload.keyword_search, true);
        assert.equal(table.payload.rows_per_page, '50');
        assert.ok(!table.added.includes('keyword_search'));
        const search = { type: 'search', name: 'S' };
        assert.equal(withViewDefaults(search).payload, search);
        assert.deepEqual(withViewDefaults(search).added, []);
    });

    it('gives a menu with no label the Builder\'s "Menu", and other types none', () => {
        const menu = withViewDefaults({ type: 'menu', name: 'Actions' });
        assert.equal(menu.payload.label, 'Menu');
        assert.deepEqual(menu.added, ['label']);
        assert.equal(
            withViewDefaults({ type: 'menu', label: 'Nav' }).payload.label,
            'Nav',
        );
        for (const type of ['table', 'list', 'details', 'form'])
            assert.equal('label' in withViewDefaults({ type }).payload, false);
    });
});

describe('emptyLabelRefusal', () => {
    it('refuses a blank or non-string label', () => {
        for (const label of ['', '   ', null, 3]) {
            const refusal = emptyLabelRefusal({ type: 'menu', label });
            assert.equal(refusal?.error, 'EMPTY_LABEL');
            assert.match(refusal.message, /Nothing was sent/);
        }
    });

    it('passes a label with text, and a payload with none', () => {
        assert.equal(emptyLabelRefusal({ type: 'menu', label: 'Menu' }), null);
        assert.equal(emptyLabelRefusal({ type: 'table' }), null);
    });
});

describe('withMenuLinkDefaults', () => {
    it('adds link_design_active: false where absent and keeps what was sent', () => {
        const result = withMenuLinkDefaults([
            { name: 'Add Booking', type: 'scene', scene: 'add-booking' },
            { name: 'Styled', type: 'scene', link_design_active: true },
            'not a link',
        ]);
        assert.equal(result.changed, true);
        assert.deepEqual(result.links[0], {
            link_design_active: false,
            name: 'Add Booking',
            type: 'scene',
            scene: 'add-booking',
        });
        assert.equal(
            (result.links[1] as { link_design_active: boolean })
                .link_design_active,
            true,
        );
        assert.equal(result.links[2], 'not a link');
    });

    it('returns the same array when nothing is missing', () => {
        const links = [{ name: 'A', type: 'url', link_design_active: false }];
        const result = withMenuLinkDefaults(links);
        assert.equal(result.changed, false);
        assert.equal(result.links, links);
    });
});

describe('recordRuleReferenceRefusal', () => {
    const context: RuleReferenceContext = {
        sourceFields: [
            { key: 'field_2615', type: 'short_text' },
            {
                key: 'field_2624',
                type: 'connection',
                connectedObject: 'object_109',
            },
            { key: 'field_2625', type: 'short_text' },
        ],
        fieldsOf: (objectKey) =>
            objectKey === 'object_109'
                ? [
                      { key: 'field_2609', type: 'short_text' },
                      { key: 'field_2622', type: 'short_text' },
                  ]
                : undefined,
        formInputKeys: new Set(['field_2615', 'field_2627']),
    };
    const rule = (values: Record<string, unknown>[], action = 'record') => [
        { action, criteria: [], values },
    ];

    it('refuses an input that is not on the form', () => {
        const refusal = recordRuleReferenceRefusal(
            rule([
                { field: 'field_2625', type: 'record', input: 'field_2626' },
            ]),
            'recordRules',
            context,
        );
        assert.equal(refusal?.error, 'INVALID_RULE_VALUE');
        assert.match(
            refusal!.message,
            /field_2626", which is not an input on this form/,
        );
    });

    it('refuses a connection_field whose first part is not a connection, or whose second is on the wrong object', () => {
        const refusal = recordRuleReferenceRefusal(
            rule([
                {
                    field: 'a',
                    type: 'connection',
                    connection_field: 'field_2615-field_2622',
                },
                {
                    field: 'b',
                    type: 'connection',
                    connection_field: 'field_2624-field_2615',
                },
                {
                    field: 'c',
                    type: 'connection',
                    connection_field: 'nonsense',
                },
            ]),
            'recordRules',
            context,
        );
        assert.match(
            refusal!.message,
            /values\[0\] \(a\).*field_2615 is not a connection field/,
        );
        assert.match(
            refusal!.message,
            /values\[1\] \(b\).*field_2615 is not a field on object_109/,
        );
        assert.match(
            refusal!.message,
            /values\[2\] \(c\).*expected "<connection field>-/,
        );
    });

    it('accepts the verified shapes', () => {
        assert.equal(
            recordRuleReferenceRefusal(
                rule([
                    {
                        field: 'field_2625',
                        type: 'connection',
                        connection_field: 'field_2624-field_2622',
                    },
                    {
                        field: 'field_2625',
                        type: 'record',
                        input: 'field_2627',
                    },
                    { field: 'field_2625', type: 'value', value: 'x' },
                ]),
                'recordRules',
                context,
            ),
            null,
        );
    });

    it('does not check connection_field on other actions, or inputs when the view has none', () => {
        assert.equal(
            recordRuleReferenceRefusal(
                rule(
                    [
                        {
                            field: 'a',
                            type: 'connection',
                            connection_field: 'whatever-x',
                        },
                    ],
                    'connection',
                ),
                'recordRules',
                context,
            ),
            null,
        );
        assert.equal(
            recordRuleReferenceRefusal(
                rule([{ field: 'a', type: 'record', input: 'field_1' }]),
                'recordRules',
                { ...context, formInputKeys: null },
            ),
            null,
        );
    });
});

describe('child_page page specifications', () => {
    it('accepts a scene that describes a page to create, and refuses one with no name', () => {
        assert.equal(
            submitRuleRefusal(
                [
                    {
                        action: 'child_page',
                        scene: {
                            name: 'Update Resident',
                            parent: 'change',
                            views: [],
                        },
                    },
                ],
                'x',
            ),
            null,
        );
        const refusal = submitRuleRefusal(
            [{ action: 'child_page', scene: { parent: 'change', views: [] } }],
            'x',
        );
        assert.match(refusal!.message, /"child_page" rule with no "scene"/);
    });
});

describe('nestedRuleRefusal', () => {
    const actionLink = (submit: unknown, record: unknown = []) => [
        {
            type: 'action_link',
            action_rules: [
                { link_text: 'Go', submit_rules: submit, record_rules: record },
            ],
        },
    ];

    it('finds a bad submit action inside an action link, with its path', () => {
        const refusal = nestedRuleRefusal(
            actionLink([{ action: 'scene', scene: 'x' }]),
            'columns',
        );
        assert.equal(refusal?.error, 'INVALID_SUBMIT_ACTION');
        assert.match(
            refusal!.message,
            /columns\[0\]\.action_rules\[0\]\.submit_rules\[0\]/,
        );
    });

    it('finds a blank record value inside an action link, and works in nested groups', () => {
        const refusal = nestedRuleRefusal(
            [
                {
                    groups: [
                        {
                            columns: [
                                [
                                    actionLink(
                                        [],
                                        [
                                            {
                                                action: 'record',
                                                values: [
                                                    {
                                                        field: 'f',
                                                        type: 'record',
                                                        input: '',
                                                    },
                                                ],
                                            },
                                        ],
                                    )[0],
                                ],
                            ],
                        },
                    ],
                },
            ],
            'columns',
        );
        assert.equal(refusal?.error, 'INVALID_RULE_VALUE');
        assert.match(refusal!.message, /record_rules\[0\]\.values\[0\]/);
    });

    it('accepts valid action-link rules and values with no rules at all', () => {
        assert.equal(
            nestedRuleRefusal(
                actionLink(
                    [{ action: 'message', message: 'Approved' }],
                    [
                        {
                            action: 'record',
                            values: [{ field: 'f', type: 'value', value: 'x' }],
                        },
                    ],
                ),
                'columns',
            ),
            null,
        );
        assert.equal(nestedRuleRefusal([{ type: 'field' }], 'columns'), null);
    });
});

describe('edit_rules', () => {
    const editRule = (key: string, values: unknown[] = []) => ({
        key,
        action: 'record',
        values,
        criteria: [],
    });

    it('refuses an edit rule key that repeats across columns, and names the next free one', () => {
        const refusal = editRuleKeyRefusal(
            [
                { field: { key: 'field_1' }, edit_rules: [editRule('1')] },
                { field: { key: 'field_2' }, edit_rules: [editRule('1')] },
                { field: { key: 'field_3' }, edit_rules: [editRule('4')] },
            ],
            'results.columns',
        );
        assert.equal(refusal?.error, 'DUPLICATE_EDIT_RULE_KEY');
        assert.match(
            refusal!.message,
            /results\.columns\[1\]\.edit_rules\[0\]/,
        );
        assert.match(refusal!.message, /next free number is 5/);
    });

    it('accepts unique keys, and display rule keys that overlap them', () => {
        assert.equal(
            editRuleKeyRefusal(
                [
                    {
                        rules: [{ key: '1' }],
                        edit_rules: [editRule('1'), editRule('2')],
                    },
                    { rules: [{ key: '1' }], edit_rules: [editRule('3')] },
                ],
                'columns',
            ),
            null,
        );
    });

    it('checks the values of edit rules like record rules', () => {
        const refusal = nestedRuleRefusal(
            [
                {
                    edit_rules: [
                        editRule('1', [
                            { type: 'connection', field: 'field_9' },
                        ]),
                    ],
                },
            ],
            'results.columns',
        );
        assert.equal(refusal?.error, 'INVALID_RULE_VALUE');
        assert.match(
            refusal!.message,
            /results\.columns\[0\]\.edit_rules\[0\]/,
        );
    });
});
