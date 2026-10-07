/**
 * Checks on a view payload that Knack accepts but then stores in a shape the Builder
 * never writes. Each one was found in GAP-Track on 6 October, where the tool answered
 * `ok: true, status: 200` and the saved config did not work.
 *
 * - A table link column of `link_type: "field"` keeps its field in `field.key` and leaves
 *   `link_field` as `""`. Sent the other way round (`link_field` set, no `field`), the
 *   view saved and then answered HTTP 500 on `/scenes/{scene}/views/{view}/records`,
 *   on the front end and in the Builder alike (`view_127`).
 * - A submit rule with `"action": "scene"` saved, but the Builder showed an empty Submit
 *   Action. "Show another page" is `redirect`, and "Open a child page" is `child_page`.
 * - A record rule value of `type: "record"` or `"connection"` with no `input` or
 *   `connection_field` saved blank, so a snapshot rule copied nothing.
 * - A source with `parent_source` alone did not connect the form or table to the page's
 *   record. The Builder writes `connection_key` and `relationship_type` with it.
 *
 * Pure: no I/O.
 */
import { asRecord } from './util.js';

const FIELD_KEY_PATTERN = /^field_\d+$/;

/**
 * The switches a Builder-made table writes out in the "off" position. Knack does not add
 * them on create, only the Builder does when it saves a view, so a table created through
 * the API lacks them until someone opens it in the Builder. Without
 * `keyword_search` and `allow_exporting` the front end shows the keyword search and the
 * export button while the Builder shows both unchecked, until someone ticks and unticks
 * each box (found 6 October on NPS Test App `view_1844`; `view_1849`, never opened in the
 * Builder, was missing all of these keys). Surveyed across 167 tables: `allow_exporting`
 * false on 162, `allow_preset_filters` false on 163, `keyword_search` false on 115 and
 * true on 49, `keyword_search_fields` "view" on 165, `totals` empty on 161.
 */
export const TABLE_VIEW_DEFAULTS: Record<string, unknown> = {
    description: '',
    filter_type: 'none',
    menu_filters: [],
    filter_fields: 'view',
    rows_per_page: '25',
    keyword_search: false,
    allow_exporting: false,
    allow_preset_filters: false,
    keyword_search_fields: 'view',
    preset_filters: [],
    totals: [],
    table_design_active: false,
};

/**
 * The same for a list. The Builder writes the search, export and filter switches off on
 * every list (31 of 31 surveyed carry `allow_exporting: false`), so a list saved without
 * them shows the keyword search and export button on the front end. The layout keys are
 * written on every list too.
 */
export const LIST_VIEW_DEFAULTS: Record<string, unknown> = {
    description: '',
    list_layout: 'one-column',
    label_format: 'left',
    rows_per_page: '25',
    allow_exporting: false,
    keyword_search: false,
    keyword_search_fields: 'view',
    menu_filters: [],
    filter_fields: 'view',
    preset_filters: [],
    allow_preset_filters: false,
};

/** A details view has no search or export; the Builder still writes these on all 184 surveyed. */
export const DETAILS_VIEW_DEFAULTS: Record<string, unknown> = {
    description: '',
    hide_fields: false,
    label_format: 'left',
    reportType: null,
};

/** A form has no search or export either; the Builder writes these on nearly all 195 surveyed. */
export const FORM_VIEW_DEFAULTS: Record<string, unknown> = {
    description: '',
    alert: 'none',
    submit_button_text: 'Submit',
    reportType: null,
};

const VIEW_DEFAULTS_BY_TYPE: Record<string, Record<string, unknown>> = {
    table: TABLE_VIEW_DEFAULTS,
    list: LIST_VIEW_DEFAULTS,
    details: DETAILS_VIEW_DEFAULTS,
    form: FORM_VIEW_DEFAULTS,
};

/**
 * A view payload with the keys the Builder always writes filled in where absent. Anything
 * the caller set stays; a view type with no defaults here (search templates already carry
 * theirs) is returned untouched.
 *
 * @param payload A whole view definition.
 * @returns The definition, and the keys that were added.
 */
export function withViewDefaults(payload: Record<string, unknown>): {
    payload: Record<string, unknown>;
    added: string[];
} {
    const defaults =
        typeof payload.type === 'string'
            ? VIEW_DEFAULTS_BY_TYPE[payload.type]
            : undefined;
    if (!defaults) return { payload, added: [] };
    const added = Object.keys(defaults).filter(
        (key) => !Object.hasOwn(payload, key),
    );
    if (added.length === 0) return { payload, added };
    return {
        payload: {
            ...payload,
            ...Object.fromEntries(added.map((key) => [key, defaults[key]])),
        },
        added,
    };
}

/** Every key a Builder-made table link column carries, with the values it starts at. */
const LINK_COLUMN_DEFAULTS: Record<string, unknown> = {
    rules: [],
    width: { type: 'default', units: 'px', amount: '50' },
    grouping: false,
    conn_link: '',
    link_type: 'text',
    group_sort: 'asc',
    link_field: '',
    ignore_edit: false,
    img_gallery: '',
    conn_separator: '',
    ignore_summary: false,
    link_design_active: false,
    icon: { icon: '', align: 'left' },
};

/**
 * A new table link column with the keys a Builder-made one has. What the caller sent
 * always wins. `add_page_link_column` used to send only type, header, link_text, icon,
 * align, remote and scene.
 */
export function withLinkColumnDefaults(
    column: Record<string, unknown>,
): Record<string, unknown> {
    return { ...LINK_COLUMN_DEFAULTS, ...column };
}

export type LinkColumnCheck = {
    columns: unknown[];
    /** What was rewritten into the Builder's shape, for the reply. */
    corrections: string[];
    /** Why the payload cannot be stored, empty when it can. */
    problems: string[];
};

/**
 * Put every table link column into the shape the Builder stores.
 *
 * - `link_type: "field"`: the field is `field.key`, or `link_field` from a caller who
 *   used the wrong shape. It is written as `field: { key }` with `link_field: ""`. No
 *   field at all, two different ones, or something that is not a field key is a problem.
 * - `link_type: "text"`: needs a `link_text`, unless the column shows an icon, which
 *   a Builder link can do with no text.
 *
 * Whether the field exists on the view's object is not checked here: a field link can
 * perhaps reach one through a connection, which has not been tested.
 *
 * @param columns The `columns` array about to be sent.
 */
export function normaliseLinkColumns(columns: unknown[]): LinkColumnCheck {
    const corrections: string[] = [];
    const problems: string[] = [];

    const normalised = columns.map((entry, index) => {
        const column = asRecord(entry);
        if (!column || column.type !== 'link') return entry;
        const name = `columns[${index}]${typeof column.header === 'string' && column.header ? ` ("${column.header}")` : ''}`;

        if (column.link_type === 'field') {
            const fromField = asRecord(column.field)?.key;
            const fromLinkField = column.link_field;
            const given = [fromField, fromLinkField].filter(
                (key): key is string => typeof key === 'string' && key !== '',
            );
            if (given.length === 0) {
                problems.push(
                    `${name} is a field link with no field. Set "field": {"key": "field_N"}`,
                );
                return entry;
            }
            if (new Set(given).size > 1) {
                problems.push(
                    `${name} names two different fields (field.key ${String(fromField)}, link_field ${String(fromLinkField)})`,
                );
                return entry;
            }
            if (!FIELD_KEY_PATTERN.test(given[0])) {
                problems.push(
                    `${name} links to "${given[0]}", which is not a field key`,
                );
                return entry;
            }
            if (fromField === given[0] && fromLinkField === '') return entry;
            corrections.push(
                `${name}: field link moved to "field": {"key": "${given[0]}"} and "link_field" set to ""`,
            );
            return {
                ...column,
                field: { ...asRecord(column.field), key: given[0] },
                link_field: '',
            };
        }

        if (column.link_type === 'text') {
            const hasText =
                typeof column.link_text === 'string' &&
                column.link_text.trim() !== '';
            const hasIcon =
                typeof asRecord(column.icon)?.icon === 'string' &&
                asRecord(column.icon)?.icon !== '';
            if (!hasText && !hasIcon)
                problems.push(
                    `${name} is a text link with an empty "link_text", so it shows nothing to click`,
                );
        }
        return entry;
    });

    return { columns: normalised, corrections, problems };
}

/** A column-level refusal in the shape the tools return. */
export function linkColumnRefusal(problems: string[]): {
    error: 'INVALID_LINK_COLUMN';
    message: string;
} | null {
    if (problems.length === 0) return null;
    return {
        error: 'INVALID_LINK_COLUMN',
        message: `${problems.join('; ')}. Knack would store ${problems.length === 1 ? 'it' : 'them'} but the Builder would not have written that shape. A field link keeps its field in "field": {"key": "field_N"} with "link_field": "" (a view that has it the other way round answers HTTP 500 on load), and a text link needs "link_text". Nothing was sent.`,
    };
}

/** The `action` values a form's Submit Action dropdown offers, as the Builder stores them. */
const SUBMIT_ACTIONS = [
    'message',
    'url',
    'existing_page',
    'parent_page',
    'child_page',
] as const;

/** The key each action reads its target from, where it needs one. */
const SUBMIT_ACTION_TARGET: Record<string, string> = {
    url: 'url',
    existing_page: 'existing_page',
    child_page: 'scene',
};

/**
 * Why a set of submit rules cannot be stored, or null. Knack stores any `action`, and a
 * value the Builder does not know leaves the Submit Action dropdown empty (`"scene"` and
 * `"redirect"` both did, GAP-Track `view_3349`). The dropdown has five options, read back
 * from NPS Test App `view_1848` and four copies on 6 October, and across all 692 views
 * of that app no view-level submit rule used anything else:
 * - `message`: shows `message`.
 * - `url`: opens `url`.
 * - `existing_page`: opens the page whose slug is in `existing_page`.
 * - `parent_page`: returns to the parent page.
 * - `child_page`: opens the child page whose slug is in `scene`, or creates one from a
 *   `scene` page specification (`{ name, parent, views }`). The child page's record is
 *   the one this form created or updated.
 * An action that opens something also needs the target that goes with it.
 *
 * @param rules Whole submit rules about to be stored.
 * @param label Where they came from, for the message.
 */
export function submitRuleRefusal(
    rules: Record<string, unknown>[],
    label: string,
): { error: 'INVALID_SUBMIT_ACTION'; message: string } | null {
    const problems = rules.flatMap((rule, index) => {
        const name = `${label}[${index}]`;
        const action = rule.action;
        if (
            typeof action !== 'string' ||
            !(SUBMIT_ACTIONS as readonly string[]).includes(action)
        )
            return [
                `${name} has ${typeof action === 'string' ? `"action": "${action}"` : 'no "action"'}, which is not a Builder submit action`,
            ];
        const target = SUBMIT_ACTION_TARGET[action];
        if (!target) return [];
        const value = rule[target];
        const isText = typeof value === 'string' && value.trim() !== '';
        // A child page may name an existing page or describe one to create:
        // `{ name, parent, views }`, which the view guard validates in detail.
        const isPageSpecification =
            action === 'child_page' &&
            typeof asRecord(value)?.name === 'string' &&
            String(asRecord(value)?.name).trim() !== '';
        if (!isText && !isPageSpecification)
            return [`${name} is a "${action}" rule with no "${target}"`];
        return [];
    });
    if (problems.length === 0) return null;
    return {
        error: 'INVALID_SUBMIT_ACTION',
        message: `${problems.join('; ')}. Knack would store ${problems.length === 1 ? 'it' : 'them'} but the Builder would show an empty Submit Action. The actions are "message" (with "message"), "url" (with "url"), "existing_page" (with "existing_page": "<slug>"), "parent_page" and "child_page" (with "scene": "<slug>"), e.g. {"key":"submit_1","scene":"update-resident-details","action":"child_page","message":"","is_default":true,"reload_show":true}. Nothing was sent.`,
    };
}

/**
 * Why a set of record rules has blank values, or null. A value copied from a form input
 * (`type: "record"`) needs its `input`, and one copied from a connected record
 * (`type: "connection"`) needs its `connection_field`
 * (`<connection on this object>-<field on the connected object>`). Knack stores either
 * with the part missing, and the rule then writes nothing.
 *
 * @param rules Whole record rules about to be stored.
 * @param label Where they came from, for the message.
 */
export function recordRuleValueRefusal(
    rules: Record<string, unknown>[],
    label: string,
): { error: 'INVALID_RULE_VALUE'; message: string } | null {
    const isBlank = (value: unknown) =>
        typeof value !== 'string' || value.trim() === '';
    const problems = rules.flatMap((rule, ruleIndex) => {
        const values = Array.isArray(rule.values) ? rule.values : [];
        return values.flatMap((entry, valueIndex) => {
            const value = asRecord(entry);
            const name = `${label}[${ruleIndex}].values[${valueIndex}]${typeof value?.field === 'string' ? ` (${value.field})` : ''}`;
            if (value?.type === 'record' && isBlank(value.input))
                return [`${name} is type "record" with no "input"`];
            if (value?.type === 'connection' && isBlank(value.connection_field))
                return [
                    `${name} is type "connection" with no "connection_field"`,
                ];
            return [];
        });
    });
    if (problems.length === 0) return null;
    return {
        error: 'INVALID_RULE_VALUE',
        message: `${problems.join('; ')}. Knack would store ${problems.length === 1 ? 'it' : 'them'} blank, so the rule would copy nothing. A value from a form input is {"type":"record","field":"<target>","input":"<form field>"}; one from a connected record is {"type":"connection","field":"<target>","connection_field":"<connectionOnThisObject>-<sourceFieldOnConnectedObject>"}. A field not on the form can only be set the second way or with a fixed value. Nothing was sent.`,
    };
}

/**
 * The same submit-action and record-value checks for rules that sit inside a payload
 * rather than at `rules.submits` / `rules.records`: an action link keeps its rules at
 * `columns[].groups[].columns[][].action_rules[].submit_rules` and `.record_rules`. Every
 * `submit_rules` and `record_rules` array found anywhere in the value is checked, and the
 * first problem is returned with the path it was found at.
 *
 * @param value Any JSON about to be sent: a columns array, a whole payload, or action links.
 * @param label Where the value sits, for the message (e.g. "columns").
 */
export function nestedRuleRefusal(
    value: unknown,
    label: string,
): {
    error: 'INVALID_SUBMIT_ACTION' | 'INVALID_RULE_VALUE';
    message: string;
} | null {
    let found: ReturnType<typeof nestedRuleRefusal> = null;
    const onlyRules = (list: unknown[]) =>
        list.filter(
            (rule): rule is Record<string, unknown> => asRecord(rule) !== null,
        );
    const walk = (node: unknown, path: string) => {
        if (found) return;
        if (Array.isArray(node)) {
            node.forEach((entry, index) => walk(entry, `${path}[${index}]`));
            return;
        }
        const record = asRecord(node);
        if (!record) return;
        for (const [key, child] of Object.entries(record)) {
            const childPath = `${path}.${key}`;
            if (key === 'submit_rules' && Array.isArray(child)) {
                found = submitRuleRefusal(onlyRules(child), childPath);
            } else if (key === 'record_rules' && Array.isArray(child)) {
                found = recordRuleValueRefusal(onlyRules(child), childPath);
            } else {
                walk(child, childPath);
            }
            if (found) return;
        }
    };
    walk(value, label);
    return found;
}

/**
 * Warnings about a view `source` that will probably not connect to the page's record.
 * `parent_source` on its own saved and submitted, but the new row had no connection
 * (GAP-Track `view_3349`); the Builder writes `connection_key` and `relationship_type`
 * beside it. `foreign` is for a connection field on the view's own object, `local` for
 * one on the other object.
 *
 * @param source The `source` block about to be sent.
 */
export function sourceWarnings(source: unknown): string[] {
    const record = asRecord(source);
    if (!record) return [];
    const warnings: string[] = [];
    const hasConnection =
        typeof record.connection_key === 'string' && record.connection_key;
    if (record.parent_source && !hasConnection)
        warnings.push(
            'source has "parent_source" but no "connection_key" and "relationship_type". On its own that did not connect rows to the page record in testing: the Builder writes the connection key and relationship type for "connected to this page\'s record", with "parent_source" left null. Use "foreign" when the connection field is on this view\'s object, "local" when it is on the other object.',
        );
    if (hasConnection && !record.relationship_type)
        warnings.push(
            'source has "connection_key" but no "relationship_type". Use "foreign" when the connection field is on this view\'s object, "local" when it is on the other object.',
        );
    return warnings;
}
