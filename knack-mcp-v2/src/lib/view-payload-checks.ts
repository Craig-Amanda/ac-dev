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
import type { SceneInfo } from '../types.js';
import { asRecord } from './util.js';
import type { CachedObject } from '../types.js';

const FIELD_KEY_PATTERN = /^field_\d+$/;

export type SourceRelationshipError =
    | 'COULD_NOT_VERIFY_SOURCE'
    | 'INVALID_SOURCE_CONNECTION'
    | 'INVALID_RELATIONSHIP_TYPE'
    | 'RELATIONSHIP_TYPE_MISMATCH';

/**
 * Check a connected source against the connection field's owner, not its cardinality.
 * Knack accepts the wrong direction and returns no rows (Spot view_2721, 20 October).
 *
 * @param source The complete source that would be sent.
 * @param objects Objects and fields from the app metadata, or null if unavailable.
 * @returns A refusal, any unverified self-connection warning and the expected direction.
 */
export function checkSourceRelationship(
    source: unknown,
    objects: readonly CachedObject[] | null | undefined,
): {
    refusal: { error: SourceRelationshipError; message: string } | null;
    warnings: string[];
    expectedRelationshipType?: 'foreign' | 'local';
} {
    const record = asRecord(source);
    const pass = { refusal: null, warnings: [] };
    if (
        !record ||
        record.connection_key === undefined ||
        record.relationship_type === undefined ||
        record.relationship_type === null ||
        record.relationship_type === ''
    )
        return pass;

    const refuse = (error: SourceRelationshipError, message: string) => ({
        refusal: { error, message: `${message} Nothing was sent.` },
        warnings: [],
    });
    if (
        record.relationship_type !== 'foreign' &&
        record.relationship_type !== 'local'
    )
        return refuse(
            'INVALID_RELATIONSHIP_TYPE',
            `source.relationship_type must be "foreign" or "local", not ${JSON.stringify(record.relationship_type)}.`,
        );
    if (!objects?.length)
        return refuse(
            'COULD_NOT_VERIFY_SOURCE',
            'App object metadata is unavailable, so the connection field ownership could not be verified.',
        );
    const sourceObject = objects.find((object) => object.key === record.object);
    if (!sourceObject)
        return refuse(
            'INVALID_SOURCE_CONNECTION',
            `source.object ${JSON.stringify(record.object)} was not found in this app's object metadata.`,
        );
    const owner = objects.find((object) =>
        object.fields?.some((field) => field.key === record.connection_key),
    );
    const field = owner?.fields?.find(
        (field) => field.key === record.connection_key,
    );
    if (!owner || !field)
        return refuse(
            'INVALID_SOURCE_CONNECTION',
            `source.connection_key ${JSON.stringify(record.connection_key)} was not found in this app's field metadata.`,
        );
    const fieldName = `${field.key}${field.name ? ` (${field.name})` : ''}`;
    const ownerName = `${owner.key}${owner.name ? ` (${owner.name})` : ''}`;
    if (field.type !== 'connection')
        return refuse(
            'INVALID_SOURCE_CONNECTION',
            `${fieldName} on ${ownerName} is not a connection field.`,
        );
    if (!field.connectedObject)
        return refuse(
            'COULD_NOT_VERIFY_SOURCE',
            `${fieldName} on ${ownerName} has no connected object in the metadata, so its relationship to source.object ${sourceObject.key} could not be verified.`,
        );
    if (!objects.some((object) => object.key === field.connectedObject))
        return refuse(
            'INVALID_SOURCE_CONNECTION',
            `${fieldName} on ${ownerName} points to ${field.connectedObject}, which was not found in this app's object metadata.`,
        );
    if (
        owner.key !== sourceObject.key &&
        field.connectedObject !== sourceObject.key
    )
        return refuse(
            'INVALID_SOURCE_CONNECTION',
            `${fieldName} belongs to ${ownerName} and connects to ${field.connectedObject}, not source.object ${sourceObject.key}.`,
        );
    if (
        owner.key === sourceObject.key &&
        field.connectedObject === sourceObject.key
    )
        return {
            refusal: null,
            warnings: [
                `${fieldName} on ${ownerName} is a self-connection. The Builder's relationship_type for this case has not been verified; "${record.relationship_type}" was left unchanged. Check the source in the Builder before relying on its rows.`,
            ],
        };

    const expectedRelationshipType =
        owner.key === sourceObject.key ? 'foreign' : 'local';
    if (record.relationship_type !== expectedRelationshipType)
        return {
            ...refuse(
                'RELATIONSHIP_TYPE_MISMATCH',
                `${fieldName} belongs to ${ownerName}; source.object is ${sourceObject.key}${sourceObject.name ? ` (${sourceObject.name})` : ''}. source.relationship_type is "${record.relationship_type}" but must be "${expectedRelationshipType}" because the connection field is on ${owner.key === sourceObject.key ? "the view's own object" : 'the other object and points back to it'}.`,
            ),
            expectedRelationshipType,
        };
    return { ...pass, expectedRelationshipType };
}

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

/**
 * A menu is the only view type the Builder gives a `label`, and it is always "Menu",
 * whatever the view is called: 202 of 202 menus across Spot, GAP-Track and NPS Test App
 * (29 October), and no table, form, details, list, rich_text, search, calendar or report
 * view carried one. The Builder refuses to save a view whose label is empty (Spot
 * `view_2751`, created with `"label": ""`).
 */
export const MENU_VIEW_DEFAULTS: Record<string, unknown> = {
    label: 'Menu',
};

const VIEW_DEFAULTS_BY_TYPE: Record<string, Record<string, unknown>> = {
    table: TABLE_VIEW_DEFAULTS,
    list: LIST_VIEW_DEFAULTS,
    details: DETAILS_VIEW_DEFAULTS,
    form: FORM_VIEW_DEFAULTS,
    menu: MENU_VIEW_DEFAULTS,
};

/**
 * Why a view payload or patch cannot be stored because of its `label`, or null. Knack
 * stores `"label": ""`, but the Builder then refuses to save the view with `"label" is
 * not allowed to be empty`, so nobody can edit it by hand (Spot `view_2751`, 29 October).
 * An absent label is fine: create fills a menu's in, and other types have none.
 *
 * @param value A whole view definition or a patch.
 */
export function emptyLabelRefusal(value: Record<string, unknown>): {
    error: 'EMPTY_LABEL';
    message: string;
} | null {
    if (!Object.hasOwn(value, 'label')) return null;
    if (typeof value.label === 'string' && value.label.trim() !== '')
        return null;
    return {
        error: 'EMPTY_LABEL',
        message: `"label" is ${JSON.stringify(value.label)}. Knack would store it, but the Builder then refuses to save the view ("label" is not allowed to be empty), so it cannot be edited by hand. Leave "label" out (a new menu gets the Builder's "Menu") or give it text. Nothing was sent.`,
    };
}

/** The keys a Builder-made menu link carries that a hand-written one usually lacks. */
const MENU_LINK_DEFAULTS: Record<string, unknown> = {
    link_design_active: false,
};

/**
 * Menu links with `link_design_active: false` added where absent, as the Builder writes
 * it: 323 of 376 menu links surveyed carry the key (29 October), false on 312 of them,
 * and the rest look older than it. What the caller sent always wins; a non-object entry is left for the guard.
 *
 * @param links A menu's `links` array.
 * @returns The links, and whether any were changed.
 */
export function withMenuLinkDefaults(links: unknown[]): {
    links: unknown[];
    changed: boolean;
} {
    let changed = false;
    const filled = links.map((entry) => {
        const link = asRecord(entry);
        if (!link) return entry;
        const missing = Object.keys(MENU_LINK_DEFAULTS).filter(
            (key) => !Object.hasOwn(link, key),
        );
        if (missing.length === 0) return entry;
        changed = true;
        return { ...MENU_LINK_DEFAULTS, ...link };
    });
    return { links: changed ? filled : links, changed };
}

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

/**
 * Page links given a page key, rewritten to that page's slug.
 *
 * Knack stores a link's `scene` verbatim, but the Builder resolves it by slug only: a
 * link saved as `"scene": "scene_1312"` opens the page on the front end and shows as
 * unlinked in the Builder (Spot `view_2725`, 22 October). The Builder itself always
 * writes the slug, so writing it here changes nothing but what the Builder can read.
 *
 * Only navigation positions are rewritten — an element of a `columns` or `links` array,
 * the same rule `collectNavigationRefs` uses — so a submit or action rule's `scene` is
 * left alone. A value naming no page, or a page with no slug, is left for the dangling
 * link check to report.
 *
 * @param value A view definition or patch.
 * @param scenes The app's pages.
 * @returns The value with keys replaced, and one line per replacement for the reply.
 */
export function sceneKeysToSlugs<T>(
    value: T,
    scenes: Pick<SceneInfo, 'sceneKey' | 'sceneSlug'>[],
): { value: T; corrections: string[] } {
    const slugByKey = new Map(
        scenes.map((scene) => [scene.sceneKey, scene.sceneSlug]),
    );
    const corrections: string[] = [];

    // `container` is the name of the nearest array the node sits in, so a nested
    // details layout (`columns[].groups[].columns[][]`) still counts as columns.
    const visit = (node: unknown, container: string | null): unknown => {
        if (Array.isArray(node)) {
            return node.map((item) => visit(item, container));
        }
        const record = asRecord(node);
        if (!record) return node;

        // fromEntries, not assignment, so a `__proto__` key stays a plain property.
        const rewritten = Object.fromEntries(
            Object.entries(record).map(([key, child]) => [
                key,
                visit(child, Array.isArray(child) ? key : null),
            ]),
        );

        const slug =
            (container === 'columns' || container === 'links') &&
            typeof record.scene === 'string'
                ? slugByKey.get(record.scene.trim())
                : undefined;
        if (!slug) return rewritten;

        const label = record.header ?? record.name ?? record.link_text;
        corrections.push(
            `${typeof label === 'string' && label ? `"${label}"` : 'A page link'}: "scene" ${record.scene} changed to its slug "${slug}" — the Builder finds a linked page by slug only`,
        );
        return { ...rewritten, scene: slug };
    };

    const result = visit(value, null) as T;
    return corrections.length
        ? { value: result, corrections }
        : { value, corrections };
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
 * `columns[].groups[].columns[][].action_rules[].submit_rules` and `.record_rules`, and a
 * table column keeps its inline-edit rules at `columns[].edit_rules`. Every
 * `submit_rules`, `record_rules` and `edit_rules` array found anywhere in the value is checked, and the
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
            } else if (
                (key === 'record_rules' || key === 'edit_rules') &&
                Array.isArray(child)
            ) {
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
 * Why a view's column inline-edit rules cannot be stored, or null. `edit_rules[].key` is
 * unique across the whole view, not per column. Rules that repeat a key saved (HTTP 200)
 * and ran on the live app, but the Builder did not show them on those columns
 * (Spot `view_1054`, 16 October: four new rules all keyed "1"; renumbering to 5-8 fixed
 * it). Display rules (`column.rules[].key`) do not share the namespace.
 *
 * @param columns A `columns` array, or a search view's `results.columns`.
 * @param label Where it came from, for the message.
 */
export function editRuleKeyRefusal(
    columns: unknown[],
    label: string,
): { error: 'DUPLICATE_EDIT_RULE_KEY'; message: string } | null {
    const seen = new Map<string, string>();
    const repeats: string[] = [];
    let highest = 0;
    columns.forEach((entry, columnIndex) => {
        const rules = asRecord(entry)?.edit_rules;
        if (!Array.isArray(rules)) return;
        rules.forEach((rule, ruleIndex) => {
            const key = asRecord(rule)?.key;
            if (typeof key !== 'string' && typeof key !== 'number') return;
            const where = `${label}[${columnIndex}].edit_rules[${ruleIndex}]`;
            const first = seen.get(String(key));
            if (first !== undefined)
                repeats.push(
                    `key "${String(key)}" at ${where} (first at ${first})`,
                );
            else seen.set(String(key), where);
            if (Number(key) > highest) highest = Number(key);
        });
    });
    if (repeats.length === 0) return null;
    return {
        error: 'DUPLICATE_EDIT_RULE_KEY',
        message: `Inline-edit rule keys must be unique across the whole view, not per column: ${repeats.join('; ')}. Knack would store them and the live app would run them, but the Builder would not show the rules on those columns. Give each rule its own key (the next free number is ${highest + 1}), keeping the keys of rules that already exist. Nothing was sent.`,
    };
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
