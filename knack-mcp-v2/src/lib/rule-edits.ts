/**
 * Remove or replace rules by their `key`, the one edit shape shared by page rules and
 * every view rule set (record, submit, display and email rules).
 *
 * Every stored rule carries a key: `submit_N` on pages and on form submit rules, and on
 * record, display and email rules either a number as a string ("10", "15") or, from the
 * newer Builder, a hash key (`record_<12 hex>`, `display_<12 hex>`). Surveyed
 * 25 September across NPS Test App's 45 pages with rules and 729 view rules (all
 * numeric), and in a review against a long-lived production app (322 numeric record
 * rules beside 122 `record_<hex>`, 4 `display_<hex>`, 8 email rules `record_<hex>`;
 * 508 of 508 submit and page rules `submit_N`). Edits go by exact key, so either kind
 * can be edited or removed. Knack's
 * rule endpoints take a whole array and replace what is stored, so an edit has to be
 * made to the live array and the lot sent back; this module does the array part.
 *
 * Pure: no I/O.
 */
import type { CachedField } from '../types.js';
import { asRecord } from './util.js';

export type RawRule = Record<string, unknown>;

export type RuleEdit = {
    /** Keys of rules to take out. */
    removeKeys?: string[];
    /** Whole rules to put in place of the stored rule with the same key. */
    replaceRules?: RawRule[];
};

export type RuleEditResult = {
    rules: RawRule[];
    removedKeys: string[];
    replacedKeys: string[];
};

/** The `action` values the Builder offers for a record rule, in the order of its Action dropdown. */
const RECORD_RULE_ACTIONS = ['record', 'connection', 'insert'];

/** Shape of a record rule's `connection`: the connected object, then the connection field on this one. */
const RULE_CONNECTION_PATTERN = /^object_\d+\.field_\d+$/;

/**
 * Why a set of record rules cannot be stored, or null. Knack accepts all of these, but
 * the rule then never runs:
 * - `action` missing or not one of RECORD_RULE_ACTIONS: the Action dropdown shows empty
 *   in the Builder (found 2 October, GAP-Track `view_3342`; an unrecognised value such as
 *   "update_all" behaves the same, tested on `view_3345`).
 * - a `connection` or `insert` rule without a valid `connection`: the Builder shows the
 *   action but an empty connection dropdown, and cannot show the rule's values (tested
 *   on `view_3345`, rules 5 and 6). Valid means `object_X.field_Y` where `field_Y` is a
 *   connection field on the view's source object and connects to `object_X`.
 *
 * - `"action": "record"` is "Update this record".
 * - `"action": "connection"` plus `"connection": "object_X.field_Y"` is "Update connected records"
 *   (as the Builder stores it: GAP-Track `view_3342`, rule 5).
 * - `"action": "insert"` plus `"connection": "object_X.field_Y"` is "Insert a connected record".
 * The Builder picks the option from `action` alone: a `"record"` rule carrying a leftover
 * `connection` key (GAP-Track `view_131`, rule 1) still shows "Update this record", so a
 * `record` rule's `connection` is not checked.
 *
 * A value copied from a connected record is
 * `{"type":"connection","field":"<target>","connection_field":"<connectionOnThisObject>-<sourceFieldOnConnectedObject>"}`.
 *
 * @param rules Whole record rules about to be stored.
 * @param label Where they came from, for the message (e.g. "recordRules").
 * @param sourceFields Fields of the view's source object, or undefined when the object could not be read.
 */
export function recordRuleActionRefusal(
    rules: RawRule[],
    label: string,
    sourceFields: CachedField[] | undefined,
): { error: 'INVALID_RULE_ACTION'; message: string } | null {
    const problems = rules.flatMap((rule, index) => {
        const name = `${label}[${index}]`;
        if (typeof rule.action !== 'string' || rule.action.trim() === '')
            return [`${name} has no "action"`];
        if (!RECORD_RULE_ACTIONS.includes(rule.action))
            return [`${name} has "action": "${rule.action}"`];
        if (rule.action !== 'record') {
            const connection =
                typeof rule.connection === 'string' ? rule.connection : '';
            if (!RULE_CONNECTION_PATTERN.test(connection))
                return [
                    `${name} is a "${rule.action}" rule with no valid "connection" (expected "object_X.field_Y")`,
                ];
            if (!sourceFields)
                return [
                    `${name} has "connection": "${connection}", which could not be checked because the view's source object could not be read`,
                ];
            const [connectedObject, fieldKey] = connection.split('.');
            const field = sourceFields.find((entry) => entry.key === fieldKey);
            if (field?.type !== 'connection')
                return [
                    `${name} has "connection": "${connection}", but ${fieldKey} is not a connection field on this view's object`,
                ];
            if (field.connectedObject !== connectedObject)
                return [
                    `${name} has "connection": "${connection}", but ${fieldKey} connects to ${field.connectedObject ?? 'no object'}, not ${connectedObject}`,
                ];
        }
        return [];
    });
    if (!problems.length) return null;
    return {
        error: 'INVALID_RULE_ACTION',
        message: `${problems.join('; ')}. Knack would store ${problems.length === 1 ? 'it' : 'them'} but the rule would not work: the Builder shows an empty Action dropdown or an empty connection dropdown. Use "action": "record" for "Update this record", "action": "connection" plus "connection": "object_X.field_Y" for "Update connected records", or "action": "insert" plus "connection": "object_X.field_Y" for "Insert a connected record" (object_X is the connected object, field_Y the connection field on this view's object). Values copied from a connected record use {"type":"connection","field":"<target>","connection_field":"<connectionOnThisObject>-<sourceFieldOnConnectedObject>"}. Nothing was sent.`,
    };
}

export type RuleReferenceContext = {
    /** Fields of the view's source object, or undefined when it could not be read. */
    sourceFields: CachedField[] | undefined;
    /** Fields of any object in the app, or undefined when it is not known. */
    fieldsOf: (objectKey: string) => CachedField[] | undefined;
    /** Keys of the form's inputs, or null when the view has no inputs (not a form). */
    formInputKeys: Set<string> | null;
};

/**
 * Why a record rule's values point at the wrong fields, or null. Each of these Knack
 * stores without complaint and the rule then copies nothing (found 6 October on NPS
 * Test App `view_1858`, where all three were accepted):
 * - `type: "record"` with an `input` that is not an input on the form. A form can only
 *   copy what is on it; a field that is not there needs `type: "connection"` or a fixed
 *   value.
 * - `type: "connection"` on an "Update this record" rule whose `connection_field` is not
 *   `<connection field on this object>-<field on the connected object>`: the first part
 *   must be a connection field of the view's object, the second a field of the object
 *   it connects to.
 * `connection` and `insert` rules are not checked for `connection_field`, since only the
 * "Update this record" shape has been verified against the Builder.
 *
 * @param rules Whole record rules about to be stored.
 * @param label Where they came from, for the message.
 * @param context What the schema and the view say about the fields involved.
 */
export function recordRuleReferenceRefusal(
    rules: RawRule[],
    label: string,
    context: RuleReferenceContext,
): { error: 'INVALID_RULE_VALUE'; message: string } | null {
    const problems = rules.flatMap((rule, ruleIndex) => {
        const values = Array.isArray(rule.values) ? rule.values : [];
        return values.flatMap((entry: unknown, valueIndex) => {
            const value = asRecord(entry);
            if (!value) return [];
            const name = `${label}[${ruleIndex}].values[${valueIndex}]${typeof value.field === 'string' ? ` (${value.field})` : ''}`;
            if (
                value.type === 'record' &&
                typeof value.input === 'string' &&
                value.input.trim() !== '' &&
                context.formInputKeys &&
                !context.formInputKeys.has(value.input)
            )
                return [
                    `${name} copies from "input": "${value.input}", which is not an input on this form`,
                ];
            if (
                value.type === 'connection' &&
                rule.action === 'record' &&
                typeof value.connection_field === 'string' &&
                value.connection_field.trim() !== '' &&
                context.sourceFields
            ) {
                const parts = value.connection_field.split('-');
                if (parts.length !== 2 || !parts[0] || !parts[1])
                    return [
                        `${name} has "connection_field": "${value.connection_field}", expected "<connection field>-<field on the connected object>"`,
                    ];
                const [connectionKey, connectedFieldKey] = parts;
                const connection = context.sourceFields.find(
                    (field) => field.key === connectionKey,
                );
                if (connection?.type !== 'connection')
                    return [
                        `${name} has "connection_field": "${value.connection_field}", but ${connectionKey} is not a connection field on this view's object`,
                    ];
                const connectedFields = connection.connectedObject
                    ? context.fieldsOf(connection.connectedObject)
                    : undefined;
                if (
                    connectedFields &&
                    !connectedFields.some(
                        (field) => field.key === connectedFieldKey,
                    )
                )
                    return [
                        `${name} has "connection_field": "${value.connection_field}", but ${connectedFieldKey} is not a field on ${connection.connectedObject}, the object ${connectionKey} connects to`,
                    ];
            }
            return [];
        });
    });
    if (problems.length === 0) return null;
    return {
        error: 'INVALID_RULE_VALUE',
        message: `${problems.join('; ')}. Knack would store ${problems.length === 1 ? 'it' : 'them'} but the rule would copy nothing. A value from a form input is {"type":"record","field":"<target>","input":"<field on the form>"}; one from a connected record is {"type":"connection","field":"<target>","connection_field":"<connectionOnThisObject>-<sourceFieldOnConnectedObject>"}. Nothing was sent.`,
    };
}

/** The live rules of a page or rule set, verbatim, with anything that is not an object dropped. */
export function readRuleArray(value: unknown): RawRule[] {
    return (Array.isArray(value) ? value : []).filter(
        (entry): entry is RawRule => asRecord(entry) !== null,
    );
}

/**
 * Give each new rule the next free key in `prefix` + number form, checked against the
 * live rules. Numbered from the highest existing number plus one, not the count, so a
 * gap left by a removed rule is never handed out again; keys outside the pattern are
 * still clash-checked, just never numbered from. The key goes first, as Knack stores it.
 *
 * Throws a plain Error for a caller-supplied key that is not a string, already stored,
 * or repeated.
 */
function assignRuleKeys(
    existing: RawRule[],
    incoming: RawRule[],
    scheme: { prefix: string; first: number; label: string; where?: string },
): RawRule[] {
    const { prefix, first, label, where } = scheme;
    const taken = new Set(existing.map((rule) => String(rule.key)));
    const numbered = new RegExp(`^${prefix}(\\d+)$`);
    let next = first;
    for (const key of taken) {
        const digits = numbered.exec(key)?.[1];
        if (digits) next = Math.max(next, Number(digits) + 1);
    }
    return incoming.map((rule, index) => {
        if (rule.key !== undefined && typeof rule.key !== 'string') {
            throw new Error(`${label}[${index}].key must be a string.`);
        }
        const key = rule.key ?? `${prefix}${next++}`;
        if (taken.has(key)) {
            throw new Error(
                `${label}[${index}].key "${key}" is already used${where ? ` on ${where}` : ''}. Omit key to have the next free one assigned. Nothing was sent.`,
            );
        }
        taken.add(key);
        return { key, ...rule };
    });
}

/**
 * Give each incoming submit rule a `submit_N` key. The Builder names page rules and a
 * form's submit rules `submit_0`, `submit_1`, … in the order they were added: all 195
 * view submit rules on NPS Test App (25 September) follow it. Knack stores whatever key
 * it is sent, including none, and a rule with no key can never be edited or removed by
 * key.
 *
 * @param where Where the rules live, for the clash message ("this page", "this view").
 */
export function assignSubmitRuleKeys(
    existing: RawRule[],
    incoming: RawRule[],
    label = 'rules',
    where = 'this view',
): RawRule[] {
    return assignRuleKeys(existing, incoming, {
        prefix: 'submit_',
        first: 0,
        label,
        where,
    });
}

/**
 * Give each new rule the next free numeric key ("1", "2", …), the older Builder scheme
 * for field, record, display and email rules. The newer Builder mints hash keys
 * (`record_<12 hex>`) instead; both are stored and edited alike, and a hash key already
 * on the view is clash-checked but never numbered from.
 */
export function assignNumericRuleKeys(
    existing: RawRule[],
    incoming: RawRule[],
    label = 'rules',
): RawRule[] {
    return assignRuleKeys(existing, incoming, { prefix: '', first: 1, label });
}

/**
 * Apply `edit` to `existing`, keeping every other rule and the order they are in. A
 * replacement takes the position of the rule it replaces.
 *
 * Throws a plain Error, naming the problem, for a key that is not stored, a key both
 * removed and replaced, a key given twice, or a replacement without a key. Nothing is
 * worked out from a partial edit.
 */
export function applyRuleEdit(
    existing: RawRule[],
    edit: RuleEdit,
    label = 'rules',
): RuleEditResult {
    const removeKeys = edit.removeKeys ?? [];
    const replaceRules = edit.replaceRules ?? [];
    if (!removeKeys.length && !replaceRules.length) {
        throw new Error(
            'Pass removeKeys and/or replaceRules. Nothing was sent.',
        );
    }

    const stored = new Set(existing.map((rule) => rule.key));
    const seen = new Set<unknown>();
    const claim = (key: unknown, where: string) => {
        if (typeof key !== 'string' || !key) {
            throw new Error(
                `${where} must carry the key of the stored rule it replaces. Nothing was sent.`,
            );
        }
        if (seen.has(key)) {
            throw new Error(
                `${label} key "${key}" is named more than once in this edit. Nothing was sent.`,
            );
        }
        if (!stored.has(key)) {
            throw new Error(
                `${label} key "${key}" is not stored. Stored keys: ${[...stored].join(', ') || 'none'}. Nothing was sent.`,
            );
        }
        seen.add(key);
    };
    removeKeys.forEach((key, index) => claim(key, `removeKeys[${index}]`));
    replaceRules.forEach((rule, index) =>
        claim(rule.key, `replaceRules[${index}]`),
    );

    const remove = new Set(removeKeys);
    const replacements = new Map(replaceRules.map((rule) => [rule.key, rule]));
    const rules = existing
        .filter((rule) => !remove.has(rule.key as string))
        .map((rule) => replacements.get(rule.key) ?? rule);

    return {
        rules,
        removedKeys: [...remove],
        replacedKeys: replaceRules.map((rule) => rule.key as string),
    };
}
