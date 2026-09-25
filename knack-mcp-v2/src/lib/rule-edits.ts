/**
 * Remove or replace rules by their `key`, the one edit shape shared by page rules and
 * every view rule set (record, submit, display and email rules).
 *
 * Every stored rule carries a key: `submit_N` on pages and on form submit rules, a
 * number as a string ("10", "15") on record, display and email rules — surveyed
 * 25 September across NPS Test App's 45 pages with rules and 729 view rules. Knack's
 * rule endpoints take a whole array and replace what is stored, so an edit has to be
 * made to the live array and the lot sent back; this module does the array part.
 *
 * Pure: no I/O.
 */
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

/** The live rules of a page or rule set, verbatim, with anything that is not an object dropped. */
export function readRuleArray(value: unknown): RawRule[] {
    return (Array.isArray(value) ? value : []).filter(
        (entry): entry is RawRule => asRecord(entry) !== null,
    );
}

/**
 * Give each new rule the next free numeric key ("1", "2", …), the scheme Knack uses for
 * field, record, display and email rules. Numbered from the highest existing key, not
 * the count, so a gap left by a removed rule is never handed out again.
 *
 * Throws a plain Error for a caller-supplied key that is already stored or repeated.
 */
/**
 * Give each incoming submit rule a `submit_N` key, checked against the live rules.
 *
 * The Builder names page rules and a form's submit rules `submit_0`, `submit_1`, … in
 * the order they were added: all 195 view submit rules on NPS Test App (25 September)
 * follow it. Knack stores whatever key it is sent, including none, and a rule with no
 * key can never be edited or removed by key.
 *
 * @param where Where the rules live, for the clash message ("this page", "this view").
 */
export function assignSubmitRuleKeys(
    existing: RawRule[],
    incoming: RawRule[],
    label = 'rules',
    where = 'this view',
): RawRule[] {
    const taken = new Set(existing.map((rule) => rule.key));
    // Highest number plus one, not the count: a deleted rule leaves a gap, so
    // submit_0 + submit_2 would otherwise hand out submit_2 again. Keys outside the
    // submit_N pattern are still clash-checked, just never numbered from.
    let next = 0;
    for (const key of taken) {
        const digits =
            typeof key === 'string' ? /^submit_(\d+)$/.exec(key)?.[1] : null;
        if (digits) next = Math.max(next, Number(digits) + 1);
    }
    return incoming.map((rule, index) => {
        if (rule.key !== undefined && typeof rule.key !== 'string') {
            throw new Error(`${label}[${index}].key must be a string.`);
        }
        const key = rule.key ?? `submit_${next++}`;
        if (taken.has(key)) {
            throw new Error(
                `${label}[${index}].key "${key}" is already used on ${where}. Omit key to have the next free one assigned. Nothing was sent.`,
            );
        }
        taken.add(key);
        return { ...rule, key };
    });
}

export function assignNumericRuleKeys(
    existing: RawRule[],
    incoming: RawRule[],
    label = 'rules',
): RawRule[] {
    const taken = new Set(existing.map((rule) => String(rule.key)));
    let next = 1;
    for (const key of taken) {
        if (/^\d+$/.test(key)) next = Math.max(next, Number(key) + 1);
    }
    return incoming.map((rule, index) => {
        if (rule.key !== undefined && typeof rule.key !== 'string') {
            throw new Error(`${label}[${index}].key must be a string.`);
        }
        const key = rule.key ?? String(next++);
        if (taken.has(key)) {
            throw new Error(
                `${label}[${index}].key "${key}" is already used. Omit key to have the next free one assigned. Nothing was sent.`,
            );
        }
        taken.add(key);
        return { key, ...rule };
    });
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
