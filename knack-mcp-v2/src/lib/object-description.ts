/**
 * An object's description, held on its auto-increment (AI) field.
 *
 * Knack has no description for an object, and every object has an AI field, so the
 * words an AI needs to read what a table is for live in that field's description. The
 * `_notes=[<name> on <date>]` stamp rides along like any other field's.
 *
 * Pure logic: it reads the fields it is given and touches nothing else.
 */
import { readFieldDescription } from './field-description.js';
import {
    descriptionAsPlainText,
    readDescriptionText,
    splitWordsAndKeywords,
    stripKtlNoteTag,
} from './field-payload.js';
import { asRecord } from './util.js';

/**
 * The name given to an auto-increment field this server has to add itself. Not
 * "Record ID": measured on the playground, Knack gives every new table a system field of
 * that name, so a second one was renamed "Record ID Copy". "AI" is the house name.
 */
export const AUTO_INCREMENT_FIELD_NAME = 'AI';

/** A list of objects carries this much of each description; get_object has it whole. */
export const LIST_DESCRIPTION_CHARS = 160;

export type ObjectDescription = {
    /** The AI field that holds (or would hold) the description, if the object has one. */
    fieldKey: string | null;
    /** The description without its `_notes` stamp; '' when there is none. */
    text: string;
    /** More than one AI field: all their keys, the described one first. */
    autoIncrementKeys: string[];
};

const readDescription = readFieldDescription;

/**
 * Find an object's description in its fields (cached or live). Where an object has
 * several AI fields the first one with words wins, so a described field is never hidden
 * behind an empty one.
 */
export function readObjectDescription(fields: unknown): ObjectDescription {
    const autoIncrement = (Array.isArray(fields) ? fields : [])
        .map((field) => asRecord(field))
        .filter(
            (field): field is Record<string, unknown> =>
                field !== null &&
                field.type === 'auto_increment' &&
                typeof field.key === 'string',
        )
        .map((field) => ({
            key: field.key as string,
            text: readDescriptionText(readDescription(field)),
        }));
    if (!autoIncrement.length) {
        return { fieldKey: null, text: '', autoIncrementKeys: [] };
    }
    const holder =
        autoIncrement.find((entry) => entry.text) ?? autoIncrement[0];
    return {
        fieldKey: holder.key,
        text: holder.text,
        autoIncrementKeys: [
            holder.key,
            ...autoIncrement
                .filter((entry) => entry.key !== holder.key)
                .map((entry) => entry.key),
        ],
    };
}

/**
 * The other KTL keywords on the description-holding field (`_ktlHide`, `_mcp_*`, ...),
 * as written, or '' when it has none. They belong to the field, not to the table's words,
 * so a change to the words must carry them along or it would look like removing them.
 */
export function readHolderKeywords(fields: unknown): string {
    const holderKey = readObjectDescription(fields).fieldKey;
    const holder = (Array.isArray(fields) ? fields : [])
        .map((field) => asRecord(field))
        .find((field) => field?.key === holderKey);
    if (!holder) return '';
    return splitWordsAndKeywords(
        stripKtlNoteTag(descriptionAsPlainText(readDescription(holder))),
    ).keywords;
}

/** The description-holding field's stored description exactly as Knack has it, or ''. */
export function readHolderRawDescription(fields: unknown): string {
    const holderKey = readObjectDescription(fields).fieldKey;
    const holder = (Array.isArray(fields) ? fields : [])
        .map((field) => asRecord(field))
        .find((field) => field?.key === holderKey);
    return holder ? readDescription(holder) : '';
}

/** The description cut for a list, marked when cut; undefined when there is none. */
export function shortObjectDescription(fields: unknown): string | undefined {
    const { text } = readObjectDescription(fields);
    if (!text) return undefined;
    const flat = text.replace(/\s+/g, ' ');
    return flat.length > LIST_DESCRIPTION_CHARS
        ? `${flat.slice(0, LIST_DESCRIPTION_CHARS - 1)}…`
        : flat;
}
