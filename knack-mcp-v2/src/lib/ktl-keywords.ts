/**
 * Generic KTL keyword-cluster editing.
 *
 * A KTL-aware description or view title can carry several underscore keywords at once
 * (e.g. `_ktlHide`, `_showFor=manager`, `_notes=Craig on 2026-09-07`), all bunched
 * together in one trailing cluster — KTL only parses keywords when the whole cluster
 * trails the text, with no prose after it. This module lets a caller add a new keyword
 * or update an existing one's value without having to hand-retype every sibling keyword
 * that must be carried forward untouched.
 *
 * Distinct from field-payload.ts's `_notes`-specific helpers (appendKtlNote,
 * preserveKtlNote, etc.), which bound their match to that one keyword's own known shape.
 * This module has no such shape to rely on — a keyword's value can be arbitrary text —
 * so it uses a weaker, best-effort rule instead: a value runs from its keyword's `_name`
 * up to (but not including) the next `_word` that starts a new keyword. That is exact for
 * every cluster this codebase itself writes, and for anything a person typed in the
 * builder the same way; it can misparse only if a keyword's own value happens to contain
 * a `_word`-shaped token of its own, which is an edge case this module does not attempt
 * to resolve.
 */

/** One keyword in a trailing cluster, in the order it appears. */
export type KtlKeywordEntry = {
    /** The keyword's name including its leading underscore, e.g. `_notes`. */
    name: string;
    /** The keyword's full text as written, e.g. `_notes=Craig on 2026-09-07`. */
    raw: string;
};

/**
 * A keyword starts at the beginning of the string, or right after whitespace, with an
 * underscore followed by one or more word characters.
 */
const KEYWORD_START_PATTERN = /(?:^|\s)(_[a-zA-Z0-9_]+)/g;

/**
 * Split text into its leading prose and its trailing KTL keyword cluster, in the order
 * the keywords appear. Text with no keyword-shaped token at all comes back as pure prose.
 */
export function parseKtlKeywordCluster(text: string): {
    prose: string;
    keywords: KtlKeywordEntry[];
} {
    const starts: Array<{ index: number; name: string }> = [];
    const pattern = new RegExp(KEYWORD_START_PATTERN);
    let match: RegExpExecArray | null;
    while ((match = pattern.exec(text)) !== null) {
        const name = match[1];
        const nameStart = match.index + (match[0].length - name.length);
        starts.push({ index: nameStart, name });
    }

    if (starts.length === 0) {
        return { prose: text.trim(), keywords: [] };
    }

    const prose = text.slice(0, starts[0].index).trim();
    const keywords: KtlKeywordEntry[] = starts.map((start, i) => {
        const end = i + 1 < starts.length ? starts[i + 1].index : text.length;
        return { name: start.name, raw: text.slice(start.index, end).trim() };
    });
    return { prose, keywords };
}

/** The inverse of parseKtlKeywordCluster: prose followed by each keyword, space-joined. */
export function serializeKtlKeywordCluster(
    prose: string,
    keywords: KtlKeywordEntry[],
): string {
    return [prose, ...keywords.map((k) => k.raw)].filter(Boolean).join(' ');
}

/**
 * Add or update KTL keywords in text, leaving the prose and every other keyword
 * untouched.
 *
 * For each entry in `edits`: if a keyword with that name already exists in the trailing
 * cluster, its value is replaced in place — same position, every sibling keyword
 * unmoved. If it does not exist yet, it is appended as a new keyword at the end of the
 * cluster (still part of the required trailing block). A `null` value means a bare
 * keyword with no `=value`; a string value produces `name=value`.
 *
 * @param text Current description or title text.
 * @param edits Keyword name (with leading underscore) → new value, or null for bare.
 */
export function applyKtlKeywordEdits(
    text: string,
    edits: Record<string, string | null>,
): string {
    const { prose, keywords } = parseKtlKeywordCluster(text);
    const indexByName = new Map(keywords.map((k, i) => [k.name, i]));
    const result = [...keywords];

    for (const [name, value] of Object.entries(edits)) {
        const raw = value === null || value === '' ? name : `${name}=${value}`;
        const existingIndex = indexByName.get(name);
        if (existingIndex !== undefined) {
            result[existingIndex] = { name, raw };
        } else {
            result.push({ name, raw });
            indexByName.set(name, result.length - 1);
        }
    }

    return serializeKtlKeywordCluster(prose, result);
}
