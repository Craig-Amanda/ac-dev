/**
 * What the model is shown, kept small without losing what it needs.
 *
 * Two costs recur on every conversation with this server. The tool catalogue is sent
 * with every request, so each tool's advertised description is paid for on every turn.
 * And every view mutation returns Knack's response, which echoes the whole view twice
 * and pads every heading with empty arrays. Both are shaped here, in one place, so the
 * rules can be tested without a running server.
 */

import { readChangedScenes } from './view-safety.js';

function asRecord(value: unknown): Record<string, unknown> | null {
    return value !== null && typeof value === 'object' && !Array.isArray(value)
        ? (value as Record<string, unknown>)
        : null;
}

/**
 * Hand-written summaries for tools whose first sentence does not carry what a caller
 * needs to choose them.
 *
 * The previous rule replaced any description over 96 characters with a phrase made
 * from the tool's name ("Knack update view."), which told the model nothing about the
 * guard or the payload shape and cost it wasted calls. A summary here is what the model
 * sees; the long description in the registration stays as the source of truth for
 * humans.
 */
export const TOOL_SUMMARIES: Record<string, string> = {
    knack_update_view:
        'Update a view. Sends the merged full definition; a payload that drops a link to a child page destroys that page, which goes to the human for confirmation.',
    knack_copy_view:
        "Copy a view to another page through Knack's copy: a table's owned child pages are duplicated, a menu's are shared.",
    knack_move_view:
        'Move a view to another page. Every link counts as dropped, so owned child pages go to the human for confirmation.',
    knack_delete_view:
        'Delete a view. Child pages reached only through it are destroyed, which goes to the human for confirmation.',
    knack_update_field:
        'Update a field. Send only changed properties; dryRun previews the merge; KTL keyword tokens in the description are protected.',
    knack_create_field:
        'Create a field on an object. dryRun validates the definition without creating it.',
    knack_get_field:
        'Full unprojected definition for one field, including format and conditional rules.',
    knack_describe_field_shape:
        'Describe a field type for two jobs: reading records, and authoring the format or relationship object.',
    knack_analyze_data_model:
        'Structured design feedback on the data model: field-count distribution, isolated objects, connection density, likely issues.',
    knack_snapshot_app:
        'Write a restore point: the scene tree with parents and slugs, a schema pointer, and optionally one view definition.',
    knack_get_view_payload_template:
        'Build a create-view payload for a common view type, with source scoping, criteria, sort and column-connection options.',
};

export const TOOL_SUMMARY_MAX_CHARS = 160;

/**
 * The description a token-based client is shown for a tool.
 *
 * A curated summary when there is one; otherwise the first sentence of the full
 * description, which is nearly always the "what" and fits; otherwise a hard cut. With
 * compaction off, the full description goes through untouched.
 *
 * @param name The tool's registered name.
 * @param description The full description from the registration.
 * @param enabled Whether compaction is on (KNACK_MCP_COMPACT_TOOL_METADATA).
 * @returns What to advertise.
 */
export function compactToolDescription(
    name: string,
    description: string,
    enabled = true,
): string {
    if (!enabled) return description;

    const curated = TOOL_SUMMARIES[name];
    if (curated) return curated;

    const trimmed = description.trim().replace(/\s+/g, ' ');
    if (trimmed.length <= TOOL_SUMMARY_MAX_CHARS) return trimmed;

    const firstSentence = trimmed.split(/(?<=\.)\s/)[0] ?? trimmed;
    if (firstSentence.length <= TOOL_SUMMARY_MAX_CHARS) return firstSentence;

    return `${trimmed.slice(0, TOOL_SUMMARY_MAX_CHARS - 1).trimEnd()}…`;
}

/**
 * Compact the `changes` block Knack returns from a view mutation.
 *
 * Knack echoes the whole created or updated view under `changes.inserts.views[].view`
 * as well as under `view`, and sends empty `objects` and `fields` arrays under every
 * heading. A caller uses the keys and the page identities; the rest doubled the size of
 * every mutation response. The scenes Knack made keep key, name, slug and parent, since
 * those are how the caller finds them; everything else reduces to keys.
 *
 * @param body Knack's response body.
 * @returns A `body` override carrying the compacted block, or nothing to override.
 */
export function compactKnackChanges(
    body: unknown,
): { body: Record<string, unknown> } | Record<string, never> {
    const record = asRecord(body);
    const changes = asRecord(record?.changes);
    if (!record || !changes) return {};

    const keysOf = (list: unknown): string[] =>
        Array.isArray(list)
            ? list
                  .map((entry) => {
                      if (typeof entry === 'string') return entry;
                      const item = asRecord(entry);
                      return asRecord(item?.view)?.key ?? item?.key ?? null;
                  })
                  .filter((key): key is string => typeof key === 'string')
            : [];

    const compacted: Record<string, unknown> = {};
    for (const kind of ['deletes', 'inserts', 'updates'] as const) {
        const block = asRecord(changes[kind]);
        if (!block) continue;
        const out: Record<string, unknown> = {};
        const scenes =
            kind === 'inserts'
                ? readChangedScenes(body, 'inserts').map((scene) => ({
                      key: scene.sceneKey,
                      name: scene.sceneName,
                      slug: scene.sceneSlug,
                      parent: scene.parentRef,
                  }))
                : keysOf(block.scenes);
        if (scenes.length > 0) out.scenes = scenes;
        for (const other of ['views', 'objects', 'fields'] as const) {
            const keys = keysOf(block[other]);
            if (keys.length > 0) out[other] = keys;
        }
        if (Object.keys(out).length > 0) compacted[kind] = out;
    }

    return { body: { ...record, changes: compacted } };
}
