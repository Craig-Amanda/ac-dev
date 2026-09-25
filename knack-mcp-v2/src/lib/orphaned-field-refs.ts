/**
 * Find references to fields the app no longer has.
 *
 * Deleting a field in the builder usually cleans it out of every page, view, rule and
 * formula, but not always. On 25 September (Spot, field_1516) Knack stripped a deleted
 * field from a record rule and left its form input on two forms; the builder's rules
 * dialog then crashed on those forms ("Field field_1516 was expected on object object_30
 * but was not found") and their display rules stopped working. This walks the runtime
 * metadata for every `field_N` token and reports each one no object defines, with where
 * it sits, so an orphan can be found without waiting for something to break.
 *
 * Pure: no I/O.
 */
import { getRuntimeArray } from './metadata.js';
import { asRecord } from './util.js';

/** At most this many paths are listed per place; the count is always complete. */
const MAX_PATHS_PER_PLACE = 20;

export type OrphanedFieldPlace = {
    kind: 'view' | 'page' | 'field' | 'task' | 'object';
    sceneKey?: string;
    viewKey?: string;
    viewType?: string;
    objectKey?: string;
    fieldKey?: string;
    taskKey?: string;
    name?: string;
    missingFieldKeys: string[];
    /** Where each reference sits, relative to the place, e.g. `groups[0].columns[1].inputs[2].field.key`. */
    paths: string[];
    pathCount: number;
};

/** Every string in `value` naming a `field_N`, with its path; `skip` names top-level keys to leave out. */
function collectRefsWithPaths(
    value: unknown,
    skip: ReadonlySet<string> = new Set(),
): Array<{ key: string; path: string }> {
    const found: Array<{ key: string; path: string }> = [];
    const walk = (entry: unknown, path: string) => {
        if (typeof entry === 'string') {
            for (const key of entry.match(/\bfield_\d+\b/g) || [])
                found.push({ key, path });
        } else if (Array.isArray(entry)) {
            entry.forEach((item, index) => walk(item, `${path}[${index}]`));
        } else {
            const record = asRecord(entry);
            if (!record) return;
            for (const [key, child] of Object.entries(record)) {
                if (!path && skip.has(key)) continue;
                walk(child, path ? `${path}.${key}` : key);
            }
        }
    };
    walk(value, '');
    return found;
}

/**
 * Every place in the metadata that names a field no object defines.
 *
 * @param onlyFieldKey Report only references to this key.
 */
export function findOrphanedFieldRefs(
    metadata: unknown,
    onlyFieldKey?: string,
): OrphanedFieldPlace[] {
    const objects = (getRuntimeArray(metadata, 'objects') || [])
        .map((entry) => asRecord(entry))
        .filter((entry): entry is Record<string, unknown> => Boolean(entry));
    const known = new Set<string>();
    for (const object of objects) {
        for (const field of Array.isArray(object.fields) ? object.fields : []) {
            const key = asRecord(field)?.key;
            if (typeof key === 'string') known.add(key);
        }
    }

    const places: OrphanedFieldPlace[] = [];
    const check = (
        place: Omit<
            OrphanedFieldPlace,
            'missingFieldKeys' | 'paths' | 'pathCount'
        >,
        value: unknown,
        skip?: ReadonlySet<string>,
    ) => {
        const orphans = collectRefsWithPaths(value, skip).filter(
            (ref) =>
                !known.has(ref.key) &&
                (!onlyFieldKey || ref.key === onlyFieldKey),
        );
        if (!orphans.length) return;
        const paths = [...new Set(orphans.map((ref) => ref.path))];
        places.push({
            ...place,
            missingFieldKeys: [...new Set(orphans.map((ref) => ref.key))],
            paths: paths.slice(0, MAX_PATHS_PER_PLACE),
            pathCount: paths.length,
        });
    };
    const text = (value: unknown) =>
        typeof value === 'string' ? value : undefined;

    for (const object of objects) {
        const objectKey = text(object.key);
        check(
            { kind: 'object', objectKey, name: text(object.name) },
            object,
            new Set(['fields', 'tasks']),
        );
        for (const entry of Array.isArray(object.fields) ? object.fields : []) {
            const field = asRecord(entry);
            if (!field) continue;
            check(
                {
                    kind: 'field',
                    objectKey,
                    fieldKey: text(field.key),
                    name: text(field.name),
                },
                field,
                new Set(['key']),
            );
        }
        for (const entry of Array.isArray(object.tasks) ? object.tasks : []) {
            const task = asRecord(entry);
            if (!task) continue;
            check(
                {
                    kind: 'task',
                    objectKey,
                    taskKey: text(task.key),
                    name: text(task.name),
                },
                task,
            );
        }
    }

    for (const entry of getRuntimeArray(metadata, 'scenes') || []) {
        const scene = asRecord(entry);
        if (!scene) continue;
        const sceneKey = text(scene.key);
        check(
            { kind: 'page', sceneKey, name: text(scene.name) },
            scene,
            new Set(['views']),
        );
        for (const viewEntry of Array.isArray(scene.views) ? scene.views : []) {
            const view = asRecord(viewEntry);
            if (!view) continue;
            check(
                {
                    kind: 'view',
                    sceneKey,
                    viewKey: text(view.key),
                    viewType: text(view.type),
                    name: text(view.name),
                },
                view,
            );
        }
    }

    return places;
}
