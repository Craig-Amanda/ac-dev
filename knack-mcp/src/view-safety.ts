/**
 * View-mutation safety rules for the Knack MCP server.
 *
 * Knack's view PUT deletes `link` columns and cascade-deletes the child scenes behind
 * them whenever the `columns` array is replaced — even when the link column is re-sent
 * byte-for-byte. Menu views carry the same hazard through `links` instead of `columns`.
 *
 * Everything in this module is pure so the rules can be unit-tested without a Knack app.
 * The orchestrator at the bottom takes its I/O as injected dependencies for the same
 * reason: a test can assert that a blocked request never reaches the transport.
 */

// -----------------------
// Types
// -----------------------

export type ViewSafetyErrorCode =
    | 'INVALID_UPDATES_JSON'
    | 'EMPTY_UPDATE_PAYLOAD'
    | 'CONFIRMATION_UPGRADE_REQUIRED'
    | 'BLOCKED_LINKS_PAYLOAD'
    | 'COULD_NOT_VERIFY_VIEW'
    | 'BLOCKED_MENU_VIEW_UPDATE'
    | 'BLOCKED_MENU_VIEW_MOVE'
    | 'UNKNOWN_VIEW_TYPE'
    | 'HUMAN_CONFIRMATION_UNAVAILABLE'
    | 'STRUCTURE_TOO_DEEP'
    | 'SCENE_TREE_UNAVAILABLE'
    | 'HUMAN_CONFIRMATION_DECLINED'
    | 'SNAPSHOT_FAILED';

export type ViewMutationAction =
    | 'create_view'
    | 'update_view'
    | 'update_view_order'
    | 'copy_view'
    | 'move_view'
    | 'delete_view';

export type LinkColumnTarget = {
    header: string | null;
    fieldKey: string | null;
    /**
     * The page this link points at, as written in the view: Knack stores a **slug**
     * (`roll-details3`) here, not a `scene_N` key. Resolved against the scene tree by
     * expandChildPages. Null when a `scene` was present but could not be read.
     */
    childSceneRef: string | null;
    /** The node's declared type — `link`, `scene_link`, or whatever Knack adds next. */
    linkType: string | null;
    /** Where in the view layout the link was found, e.g. `$.groups[0].columns[2]`. */
    sourcePath: string;
};

export type MenuLinkTarget = {
    name: string | null;
    linkType: string | null;
    childSceneRef: string | null;
    sourcePath: string;
};

export type LinkTargets = {
    linkColumns: LinkColumnTarget[];
    menuLinks: MenuLinkTarget[];
    /**
     * Every page reference found, deduped and sorted. These are Knack's own strings —
     * slugs in practice — and are **not** scene keys until expandChildPages resolves
     * them. Named `Refs` rather than `Keys` deliberately: an earlier version called
     * these keys, and the walk that consumed them matched on `scene_N`, so every
     * grandchild silently dropped out of the confirmation prompt.
     */
    childSceneRefs: string[];
};

export type SceneNode = {
    sceneKey: string;
    sceneName?: string;
    sceneSlug?: string;
    /**
     * Whatever Knack put in `scene.parent` — a **slug**, not a key. Resolved through
     * the slug index rather than compared against sceneKey.
     */
    parentRef?: string;
};

export type ChildPage = {
    sceneKey: string;
    sceneName: string | null;
    sceneSlug: string | null;
    /** 0 for a page linked directly, 1+ for a page that dies because its ancestor does. */
    depth: number;
};

/**
 * Whether a linked page belongs to the page being mutated, or merely sits at the far
 * end of a link.
 *
 * Knack creates a child page *because* a view links to it, and deletes it when that
 * link goes. A page that already lives elsewhere in the tree does not owe its
 * existence to the link, so removing the link removes navigation and nothing else —
 * confirmed against a real app, where the external page and its connection both
 * survived a view update.
 *
 * `unknown` exists because the two evidence-free cases are indistinguishable: a page
 * with no `parent` may be genuinely top-level, or its parent may simply be absent from
 * the metadata we read. Treating either as external is how a human confirms and loses
 * more than they agreed to, so both stay at risk.
 */
export type LinkTargetClass = 'owned' | 'external' | 'unknown';

export type ClassifiedLinkTarget = {
    /** The reference as it appeared on the link — a slug, usually. */
    ref: string;
    sceneKey: string | null;
    sceneName: string | null;
    sceneSlug: string | null;
    classification: LinkTargetClass;
    /** The owner this page declares, resolved to a scene key where possible. */
    parentSceneKey: string | null;
    /** Why it landed in this class, stated plainly enough for a confirmation prompt. */
    reason: string;
};

const MAX_WALK_DEPTH = 24;

// -----------------------
// Small shared helpers
// -----------------------

function asPlainObject(value: unknown): Record<string, unknown> | null {
    if (!value || typeof value !== 'object' || Array.isArray(value))
        return null;
    return value as Record<string, unknown>;
}

function asTrimmedString(value: unknown): string | null {
    if (typeof value !== 'string') return null;
    const trimmed = value.trim();
    return trimmed ? trimmed : null;
}

/**
 * Read a scene reference that Knack writes either as a bare key or as an object.
 *
 * @param value Raw `scene` property from a link column or menu link.
 * @returns The scene key, or null when the reference is absent or unrecognised.
 */
function readSceneReference(value: unknown): string | null {
    const direct = asTrimmedString(value);
    if (direct) return direct;

    const record = asPlainObject(value);
    if (!record) return null;

    return (
        asTrimmedString(record.key) ??
        asTrimmedString(record.scene) ??
        asTrimmedString(record.slug)
    );
}

/**
 * Read a `key` off a nested reference object, e.g. a column's `field`.
 *
 * Same shapes as readSceneReference, but named for what it does at the call site —
 * reading a field key through a function called readSceneReference read as a bug
 * every time someone looked at it.
 *
 * @param value Raw property value.
 * @returns The key, or null.
 */
function readKey(value: unknown): string | null {
    return readSceneReference(value);
}

/**
 * Classify a node's `scene` property: absent, or present and possibly unreadable.
 *
 * The distinction decides whether a node is a page link at all, and the two failures
 * are opposites. A node with **no** `scene` is not a page link — a form's Link/URL
 * field input is `type: "link"` with a `field` and no scene, and counting it made
 * ordinary forms un-editable. A node whose `scene` is present but unreadable **is** a
 * page link whose target we cannot name, which has to count as risk rather than be
 * skipped.
 *
 * An explicitly empty `scene` (`""`, null) is treated as absent: Knack writing an
 * empty string is saying there is no child page, not hiding which one.
 *
 * @param record The node being inspected.
 * @returns Whether a scene reference is present, and its value when readable.
 */
function readSceneProperty(record: Record<string, unknown>): {
    present: boolean;
    ref: string | null;
} {
    if (!Object.hasOwn(record, 'scene')) return { present: false, ref: null };

    const raw = record.scene;
    if (raw === null || raw === undefined || raw === '') {
        return { present: false, ref: null };
    }

    return { present: true, ref: readSceneReference(raw) };
}

/**
 * Resolve the view attributes from whichever shape the caller has.
 *
 * Knack returns a view as `{view: {...}}` from the scenes endpoint but as a bare object
 * elsewhere, and nests the real properties under `attributes` in runtime metadata.
 *
 * @param value A view response body, a view object, or raw view attributes.
 * @returns The attribute record, or null when nothing view-shaped was supplied.
 */
export function resolveViewAttributes(
    value: unknown,
): Record<string, unknown> | null {
    const record = asPlainObject(value);
    if (!record) return null;

    const inner =
        asPlainObject(record.view) ?? asPlainObject(record.attributes) ?? null;

    if (inner) {
        // `{view: {attributes: {...}}}` nests one level further.
        return asPlainObject(inner.attributes) ?? inner;
    }

    return record;
}

/**
 * Read a view's declared type.
 *
 * @param attributes View attributes as returned by resolveViewAttributes.
 * @returns The lowercased type, or null when the view declares none.
 */
export function getViewType(
    attributes: Record<string, unknown> | null,
): string | null {
    if (!attributes) return null;
    const type = asTrimmedString(attributes.type);
    return type ? type.toLowerCase() : null;
}

/**
 * @param attributes View attributes as returned by resolveViewAttributes.
 * @returns True when the view is a navigation menu.
 */
export function isMenuView(
    attributes: Record<string, unknown> | null,
): boolean {
    return getViewType(attributes) === 'menu';
}

// -----------------------
// Link discovery
// -----------------------

/**
 * Collect every link column and menu link in a view, at any nesting depth.
 *
 * Knack layouts nest as `groups[].columns[]`, so a single-level read of `columns` misses
 * link columns inside groups — they pass a flat check and still cascade-delete their
 * child page. This walks the whole attribute tree instead.
 *
 * @param attributes View attributes as returned by resolveViewAttributes.
 * @returns Link columns, menu links, and the union of child scene keys they reference.
 */
export function collectLinkTargets(
    attributes: Record<string, unknown> | null,
): LinkTargets {
    const linkColumns: LinkColumnTarget[] = [];
    const menuLinks: MenuLinkTarget[] = [];
    const childSceneRefs = new Set<string>();

    if (!attributes) {
        return { linkColumns, menuLinks, childSceneRefs: [] };
    }

    const visit = (value: unknown, path: string, depth: number): void => {
        if (depth > MAX_WALK_DEPTH) return;

        if (Array.isArray(value)) {
            value.forEach((item, index) =>
                visit(item, `${path}[${index}]`, depth + 1),
            );
            return;
        }

        const record = asPlainObject(value);
        if (!record) return;

        const type = asTrimmedString(record.type)?.toLowerCase() ?? null;
        const sceneRef = readSceneProperty(record);

        // Entries of a `links` array are menu links regardless of their own `type`,
        // which Knack sets to `scene`, `url` or omits entirely.
        if (/\.links\[\d+\]$/.test(path)) {
            menuLinks.push({
                name:
                    asTrimmedString(record.name) ??
                    asTrimmedString(record.label),
                linkType: type,
                childSceneRef: sceneRef.ref,
                sourcePath: path,
            });
            if (sceneRef.ref) childSceneRefs.add(sceneRef.ref);
        } else if (sceneRef.present) {
            // A carried `scene` property is what makes a node a page link — not its
            // type string. Knack writes `type: "link"` on table and search columns but
            // `type: "scene_link"` on details and calendar columns, so matching the
            // string missed every details view: the guard reported no link columns and
            // let a layout replacement through with no confirmation at all.
            //
            // The converse matters just as much. A form's Link/URL field input is also
            // `type: "link"`, carries a `field` and no `scene`, and points at no page
            // whatsoever. Treating those as links made every structural edit to such a
            // form refuse on a client that cannot prompt.
            linkColumns.push({
                header: asTrimmedString(record.header),
                fieldKey: readKey(record.field),
                childSceneRef: sceneRef.ref,
                linkType: type,
                sourcePath: path,
            });
            if (sceneRef.ref) childSceneRefs.add(sceneRef.ref);
        }

        for (const [key, nested] of Object.entries(record)) {
            visit(nested, `${path}.${key}`, depth + 1);
        }
    };

    visit(attributes, '$', 0);

    return {
        linkColumns,
        menuLinks,
        childSceneRefs: [...childSceneRefs].sort(),
    };
}

/**
 * Report whether a structure nests deeper than the walkers below will follow.
 *
 * Every walk in this module stops at MAX_WALK_DEPTH. Stopping silently means the
 * answer flips to the permissive one exactly where the structure is most unusual:
 * past the cap, `payloadTouchesLinks` reports no links, `collectPayloadKeys` omits
 * denied keys, and `collectLinkTargets` finds no link columns. The guard runs this
 * first and refuses, so no later check can be reached with input it cannot see to
 * the bottom of.
 *
 * @param value Payload or view attributes.
 * @returns True when anything sits deeper than the walkers will follow.
 */
export function exceedsMaxDepth(value: unknown): boolean {
    const visit = (current: unknown, depth: number): boolean => {
        if (depth > MAX_WALK_DEPTH) return true;

        if (Array.isArray(current)) {
            return current.some((item) => visit(item, depth + 1));
        }

        const record = asPlainObject(current);
        if (!record) return false;

        return Object.values(record).some((nested) => visit(nested, depth + 1));
    };

    return visit(value, 0);
}

/**
 * Detect a `links` array anywhere in an update payload.
 *
 * Deliberately broad: a false positive costs one refused call and a Builder edit, while
 * a false negative silently rewrites navigation.
 *
 * @param payload Parsed update payload.
 * @returns True when any `links` array is present at any depth.
 */
export function payloadTouchesLinks(payload: unknown): boolean {
    const visit = (value: unknown, depth: number): boolean => {
        if (depth > MAX_WALK_DEPTH) return false;

        if (Array.isArray(value)) {
            return value.some((item) => visit(item, depth + 1));
        }

        const record = asPlainObject(value);
        if (!record) return false;

        if (Object.hasOwn(record, 'links') && Array.isArray(record.links)) {
            return true;
        }

        return Object.values(record).some((nested) => visit(nested, depth + 1));
    };

    return visit(payload, 0);
}

/**
 * Property names that carry no layout, and so cannot take a link column with them.
 *
 * This is an allowlist because the destructive set cannot be enumerated. An earlier
 * version asked the opposite question — "does this payload replace a `columns` array?"
 * — and a details or form view's layout nests as `groups[].columns[]`, so `{groups: []}`
 * cleared the whole layout, link columns included, while presenting as a `groups` write
 * with no `columns` array anywhere in it. Every shape that did not name `columns`
 * outright had the same free pass: `{groups: [{label: "x"}]}`, a non-array `columns`,
 * and whatever layout key Knack adds next.
 *
 * Listing the few provably-flat properties instead means an unfamiliar key is treated as
 * structural, which is the direction that fails closed. The cost of a false positive is
 * one confirmation prompt on a harmless edit; the cost of a false negative is a page.
 */
const SCALAR_SAFE_UPDATE_KEYS = new Set([
    'description',
    'label',
    'name',
    'title',
]);

/**
 * Report whether a payload writes anything that could carry a link column away with it.
 *
 * @param payload Parsed update payload.
 * @returns True unless every property it writes is a known layout-free one.
 */
export function payloadTouchesStructure(payload: unknown): boolean {
    const keys = collectPayloadKeys(payload);
    if (keys.length === 0) return false;
    return keys.some((key) => !SCALAR_SAFE_UPDATE_KEYS.has(key));
}

/**
 * Sort each linked page into owned, external, or unknown.
 *
 * Only a page whose parent resolves to *a different, real scene* is downgraded to
 * external. That is the one case carrying positive evidence that the page exists
 * independently of this link. Everything else — no parent, an unresolvable parent, an
 * unresolvable reference — stays at risk, because absence of evidence is not evidence
 * of safety and the cost of being wrong here is pages nobody agreed to lose.
 *
 * @param directSceneRefs Scene references taken from link columns and menu links.
 * @param scenes The app's scene list, carrying parent pointers.
 * @param ownerSceneKey The scene holding the view being mutated.
 * @returns One entry per reference, in the order given.
 */
export function classifyLinkTargets(
    directSceneRefs: string[],
    scenes: SceneNode[],
    ownerSceneKey: string,
): ClassifiedLinkTarget[] {
    const byKey = new Map<string, SceneNode>();
    const bySlug = new Map<string, SceneNode>();
    for (const scene of scenes) {
        byKey.set(scene.sceneKey, scene);
        if (scene.sceneSlug) bySlug.set(scene.sceneSlug.toLowerCase(), scene);
    }

    const resolve = (ref: string): SceneNode | null =>
        byKey.get(ref) ?? bySlug.get(ref.trim().toLowerCase()) ?? null;

    return directSceneRefs.map((ref): ClassifiedLinkTarget => {
        const scene = resolve(ref);

        if (!scene) {
            return {
                ref,
                sceneKey: null,
                sceneName: null,
                sceneSlug: null,
                classification: 'unknown',
                parentSceneKey: null,
                reason: 'this reference matches no page in the app, so what it points at cannot be established',
            };
        }

        const base = {
            ref,
            sceneKey: scene.sceneKey,
            sceneName: scene.sceneName ?? null,
            sceneSlug: scene.sceneSlug ?? null,
        };

        if (!scene.parentRef) {
            return {
                ...base,
                classification: 'unknown',
                parentSceneKey: null,
                reason: 'this page declares no parent. It may be a top-level page that survives, or its parent may be missing from the metadata — the two are indistinguishable here',
            };
        }

        const parent = resolve(scene.parentRef);
        if (!parent) {
            return {
                ...base,
                classification: 'unknown',
                parentSceneKey: null,
                reason: `this page names "${scene.parentRef}" as its parent, which matches no page in the app, so its ownership cannot be established`,
            };
        }

        if (parent.sceneKey === ownerSceneKey) {
            return {
                ...base,
                classification: 'owned',
                parentSceneKey: parent.sceneKey,
                reason: 'this page hangs off the page being changed, so it exists only because of this link',
            };
        }

        return {
            ...base,
            classification: 'external',
            parentSceneKey: parent.sceneKey,
            reason: `this page hangs off ${parent.sceneKey}${
                parent.sceneName ? ` ("${parent.sceneName}")` : ''
            }, not off the page being changed, so removing the link removes navigation and leaves the page in place`,
        };
    });
}

/**
 * Expand directly-linked child pages into every page that dies with them.
 *
 * A child page may own children of its own, and those are lost too when the parent is
 * cascade-deleted. Callers must acknowledge the whole set, not just the first level.
 *
 * @param directSceneKeys Scene keys referenced by link columns or menu links.
 * @param scenes The app's scene list, carrying parent pointers.
 * @returns Every affected page plus whether the walk stopped before the tree ended.
 */
export function expandChildPages(
    directSceneRefs: string[],
    scenes: SceneNode[],
): { pages: ChildPage[]; truncated: boolean; unresolvedRefs: string[] } {
    // Knack writes slugs in both places a reference appears — a link column's `scene`
    // and a scene's `parent` — while `key` holds the scene_N identifier. Indexing only
    // by key meant the seed matched nothing and the parent map matched nothing, so the
    // walk reported the directly-linked page (unnamed, since the lookup missed) and
    // stopped. On a real app that under-reported 5 doomed pages as 3, which is the
    // worst failure available here: the human confirms, and loses more than they
    // agreed to. Resolve every reference through both indexes before walking.
    const byKey = new Map<string, SceneNode>();
    const bySlug = new Map<string, SceneNode>();
    for (const scene of scenes) {
        byKey.set(scene.sceneKey, scene);
        if (scene.sceneSlug) bySlug.set(scene.sceneSlug.toLowerCase(), scene);
    }

    const resolve = (ref: string): SceneNode | null =>
        byKey.get(ref) ?? bySlug.get(ref.trim().toLowerCase()) ?? null;

    const childrenByParentKey = new Map<string, SceneNode[]>();
    for (const scene of scenes) {
        if (!scene.parentRef) continue;
        const parent = resolve(scene.parentRef);
        if (!parent) continue;
        const siblings = childrenByParentKey.get(parent.sceneKey) ?? [];
        siblings.push(scene);
        childrenByParentKey.set(parent.sceneKey, siblings);
    }

    // A reference matching neither a key nor a slug names a page we cannot describe.
    // Reported so the guard counts it as risk rather than quietly omitting it.
    const unresolvedRefs: string[] = [];
    const seen = new Set<string>();
    const pages: ChildPage[] = [];

    let frontier: SceneNode[] = [];
    for (const ref of directSceneRefs) {
        const scene = resolve(ref);
        if (!scene) {
            if (!unresolvedRefs.includes(ref)) unresolvedRefs.push(ref);
            continue;
        }
        if (seen.has(scene.sceneKey)) continue;
        seen.add(scene.sceneKey);
        frontier.push(scene);
    }

    let depth = 0;
    while (frontier.length > 0 && depth <= MAX_WALK_DEPTH) {
        for (const scene of frontier) {
            pages.push({
                sceneKey: scene.sceneKey,
                sceneName: scene.sceneName ?? null,
                sceneSlug: scene.sceneSlug ?? null,
                depth,
            });
        }

        const next: SceneNode[] = [];
        for (const scene of frontier) {
            for (const child of childrenByParentKey.get(scene.sceneKey) ?? []) {
                if (seen.has(child.sceneKey)) continue;
                seen.add(child.sceneKey);
                next.push(child);
            }
        }

        frontier = next;
        depth += 1;
    }

    // Frontier still populated means the page tree ran deeper than the walk. Reporting
    // that lets the guard refuse rather than confirm a partial list of what dies.
    return {
        pages,
        truncated: frontier.length > 0,
        unresolvedRefs: unresolvedRefs.sort(),
    };
}

/**
 * Reduce a caller-supplied key to something safe to put in a filename.
 *
 * Scene and view keys arrive as tool arguments, so they are untrusted input. Joined
 * into a path unsanitised, a value containing `../` escapes the snapshots directory
 * and can overwrite an unrelated file — including an earlier restore point.
 *
 * @param value Raw scene/view key, or any caller-supplied label.
 * @returns The value reduced to `[A-Za-z0-9_-]`, truncated, never empty.
 */
export function sanitiseFileNameComponent(value: string): string {
    const cleaned = value.replace(/[^A-Za-z0-9_-]+/g, '_').slice(0, 64);
    // A value of only separators collapses to underscores; treat that as unusable
    // rather than emitting a filename made entirely of them.
    return /[A-Za-z0-9]/.test(cleaned) ? cleaned : 'unnamed';
}

/**
 * Collect every property name a payload would write, at any depth.
 *
 * The walk has to be recursive because payloadTouchesStructure is built on it: a
 * top-level-only scan reads `{groups: [{columns: []}]}` as a `groups` write and never
 * sees the layout underneath, which is how a structural payload passed for a flat one.
 *
 * @param payload Parsed update payload.
 * @returns Sorted, deduped property names found anywhere in the payload.
 */
export function collectPayloadKeys(payload: unknown): string[] {
    const keys = new Set<string>();

    const visit = (value: unknown, depth: number): void => {
        if (depth > MAX_WALK_DEPTH) return;

        if (Array.isArray(value)) {
            value.forEach((item) => visit(item, depth + 1));
            return;
        }

        const record = asPlainObject(value);
        if (!record) return;

        for (const [key, nested] of Object.entries(record)) {
            keys.add(key);
            visit(nested, depth + 1);
        }
    };

    visit(payload, 0);
    return [...keys].sort();
}

// -----------------------
// Orchestrator
// -----------------------

export type FetchViewResult = {
    ok: boolean;
    status: number;
    body?: unknown;
};

export type SnapshotResult =
    | { ok: true; path: string; schemaIncluded?: boolean }
    | { ok: false; error: string };

export type PageDeletionConfirmation =
    | { supported: false; reason?: string }
    | {
          supported: true;
          accepted: boolean;
          outcome?: 'accept' | 'decline' | 'cancel' | 'timeout' | 'error';
      };

export type ViewMutationDeps = {
    /** Read the live view. Must not mutate anything. */
    fetchView: (sceneKey: string, viewKey: string) => Promise<FetchViewResult>;
    /**
     * The app's scene list, carrying parent pointers for descendant expansion.
     *
     * Returns a failure rather than an empty list when the tree cannot be read. The
     * two are indistinguishable to a caller, and treating an unreadable tree as "no
     * descendants" is what would let a confirmation prompt under-report the damage.
     */

    listScenes: () => Promise<
        { ok: true; scenes: SceneNode[] } | { ok: false; reason: string }
    >;
    /** Persist a timestamped restore point. A failure must block the mutation. */
    writeSnapshot: (input: {
        action: ViewMutationAction;
        sceneKey: string;
        viewKey?: string;
        view: unknown;
    }) => Promise<SnapshotResult>;
    /** Builder deep link for the scene, used in refusal messages. */
    builderUrlForScene?: (sceneKey: string) => string | null;

    /**
     * Ask the human — not the model — to confirm a cascade delete.
     *
     * This goes to the MCP client, so the calling agent cannot answer it on the
     * user's behalf. Omitted or reporting `supported: false`, the guard falls back
     * to whatever the app's cascadeConfirmationFallback allows.
     */

    confirmPageDeletion?: (input: {
        action: ViewMutationAction;
        sceneKey: string;
        viewKey?: string;
        childPages: ChildPage[];
        /**
         * Links whose pages live elsewhere in the tree and survive. Shown so the person
         * confirming sees the whole consequence, not only the destructive half.
         */
        externalPages: ClassifiedLinkTarget[];
        /** Links whose target page could not be identified, so cannot be listed. */
        unresolvedLinkCount: number;
    }) => Promise<PageDeletionConfirmation>;
};

export type ViewMutationRequest = {
    action: ViewMutationAction;
    sceneKey: string;
    viewKey?: string;
    /** Raw JSON string exactly as the caller supplied it. */
    updates?: string;
    /** Legacy flag. Any use is refused so old callers fail closed. */
    confirmDestructive?: boolean;
};

export type ViewMutationDecision =
    | {
          allowed: true;
          viewType: string | null;
          snapshotPath?: string;
          childPages: ChildPage[];
          acknowledgedPages: string[];
          /**
           * The view's live definition as the preflight read it.
           *
           * Knack's existing-view PUT is a replace, not a patch: a partial body is
           * rejected with an opaque HTTP 500. A caller that wants to change one
           * property has to send the whole definition with that property altered, so
           * the definition the guard already fetched is handed on rather than read
           * twice.
           */
          currentAttributes: Record<string, unknown> | null;
          /**
           * Whether the view carries any node pointing at a page.
           *
           * Decides whether a complete definition can be safely reconstructed: for a
           * view with no page links it can, and that is measured to work. For one that
           * has them, a complete definition necessarily re-sends the link columns, and
           * whether that cascades is exactly the premise still unmeasured.
           */
          hasPageLinks: boolean;
      }
    | {
          allowed: false;
          code: ViewSafetyErrorCode;
          message: string;
          details?: Record<string, unknown>;
      };

function refuse(
    code: ViewSafetyErrorCode,
    message: string,
    details?: Record<string, unknown>,
): ViewMutationDecision {
    return { allowed: false, code, message, ...(details ? { details } : {}) };
}

/**
 * Decide whether a view mutation may proceed, and take its restore point if so.
 *
 * The checks run before the snapshot so a refused call costs no disk, but no `allowed`
 * result is ever returned without a snapshot on disk first. Every branch that refuses
 * returns before any caller-visible mutation, which is what the tool-level tests assert.
 *
 * @param deps Injected I/O so the rules can be tested without a Knack app.
 * @param request The mutation the caller is attempting.
 * @returns A decision carrying either the snapshot path or a specific refusal.
 */
export async function guardViewMutation(
    deps: ViewMutationDeps,
    request: ViewMutationRequest,
): Promise<ViewMutationDecision> {
    const { action, sceneKey, viewKey } = request;
    const builderUrl = deps.builderUrlForScene?.(sceneKey) ?? null;
    const builderHint = builderUrl
        ? ` Make this change in the Knack builder instead: ${builderUrl}`
        : ' Make this change in the Knack builder instead.';

    // 1. Parse the payload once. An unparseable payload is refused rather than passed
    //    through unchecked.
    let parsedUpdates: unknown;
    if (request.updates !== undefined) {
        try {
            parsedUpdates = JSON.parse(request.updates);
        } catch (error) {
            return refuse(
                'INVALID_UPDATES_JSON',
                `updates could not be parsed as JSON, so it cannot be checked for navigation changes: ${
                    error instanceof Error ? error.message : String(error)
                }`,
            );
        }
    }

    // 2. Nothing below can see past MAX_WALK_DEPTH, and every check fails permissive
    //    when it runs out of depth. Refuse first rather than analyse a structure only
    //    partly, then report the partial answer as though it were the whole one.
    if (parsedUpdates !== undefined && exceedsMaxDepth(parsedUpdates)) {
        return refuse(
            'STRUCTURE_TOO_DEEP',
            'This payload nests deeper than the safety checks will follow, so it cannot be searched reliably for links or layout structure. Refusing rather than reporting a partial answer as a clean one. Flatten the payload, or make the change in the Knack builder.',
        );
    }

    // 2b. A payload writing no properties reaches the API unexamined: every check
    //     below keys off what it writes, and it writes nothing. It cannot do anything
    //     useful either, so refuse rather than send a PUT no rule has looked at.
    if (
        action === 'update_view' &&
        parsedUpdates !== undefined &&
        collectPayloadKeys(parsedUpdates).length === 0
    ) {
        return refuse(
            'EMPTY_UPDATE_PAYLOAD',
            'This update writes no properties, so it would send a PUT that none of the safety checks can evaluate and that changes nothing. Send the properties you mean to change.',
        );
    }

    // 3. A links payload is refused on every view type that already exists. The hazard
    //    is replacement: Knack rebuilds navigation from what it receives and deletes the
    //    child pages of links it no longer sees, so `links: []` is the most destructive
    //    payload of all, not the most harmless.
    //
    //    A create has nothing to replace, and every payload this server generates carries
    //    `links: []` — knack_get_view_payload_template emits it for table, form, details
    //    and list — so refusing there blocked view creation outright.
    if (
        action !== 'create_view' &&
        parsedUpdates !== undefined &&
        payloadTouchesLinks(parsedUpdates)
    ) {
        return refuse(
            'BLOCKED_LINKS_PAYLOAD',
            `This payload contains a links array, which replaces the view's navigation. Knack rebuilds navigation from what it receives and cascade-deletes the child pages of any link it no longer sees — an empty links array clears all of them. Send only the properties you are changing, without links.${builderHint}`,
        );
    }

    // 4. Preflight only actions that can delete an existing view's child pages. An
    //    unreadable source is indistinguishable from one with no links, while copying
    //    does not change the source and creating and sorting have none to inspect.
    let attributes: Record<string, unknown> | null = null;
    let rawView: unknown;
    const requiresExistingView =
        action === 'update_view' ||
        action === 'move_view' ||
        action === 'delete_view';

    if (viewKey && requiresExistingView) {
        const current = await deps.fetchView(sceneKey, viewKey);
        if (!current.ok) {
            return refuse(
                'COULD_NOT_VERIFY_VIEW',
                `Could not read ${viewKey} (status ${current.status}) to check it for links and link columns before mutating it. Refusing to proceed without that check — retry once the view is readable.`,
                { status: current.status },
            );
        }
        rawView = current.body;
        attributes = resolveViewAttributes(current.body);

        // Same reasoning for the live view: a layout we cannot walk to the bottom of
        // may hide link columns from collectLinkTargets.
        if (exceedsMaxDepth(attributes)) {
            return refuse(
                'STRUCTURE_TOO_DEEP',
                `${viewKey} nests deeper than the safety checks will follow, so it cannot be searched reliably for link columns. Refusing rather than assuming the links it may contain are not there. Make this change in the Knack builder.${builderHint}`,
                { viewKey },
            );
        }
    }

    const viewType = getViewType(attributes);

    // 5. A view we read but cannot identify could be anything, a menu included. This
    //    has to run before any action-specific classification: leaving it to the
    //    update_view branch let an untyped view be moved, since isMenuView() is false
    //    for a view with no type and nothing downstream re-checked it.
    if (requiresExistingView && viewKey && !viewType) {
        return refuse(
            'UNKNOWN_VIEW_TYPE',
            `${viewKey} was read successfully but declares no view type, so it cannot be checked against the menu rule. Refusing rather than assuming it is safe.${builderHint}`,
            { viewKey },
        );
    }

    // 6. Menus are disqualified on their type alone — not on what the payload happens
    //    to contain. There is deliberately no override parameter for this.
    if (isMenuView(attributes)) {
        if (action === 'update_view') {
            return refuse(
                'BLOCKED_MENU_VIEW_UPDATE',
                `${viewKey} is a menu view. Menu views are never updatable through this server: a menu PUT rebuilds the links array and cascade-deletes the child pages behind any link it no longer sees. There is no override.${builderHint}`,
                { viewKey, viewType },
            );
        }
        if (action === 'move_view') {
            return refuse(
                'BLOCKED_MENU_VIEW_MOVE',
                `${viewKey} is a menu view. Moving a menu between scenes is a navigation change and is not supported through this server. There is no override.${builderHint}`,
                { viewKey, viewType },
            );
        }
    }

    // 7. Cascade check. These are the actions that can take a link column's child page
    //    with them: writing any part of the view's structure, deleting the view, or
    //    moving it off its scene.
    const linkTargets = collectLinkTargets(attributes);

    // A link whose `scene` we could not read is not evidence that no child page
    // exists — it is evidence that we cannot see which one. Counting those as risk
    // keeps an unfamiliar or malformed link shape from skipping confirmation, which
    // is exactly how a silent page deletion would get through.
    const unresolvedLinks = [
        ...linkTargets.linkColumns,
        // A `url` link points outside the app and has no child scene by definition.
        // Counting its absent scene as "could not resolve" made every view holding an
        // external link permanently risky, and undeletable on the acknowledgement
        // fallback, with nothing the user could do to clear it.
        ...linkTargets.menuLinks.filter((link) => link.linkType !== 'url'),
    ].filter((link) => !link.childSceneRef);

    const destructiveAction =
        (action === 'update_view' && payloadTouchesStructure(parsedUpdates)) ||
        action === 'delete_view' ||
        action === 'move_view';

    const cascadeRisked =
        destructiveAction &&
        (linkTargets.childSceneRefs.length > 0 || unresolvedLinks.length > 0);

    let childPages: ChildPage[] = [];

    if (cascadeRisked) {
        // A legacy boolean must never authorize a cascade delete. It is harmless on
        // non-cascading updates, though, so accepting those preserves old clients'
        // ordinary title and configuration edits.
        if (request.confirmDestructive === true) {
            return refuse(
                'CONFIRMATION_UPGRADE_REQUIRED',
                'confirmDestructive cannot authorize a cascade delete. Re-run without it so the human confirmation can identify the pages at risk.',
            );
        }

        const sceneTree = await deps.listScenes();
        if (!sceneTree.ok) {
            return refuse(
                'SCENE_TREE_UNAVAILABLE',
                `This ${action} destroys child pages, but the app's page tree could not be read (${sceneTree.reason}), so the full set cannot be worked out. An unreadable tree is not an empty one — refusing rather than confirming a list that may be missing pages.`,
                { linkColumns: linkTargets.linkColumns },
            );
        }

        // Only pages that owe their existence to this link are destroyed by removing
        // it. A page living elsewhere in the tree keeps its place, and its link stays
        // connected — measured on a real app. Counting those as doomed made the prompt
        // overstate, and a prompt that exaggerates is one people learn to click past.
        const classified = classifyLinkTargets(
            linkTargets.childSceneRefs,
            sceneTree.scenes,
            sceneKey,
        );
        const externalTargets = classified.filter(
            (target) => target.classification === 'external',
        );
        const atRiskRefs = classified
            .filter((target) => target.classification !== 'external')
            .map((target) => target.ref);

        const expansion = expandChildPages(atRiskRefs, sceneTree.scenes);
        if (expansion.truncated) {
            return refuse(
                'STRUCTURE_TOO_DEEP',
                `This ${action} destroys a page tree that nests deeper than this server will walk, so the full list of pages cannot be enumerated. Refusing rather than asking for consent to a partial list. Make this change in the Knack builder.`,
                { knownChildPages: expansion.pages },
            );
        }

        childPages = expansion.pages;
        const requiredKeys = childPages.map((page) => page.sceneKey);

        // A reference naming no scene in the tree is a page we cannot list, exactly
        // like a link whose `scene` could not be read. Both have to reach the prompt
        // as "more may die than are shown" rather than being silently dropped.
        const unresolvedCount =
            unresolvedLinks.length + expansion.unresolvedRefs.length;

        // Every link points at a page that survives, so this mutation destroys
        // nothing and there is nothing to put to a human. The snapshot below is still
        // written: the classification rests on Knack's metadata being accurate about
        // parentage, and a restore point costs nothing set against that assumption
        // being wrong.
        const destroysNothing =
            requiredKeys.length === 0 && unresolvedCount === 0;

        // Ask the human. This request goes to the MCP client, not the model, so the
        // calling agent cannot answer it for the user. There is no second route: a
        // client that cannot prompt cannot cascade-delete through this server.
        const confirmation = destroysNothing
            ? ({ supported: true, accepted: true, outcome: 'accept' } as const)
            : deps.confirmPageDeletion
              ? await deps.confirmPageDeletion({
                    action,
                    sceneKey,
                    viewKey,
                    childPages,
                    externalPages: externalTargets,
                    unresolvedLinkCount: unresolvedCount,
                })
              : ({ supported: false } as PageDeletionConfirmation);

        if (confirmation.supported) {
            if (!confirmation.accepted) {
                return refuse(
                    'HUMAN_CONFIRMATION_DECLINED',
                    `This ${action} destroys ${requiredKeys.length} page(s) and was not confirmed (${confirmation.outcome ?? 'declined'}). Nothing was changed. Do not retry without being asked to — the person who declined has seen exactly which pages were at stake.`,
                    { childPages, outcome: confirmation.outcome ?? 'decline' },
                );
            }
        } else {
            // No human reachable, no deletion. There was once a typed-acknowledgement
            // fallback here, where the caller echoed the page keys back. It was removed
            // because it proved only that the preflight had been read: the refusal
            // handed over the exact sentence needed to satisfy it, so an agent could
            // retry in the same turn without surfacing anything to a person. A consent
            // mechanism the caller can satisfy alone is not consent.
            return refuse(
                'HUMAN_CONFIRMATION_UNAVAILABLE',
                `This ${action} destroys ${requiredKeys.length} page(s), and this MCP client cannot prompt a human to confirm it${confirmation.reason ? ` (${confirmation.reason})` : ''}. Refusing rather than letting the caller confirm on the user's behalf — there is no override.${builderHint}`,
                {
                    childPages,
                    linkColumns: linkTargets.linkColumns,
                    menuLinks: linkTargets.menuLinks,
                    unresolvedLinkCount: unresolvedCount,
                },
            );
        }
    }

    let snapshotPath: string | undefined;
    if (requiresExistingView) {
        // 8. Restore point. No snapshot on disk, no source mutation — a full disk or a
        //    misconfigured KNACK_APPS_DIR halts mutations that can remove an existing
        //    view or its child pages. It does not block safe creates, copies or sorting.
        const snapshot = await deps.writeSnapshot({
            action,
            sceneKey,
            viewKey,
            view: rawView,
        });
        if (!snapshot.ok) {
            return refuse(
                'SNAPSHOT_FAILED',
                `Could not write the pre-mutation snapshot, so nothing was sent to Knack: ${snapshot.error}. Fix the snapshot path (KNACK_APPS_DIR / the app folder) and retry.`,
            );
        }
        snapshotPath = snapshot.path;
    }

    return {
        allowed: true,
        viewType,
        snapshotPath,
        childPages,
        acknowledgedPages: childPages.map((page) => page.sceneKey),
        currentAttributes: attributes,
        hasPageLinks:
            linkTargets.childSceneRefs.length > 0 || unresolvedLinks.length > 0,
    };
}

/**
 * Run a view mutation only if the guard allows it.
 *
 * The `perform` callback holds the actual Knack request, and is unreachable from every
 * refusal branch above. Routing all six view tools through this one function is what
 * makes the rule a property of the server rather than something a caller remembers —
 * and lets the tests assert, against this same code path, that a refused request never
 * reaches the transport.
 *
 * @param deps Injected I/O.
 * @param request The mutation being attempted.
 * @param perform Sends the real request. Destructive source mutations are invoked only
 *     after a snapshot is on disk.
 * @returns The performed result, or the guard's refusal.
 */
export async function runGuardedViewMutation<T>(
    deps: ViewMutationDeps,
    request: ViewMutationRequest,
    perform: (context: {
        snapshotPath?: string;
        viewType: string | null;
        childPages: ChildPage[];
        currentAttributes: Record<string, unknown> | null;
        hasPageLinks: boolean;
    }) => Promise<T>,
): Promise<
    | {
          ok: true;
          result: T;
          snapshotPath?: string;
          viewType: string | null;
          acknowledgedPages: string[];
      }
    | {
          ok: false;
          code: ViewSafetyErrorCode;
          message: string;
          details?: Record<string, unknown>;
      }
> {
    const decision = await guardViewMutation(deps, request);

    if (!decision.allowed) {
        return {
            ok: false,
            code: decision.code,
            message: decision.message,
            ...(decision.details ? { details: decision.details } : {}),
        };
    }

    const result = await perform({
        snapshotPath: decision.snapshotPath,
        viewType: decision.viewType,
        childPages: decision.childPages,
        currentAttributes: decision.currentAttributes,
        hasPageLinks: decision.hasPageLinks,
    });

    return {
        ok: true,
        result,
        snapshotPath: decision.snapshotPath,
        viewType: decision.viewType,
        acknowledgedPages: decision.acknowledgedPages,
    };
}
