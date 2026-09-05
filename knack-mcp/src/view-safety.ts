/**
 * View-mutation safety rules for the Knack MCP server.
 *
 * Knack's view PUT **replaces rather than patches**, and cascade-deletes the child page
 * behind any link the definition it receives no longer carries. Re-sending a link is
 * safe; dropping one is what destroys the page behind it, and only when that was its
 * last referring link. Measured on a real app, for link columns and for a menu's
 * `links` array alike — the container makes no difference.
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
    | 'MALFORMED_PAGE_SPECIFICATION'
    | 'STORED_PAGE_SPECIFICATION'
    | 'CONFIRMATION_UPGRADE_REQUIRED'
    | 'COULD_NOT_VERIFY_VIEW'
    | 'HUMAN_CONFIRMATION_UNAVAILABLE'
    | 'STRUCTURE_TOO_DEEP'
    | 'SOURCE_OBJECT_CHANGE_REFUSED'
    | 'PARTIAL_SOURCE_REPLACEMENT'
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
    /**
     * Whether the entry carries a `url`, which settles that it points outside the app
     * whatever its type says. Knack writes `type: "url"` for these, but not always —
     * and a missing type is not evidence of a page.
     */
    hasUrl: boolean;
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

/**
 * One view and the pages its links point at.
 *
 * The refs are Knack's own strings — slugs in practice — exactly as
 * collectLinkTargets returns them, and are resolved through the slug index like every
 * other reference here.
 */
export type SceneViewLinks = {
    viewKey: string;
    childSceneRefs: string[];
};

/** A view that links to a page, named alongside the page it lives on. */
export type PageReferrer = {
    sceneKey: string;
    viewKey: string;
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
    /**
     * Every view on this page, and the pages each of those views links to.
     *
     * Optional deliberately. Knack deletes a child page when its **last** referring
     * link goes — not when *a* link to it goes. A page a second view still points at
     * survives and re-parents onto that view instead, which is what the builder does
     * and what a two-arm test on a real app measured. Deciding that needs the whole
     * app's links rather than the mutating view's, so a deps implementation that
     * cannot supply them leaves this undefined and gets the older, more pessimistic
     * answer rather than a wrong one.
     */
    views?: SceneViewLinks[];
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
 * `transferred` is the same page-survives outcome reached by the other route: the page
 * does hang off the page being changed, but another view links to it too, so this link
 * is not the last one. Measured on a real app — removing the link column moved the
 * child page under the other referring view rather than deleting it, which is exactly
 * what the Knack builder does.
 *
 * `unknown` exists because the two evidence-free cases are indistinguishable: a page
 * with no `parent` may be genuinely top-level, or its parent may simply be absent from
 * the metadata we read. Treating either as external is how a human confirms and loses
 * more than they agreed to, so both stay at risk — including when another view appears
 * to link to them, since an index built from metadata that lost a parent pointer is no
 * sounder than the pointer it lost.
 */
export type LinkTargetClass = 'owned' | 'external' | 'transferred' | 'unknown';

export type ClassifiedLinkTarget = {
    /** The reference as it appeared on the link — a slug, usually. */
    ref: string;
    sceneKey: string | null;
    sceneName: string | null;
    sceneSlug: string | null;
    classification: LinkTargetClass;
    /** The owner this page declares, resolved to a scene key where possible. */
    parentSceneKey: string | null;
    /**
     * Views other than the one being mutated that also link to this page.
     *
     * Empty both when nothing else links here and when the scene list carried no view
     * lists to count. `classification` tells those apart; this array does not.
     */
    otherReferrers: PageReferrer[];
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
 * Resolve a Knack page reference against a scene list.
 *
 * Knack writes slugs where a reference appears — a link column's `scene`, a scene's
 * `parent` — while `key` holds the `scene_N` identifier, so every lookup has to try
 * both. Shared rather than rebuilt per call site because a resolver that indexes only
 * by key silently matches nothing, and the symptom is an under-reported cascade.
 *
 * @param scenes The app's scene list.
 * @returns A lookup taking either form of reference.
 */
function makeSceneResolver(
    scenes: SceneNode[],
): (ref: string) => SceneNode | null {
    const byKey = new Map<string, SceneNode>();
    const bySlug = new Map<string, SceneNode>();
    for (const scene of scenes) {
        byKey.set(scene.sceneKey, scene);
        if (scene.sceneSlug) bySlug.set(scene.sceneSlug.toLowerCase(), scene);
    }

    return (ref: string) =>
        byKey.get(ref) ?? bySlug.get(ref.trim().toLowerCase()) ?? null;
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
 * The names of pages a payload asks Knack to create.
 *
 * A menu create posts `scene` as `{name, parent, views}` — a page specification rather
 * than a reference. `readSceneProperty` cannot resolve one, so it counts as an
 * unreadable link, and that is left exactly as it was: an unreadable link in the
 * outgoing body counts toward *retention*, so a specification nets zero drops and asks
 * nothing, which is the right answer. The arithmetic is untouched here on purpose.
 *
 * What was missing is the reporting. Without this, an operator was told "unreadable
 * link" about a page they were deliberately adding, and a review caught that the
 * predicate had been added and then wired nowhere.
 *
 * @param value A view schema or update payload.
 * @returns Each specified page's name, in walk order, deduped.
 */
export function collectScenePageSpecifications(value: unknown): string[] {
    const names = new Set<string>();

    const walk = (node: unknown, depth: number): void => {
        if (depth > MAX_WALK_DEPTH) return;

        if (Array.isArray(node)) {
            for (const item of node) walk(item, depth + 1);
            return;
        }

        const record = asPlainObject(node);
        if (!record) return;

        if (
            Object.hasOwn(record, 'scene') &&
            isScenePageSpecification(record.scene)
        ) {
            const name = asTrimmedString(asPlainObject(record.scene)?.name);
            if (name) names.add(name);
        }

        for (const nested of Object.values(record)) walk(nested, depth + 1);
    };

    walk(value, 0);
    return [...names];
}

export type MalformedPageSpecification = {
    /** The page the specification names. */
    name: string;
    /** What is wrong with it, in the words the refusal uses. */
    problem:
        'the link has no type "scene"' | 'the specification has no views array';
};

/**
 * Page specifications this payload posts in a shape Knack mishandles.
 *
 * Measured live on 5 September (TESTED.md §9), one `PUT` per row. A well-formed
 * specification — `{name, type: "scene", scene: {name, parent, views: []}}` — creates
 * its page on a create and on an update alike, and the stored link comes back as the
 * new page's slug. Two departures from it do not:
 *
 * - **No `views` array.** Knack stores the specification object in the link verbatim
 *   and creates no page. The link points at nothing, and it was our request that
 *   wrote it.
 * - **No `type: "scene"` on the link.** Knack creates the page and *keeps* the object
 *   in the link, so every later save of the view — this server merges and re-sends
 *   the whole definition, and so does the builder — creates the page again under a
 *   new slug.
 *
 * Neither can be what a caller meant, and the well-formed shape costs them nothing.
 *
 * @param value A caller-supplied payload.
 * @returns One entry per problem, in walk order. Empty for a clean payload.
 */
export function collectMalformedScenePageSpecifications(
    value: unknown,
): MalformedPageSpecification[] {
    const found: MalformedPageSpecification[] = [];

    const walk = (node: unknown, depth: number): void => {
        if (depth > MAX_WALK_DEPTH) return;

        if (Array.isArray(node)) {
            for (const item of node) walk(item, depth + 1);
            return;
        }

        const record = asPlainObject(node);
        if (!record) return;

        if (
            Object.hasOwn(record, 'scene') &&
            isScenePageSpecification(record.scene)
        ) {
            const spec = asPlainObject(record.scene);
            const name = asTrimmedString(spec?.name) ?? '';
            if (record.type !== 'scene') {
                found.push({ name, problem: 'the link has no type "scene"' });
            }
            if (!Array.isArray(spec?.views)) {
                found.push({
                    name,
                    problem: 'the specification has no views array',
                });
            }
        }

        for (const nested of Object.values(record)) walk(nested, depth + 1);
    };

    walk(value, 0);
    return found;
}

export type CreatedPage = {
    sceneKey: string;
    sceneName: string | null;
    sceneSlug: string | null;
    /** The parent as Knack stored it — a slug when the request gave one. */
    parentRef: string | null;
};

/**
 * The pages Knack says it created, read out of a view-mutation response.
 *
 * Knack's response to a view `POST` or `PUT` carries `changes.inserts.scenes`, one
 * entry per page it made, each with its new key and slug. That is the only account
 * of what was created that does not come from this server's own reading of the
 * request — and the two differ: on 5 September a specification with no `views`
 * array was reported as created from the request while Knack's response listed
 * nothing (TESTED.md §9). What the caller is told was created has to come from here.
 *
 * @param body The response body from Knack.
 * @returns One entry per created page, in Knack's order. Empty when none is reported.
 */
export function readCreatedPagesFromResponse(body: unknown): CreatedPage[] {
    const scenes = asPlainObject(
        asPlainObject(asPlainObject(body)?.changes)?.inserts,
    )?.scenes;
    if (!Array.isArray(scenes)) return [];

    const pages: CreatedPage[] = [];
    for (const entry of scenes) {
        const scene = asPlainObject(entry);
        const sceneKey = asTrimmedString(scene?.key);
        if (!sceneKey) continue;
        pages.push({
            sceneKey,
            sceneName: asTrimmedString(scene?.name),
            sceneSlug: asTrimmedString(scene?.slug),
            parentRef: asTrimmedString(scene?.parent),
        });
    }
    return pages;
}

/**
 * The object a view's source names, when it can be read.
 *
 * @param value A view's attributes, or an update payload.
 * @returns The object key, or null when there is no readable `source.object`.
 */
function readSourceObject(value: unknown): string | null {
    const record = asPlainObject(value);
    if (!record) return null;

    const source = asPlainObject(record.source);
    if (!source) return null;

    return asTrimmedString(source.object);
}

/**
 * Whether a `scene` value is a **page specification** rather than a reference.
 *
 * A menu create posts its links as
 * `{name: "New Page 1", type: "scene", scene: {name, parent, views}}` — the `scene`
 * is not a pointer to an existing page, it is an instruction to make one. Knack
 * resolves it on save, and the stored form is always a slug: measured across the
 * export, 457 stored `scene` properties are slug strings, 2 are null, and **none**
 * is an object. So this shape reaches the guard only from a caller-supplied payload,
 * never from stored metadata.
 *
 * It matters for reporting rather than for safety. `readSceneProperty` cannot read a
 * reference out of it, so it already counts as an unreadable link — and because an
 * unreadable link in the outgoing body counts toward *retention*, a specification
 * nets zero drops and asks nothing. For a well-formed specification that is the right
 * outcome on a create and — measured live, 5 September — on an update too: Knack
 * creates the page and rewrites the link to its slug. A malformed one is another
 * matter. Without `views` Knack stores the object as-is and creates nothing; without
 * `type: "scene"` on the link it creates the page and still stores the object, so
 * every later save of the view creates the page again. Both are open in TESTING.md
 * §7. Naming the shape stops an operator being told "unreadable link" about a page
 * they are deliberately adding.
 *
 * @param value A node's raw `scene` value.
 * @returns True when the value describes a page to create rather than one to find.
 */
export function isScenePageSpecification(value: unknown): boolean {
    const record = asPlainObject(value);
    if (!record) return false;

    // A readable reference wins: `{key}`, `{scene}` and `{slug}` are pointers, and a
    // specification is only a specification when there is nothing to resolve.
    if (readSceneReference(record)) return false;

    return typeof record.name === 'string' && record.name.trim() !== '';
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
                hasUrl: asTrimmedString(record.url) !== null,
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
 * past the cap, `collectLinkTargets` finds no link columns, `collectPayloadKeys` omits
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
 * Does this payload still carry a link to the given page?
 *
 * The guard builds its link targets from the *current view*, never from the payload,
 * and then reports every one of them as severed. That is wrong whenever the payload
 * re-sends a link it already had: measured on a real app, a same-columns update
 * reported two links as removed while both the links and their pages stayed exactly
 * where they were. A field that says "removed" about something still present is worse
 * than no field, because it is the account a caller repeats to the user.
 *
 * It narrows both what is *reported* as severed and what is treated as at risk, and
 * the second of those is load-bearing: `dropsRef` in the guard decides the doomed set
 * with it. That is licensed by measurement, not by assumption — re-sending a link
 * inside the complete merged body was measured to destroy nothing, on two apps, and a
 * page was measured to die only with its last remaining link. This docblock used to
 * say the opposite, back when the premise was unmeasured and this only touched
 * reporting. Anything loosening the definition below now changes which pages get a
 * confirmation, so treat it as a safety change and re-measure rather than reason.
 *
 * @param payload Parsed update payload.
 * @param ref The scene reference to look for, as it appeared on the current view.
 * @returns True when the payload still carries a navigation link to that page.
 */
export function payloadRetainsSceneRef(payload: unknown, ref: string): boolean {
    const wanted = ref.trim().toLowerCase();
    if (!wanted) return false;

    // A `scene` value outside a navigation container does not retain a child page.
    // In particular, a form submit rule may redirect to the same page after saving;
    // treating that as a retained link lets a PUT drop the real, sole link without a
    // confirmation. Keep this definition identical to referrer indexing.
    return collectNavigationRefs(asPlainObject(payload)).some(
        (found) => found.trim().toLowerCase() === wanted,
    );
}

/**
 * The subset of a view's page references that sit in a navigation position.
 *
 * `collectLinkTargets` is deliberately broad: any node carrying a `scene` counts as a
 * link, whatever its type, because Knack names them inconsistently and on the view
 * being mutated a false positive costs one prompt while a false negative costs a page.
 *
 * **That polarity inverts when the same collector counts who _else_ links to a page.**
 * There an extra "link" makes a page look multi-referenced, which spares it from the
 * doomed set and skips the prompt — so on this side the broad reading destroys pages
 * rather than protecting them. A form's submit-rule redirect is enough to do it: it
 * carries a `scene`, it is not a link to anywhere, and counting it lets the last real
 * link to a page be cut with no confirmation at all.
 *
 * So referrer counting takes only nodes in a navigation position — an element of a
 * `links[]` array, or of a `columns[]` array, which covers table, search, details and
 * calendar layouts including their nested forms. Submit rules, record rules and action
 * rules are not navigation and are not counted, nor is whatever Knack adds next.
 * Missing a real referrer leaves a page in the doomed set and puts it to a human,
 * which is the direction that fails safe here.
 *
 * @param attributes View attributes as returned by resolveViewAttributes.
 * @returns Page references reached through navigation, deduped and sorted.
 */
export function collectNavigationRefs(
    attributes: Record<string, unknown> | null,
): string[] {
    const { linkColumns, menuLinks } = collectLinkTargets(attributes);

    const refs = new Set<string>();
    for (const link of menuLinks) {
        if (link.childSceneRef) refs.add(link.childSceneRef);
    }
    for (const column of linkColumns) {
        if (!column.childSceneRef) continue;
        if (isNavigationColumn(column)) {
            refs.add(column.childSceneRef);
        }
    }

    return [...refs].sort();
}

/**
 * Whether the candidate sits in a view layout's columns array.
 *
 * Action and submit rules can carry a `scene` destination too, but that is where an
 * action navigates after firing — it neither owns nor preserves a child page.
 */
function isNavigationColumn(column: LinkColumnTarget): boolean {
    const named = [
        ...column.sourcePath.matchAll(/([A-Za-z_][A-Za-z0-9_]*)\[\d+\]/g),
    ];
    return named.length > 0 && named[named.length - 1][1] === 'columns';
}

function countUnresolvedNavigationLinks(
    attributes: Record<string, unknown>,
    pointsOutsideTheApp: (link: MenuLinkTarget) => boolean,
): number {
    const targets = collectLinkTargets(attributes);
    return (
        targets.linkColumns.filter(
            (link) => isNavigationColumn(link) && !link.childSceneRef,
        ).length +
        targets.menuLinks.filter(
            (link) => !pointsOutsideTheApp(link) && !link.childSceneRef,
        ).length
    );
}

/**
 * Index every page in the app by the views that link to it.
 *
 * This is the whole-app view of navigation the cascade rule needs. Nothing in one
 * view's definition says whether another view still points at the same page, and that
 * is the difference between a link removal that deletes the page and one that hands it
 * to whoever else links to it.
 *
 * @param scenes The app's scene list, with per-scene view links where available.
 * @returns Referrers keyed by scene key, or null when no scene carried a view list.
 *     Null means "not measured", which is not the same as an app where nothing links
 *     anywhere — no page may be spared on it.
 */
export function buildReferrerIndex(
    scenes: SceneNode[],
): Map<string, PageReferrer[]> | null {
    if (!scenes.some((scene) => scene.views !== undefined)) return null;

    const resolve = makeSceneResolver(scenes);
    const index = new Map<string, PageReferrer[]>();

    for (const scene of scenes) {
        for (const view of scene.views ?? []) {
            for (const ref of view.childSceneRefs) {
                const target = resolve(ref);
                // An unresolvable reference is counted nowhere. It cannot spare a page
                // it does not name, and the page it might have named is already at
                // risk through expandChildPages' unresolved-ref reporting.
                if (!target) continue;

                const referrers = index.get(target.sceneKey) ?? [];
                // One view linking twice to the same page is one referrer, not two.
                // Counted twice, a view that is its page's sole referrer would look
                // like two and spare a page that is about to die.
                if (
                    referrers.some(
                        (entry) =>
                            entry.sceneKey === scene.sceneKey &&
                            entry.viewKey === view.viewKey,
                    )
                ) {
                    continue;
                }

                referrers.push({
                    sceneKey: scene.sceneKey,
                    viewKey: view.viewKey,
                });
                index.set(target.sceneKey, referrers);
            }
        }
    }

    return index;
}

/**
 * Build the body an update will actually send.
 *
 * Knack's existing-view PUT replaces rather than patches — measured: a body one column
 * short left the view with one column fewer, and the page behind that column was
 * deleted. So a caller changing one property has to send everything else unchanged,
 * and the guard has to judge the *merged* body rather than the caller's fragment.
 * Judging the fragment was the older behaviour and it read `{"title": "x"}` as
 * dropping every link in the view, which is true of the fragment and false of what
 * gets sent.
 *
 * `links` is carried through like any other property, and that is a deliberate
 * reversal. It used to be stripped, on the reading that a supplied `links` array is a
 * navigation replacement. But this body is a *complete definition*, so omitting
 * `links` does not leave navigation alone — against a route that replaces, it sends a
 * view with no links at all. On a table that is harmless, because its `links` is empty
 * anyway; on a menu it is the single most destructive body available, dropping every
 * link the menu has and every page behind them. Stripping was safe only for as long as
 * menus could never reach here.
 *
 * `key` and `_id` are still dropped — the identifier lives in the URL, and Knack
 * accepts the body without them.
 *
 * **The completeness of the source, which was long assumed:** the merge reads from
 * `applications/{appId}`, so it presumes that payload holds the whole view. A property
 * the builder kept and the payload omitted would be silently reset here on every edit,
 * and no amount of reading the view back could show it — the read comes from the same
 * payload that sourced the write.
 *
 * Settled by asking a different observer. The Knack builder is a web app, and its own
 * save request carries the definition as Knack's client believes it; diffing that
 * against the payload enumerates the gap rather than sampling for damage. On two
 * tables configured differently — one carrying `options` and `reportType`, the other
 * `allow_limit` and a populated `table_design` — the two agreed on every key but one.
 * Filters, sorts, totals, per-column rules, link designs, action rules with their
 * record and submit rules, and the table design block all appear in the payload with
 * the values the builder sends.
 *
 * The exception is `design`, which the builder sends and the payload omits. It was
 * `{}` on both views, including the one with table design fully switched on — the
 * populated settings live in `table_design`, which the payload does carry. So the one
 * key at risk holds nothing, on either side of that toggle.
 *
 * Two limits worth stating. The key set varies per view, so the diff is a per-view
 * check and not a fact about tables in general; and only tables were checked, leaving
 * details, form and calendar views unverified. The method is cheap now, though — one
 * builder save and one snapshot.
 *
 * @param attributes The view's live definition from the preflight.
 * @param patch The caller's requested changes, already parsed.
 * @returns The merged body, or null when there is no live definition to merge into.
 */
export function buildEffectiveUpdateBody(
    attributes: Record<string, unknown> | null,
    patch: unknown,
): Record<string, unknown> | null {
    if (!attributes) return null;
    const patchRecord = asPlainObject(patch);
    if (!patchRecord) return null;

    const body: Record<string, unknown> = { ...attributes, ...patchRecord };
    delete body.key;
    delete body._id;
    return body;
}

/**
 * Sort each linked page into owned, external, transferred, or unknown.
 *
 * Two cases carry positive evidence that a page survives this link being cut, and only
 * those two are downgraded. A page whose parent resolves to *a different, real scene*
 * exists independently of the link (external). A page that does hang off the page
 * being changed, but which another view also links to, is not losing its last referrer
 * (transferred) — Knack re-parents it onto that other view, the same thing the builder
 * does. Everything else — no parent, an unresolvable parent, an unresolvable
 * reference — stays at risk, because absence of evidence is not evidence of safety and
 * the cost of being wrong here is pages nobody agreed to lose.
 *
 * @param directSceneRefs Scene references taken from link columns and menu links.
 * @param scenes The app's scene list, carrying parent pointers and, where available,
 *     each scene's view links.
 * @param ownerSceneKey The scene holding the view being mutated.
 * @param ownerViewKey The view being mutated. Without it no referrer can be told from
 *     the link being cut, so the transferred class is not available and every page
 *     that would qualify stays owned.
 * @returns One entry per reference, in the order given.
 */
export function classifyLinkTargets(
    directSceneRefs: string[],
    scenes: SceneNode[],
    ownerSceneKey: string,
    ownerViewKey?: string,
): ClassifiedLinkTarget[] {
    const resolve = makeSceneResolver(scenes);
    const referrerIndex = ownerViewKey ? buildReferrerIndex(scenes) : null;

    const isOwnerView = (entry: PageReferrer): boolean =>
        entry.sceneKey === ownerSceneKey && entry.viewKey === ownerViewKey;

    /**
     * Referrers other than the view being mutated, or null when the index cannot be
     * trusted for this page.
     *
     * The index has to list this view's own link before it can be believed complete
     * for this page. Without that check an index built from partial metadata reads as
     * "no other referrers" — indistinguishable from a genuine sole referrer — and the
     * silence would be taken as evidence either way.
     */
    const otherReferrersFor = (sceneKey: string): PageReferrer[] | null => {
        if (!referrerIndex) return null;
        const all = referrerIndex.get(sceneKey);
        if (!all || !all.some(isOwnerView)) return null;
        return all.filter((entry) => !isOwnerView(entry));
    };

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
                otherReferrers: [],
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
                otherReferrers: [],
                reason: 'this page declares no parent. It may be a top-level page that survives, or its parent may be missing from the metadata — the two are indistinguishable here',
            };
        }

        const parent = resolve(scene.parentRef);
        if (!parent) {
            return {
                ...base,
                classification: 'unknown',
                parentSceneKey: null,
                otherReferrers: [],
                reason: `this page names "${scene.parentRef}" as its parent, which matches no page in the app, so its ownership cannot be established`,
            };
        }

        if (parent.sceneKey === ownerSceneKey) {
            // A page owned here still survives if something else points at it. Knack
            // deletes a child page when its last referring link goes, not when a link
            // to it goes — and the page then re-parents onto whichever view still
            // links to it. Calling these doomed is what made a prompt name two pages
            // and destroy neither.
            const otherReferrers = otherReferrersFor(scene.sceneKey) ?? [];
            if (otherReferrers.length > 0) {
                return {
                    ...base,
                    classification: 'transferred',
                    parentSceneKey: parent.sceneKey,
                    otherReferrers,
                    reason: `this page hangs off the page being changed, but ${otherReferrers.length} other view(s) link to it as well (${otherReferrers
                        .map((entry) => entry.viewKey)
                        .join(
                            ', ',
                        )}). Removing this link is not removing its last one, so Knack moves the page under the view that still links to it rather than deleting it`,
                };
            }

            return {
                ...base,
                classification: 'owned',
                parentSceneKey: parent.sceneKey,
                otherReferrers: [],
                reason: 'this page hangs off the page being changed, so it exists only because of this link',
            };
        }

        return {
            ...base,
            classification: 'external',
            parentSceneKey: parent.sceneKey,
            otherReferrers: otherReferrersFor(scene.sceneKey) ?? [],
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
    const resolve = makeSceneResolver(scenes);

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
 * The walk has to be recursive because the empty-payload check is built on it: a
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
    { ok: true; path: string } | { ok: false; error: string };

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
        /**
         * Pages that survive because another view still links to them, and move under
         * that view instead. A navigation change, not a deletion, but the person
         * confirming is the one who will go looking for the page afterwards.
         */
        transferredPages: ClassifiedLinkTarget[];
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
          /**
           * Links this mutation severs whose pages survive.
           *
           * On the prompt-free path nobody is told anything by definition, so without
           * this the caller cannot report what it removed. Naming them is the whole
           * difference between "done" and "removed the link to Monthly report".
           */
          externalPages: ClassifiedLinkTarget[];
          /**
           * Links this mutation severs whose pages move to another view.
           *
           * Separate from externalPages because the outcome differs where it matters
           * to whoever has to find the page again: an external page stays where it
           * was and merely loses a route in, while these change parent.
           */
          transferredPages: ClassifiedLinkTarget[];
          /**
           * The body to send, merged from the live definition and the caller's patch.
           *
           * Returned rather than left to the caller so that the body the guard judged
           * is the body that goes to Knack. Two places reasoning separately about the
           * payload is how a guard and its tool come to disagree about what a request
           * does.
           */
          outgoingBody: Record<string, unknown> | null;
          currentAttributes: Record<string, unknown> | null;
          /**
           * Pages this request asks Knack to create, by name.
           *
           * Empty for almost everything. A menu create can post a `scene` as a page
           * specification, and this is how the response says so rather than leaving the
           * operator to read "unreadable link" about a page they meant to add.
           */
          createsPages: string[];
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
 * One clause naming what a refused mutation would have put at risk.
 *
 * A confirmation is asked for two reasons the sentence has to keep apart: pages the
 * guard can name, and links whose target page it could not identify. The refusals
 * once interpolated only the first, so a decline caused entirely by unresolved links
 * came back as *"destroys 0 page(s) and was not confirmed"* — a safety refusal that
 * understated its own reason, on the very call that had asked someone to decide.
 * That was D1 in TESTING.md. The elicitation headline in server.ts already branched
 * on this split; this keeps the refusal that follows it in step.
 *
 * @param action The mutation being refused.
 * @param namedPages Pages the guard could name as doomed.
 * @param unresolvedLinks Links this mutation drops whose target it could not identify.
 * @returns A clause beginning "This <action> ...", with no trailing punctuation.
 */
export function describeRefusedStakes(
    action: string,
    namedPages: number,
    unresolvedLinks: number,
): string {
    const named = `destroys ${namedPages} page(s)`;
    const unresolved = `removes ${unresolvedLinks} link(s) whose target page this server could not identify, so pages it cannot list may be destroyed`;

    if (namedPages > 0 && unresolvedLinks > 0) {
        return `This ${action} ${named} and ${unresolved}`;
    }
    if (unresolvedLinks > 0) {
        return `This ${action} ${unresolved}`;
    }
    return `This ${action} ${named}`;
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

    // 1b. Everything downstream rests on one invariant: the body Knack receives is the
    //     body the guard judged. That holds because the guard merges the patch into the
    //     live definition itself — and it can only merge into an object. A payload that
    //     parses but is not one (`[{"columns": []}]` clears the empty-payload check,
    //     since the walk finds `columns` inside the array) was forwarded raw, which is
    //     the single path where what went to Knack was not what had been examined.
    if (
        request.updates !== undefined &&
        parsedUpdates !== null &&
        (typeof parsedUpdates !== 'object' || Array.isArray(parsedUpdates))
    ) {
        return refuse(
            'INVALID_UPDATES_JSON',
            'updates parsed, but not as a JSON object. A view update is a set of properties to write, so the payload has to be an object — an array or a bare value cannot be merged into the view definition, and forwarding it unmerged would send Knack something these checks never looked at.',
        );
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

    // 2c. A page specification in a shape Knack mishandles is refused before anything
    //     is read. Measured live, 5 September (TESTED.md §9): with no `views` array
    //     Knack stores the object and creates no page, a dangling link written by our
    //     own request; with no `type: "scene"` on the link it creates the page and
    //     keeps the object, so every later save of the view creates the page again.
    //     Refused rather than warned about: a warning beside an `ok` is how the first
    //     shape went unnoticed for a day, and the well-formed shape costs nothing.
    if (parsedUpdates !== undefined) {
        const malformed =
            collectMalformedScenePageSpecifications(parsedUpdates);
        if (malformed.length > 0) {
            const pageCount = new Set(malformed.map((entry) => entry.name))
                .size;
            return refuse(
                'MALFORMED_PAGE_SPECIFICATION',
                `This payload asks Knack to create ${pageCount} page(s) in a shape Knack mishandles: ${malformed
                    .map((entry) => `"${entry.name}" — ${entry.problem}`)
                    .join(
                        '; ',
                    )}. A specification with no views array is stored as the raw object and creates no page. A link with no type "scene" creates the page and keeps the object, so every later save of the view creates it again. Nothing was sent. Post each new page as {"name": <label>, "type": "scene", "scene": {"name": <page name>, "parent": <parent page slug>, "views": []}}.`,
                { malformedPageSpecifications: malformed },
            );
        }
    }

    // 3. Preflight only actions that can delete an existing view's child pages. An
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

    // 4b. A retarget — changing which object the view lists — is refused outright.
    //
    //     Knack's view PUT replaces rather than patches, so this is not a mis-scope
    //     that returns the wrong rows: every column, display connection, filter, sort
    //     and rule in the view names a field on the object being replaced, and all of
    //     them are written in one request. The view does not move to a new object, it
    //     is overwritten with one whose configuration no longer refers to anything.
    //
    //     No builder path produces this. A view is bound to its object when it is
    //     added, and eleven captured builder requests never once changed `object` —
    //     three of them were rescopes, which changed filter rules and source keys and
    //     left the object alone. A payload that carries a different object is
    //     therefore far likelier to be a mistake than an intention, and refusing costs
    //     a legitimate caller nothing: nothing here needs to retarget a view. Rebuild
    //     it against the object you want instead.
    //
    //     Both sides have to be readable. Refusing on an unreadable current object
    //     would block ordinary edits, and the overwhelming majority of update payloads
    //     carry no `source` at all, so this never fires for them.
    if (
        (action === 'update_view' || action === 'move_view') &&
        parsedUpdates !== undefined
    ) {
        const currentObject = readSourceObject(attributes);
        const incomingObject = readSourceObject(parsedUpdates);

        if (
            currentObject &&
            incomingObject &&
            currentObject !== incomingObject
        ) {
            return refuse(
                'SOURCE_OBJECT_CHANGE_REFUSED',
                `This payload changes the view's source object from ${currentObject} to ${incomingObject}. Refused outright: a view PUT replaces rather than patches, so every column, filter, sort and rule — all of which name fields on ${currentObject} — would be written against ${incomingObject} in the same request, and none of them would refer to anything. This is destructive rather than a rescope, and no Knack builder path produces it. To scope the same view differently, change connection_key, parent_source, authenticated_user or the filter criteria and leave source.object alone. To list a different object, build a new view against it.`,
                { viewKey, currentObject, incomingObject },
            );
        }

        //  A partial `source` is destructive for a subtler reason, and it took a
        //  review to see it. buildEffectiveUpdateBody merges **top-level** properties
        //  only, so a payload carrying `source` replaces the stored block whole
        //  rather than merging into it. `{source: {criteria: {...}}}` therefore does
        //  not edit the criteria — it discards `object`, `connection_key`,
        //  `relationship_type`, `sort` and `limit` along with it, and sends Knack a
        //  view whose source names no object at all. Measured on a stored source
        //  carrying six keys: one survived.
        //
        //  So the check cannot be about `object` alone. Any stored source key the
        //  payload omits is silently dropped, and the refusal names them rather than
        //  making the caller work it out.
        const currentSource = asPlainObject(
            asPlainObject(attributes)?.source ?? null,
        );
        const incomingSource = asPlainObject(
            asPlainObject(parsedUpdates)?.source ?? null,
        );

        if (currentSource && incomingSource) {
            const dropped = Object.keys(currentSource).filter(
                (name) => !Object.hasOwn(incomingSource, name),
            );

            if (dropped.length > 0) {
                return refuse(
                    'PARTIAL_SOURCE_REPLACEMENT',
                    `This payload's source omits ${dropped.length} key(s) the view currently has: ${dropped.join(', ')}. A view PUT replaces rather than patches, and the merge here works on top-level properties, so the source block goes across whole — the omitted keys would be discarded rather than kept${dropped.includes('object') ? ', leaving the view with no source object at all' : ''}. Send the complete source block: read the current one, change what you mean to change, and include every key you mean to keep. Omitting a key is how you delete it, which is rarely what a partial payload intends.`,
                    { viewKey, droppedSourceKeys: dropped },
                );
            }
        }
    }

    const viewType = getViewType(attributes);

    // 5. Cascade check. There is no view-type gate ahead of this any more. Menus were
    //    disqualified on their type; a second rule refused any payload carrying a
    //    links array; a third refused a view whose type could not be read, on the
    //    grounds that it might be a menu. All three said one thing — a menu's
    //    navigation is too dangerous to touch — and all three are replaced by asking
    //    the question that decides it for any view: which pages lose their last link.
    //    A menu is now promptable rather than impossible, and a client that cannot
    //    prompt still cannot change one.
    const allLinkTargets = collectLinkTargets(attributes);
    // Use the same navigation-only definition for every side of the decision. A rule
    // redirect may carry a `scene`, but it cannot delete or preserve a child page.
    const linkTargets: LinkTargets = {
        linkColumns: allLinkTargets.linkColumns.filter(isNavigationColumn),
        menuLinks: allLinkTargets.menuLinks,
        childSceneRefs: collectNavigationRefs(attributes),
    };

    // A link whose `scene` we could not read is not evidence that no child page
    // exists — it is evidence that we cannot see which one. Counting those as risk
    // keeps an unfamiliar or malformed link shape from skipping confirmation, which
    // is exactly how a silent page deletion would get through.
    // A menu entry pointing outside the app has no child scene by definition, and
    // counting its absent `scene` as "could not resolve" made every view holding one
    // permanently risky — an ordinary edit could never be cleared, because it would
    // ask about a page that does not exist, every time. Knack marks these
    // `type: "url"`, but not always: an older or hand-built entry can carry a `url`
    // and no type at all, which the type test alone reads as unreadable forever.
    const pointsOutsideTheApp = (link: MenuLinkTarget): boolean =>
        link.linkType === 'url' || link.hasUrl;

    const unresolvedLinks = [
        ...linkTargets.linkColumns,
        ...linkTargets.menuLinks.filter((link) => !pointsOutsideTheApp(link)),
    ].filter((link) => !link.childSceneRef);

    // Every update to a view carrying page links is a candidate, not only one that
    // looks structural. The PUT replaces, so a body that omits `columns` removes them
    // just as surely as one that sends a shorter array. A structural-payload test
    // gated this before — an allowlist of layout-free property names — and a scalar
    // payload on a linked view skipped the check entirely on the strength of Knack
    // happening to reject it. What actually decides the risk is which links survive
    // in the merged body, worked out below, so the allowlist is gone.
    const destructiveAction =
        action === 'update_view' ||
        action === 'delete_view' ||
        action === 'move_view';

    const cascadeRisked =
        destructiveAction &&
        (linkTargets.childSceneRefs.length > 0 || unresolvedLinks.length > 0);

    // The body this update will actually put on the wire. Every retention question
    // below is asked of this, not of the caller's fragment.
    //
    // Null for delete and move, and that is not an oversight in either case. A delete
    // removes every link by definition. A move sends the view to another scene, and
    // every link counts as dropped so the whole set goes to a human. Measured on
    // 5 September, both containers, moved in the builder (TESTING.md C8). A menu:
    // the link went with the view and the child page kept its original parent —
    // nothing destroyed, so this over-warns. Two tables, eight owned pages between
    // them: Knack rebuilt each tree under the new page, rewrote the link columns to
    // the copies, deleted every original page holding a form (descendants with it)
    // and left every page holding a details view alive and orphaned. So on a table
    // the rule is right to ask and over-counts by the details pages, which is the
    // safe side. It stays as it is: naming which pages die and which are orphaned
    // is the refinement the evidence points at, once a page holding both kinds of
    // view has been moved.
    const outgoingBody =
        action === 'update_view'
            ? buildEffectiveUpdateBody(attributes, parsedUpdates)
            : null;

    // 4c. A specification object that Knack *kept* in a stored link is a page factory.
    //     Measured live, 5 September (TESTED.md §9): a link saved without
    //     `type: "scene"` had its page created and its `scene` left as the object,
    //     and the next save — a byte-identical re-send — created the page again under
    //     a second slug. Every update through this server re-sends the whole merged
    //     definition, so a title change here would make another page. Refused when
    //     the merged body still carries the stored object; an update that replaces it
    //     with the page's slug is the repair, and falls through to the cascade check
    //     below, which will ask a human about the unreadable link it drops.
    if (outgoingBody !== null) {
        const stored = collectScenePageSpecifications(attributes);
        if (stored.length > 0) {
            const outgoing = new Set(
                collectScenePageSpecifications(outgoingBody),
            );
            const reSent = stored.filter((name) => outgoing.has(name));
            if (reSent.length > 0) {
                return refuse(
                    'STORED_PAGE_SPECIFICATION',
                    `${viewKey ?? 'This view'} carries ${reSent.length} link(s) whose scene is still a page specification object rather than a page slug: ${reSent
                        .map((name) => `"${name}"`)
                        .join(
                            ', ',
                        )}. Knack kept the object when the link was saved, and it creates the page again on every save that re-sends it — so this update, which re-sends the whole definition, would make another page. Nothing was sent. Repair the link in the Knack builder first${builderUrl ? `: ${builderUrl}` : ''}. Or, on a client that can prompt, send a links array with the object replaced by the existing page's slug and confirm the prompt that follows.`,
                    { viewKey, storedPageSpecifications: reSent },
                );
            }
        }
    }

    let childPages: ChildPage[] = [];
    let severedExternalPages: ClassifiedLinkTarget[] = [];
    let transferredPages: ClassifiedLinkTarget[] = [];

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
            viewKey,
        );
        const externalCandidates = classified.filter(
            (target) => target.classification === 'external',
        );
        // Not doomed either, and for a sharper reason: this link is not the page's
        // last one. Measured — a child page linked from two views kept both its
        // content and its connection when one of the two link columns was removed,
        // and turned up under the view that still linked to it.
        const transferCandidates = classified.filter(
            (target) => target.classification === 'transferred',
        );
        // A link the outgoing body re-sends is not being removed, and a page whose
        // link is not removed does not die. Measured twice on a live app: a complete
        // definition with every link column re-sent byte-for-byte deleted nothing,
        // while the same body one column short deleted exactly that column's page and
        // nothing else. This used to narrow only what was reported; it now narrows
        // the risk, which is what the measurement licenses.
        // One question, asked the same way of every container. A menu holds its
        // navigation in `links` rather than `columns`, and that used to buy it no
        // narrowing at all: whether re-sending a `links` entry preserved its page was
        // untested, so every page a menu reached was treated as losing its link.
        //
        // It is tested now. One update to a seven-link menu omitted a single entry and
        // re-sent the other six. Knack deleted exactly the omitted link's page and its
        // two descendants; the re-sent links kept theirs, including three the guard
        // had itself classified as owned and singly referenced — so their survival
        // cannot be put down to a second referrer. Same rule as link columns, same
        // arithmetic, different array.
        const dropsRef = (target: ClassifiedLinkTarget): boolean =>
            outgoingBody === null ||
            !payloadRetainsSceneRef(outgoingBody, target.ref);

        const atRiskRefs = classified
            .filter(
                (target) =>
                    target.classification !== 'external' &&
                    target.classification !== 'transferred' &&
                    dropsRef(target),
            )
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

        // A page can be external by parentage and doomed anyway: page Q linked
        // directly from this view, whose parent is owned page P, dies when P does.
        // Left unfiltered it appeared in the doomed list *and* under "NOT being
        // deleted" — and the prompt is the one artefact that must never contradict
        // itself. Doomed wins: it is the claim with consequences.
        const doomedKeys = new Set(requiredKeys);
        const externalTargets = externalCandidates.filter(
            (target) => !target.sceneKey || !doomedKeys.has(target.sceneKey),
        );
        // Same rule for the same reason. A transferred page should never also be in
        // the doomed set — its parent is the page being changed, which is not itself
        // a cascade seed — but the prompt contradicting itself is bad enough that the
        // filter is worth its two lines.
        const transferTargets = transferCandidates.filter(
            (target) => !target.sceneKey || !doomedKeys.has(target.sceneKey),
        );
        // Report as severed only what this payload actually drops. On a delete or a
        // move there is no payload and every link goes, so the unfiltered set is right
        // there. On an update, a link the payload re-sends is not being removed, and
        // saying otherwise misdescribes the change to whoever is asked to approve it.
        severedExternalPages = externalTargets.filter(dropsRef);
        transferredPages = transferTargets.filter(dropsRef);

        // A reference naming no scene in the tree is a page we cannot list, exactly
        // like a link whose `scene` could not be read. Both have to reach the prompt
        // as "more may die than are shown" rather than being silently dropped.
        //
        // But only when this mutation is actually removing them. An unreadable link
        // has no ref to match against the outgoing body, so retention is counted
        // instead: as long as the body still carries at least as many unreadable
        // links as the view does, none has been dropped. Without this a single
        // malformed node made a view permanently un-editable — every edit down to a
        // title change refused, with nothing the user could do to clear it, which is
        // the same false-positive trap that url links and form inputs already sprang.
        //
        // It is a tally, not a match, because an unreadable link has no identity to
        // match on. So a payload that swaps one unreadable link for a *different*
        // unreadable one nets zero drops and asks nothing. Accepted: naming the swap
        // would need the identity the link does not have, and the alternative — any
        // change to an unreadable link asks — is the permanent refusal this replaced.
        const unresolvedInOutgoing =
            outgoingBody === null
                ? 0
                : countUnresolvedNavigationLinks(
                      outgoingBody,
                      pointsOutsideTheApp,
                  );
        const unresolvedDropped =
            outgoingBody === null
                ? unresolvedLinks.length
                : Math.max(0, unresolvedLinks.length - unresolvedInOutgoing);

        const unresolvedCount =
            unresolvedDropped + expansion.unresolvedRefs.length;

        // Every link points at a page that survives, so this mutation destroys
        // nothing and there is nothing to put to a human. The snapshot below is still
        // written: the classification rests on Knack's metadata being accurate about
        // parentage, and a restore point costs nothing set against that assumption
        // being wrong.
        const destroysNothing =
            requiredKeys.length === 0 && unresolvedCount === 0;

        // Both refusals below say what was at stake, and both have to agree with the
        // prompt the human was (or could not be) shown. Built once so they cannot
        // drift apart again.
        const stakes = describeRefusedStakes(
            action,
            requiredKeys.length,
            unresolvedCount,
        );

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
                    externalPages: severedExternalPages,
                    transferredPages,
                    unresolvedLinkCount: unresolvedCount,
                })
              : ({ supported: false } as PageDeletionConfirmation);

        if (confirmation.supported) {
            if (!confirmation.accepted) {
                return refuse(
                    'HUMAN_CONFIRMATION_DECLINED',
                    `${stakes}, and was not confirmed (${confirmation.outcome ?? 'declined'}). Nothing was changed. Do not retry without being asked to — the person who declined was shown exactly what was at stake.`,
                    {
                        childPages,
                        unresolvedLinkCount: unresolvedCount,
                        outcome: confirmation.outcome ?? 'decline',
                    },
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
                `${stakes}, and this MCP client cannot prompt a human to confirm it${confirmation.reason ? ` (${confirmation.reason})` : ''}. Refusing rather than letting the caller confirm on the user's behalf — there is no override.${builderHint}`,
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
        createsPages: collectScenePageSpecifications(
            parsedUpdates ?? attributes,
        ),
        acknowledgedPages: childPages.map((page) => page.sceneKey),
        externalPages: severedExternalPages,
        transferredPages,
        outgoingBody,
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
        outgoingBody: Record<string, unknown> | null;
        currentAttributes: Record<string, unknown> | null;
        hasPageLinks: boolean;
    }) => Promise<T>,
): Promise<
    | {
          ok: true;
          result: T;
          snapshotPath?: string;
          viewType: string | null;
          /** Pages the request asked Knack to create, by name. Usually empty. */
          createsPages: string[];
          acknowledgedPages: string[];
          externalPages: ClassifiedLinkTarget[];
          transferredPages: ClassifiedLinkTarget[];
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
        outgoingBody: decision.outgoingBody,
        currentAttributes: decision.currentAttributes,
        hasPageLinks: decision.hasPageLinks,
    });

    return {
        ok: true,
        result,
        snapshotPath: decision.snapshotPath,
        viewType: decision.viewType,
        createsPages: decision.createsPages,
        acknowledgedPages: decision.acknowledgedPages,
        externalPages: decision.externalPages,
        transferredPages: decision.transferredPages,
    };
}
