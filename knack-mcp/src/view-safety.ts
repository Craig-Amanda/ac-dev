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
    | 'CONFIRMATION_UPGRADE_REQUIRED'
    | 'BLOCKED_LINKS_PAYLOAD'
    | 'COULD_NOT_VERIFY_VIEW'
    | 'BLOCKED_MENU_VIEW_UPDATE'
    | 'BLOCKED_MENU_VIEW_MOVE'
    | 'BLOCKED_VIEW_TYPE'
    | 'BLOCKED_UPDATE_KEY'
    | 'UNKNOWN_VIEW_TYPE'
    | 'BLOCKED_LINK_COLUMN_LOSS'
    | 'ACKNOWLEDGEMENT_MISMATCH'
    | 'HUMAN_CONFIRMATION_UNAVAILABLE'
    | 'UNRESOLVED_LINK_TARGET'
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
    childSceneKey: string | null;
    /** Where in the view layout the link was found, e.g. `$.groups[0].columns[2]`. */
    sourcePath: string;
};

export type MenuLinkTarget = {
    name: string | null;
    linkType: string | null;
    childSceneKey: string | null;
    sourcePath: string;
};

export type LinkTargets = {
    linkColumns: LinkColumnTarget[];
    menuLinks: MenuLinkTarget[];
    /** Every scene key referenced by a link column or menu link, deduped and sorted. */
    childSceneKeys: string[];
};

export type SceneNode = {
    sceneKey: string;
    sceneName?: string;
    sceneSlug?: string;
    parentSceneKey?: string;
};

export type ChildPage = {
    sceneKey: string;
    sceneName: string | null;
    sceneSlug: string | null;
    /** 0 for a page linked directly, 1+ for a page that dies because its ancestor does. */
    depth: number;
};

export type ViewUpdatePolicy = {
    deniedViewTypes: string[];
    deniedKeys: string[];

    /**
     * What to do about a cascade delete when the MCP client cannot prompt a human.
     *
     * `refuse` is the default: no human reachable, no deletion. `acknowledgement`
     * falls back to requiring the caller to echo the exact page keys — usable, but
     * an agent can satisfy it from the refusal message without asking anyone, so it
     * is an explicit per-app opt-in rather than a silent degradation.
     */

    cascadeConfirmationFallback: 'refuse' | 'acknowledgement';
};

/**
 * View types and update keys this server refuses to write. Anything not listed is
 * allowed — this is a denylist, not an allowlist.
 *
 * `menu` is listed for the record, but removing it does not make menus updatable: the
 * menu block is unconditional and runs before this policy is consulted. Editing app.json
 * can tighten this policy, never loosen that rule.
 *
 * `deniedKeys` is empty by default, so `columns` is writable. What still protects link
 * columns and their child pages is the acknowledgement in guardViewMutation: a columns
 * replacement on a view with link targets is refused until the caller names the exact
 * pages it would destroy. Add `columns` here to refuse it outright instead.
 */
export const DEFAULT_VIEW_UPDATE_POLICY: ViewUpdatePolicy = {
    deniedViewTypes: ['menu'],
    deniedKeys: [],
    cascadeConfirmationFallback: 'refuse',
};

export const ACKNOWLEDGEMENT_PREFIX = 'I accept deletion of these exact pages:';

const MAX_WALK_DEPTH = 24;
const SCENE_KEY_PATTERN = /scene_\d+/gi;

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
    const childSceneKeys = new Set<string>();

    if (!attributes) {
        return { linkColumns, menuLinks, childSceneKeys: [] };
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
        const childSceneKey = readSceneReference(record.scene);

        if (type === 'link') {
            linkColumns.push({
                header: asTrimmedString(record.header),
                fieldKey: readSceneReference(record.field),
                childSceneKey,
                sourcePath: path,
            });
            if (childSceneKey) childSceneKeys.add(childSceneKey);
        }

        // Entries of a `links` array are menu links regardless of their own `type`,
        // which Knack sets to `scene`, `url` or omits entirely.
        if (/\.links\[\d+\]$/.test(path)) {
            menuLinks.push({
                name:
                    asTrimmedString(record.name) ??
                    asTrimmedString(record.label),
                linkType: type,
                childSceneKey,
                sourcePath: path,
            });
            if (childSceneKey) childSceneKeys.add(childSceneKey);
        }

        for (const [key, nested] of Object.entries(record)) {
            visit(nested, `${path}.${key}`, depth + 1);
        }
    };

    visit(attributes, '$', 0);

    return {
        linkColumns,
        menuLinks,
        childSceneKeys: [...childSceneKeys].sort(),
    };
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
 * @param payload Parsed update payload.
 * @returns True when the payload replaces a `columns` array at any depth.
 */
export function payloadTouchesColumns(payload: unknown): boolean {
    const visit = (value: unknown, depth: number): boolean => {
        if (depth > MAX_WALK_DEPTH) return false;

        if (Array.isArray(value)) {
            return value.some((item) => visit(item, depth + 1));
        }

        const record = asPlainObject(value);
        if (!record) return false;

        if (Object.hasOwn(record, 'columns') && Array.isArray(record.columns)) {
            return true;
        }

        return Object.values(record).some((nested) => visit(nested, depth + 1));
    };

    return visit(payload, 0);
}

/**
 * Expand directly-linked child pages into every page that dies with them.
 *
 * A child page may own children of its own, and those are lost too when the parent is
 * cascade-deleted. Callers must acknowledge the whole set, not just the first level.
 *
 * @param directSceneKeys Scene keys referenced by link columns or menu links.
 * @param scenes The app's scene list, carrying parent pointers.
 * @returns Every affected page, breadth-first, deduped, each tagged with its depth.
 */
export function expandChildPages(
    directSceneKeys: string[],
    scenes: SceneNode[],
): ChildPage[] {
    const byKey = new Map(scenes.map((scene) => [scene.sceneKey, scene]));

    const childrenByParent = new Map<string, SceneNode[]>();
    for (const scene of scenes) {
        if (!scene.parentSceneKey) continue;
        const siblings = childrenByParent.get(scene.parentSceneKey) ?? [];
        siblings.push(scene);
        childrenByParent.set(scene.parentSceneKey, siblings);
    }

    const seen = new Set<string>();
    const pages: ChildPage[] = [];
    let frontier = directSceneKeys.filter((key) => {
        if (seen.has(key)) return false;
        seen.add(key);
        return true;
    });
    let depth = 0;

    while (frontier.length > 0 && depth <= MAX_WALK_DEPTH) {
        for (const sceneKey of frontier) {
            const scene = byKey.get(sceneKey);
            pages.push({
                sceneKey,
                sceneName: scene?.sceneName ?? null,
                sceneSlug: scene?.sceneSlug ?? null,
                depth,
            });
        }

        const next: string[] = [];
        for (const sceneKey of frontier) {
            for (const child of childrenByParent.get(sceneKey) ?? []) {
                if (seen.has(child.sceneKey)) continue;
                seen.add(child.sceneKey);
                next.push(child.sceneKey);
            }
        }

        frontier = next;
        depth += 1;
    }

    return pages;
}

// -----------------------
// Acknowledgement
// -----------------------

/**
 * Build the exact sentence a caller must echo back to accept a cascade delete.
 *
 * @param sceneKeys The page keys that will be destroyed.
 * @returns The literal acknowledgement string.
 */
export function buildAcknowledgementSentence(sceneKeys: string[]): string {
    return `${ACKNOWLEDGEMENT_PREFIX} ${[...sceneKeys].sort().join(', ')}`;
}

/**
 * Check an acknowledgement against the pages the preflight actually found.
 *
 * A boolean flag can be set from a generic instinct; a sentence naming exact page keys
 * cannot be produced without having read the preflight output. Scene keys are compared
 * as a set — order and spacing are free, but a missing or extra key fails.
 *
 * @param acknowledgement Raw caller-supplied string.
 * @param requiredSceneKeys Page keys that must be named.
 * @returns Whether it matches, plus the specific mismatch for the error message.
 */
export function checkAcknowledgement(
    acknowledgement: string | undefined,
    requiredSceneKeys: string[],
): {
    matches: boolean;
    reason?: 'missing-phrase' | 'set-mismatch';
    missing: string[];
    unexpected: string[];
} {
    const text = (acknowledgement ?? '').trim();
    const normalised = text.toLowerCase().replace(/\s+/g, ' ');
    const expectedPhrase = ACKNOWLEDGEMENT_PREFIX.toLowerCase().replace(
        /\s+/g,
        ' ',
    );

    if (!normalised.includes(expectedPhrase)) {
        return {
            matches: false,
            reason: 'missing-phrase',
            missing: [],
            unexpected: [],
        };
    }

    const supplied = new Set(
        (text.match(SCENE_KEY_PATTERN) ?? []).map((key) => key.toLowerCase()),
    );
    const required = new Set(requiredSceneKeys.map((key) => key.toLowerCase()));

    const missing = [...required].filter((key) => !supplied.has(key)).sort();
    const unexpected = [...supplied].filter((key) => !required.has(key)).sort();

    if (missing.length > 0 || unexpected.length > 0) {
        return { matches: false, reason: 'set-mismatch', missing, unexpected };
    }

    return { matches: true, missing: [], unexpected: [] };
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

// -----------------------
// Policy
// -----------------------

/**
 * Resolve an app's view-update allowlist, falling back to the conservative default.
 *
 * @param policy Optional `viewUpdatePolicy` from app.json.
 * @returns A fully-populated policy with lowercased view types.
 */
export function resolveViewUpdatePolicy(policy?: {
    deniedViewTypes?: string[];
    deniedKeys?: string[];
    cascadeConfirmationFallback?: 'refuse' | 'acknowledgement';
}): ViewUpdatePolicy {
    const deniedViewTypes = (
        policy?.deniedViewTypes ?? DEFAULT_VIEW_UPDATE_POLICY.deniedViewTypes
    ).map((type) => type.trim().toLowerCase());

    // `menu` is re-added even if an app.json omits it. The unconditional menu block
    // already covers this, but a policy that reads as though menus were permitted
    // would be a misleading thing to leave in a config file.
    if (!deniedViewTypes.includes('menu')) {
        deniedViewTypes.push('menu');
    }

    return {
        deniedViewTypes,
        deniedKeys: policy?.deniedKeys ?? DEFAULT_VIEW_UPDATE_POLICY.deniedKeys,
        cascadeConfirmationFallback:
            policy?.cascadeConfirmationFallback ??
            DEFAULT_VIEW_UPDATE_POLICY.cascadeConfirmationFallback,
    };
}

/**
 * Collect every property name a payload would write, at any depth.
 *
 * The walk has to be recursive to match how the denylist is read. A top-level-only
 * scan let `{groups: [{columns: []}]}` past a `deniedKeys: ["columns"]` policy, since
 * only `groups` was visible from the outside. The cascade check was already recursive,
 * so child pages stayed protected either way — but the denylist's own promise did not
 * hold, which is worse than not offering it.
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
    /** The app's scene list, carrying parent pointers for descendant expansion. */
    listScenes: () => Promise<SceneNode[]>;
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
    acknowledgeDeletionOfPages?: string;
    /** Legacy flag. Any use is refused so old callers fail closed. */
    confirmDestructive?: boolean;
    policy?: ViewUpdatePolicy;
};

export type ViewMutationDecision =
    | {
          allowed: true;
          viewType: string | null;
          snapshotPath: string;
          childPages: ChildPage[];
          acknowledgedPages: string[];
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
    const policy = request.policy ?? DEFAULT_VIEW_UPDATE_POLICY;
    const builderUrl = deps.builderUrlForScene?.(sceneKey) ?? null;
    const builderHint = builderUrl
        ? ` Make this change in the Knack builder instead: ${builderUrl}`
        : ' Make this change in the Knack builder instead.';

    // 1. The legacy override is refused outright rather than honoured or ignored, so a
    //    caller written against the old signature fails closed instead of proceeding.
    if (request.confirmDestructive === true) {
        return refuse(
            'CONFIRMATION_UPGRADE_REQUIRED',
            'confirmDestructive is no longer accepted. A boolean cannot show that the caller knows which pages would be destroyed. Re-run without it to receive the preflight, then pass acknowledgeDeletionOfPages naming the exact pages it reports.',
        );
    }

    // 2. Parse the payload once. An unparseable payload is refused rather than passed
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

    // 3. A links payload is refused for every view type, before any network call. This
    //    is the rule that still holds if navigation turns up on a view type nobody has
    //    classified yet.
    if (parsedUpdates !== undefined && payloadTouchesLinks(parsedUpdates)) {
        return refuse(
            'BLOCKED_LINKS_PAYLOAD',
            `This payload contains a links array. Knack rebuilds navigation from links and cascade-deletes the child pages of any link it no longer sees, so the REST view endpoint is not a supported route for navigation changes.${builderHint}`,
        );
    }

    // 4. Preflight. Fail closed: an unreadable view is indistinguishable from a view
    //    with no links, and guessing in that gap is what this whole module prevents.
    let attributes: Record<string, unknown> | null = null;
    let rawView: unknown;

    if (viewKey) {
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
    }

    const viewType = getViewType(attributes);

    // 5. A view we read but cannot identify could be anything, a menu included. This
    //    has to run before any action-specific classification: leaving it to the
    //    update_view branch let an untyped view be moved, since isMenuView() is false
    //    for a view with no type and nothing downstream re-checked it.
    if (viewKey && !viewType) {
        return refuse(
            'UNKNOWN_VIEW_TYPE',
            `${viewKey} was read successfully but declares no view type, so it cannot be checked against the menu rule or this app's denied view types. Refusing rather than assuming it is safe.${builderHint}`,
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

    // 7. Denylist. Only update_view writes arbitrary caller-supplied properties.
    if (action === 'update_view' && viewType) {
        if (policy.deniedViewTypes.includes(viewType)) {
            return refuse(
                'BLOCKED_VIEW_TYPE',
                `View type "${viewType}" is on this app's denied list, so updates to it are refused. Remove it from viewUpdatePolicy.deniedViewTypes in app.json to allow it — except "menu", which stays blocked regardless.`,
                {
                    viewType,
                    deniedViewTypes: policy.deniedViewTypes,
                    appJsonPath: 'viewUpdatePolicy.deniedViewTypes',
                },
            );
        }

        const deniedKeys = collectPayloadKeys(parsedUpdates).filter((key) =>
            policy.deniedKeys.includes(key),
        );
        if (deniedKeys.length > 0) {
            return refuse(
                'BLOCKED_UPDATE_KEY',
                `This update writes ${deniedKeys.join(', ')}, which this app denies. Nesting them inside another property does not change that — the payload is checked at every depth. Remove them from viewUpdatePolicy.deniedKeys in app.json to allow them.`,
                {
                    deniedKeys,
                    policyDeniedKeys: policy.deniedKeys,
                    appJsonPath: 'viewUpdatePolicy.deniedKeys',
                },
            );
        }
    }

    // 8. Cascade check. These are the actions that can take a link column's child page
    //    with them: replacing columns, deleting the view, or moving it off its scene.
    const linkTargets = collectLinkTargets(attributes);

    // A link whose `scene` we could not read is not evidence that no child page
    // exists — it is evidence that we cannot see which one. Counting those as risk
    // keeps an unfamiliar or malformed link shape from skipping confirmation, which
    // is exactly how a silent page deletion would get through.
    const unresolvedLinks = [
        ...linkTargets.linkColumns,
        ...linkTargets.menuLinks,
    ].filter((link) => !link.childSceneKey);

    const destructiveAction =
        (action === 'update_view' && payloadTouchesColumns(parsedUpdates)) ||
        action === 'delete_view' ||
        action === 'move_view';

    const cascadeRisked =
        destructiveAction &&
        (linkTargets.childSceneKeys.length > 0 || unresolvedLinks.length > 0);

    let childPages: ChildPage[] = [];

    if (cascadeRisked) {
        const scenes = await deps.listScenes();
        childPages = expandChildPages(linkTargets.childSceneKeys, scenes);
        const requiredKeys = childPages.map((page) => page.sceneKey);

        // Ask the human first. This request goes to the MCP client, not the model, so
        // the calling agent cannot answer it for the user. Everything below is the
        // degraded path for clients that cannot prompt.
        const confirmation = deps.confirmPageDeletion
            ? await deps.confirmPageDeletion({
                  action,
                  sceneKey,
                  viewKey,
                  childPages,
                  unresolvedLinkCount: unresolvedLinks.length,
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
        } else if (policy.cascadeConfirmationFallback === 'refuse') {
            return refuse(
                'HUMAN_CONFIRMATION_UNAVAILABLE',
                `This ${action} destroys ${requiredKeys.length} page(s), and this MCP client cannot prompt a human to confirm it${confirmation.reason ? ` (${confirmation.reason})` : ''}. Refusing rather than letting the caller confirm on the user's behalf. Make the change in the Knack builder, or set viewUpdatePolicy.cascadeConfirmationFallback to "acknowledgement" in app.json to allow the typed-acknowledgement route on this app.`,
                {
                    childPages,
                    linkColumns: linkTargets.linkColumns,
                    menuLinks: linkTargets.menuLinks,
                    appJsonPath: 'viewUpdatePolicy.cascadeConfirmationFallback',
                },
            );
        } else if (unresolvedLinks.length > 0) {
            // The fallback works by naming every page at stake. When a link's target
            // cannot be resolved there is no complete list to name, so the mechanism
            // cannot express this consent honestly — refuse instead of accepting an
            // acknowledgement that silently omits whatever those links point at.
            return refuse(
                'UNRESOLVED_LINK_TARGET',
                `This ${action} touches ${unresolvedLinks.length} link(s) whose target page could not be identified, so the pages it would destroy cannot be listed in full. The typed-acknowledgement fallback cannot express consent for pages it cannot name. Make this change in the Knack builder.${builderHint}`,
                {
                    unresolvedLinks,
                    knownChildPages: childPages,
                },
            );
        } else {
            // Opted-in fallback: the caller must echo the exact page keys. This proves
            // the preflight was read, not that a human agreed — which is why it is not
            // the default.
            const sentence = buildAcknowledgementSentence(requiredKeys);

            if (request.acknowledgeDeletionOfPages === undefined) {
                return refuse(
                    'BLOCKED_LINK_COLUMN_LOSS',
                    `This ${action} destroys ${requiredKeys.length} page(s) along with the link column(s) that reach them. Knack cascade-deletes a link column's child page even when the column is re-sent unchanged. This client cannot prompt a human, so confirm with the user yourself before retrying, then pass acknowledgeDeletionOfPages exactly as: "${sentence}"`,
                    {
                        requiredAcknowledgement: sentence,
                        childPages,
                        linkColumns: linkTargets.linkColumns,
                        menuLinks: linkTargets.menuLinks,
                    },
                );
            }

            const check = checkAcknowledgement(
                request.acknowledgeDeletionOfPages,
                requiredKeys,
            );
            if (!check.matches) {
                const detail =
                    check.reason === 'missing-phrase'
                        ? 'the acknowledgement did not contain the required phrase'
                        : `it named the wrong pages (missing: ${
                              check.missing.join(', ') || 'none'
                          }; unexpected: ${check.unexpected.join(', ') || 'none'})`;
                return refuse(
                    'ACKNOWLEDGEMENT_MISMATCH',
                    `The acknowledgement does not match the pages this ${action} would destroy — ${detail}. Pass it exactly as: "${sentence}"`,
                    {
                        requiredAcknowledgement: sentence,
                        missing: check.missing,
                        unexpected: check.unexpected,
                        childPages,
                    },
                );
            }
        }
    }

    // 9. Restore point. No snapshot on disk, no mutation — a full disk or a misconfigured
    //    KNACK_APPS_DIR halts view edits rather than letting them run unprotected.
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

    return {
        allowed: true,
        viewType,
        snapshotPath: snapshot.path,
        childPages,
        acknowledgedPages: childPages.map((page) => page.sceneKey),
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
 * @param perform Sends the real request. Invoked only after a snapshot is on disk.
 * @returns The performed result, or the guard's refusal.
 */
export async function runGuardedViewMutation<T>(
    deps: ViewMutationDeps,
    request: ViewMutationRequest,
    perform: (context: {
        snapshotPath: string;
        viewType: string | null;
        childPages: ChildPage[];
    }) => Promise<T>,
): Promise<
    | {
          ok: true;
          result: T;
          snapshotPath: string;
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
    });

    return {
        ok: true,
        result,
        snapshotPath: decision.snapshotPath,
        viewType: decision.viewType,
        acknowledgedPages: decision.acknowledgedPages,
    };
}
