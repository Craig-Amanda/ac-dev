/**
 * Who can reach a page, worked out from where it sits in the tree.
 *
 * Measured live on 7 September (TESTING.md Tier 7, T22), on a page tree with one login:
 *
 * - Adding a login to a page did not mark that page. Knack inserted a new scene above
 *   it, `type: "authentication"`, holding a `login` view, and re-parented the page under
 *   it. The protected page's own fields did not change.
 * - The roles live on that `login` view — `allowed_profiles` (profile keys) and
 *   `limit_profile_access` — and nowhere else. The authentication scene carries no role
 *   fields; no page beneath it carries any.
 * - `authenticated: false` appeared on the page directly under the login, identical to a
 *   public page. So a scene's own fields cannot say whether it is protected. Only its
 *   ancestry can, which is what this module walks.
 * - `parent` is a slug, not a `scene_N` key. Every step of the walk resolves through both.
 *
 * Pure: no I/O. Everything here runs on the SceneInfo list parseRuntimeScenes produces.
 */
import type { RuntimeMetadata, SceneInfo, SceneViewInfo } from '../types.js';

/**
 * The same ceiling view-safety's walks use. A tree that nests deeper than this is not
 * one whose answer we report as certain, and a parent cycle must terminate somewhere.
 */
const MAX_ANCESTRY_DEPTH = 24;

/** Knack's profile key for "every registered user", as written on user objects. */
export const ALL_USERS_PROFILE_KEY = 'all_users';

export type PageAccess =
    | {
          status: 'public';
          sceneKey: string;
          /** Keys from the page up to its top-level ancestor, inclusive. */
          ancestry: string[];
          loginSceneKey: null;
          loginViewKey: null;
          roles: null;
          anyLoggedInUser: false;
          reason: string;
      }
    | {
          status: 'protected';
          sceneKey: string;
          ancestry: string[];
          /** The `type: "authentication"` ancestor (or the page itself) holding the login. */
          loginSceneKey: string;
          /** The `login` view on it, when there is one. */
          loginViewKey: string | null;
          /**
           * Profile keys allowed through. Null when the login carries no role fields at
           * all, which is "unknown", not "nobody" — the two must never be confused.
           */
          roles: string[] | null;
          /** True when the login admits every registered user rather than listed roles. */
          anyLoggedInUser: boolean;
          reason: string;
      }
    | {
          status: 'unknown';
          sceneKey: string;
          ancestry: string[];
          loginSceneKey: null;
          loginViewKey: null;
          roles: null;
          anyLoggedInUser: false;
          reason: string;
      };

export type ProfileNameIndex = Map<
    string,
    { profileKey: string; objectKey: string; objectName: string | null }
>;

function indexScenes(scenes: SceneInfo[]) {
    const byKey = new Map<string, SceneInfo>();
    const bySlug = new Map<string, SceneInfo>();
    for (const scene of scenes) {
        // First wins, as in view-safety's resolver, so a duplicate cannot silently
        // redirect a walk.
        if (!byKey.has(scene.sceneKey)) byKey.set(scene.sceneKey, scene);
        if (scene.sceneSlug && !bySlug.has(scene.sceneSlug)) {
            bySlug.set(scene.sceneSlug, scene);
        }
    }
    return (ref: string): SceneInfo | undefined =>
        byKey.get(ref) ?? bySlug.get(ref);
}

/** The login view on a scene, if it holds one. */
export function findLoginView(scene: SceneInfo): SceneViewInfo | undefined {
    return scene.views.find((view) => view.viewType === 'login');
}

/**
 * Whether this scene is the one that puts a login in front of everything beneath it.
 *
 * Either signal alone is enough: Knack's inserted scene carries both, but a payload
 * that lost the `type` (or a login view placed by hand on an ordinary page) should
 * still read as protected — the cost of a false "public" is a page handed to the wrong
 * audience, and the cost of a false "protected" is a person checking something safe.
 */
export function isLoginScene(scene: SceneInfo): boolean {
    return (
        scene.sceneType === 'authentication' ||
        findLoginView(scene) !== undefined
    );
}

/**
 * Resolve who can reach a page by walking its ancestry to the nearest login.
 *
 * @param sceneKey The page to describe, by key.
 * @param scenes The app's scenes, as parseRuntimeScenes returns them.
 * @returns Public, protected (with the roles the login names), or unknown with a reason.
 */
export function resolvePageAccess(
    sceneKey: string,
    scenes: SceneInfo[],
): PageAccess {
    const resolve = indexScenes(scenes);
    const ancestry: string[] = [];
    const unknown = (reason: string): PageAccess => ({
        status: 'unknown',
        sceneKey,
        ancestry,
        loginSceneKey: null,
        loginViewKey: null,
        roles: null,
        anyLoggedInUser: false,
        reason,
    });

    let scene = resolve(sceneKey);
    if (!scene) {
        return unknown(
            `no page ${sceneKey} in this app, so its ancestry cannot be walked`,
        );
    }
    // The walk starts from the page itself: a login scene asked about directly is
    // protected by its own login.
    const seen = new Set<string>();
    let depth = 0;
    while (scene) {
        if (seen.has(scene.sceneKey)) {
            return unknown(
                `the parent chain loops back to ${scene.sceneKey}, so no top-level page is ever reached and nothing about who can reach this page can be established`,
            );
        }
        if (depth > MAX_ANCESTRY_DEPTH) {
            return unknown(
                `the parent chain is deeper than this server will walk (${MAX_ANCESTRY_DEPTH}), so the login that governs this page, if any, was never reached`,
            );
        }
        seen.add(scene.sceneKey);
        ancestry.push(scene.sceneKey);

        if (isLoginScene(scene)) {
            const login = findLoginView(scene);
            const viaOwn = scene.sceneKey === sceneKey;
            const where = viaOwn
                ? 'this page holds the login itself'
                : `the nearest login above it is on ${scene.sceneKey}`;
            if (!login) {
                return {
                    status: 'protected',
                    sceneKey,
                    ancestry,
                    loginSceneKey: scene.sceneKey,
                    loginViewKey: null,
                    roles: null,
                    anyLoggedInUser: false,
                    reason: `${where}, an authentication page, but it carries no login view in this metadata, so which roles it admits cannot be read`,
                };
            }
            if (login.limitProfileAccess === true) {
                const roles = login.allowedProfiles ?? [];
                return {
                    status: 'protected',
                    sceneKey,
                    ancestry,
                    loginSceneKey: scene.sceneKey,
                    loginViewKey: login.viewKey,
                    roles,
                    anyLoggedInUser: false,
                    reason:
                        roles.length > 0
                            ? `${where} (${login.viewKey}), which limits access to ${roles.length} role(s)`
                            : `${where} (${login.viewKey}), which limits access to a role list that is empty — as written, no role gets through`,
                };
            }
            if (login.limitProfileAccess === false) {
                return {
                    status: 'protected',
                    sceneKey,
                    ancestry,
                    loginSceneKey: scene.sceneKey,
                    loginViewKey: login.viewKey,
                    roles: login.allowedProfiles ?? [],
                    anyLoggedInUser: true,
                    reason: `${where} (${login.viewKey}), which admits any registered user`,
                };
            }
            return {
                status: 'protected',
                sceneKey,
                ancestry,
                loginSceneKey: scene.sceneKey,
                loginViewKey: login.viewKey,
                roles: null,
                anyLoggedInUser: false,
                reason: `${where} (${login.viewKey}), but the view carries no role fields in this metadata, so which roles it admits cannot be read`,
            };
        }

        if (!scene.parentRef) {
            return {
                status: 'public',
                sceneKey,
                ancestry,
                loginSceneKey: null,
                loginViewKey: null,
                roles: null,
                anyLoggedInUser: false,
                reason:
                    ancestry.length === 1
                        ? 'a top-level page with no login on it'
                        : `no login on this page or on any of the ${ancestry.length - 1} page(s) above it, up to top-level ${ancestry[ancestry.length - 1]}`,
            };
        }

        const parent = resolve(scene.parentRef);
        if (!parent) {
            return unknown(
                `${scene.sceneKey} names "${scene.parentRef}" as its parent, which matches no page in the app, so the walk cannot continue and whether a login sits above cannot be established`,
            );
        }
        scene = parent;
        depth += 1;
    }
    return unknown('the walk ended without reaching a top-level page');
}

/**
 * Profile key → the user object that defines it, from the raw application payload.
 *
 * Knack identifies a role by a profile key (`profile_2`), and the only thing in the
 * payload that maps it to something a person recognises is the object carrying that
 * `profile_key`. The app's own profiles list was empty on the app measured, so the
 * objects are the source.
 */
export function buildProfileNameIndex(
    metadata: RuntimeMetadata | null,
): ProfileNameIndex {
    const index: ProfileNameIndex = new Map();
    const application =
        metadata && typeof metadata === 'object'
            ? ((metadata as Record<string, unknown>).application ?? metadata)
            : null;
    const objects =
        application && typeof application === 'object'
            ? (application as Record<string, unknown>).objects
            : null;
    if (!Array.isArray(objects)) return index;
    for (const item of objects) {
        if (!item || typeof item !== 'object') continue;
        const object = item as Record<string, unknown>;
        const profileKey =
            typeof object.profile_key === 'string' ? object.profile_key : null;
        const objectKey = typeof object.key === 'string' ? object.key : null;
        if (!profileKey || !objectKey || index.has(profileKey)) continue;
        index.set(profileKey, {
            profileKey,
            objectKey,
            objectName: typeof object.name === 'string' ? object.name : null,
        });
    }
    return index;
}

/** A role list with whatever human-readable label the payload can supply. */
export function describeRoles(
    roles: string[],
    names: ProfileNameIndex,
): Array<{
    profileKey: string;
    objectKey: string | null;
    objectName: string | null;
}> {
    return roles.map((profileKey) => {
        const entry = names.get(profileKey);
        return {
            profileKey,
            objectKey: entry?.objectKey ?? null,
            objectName: entry?.objectName ?? null,
        };
    });
}

/**
 * One clause naming an audience, for a prompt a person reads under time pressure.
 *
 * Role names are used when the payload supplies them, because "profile_2" tells the
 * person nothing about who is losing the page. The key is kept alongside so the
 * sentence can be checked against the builder.
 */
export function describeAudience(
    access: PageAccess,
    names: ProfileNameIndex,
): string {
    switch (access.status) {
        case 'public':
            return 'anyone (no login above it)';
        case 'unknown':
            return `unknown — ${access.reason}`;
        case 'protected': {
            const via = `login on ${access.loginSceneKey}`;
            if (access.roles === null) {
                return `logged-in users, roles unreadable (${via})`;
            }
            if (access.anyLoggedInUser) {
                return `any registered user (${via})`;
            }
            if (access.roles.length === 0) {
                return `no role at all — the login limits access to an empty list (${via})`;
            }
            const labels = describeRoles(access.roles, names).map((role) =>
                role.objectName
                    ? `${role.objectName} [${role.profileKey}]`
                    : role.profileKey,
            );
            return `only ${labels.join(', ')} (${via})`;
        }
    }
}

export type AudienceChange = 'same' | 'changed' | 'unknown';

/**
 * Whether two resolutions describe the same audience.
 *
 * Unknown on either side is unknown overall: "we could not read one of them" must never
 * collapse into "no change", because no change is the answer that lets a prompt go
 * quiet. Role lists compare as sets.
 */
export function compareAudience(
    before: PageAccess,
    after: PageAccess,
): AudienceChange {
    if (before.status === 'unknown' || after.status === 'unknown') {
        return 'unknown';
    }
    if (before.status !== after.status) return 'changed';
    if (before.status === 'public') return 'same';
    if (after.status !== 'protected') return 'changed';
    if (before.roles === null || after.roles === null) return 'unknown';
    if (before.anyLoggedInUser !== after.anyLoggedInUser) return 'changed';
    if (before.anyLoggedInUser) return 'same';
    const a = new Set(before.roles);
    const b = new Set(after.roles);
    if (a.size !== b.size) return 'changed';
    for (const role of a) if (!b.has(role)) return 'changed';
    return 'same';
}
