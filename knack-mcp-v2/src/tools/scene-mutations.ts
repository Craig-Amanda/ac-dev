/**
 * Scene (page) mutation tools — changes to a page's own properties, as opposed to the
 * views on it, which view-mutations.ts owns.
 *
 * The endpoints here were captured from Knack's Builder UI network traffic, not from the
 * public REST API reference, and authenticate the same way as every other request in this
 * server. None of them can delete or rebuild a page, so the view guard's cascade check,
 * human confirmation and scene-tree snapshot do not apply; what each tool does instead is
 * read fresh, change only what it was asked to, and read back.
 */
import { z } from 'zod';

import type { AppConfig } from '../config.js';
import type { KnackContext } from '../context.js';
import { VIEW_CACHE_STALE_NOTE } from '../lib/field-payload.js';
import {
    collectSceneViewLinks,
    findRawSceneInMetadata,
    getObjectAtPath,
    parseRuntimeScenes,
} from '../lib/metadata.js';
import {
    buildProfileNameIndex,
    isLoginScene,
    resolvePageAccess,
} from '../lib/page-access.js';
import {
    applyRuleEdit,
    assignSubmitRuleKeys,
    readRuleArray,
} from '../lib/rule-edits.js';
import { ruleFieldRefusal } from '../lib/field-exclusion.js';
import { deepEqual } from '../lib/structural-diff.js';
import { asRecord, parseJsonObjectArray } from '../lib/util.js';
import {
    type SceneNode,
    buildReferrerIndex,
    expandChildPages,
    readChangedScenes,
} from '../lib/view-safety.js';
import { type AnyToolDef, defineTool } from '../registry.js';
import { toolReplies } from '../response.js';
import { getFreshSceneTree } from '../view-mutation.js';
import { metadataCarriesViewLinks } from './views.js';

type RawRule = Record<string, unknown>;

/**
 * The scene verbatim off fresh metadata, which every tool here reads before and after
 * writing. The metadata comes back too, for a check that needs the rest of the app.
 */
async function readLiveScene(
    ctx: KnackContext,
    app: AppConfig,
    sceneKey: string,
) {
    ctx.caches.runtimeMetadata.delete(app.appKey);
    const metadata = await ctx.getRuntimeMetadata(app);
    return { metadata, scene: findRawSceneInMetadata(metadata, sceneKey) };
}

/** The reply envelope every tool here shares: which app, which action, which page. */
function sceneToolReplies(action: string, appKey: string, sceneKey: string) {
    const { respond, refuse } = toolReplies(appKey, action, { sceneKey });
    const refuseMissingScene = () =>
        refuse(
            'SCENE_NOT_FOUND',
            `${sceneKey} was not found in this app's metadata. Nothing was sent.`,
        );
    return { respond, refuse, refuseMissingScene };
}

/**
 * Give each incoming page rule the `key` it will be stored under, checked against the
 * rules the page already has.
 *
 * The Builder names page rules `submit_0`, `submit_1`, … in the order they were added
 * (captured 24 September on NPS Test App scene_220). Knack stores whatever key it is
 * sent, and two rules sharing one key on a page is a state the Builder never creates.
 *
 * @param existing The page's live rules, verbatim.
 * @param incoming The caller's new rules, in the order they will be appended.
 * @returns `incoming` with every rule carrying a key; throw a plain Error to refuse.
 */
export function assignPageRuleKeys(
    existing: RawRule[],
    incoming: RawRule[],
): RawRule[] {
    return assignSubmitRuleKeys(existing, incoming, 'rules', 'this page');
}

/**
 * Append page rules to a scene without disturbing the ones it already has.
 *
 * `POST /scenes/:key/rules` takes the page's **whole** rules array and replaces what is
 * stored — measured 24 September on NPS Test App scene_220: a second rule was saved by
 * a POST whose body carried both `submit_0` and `submit_1`. Sending only the new rule
 * would therefore delete every rule already on the page. This tool reads the live array
 * verbatim off fresh metadata (not through `readSceneRules`, which keeps only the fields
 * the read tools display and would strip `message`, `url`, `existing_page` and the rest
 * from every existing rule on the way back), appends, sends the lot, and reads it back.
 *
 * No snapshot file: `rulesBefore` in the response is the complete restore point, since
 * POSTing it unchanged puts the page back exactly as it was.
 */
export const addPageRules = defineTool({
    name: 'knack_add_page_rules',
    description:
        "Append page rules (show/hide views, message, redirect) to a page's existing rules; reads them live first.",
    access: 'view',
    input: {
        appKey: z.string().optional(),
        sceneKey: z.string(),
        rules: z.string().describe('JSON array of page rule objects to append'),
        previewOnly: z
            .boolean()
            .optional()
            .describe('Return the merged rules without sending them'),
    },
    handler: async ({ appKey, sceneKey, rules, previewOnly }, ctx) => {
        const app = ctx.getApp(appKey);
        ctx.getApiKey(app.appKey);

        const { respond, refuse, refuseMissingScene } = sceneToolReplies(
            'add_page_rules',
            app.appKey,
            sceneKey,
        );

        const incoming = parseJsonObjectArray('rules', rules, 'rule');
        // A rule reading a no-data field, or writing one without _mcp_allowwrite, is refused, as it is
        // on every rule and task tool.
        const refusal = ruleFieldRefusal(
            await ctx.getFieldExclusions(app),
            incoming,
            'a rule',
        );
        if (refusal) return refuse(refusal.error, refusal.message);

        const { scene } = await readLiveScene(ctx, app, sceneKey);
        if (!scene) return refuseMissingScene();
        const existing: RawRule[] = (
            Array.isArray(scene.rules) ? scene.rules : []
        ).filter((entry): entry is RawRule => asRecord(entry) !== null);

        const added = assignPageRuleKeys(existing, incoming);

        // A rule naming a view that is not on this page hides nothing, and knack_get_scene
        // would report it as dangling from the moment it was written.
        const viewsOnPage = new Set(
            (Array.isArray(scene.views) ? scene.views : []).map(
                (view) => asRecord(view)?.key,
            ),
        );
        const foreignViewKeys = added.flatMap((rule) =>
            (Array.isArray(rule.view_keys) ? rule.view_keys : []).filter(
                (key) => !viewsOnPage.has(key),
            ),
        );
        if (foreignViewKeys.length) {
            return refuse(
                'VIEW_NOT_ON_PAGE',
                `view_keys ${foreignViewKeys.join(', ')} are not on ${sceneKey}. A page rule can only show or hide that page's own views. Nothing was sent.`,
            );
        }

        const merged = [...existing, ...added];
        const summary = {
            ruleCountBefore: existing.length,
            ruleCountAfter: merged.length,
            addedKeys: added.map((rule) => rule.key),
        };

        if (previewOnly) {
            return respond({
                ok: true,
                previewOnly: true,
                ...summary,
                rules: merged,
            });
        }

        const result = await ctx.request(app, `/scenes/${sceneKey}/rules`, {
            method: 'POST',
            body: JSON.stringify({ rules: merged }),
        });
        if (!result.ok) {
            return respond({
                ok: false,
                status: result.status,
                body: result.body,
                message:
                    'Knack refused the write. The rules below are what the page had before, unchanged.',
                rulesBefore: existing,
            });
        }

        // Knack answers {"success":true} whatever it stored, so the stored array is
        // read back and compared rather than trusted.
        const { scene: after } = await readLiveScene(ctx, app, sceneKey);
        const storedRules = Array.isArray(after?.rules) ? after.rules : null;
        const verified = storedRules !== null && deepEqual(storedRules, merged);

        return respond({
            ok: true,
            status: result.status,
            ...summary,
            verified,
            ...(verified
                ? {}
                : {
                      warning:
                          'The rules read back from Knack differ from what was sent. Check the page in the Builder; rulesBefore restores the previous state.',
                      storedRules,
                  }),
            rulesBefore: existing,
            cacheNote: VIEW_CACHE_STALE_NOTE,
        });
    },
});

/**
 * Remove or replace a page's existing rules by key.
 *
 * The same endpoint and the same hazard as knack_add_page_rules: `POST /scenes/:key/rules`
 * replaces the stored array with the one sent. So this reads the live array verbatim,
 * edits it (lib/rule-edits.ts: unknown keys are refused, order is kept, a replacement
 * takes its original's place), sends the whole result and reads it back. `rulesBefore`
 * in the response is the complete restore point.
 */
export const editPageRules = defineTool({
    name: 'knack_edit_page_rules',
    description:
        "Remove or replace a page's existing rules by key; reads them live and sends the rest back unchanged.",
    access: 'view',
    input: {
        appKey: z.string().optional(),
        sceneKey: z.string(),
        removeKeys: z
            .array(z.string())
            .optional()
            .describe('Keys of rules to remove, e.g. ["submit_1"]'),
        replaceRules: z
            .string()
            .optional()
            .describe(
                'JSON array of whole rules, each carrying the key of the stored rule it replaces',
            ),
        previewOnly: z
            .boolean()
            .optional()
            .describe('Return the edited rules without sending them'),
    },
    handler: async (
        { appKey, sceneKey, removeKeys, replaceRules, previewOnly },
        ctx,
    ) => {
        const app = ctx.getApp(appKey);
        ctx.getApiKey(app.appKey);

        const { respond, refuse, refuseMissingScene } = sceneToolReplies(
            'edit_page_rules',
            app.appKey,
            sceneKey,
        );

        const replacements = replaceRules
            ? parseJsonObjectArray('replaceRules', replaceRules, 'rule')
            : undefined;
        // A rule reading a no-data field, or writing one without _mcp_allowwrite, is refused, as it is
        // on every rule and task tool.
        const refusal = ruleFieldRefusal(
            await ctx.getFieldExclusions(app),
            replacements ?? [],
            'a rule',
        );
        if (refusal) return refuse(refusal.error, refusal.message);

        const { scene } = await readLiveScene(ctx, app, sceneKey);
        if (!scene) return refuseMissingScene();
        const existing = readRuleArray(scene.rules);

        let edited: ReturnType<typeof applyRuleEdit>;
        try {
            edited = applyRuleEdit(
                existing,
                { removeKeys, replaceRules: replacements },
                'page rule',
            );
        } catch (error) {
            return refuse('INVALID_EDIT', (error as Error).message);
        }

        // A replacement naming a view that is not on this page hides nothing.
        const viewsOnPage = new Set(
            (Array.isArray(scene.views) ? scene.views : []).map(
                (view) => asRecord(view)?.key,
            ),
        );
        const foreignViewKeys = (replacements ?? []).flatMap((rule) =>
            (Array.isArray(rule.view_keys) ? rule.view_keys : []).filter(
                (key) => !viewsOnPage.has(key),
            ),
        );
        if (foreignViewKeys.length) {
            return refuse(
                'VIEW_NOT_ON_PAGE',
                `view_keys ${foreignViewKeys.join(', ')} are not on ${sceneKey}. A page rule can only show or hide that page's own views. Nothing was sent.`,
            );
        }

        const summary = {
            ruleCountBefore: existing.length,
            ruleCountAfter: edited.rules.length,
            removedKeys: edited.removedKeys,
            replacedKeys: edited.replacedKeys,
        };

        if (previewOnly) {
            return respond({
                ok: true,
                previewOnly: true,
                ...summary,
                rules: edited.rules,
                rulesBefore: existing,
            });
        }

        const result = await ctx.request(app, `/scenes/${sceneKey}/rules`, {
            method: 'POST',
            body: JSON.stringify({ rules: edited.rules }),
        });
        if (!result.ok) {
            return respond({
                ok: false,
                status: result.status,
                body: result.body,
                message:
                    'Knack refused the write. The rules below are what the page had before, unchanged.',
                rulesBefore: existing,
            });
        }

        const { scene: after } = await readLiveScene(ctx, app, sceneKey);
        const storedRules = Array.isArray(after?.rules) ? after.rules : null;
        const verified =
            storedRules !== null && deepEqual(storedRules, edited.rules);

        return respond({
            ok: true,
            status: result.status,
            ...summary,
            verified,
            ...(verified
                ? {}
                : {
                      warning:
                          'The rules read back from Knack differ from what was sent. Check the page in the Builder; rulesBefore restores the previous state.',
                      storedRules,
                  }),
            rulesBefore: existing,
            cacheNote: VIEW_CACHE_STALE_NOTE,
        });
    },
});

/**
 * Tool input name → Knack's scene property, in the order the Builder dialog shows them.
 *
 * Deliberately a closed list, and it must never grow an access property. Measured with
 * the REST API key on NP Place Playground (25 September):
 * - `PUT /scenes/:key {authenticated: false}` on a **public** page answered 200 and
 *   **deleted the page** with its views (`changes.deletes` listed it).
 * - `{authenticated: true, allowed_profiles, limit_profile_access}` (the Builder's
 *   "require login" save) answered 500 and changed nothing, with or without `views`.
 * - Every `PUT` of a login view's `allowed_profiles` / `limit_profile_access` answered
 *   500: the Builder's own body, a minimal one, and the whole view.
 * - The Builder removes a login with `PUT /scenes/<login scene> {views, authenticated:
 *   false}`, which deletes the login scene and lifts its page to the top level. That is
 *   what `authenticated: false` means to Knack: "delete this login scene". Sent to an
 *   ordinary page, it deletes that page. With the API key, the Builder's exact removal
 *   body answered 500, as did its "add login to a page with views" body.
 * So page access is only ever set at creation (knack_create_page's `login`).
 */
export const PAGE_SETTINGS = {
    name: 'name',
    slug: 'slug',
    print: 'print',
    modal: 'modal',
    keepModalOpen: 'modal_prevent_background_click_close',
} as const;
type PageSettingInput = keyof typeof PAGE_SETTINGS;

/**
 * Change a page's name, URL slug, print link and modal options — the Builder's Page
 * Settings dialog.
 *
 * `PUT /scenes/:key` merges: measured 24 September on NPS Test App scene_220, a body of
 * `{"name":"Finance"}` alone changed the name and left views, layout, rules and every
 * other property identical to a backup taken just before. The Builder resends the page's
 * whole `views` array with every save; this tool deliberately does not, since its copy
 * would come from runtime metadata rather than the Builder's own, and a wrong `views` is
 * the one thing here that could damage the page.
 *
 * A slug change is the consequential one. Knack rewrites the menu links that pointed at
 * the old slug and lists them in `changes.updates` (captured the same day: view_370 on
 * scene_38), which this tool passes back. Bookmarks and any link Knack did not list still
 * point at the old URL. `allowed_profiles` is left out on purpose: who may reach a page
 * is an access change and belongs in its own tool.
 */
export const updatePageSettings = defineTool({
    name: 'knack_update_page_settings',
    description:
        "Change a page's name, URL slug, print link or modal options; sends only what changes, never the views.",
    access: 'view',
    input: {
        appKey: z.string().optional(),
        sceneKey: z.string(),
        name: z.string().optional(),
        slug: z.string().optional(),
        print: z.boolean().optional(),
        modal: z.boolean().optional(),
        keepModalOpen: z
            .boolean()
            .optional()
            .describe('Modal ignores background clicks until an action'),
        previewOnly: z
            .boolean()
            .optional()
            .describe('Return the changes without sending them'),
    },
    handler: async ({ appKey, sceneKey, previewOnly, ...settings }, ctx) => {
        const app = ctx.getApp(appKey);
        ctx.getApiKey(app.appKey);

        const { respond, refuse, refuseMissingScene } = sceneToolReplies(
            'update_page_settings',
            app.appKey,
            sceneKey,
        );

        const requested = (
            Object.keys(PAGE_SETTINGS) as PageSettingInput[]
        ).filter((input) => settings[input] !== undefined);
        if (!requested.length) {
            return refuse(
                'NOTHING_TO_CHANGE',
                `Pass at least one of ${Object.keys(PAGE_SETTINGS).join(', ')}. Nothing was sent.`,
            );
        }
        if (settings.name !== undefined && !settings.name.trim()) {
            return refuse(
                'INVALID_NAME',
                'name cannot be blank. Nothing was sent.',
            );
        }

        // Knack slugs are lower-case words joined by hyphens (finance2, roll-details3);
        // anything else would be a URL the Builder itself never produces. Checked before
        // the fetch, since it needs nothing from the app.
        if (
            settings.slug !== undefined &&
            !/^[a-z0-9]+(?:-[a-z0-9]+)*$/.test(settings.slug)
        ) {
            return refuse(
                'INVALID_SLUG',
                `slug "${settings.slug}" must be lower-case letters and digits joined by single hyphens, e.g. "finance-reports". Nothing was sent.`,
            );
        }

        const { metadata, scene } = await readLiveScene(ctx, app, sceneKey);
        if (!scene) return refuseMissingScene();

        if (settings.slug !== undefined) {
            const clash = parseRuntimeScenes(metadata).find(
                (other) =>
                    other.sceneSlug === settings.slug &&
                    other.sceneKey !== sceneKey,
            );
            if (clash) {
                return refuse(
                    'SLUG_IN_USE',
                    `slug "${settings.slug}" already belongs to ${clash.sceneKey} ("${clash.sceneName}"). Nothing was sent.`,
                );
            }
        }

        const modalAfter = settings.modal ?? scene.modal === true;
        if (settings.keepModalOpen === true && !modalAfter) {
            return refuse(
                'NOT_A_MODAL',
                'keepModalOpen only applies to a page shown as a modal. Pass modal: true as well, or leave keepModalOpen out. Nothing was sent.',
            );
        }

        // Only what actually differs goes on the wire, so a repeated call is a no-op.
        const body: Record<string, unknown> = {};
        const before: Record<string, unknown> = {};
        for (const input of requested) {
            const property = PAGE_SETTINGS[input];
            if (deepEqual(scene[property], settings[input])) continue;
            body[property] = settings[input];
            // null, not undefined: an unset property must still show in the JSON reply.
            before[input] = scene[property] ?? null;
        }
        if (!Object.keys(body).length) {
            return respond({
                ok: true,
                unchanged: true,
                message:
                    'The page already has every value passed. Nothing was sent.',
            });
        }

        const slugNote =
            'slug' in body
                ? `The page's URL changes from "${String(scene.slug)}" to "${String(body.slug)}". Bookmarks and outside links to the old URL will stop working. Knack repoints the in-app links it lists in knackUpdated; run knack_list_page_referrers to check nothing else still names the old slug.`
                : undefined;

        if (previewOnly) {
            return respond({
                ok: true,
                previewOnly: true,
                wouldSend: body,
                before,
                ...(slugNote ? { slugNote } : {}),
            });
        }

        const result = await ctx.request(app, `/scenes/${sceneKey}`, {
            method: 'PUT',
            body: JSON.stringify(body),
        });
        if (!result.ok) {
            return respond({
                ok: false,
                status: result.status,
                body: result.body,
                message: 'Knack refused the change. The page is as it was.',
            });
        }

        // What else Knack edited as a consequence — for a slug change, the views whose
        // links it repointed. Keys only: the full view definitions would swamp the reply.
        // `?? undefined`: a key missing from Knack's reply is dropped, not sent as null.
        const updated = (kind: string) => {
            const list = getObjectAtPath(
                result.body,
                'changes',
                'updates',
                kind,
            );
            return Array.isArray(list) ? list : [];
        };
        const knackUpdated = {
            scenes: updated('scenes').map(
                (entry) => getObjectAtPath(entry, 'key') ?? undefined,
            ),
            views: updated('views').map((entry) => ({
                sceneKey: getObjectAtPath(entry, 'scene', 'key') ?? undefined,
                viewKey: getObjectAtPath(entry, 'view', 'key') ?? undefined,
            })),
        };

        const { scene: after } = await readLiveScene(ctx, app, sceneKey);
        const mismatched = Object.keys(body).filter(
            (property) => !deepEqual(after?.[property], body[property]),
        );

        return respond({
            ok: true,
            status: result.status,
            sent: body,
            before,
            verified: after !== null && mismatched.length === 0,
            ...(mismatched.length
                ? {
                      warning: `Read back from Knack, ${mismatched.join(', ')} did not take the value sent. Check the page in the Builder.`,
                  }
                : {}),
            knackUpdated,
            ...(slugNote ? { slugNote } : {}),
            cacheNote: VIEW_CACHE_STALE_NOTE,
        });
    },
});

/** The keys Knack reports under `changes.<kind>.scenes` in a write's reply. */
function changedSceneKeys(
    body: unknown,
    kind: 'inserts' | 'deletes',
): string[] {
    return readChangedScenes(body, kind).map((scene) => scene.sceneKey);
}

/**
 * Create a top-level page, public or behind a login.
 *
 * Captured from the Builder on 25 September (NPS Test App, scene_608 to scene_610):
 * `POST /scenes` with `login_vars: null` makes a public page. With `login_vars`
 * (`authenticated`, `allowed_profiles`, `limit_profile_access`) Knack also inserts a
 * `type: "authentication"` scene holding a login view, and parents the new page under
 * it — the same shape lib/page-access.ts reads. So this is also the one way to set a
 * page's roles through MCP: at creation. Changing them later is not measured yet.
 *
 * The new page is empty; add views with knack_create_view.
 */
export const createPage = defineTool({
    name: 'knack_create_page',
    description:
        'Create an empty top-level page, public or behind a login limited to chosen roles; reads it back to verify.',
    access: 'view',
    input: {
        appKey: z.string().optional(),
        name: z.string(),
        login: z
            .object({
                roles: z
                    .array(z.string())
                    .optional()
                    .describe('Profile keys allowed in, e.g. ["profile_8"]'),
                anyLoggedInUser: z
                    .boolean()
                    .optional()
                    .describe('Let every logged-in user in instead of roles'),
            })
            .optional()
            .describe('Put the page behind a login; omit for a public page'),
        previewOnly: z
            .boolean()
            .optional()
            .describe('Return the request without sending it'),
    },
    handler: async ({ appKey, name, login, previewOnly }, ctx) => {
        const app = ctx.getApp(appKey);
        ctx.getApiKey(app.appKey);
        const { respond, refuse } = toolReplies(app.appKey, 'create_page');

        if (!name.trim()) {
            return refuse(
                'INVALID_NAME',
                'name cannot be blank. Nothing was sent.',
            );
        }
        const roles = login?.roles ?? [];
        if (login && Boolean(login.anyLoggedInUser) === roles.length > 0) {
            return refuse(
                'INVALID_LOGIN',
                'login needs exactly one of roles (a non-empty list) or anyLoggedInUser: true. Nothing was sent.',
            );
        }

        ctx.caches.runtimeMetadata.delete(app.appKey);
        const metadata = await ctx.getRuntimeMetadata(app);
        if (!metadata) {
            return refuse(
                'METADATA_UNAVAILABLE',
                'Runtime metadata could not be fetched, so roles and existing pages could not be checked. Nothing was sent.',
            );
        }
        const knownRoles = buildProfileNameIndex(metadata);
        const unknownRoles = roles.filter((role) => !knownRoles.has(role));
        if (unknownRoles.length) {
            return refuse(
                'UNKNOWN_ROLE',
                `${unknownRoles.join(', ')} ${unknownRoles.length === 1 ? 'is not a role' : 'are not roles'} in this app. Known: ${[...knownRoles.keys()].join(', ') || 'none'}. Nothing was sent.`,
            );
        }

        // The two bodies are what the Builder sent, key for key: the login variant
        // carries neither type nor parent, since Knack decides both.
        const body: Record<string, unknown> = login
            ? {
                  name,
                  views: [],
                  authenticated: false,
                  login_vars: {
                      authenticated: true,
                      allowed_profiles: roles,
                      limit_profile_access: roles.length > 0,
                  },
              }
            : {
                  name,
                  type: 'page',
                  views: [],
                  authenticated: false,
                  login_vars: null,
                  menu_pages: null,
                  parent: null,
              };

        const sameName = parseRuntimeScenes(metadata).filter(
            (scene) => scene.sceneName === name,
        );
        const nameNote = sameName.length
            ? `${sameName.map((scene) => scene.sceneKey).join(', ')} already ${sameName.length === 1 ? 'has' : 'have'} this name; Knack gives the new page its own slug.`
            : undefined;

        if (previewOnly) {
            return respond({
                ok: true,
                previewOnly: true,
                wouldSend: body,
                ...(nameNote ? { nameNote } : {}),
            });
        }

        const result = await ctx.request(app, '/scenes', {
            method: 'POST',
            body: JSON.stringify(body),
        });
        const created = asRecord(asRecord(result.body)?.scene);
        const sceneKey = typeof created?.key === 'string' ? created.key : null;
        if (!result.ok || !sceneKey) {
            return respond({
                ok: false,
                status: result.status,
                body: result.body,
                message: 'Knack did not create the page.',
            });
        }
        const insertedScenes = changedSceneKeys(result.body, 'inserts');

        // Read back: the page exists, and a login page admits exactly who was asked for.
        const tree = await getFreshSceneTree(ctx, app);
        const exists =
            tree.ok && tree.scenes.some((scene) => scene.sceneKey === sceneKey);
        const access = tree.ok
            ? resolvePageAccess(sceneKey, tree.scenes)
            : null;
        const accessMatches = !login
            ? access?.status === 'public'
            : access?.status === 'protected' &&
              (login.anyLoggedInUser
                  ? access.anyLoggedInUser
                  : deepEqual(
                        [...(access.roles ?? [])].sort(),
                        [...roles].sort(),
                    ));
        const verified = exists && accessMatches;

        return respond({
            ok: true,
            status: result.status,
            sceneKey,
            sceneSlug: created?.slug ?? null,
            ...(login
                ? {
                      loginSceneKey:
                          access?.loginSceneKey ?? insertedScenes[0] ?? null,
                      loginViewKey: access?.loginViewKey ?? null,
                  }
                : {}),
            access: access
                ? { status: access.status, roles: access.roles }
                : null,
            verified,
            ...(verified
                ? {}
                : {
                      warning:
                          'Read back from Knack, the page or its access does not match what was asked for. Check it in the Builder.',
                  }),
            ...(nameNote ? { nameNote } : {}),
            next: 'The page is empty. Add views with knack_create_view, and change name or slug with knack_update_page_settings.',
            cacheNote: VIEW_CACHE_STALE_NOTE,
        });
    },
});

/**
 * Delete a page, refusing when it would take any page with it but its own login.
 *
 * Captured 25 September: deleting a page with a login, the Builder sent
 * `DELETE /scenes/<the login scene>`, and Knack deleted that scene and the page under
 * it (`changes.deletes.scenes` listed both). So a delete takes every child page too.
 *
 * With the REST API key that request is refused: measured the same day on NP Place
 * Playground, `DELETE /scenes/<login scene>` answered 500 twice and changed nothing.
 * Deleting the **page** instead answered 200, and Knack removed its login scene with
 * it, since that login then guarded nothing (both keys in `changes.deletes`). So this
 * tool always deletes the page it was given and expects its lone login to go too. It
 * refuses any delete that would remove other pages: this client cannot put a cascade to a person
 * (see the cascade rule in knack_list_apps), so those stay in the Builder. The home page
 * is never deleted. Views elsewhere that link here are listed, since they are left
 * pointing at a page that no longer exists.
 */
export const deletePage = defineTool({
    name: 'knack_delete_page',
    description:
        'Delete a page (and its own login page); refuses if other pages would go too. Previews unless confirm is true.',
    access: 'view-delete',
    input: {
        appKey: z.string().optional(),
        sceneKey: z.string(),
        confirm: z
            .boolean()
            .optional()
            .default(false)
            .describe(
                'Must be true to delete; otherwise a preview is returned',
            ),
    },
    handler: async ({ appKey, sceneKey, confirm }, ctx) => {
        const app = ctx.getApp(appKey);
        ctx.getApiKey(app.appKey);
        const { respond, refuse, refuseMissingScene } = sceneToolReplies(
            'delete_page',
            app.appKey,
            sceneKey,
        );

        const tree = await getFreshSceneTree(ctx, app);
        if (!tree.ok) {
            return refuse(
                'SCENE_TREE_UNAVAILABLE',
                `The page tree could not be read (${tree.reason}), so what a delete would take with it is unknown. Nothing was sent.`,
            );
        }
        const target = tree.scenes.find((scene) => scene.sceneKey === sceneKey);
        if (!target) return refuseMissingScene();

        const metadata = await ctx.getRuntimeMetadata(app);
        const home = asRecord(
            getObjectAtPath(metadata, 'application', 'home_scene'),
        );
        if (home?.key === sceneKey || home?.slug === target.sceneSlug) {
            return refuse(
                'HOME_PAGE',
                `${sceneKey} is the app's home page and cannot be deleted. Nothing was sent.`,
            );
        }

        // Delete the login with the page when that login guards this page alone, as the
        // Builder does; otherwise the login is left guarding nothing.
        const sceneByRef = new Map<string, (typeof tree.scenes)[number]>();
        // First match wins, as the scan this replaces did.
        for (const scene of tree.scenes) {
            for (const ref of [scene.sceneSlug, scene.sceneKey]) {
                if (ref && !sceneByRef.has(ref)) sceneByRef.set(ref, scene);
            }
        }
        const bySlugOrKey = (ref: string | undefined) =>
            ref ? sceneByRef.get(ref) : undefined;
        const parent = bySlugOrKey(target.parentRef);
        const parentChildren = parent
            ? tree.scenes.filter(
                  (scene) =>
                      bySlugOrKey(scene.parentRef)?.sceneKey ===
                      parent.sceneKey,
              )
            : [];
        const root =
            parent && isLoginScene(parent) && parentChildren.length === 1
                ? parent
                : target;
        // The login going with the page may itself be the app's home page.
        if (
            root !== target &&
            (home?.key === root.sceneKey || home?.slug === root.sceneSlug)
        ) {
            return refuse(
                'HOME_PAGE',
                `${sceneKey} is the only page behind ${root.sceneKey}, the app's home page, so deleting it would delete the home page too. Nothing was sent.`,
            );
        }

        const nodes: SceneNode[] = tree.scenes.map((scene) => ({
            sceneKey: scene.sceneKey,
            sceneName: scene.sceneName,
            sceneSlug: scene.sceneSlug,
            parentRef: scene.parentRef,
        }));
        const expansion = expandChildPages([root.sceneKey], nodes);
        const doomed = expansion.pages.map((page) => page.sceneKey);
        const expected = new Set([root.sceneKey, target.sceneKey]);
        const others = expansion.pages.filter(
            (page) => !expected.has(page.sceneKey),
        );
        if (others.length || expansion.truncated) {
            return refuse(
                'WOULD_DELETE_OTHER_PAGES',
                `Deleting ${sceneKey} would also delete ${others.map((page) => page.sceneKey).join(', ')}${expansion.truncated ? ' and more (the tree runs deeper than can be walked)' : ''}. This client cannot put a cascade to a person, so delete those pages first or use the Knack builder. Nothing was sent.`,
            );
        }

        const linksByScene =
            metadata && metadataCarriesViewLinks(metadata)
                ? collectSceneViewLinks(metadata)
                : null;
        const index = linksByScene
            ? buildReferrerIndex(
                  nodes.map((node) => ({
                      ...node,
                      views: linksByScene.get(node.sceneKey) ?? [],
                  })),
              )
            : null;
        const referrers = index
            ? doomed.flatMap((key) => index.get(key) ?? [])
            : null;

        const preview = {
            deletes: doomed,
            deleteRequest: `DELETE /scenes/${sceneKey}`,
            ...(root.sceneKey !== sceneKey
                ? {
                      loginNote: `${root.sceneKey} is the login page guarding only ${sceneKey}, so Knack deletes it with the page.`,
                  }
                : {}),
            linkedFrom: referrers,
            ...(referrers === null
                ? {
                      linkedFromNote:
                          'Which views link here could not be read, so links left pointing at the deleted page cannot be listed.',
                  }
                : referrers.length
                  ? {
                        linkedFromNote:
                            'These views keep a link to a page that will no longer exist. Remove or repoint them afterwards.',
                    }
                  : {}),
        };

        if (!confirm) {
            return respond({
                ok: false,
                action: 'delete_page_preflight',
                message: `This would permanently delete ${doomed.join(' and ')}, with every view on ${doomed.length === 1 ? 'it' : 'them'}. This cannot be undone. Pass confirm: true only after explicitly confirming this with the user.`,
                ...preview,
            });
        }

        const result = await ctx.request(app, `/scenes/${sceneKey}`, {
            method: 'DELETE',
        });
        if (!result.ok) {
            return respond({
                ok: false,
                status: result.status,
                body: result.body,
                message: 'Knack refused the delete. Nothing was removed.',
            });
        }

        // Read back: exactly the expected pages are gone.
        const before = new Set(tree.scenes.map((scene) => scene.sceneKey));
        const after = await getFreshSceneTree(ctx, app);
        const remaining = after.ok
            ? new Set(after.scenes.map((scene) => scene.sceneKey))
            : null;
        const removed = remaining
            ? [...before].filter((key) => !remaining.has(key))
            : null;
        const verified =
            removed !== null &&
            removed.length === doomed.length &&
            doomed.every((key) => removed.includes(key));

        return respond({
            ok: true,
            status: result.status,
            deleted: removed ?? changedSceneKeys(result.body, 'deletes'),
            verified,
            ...(verified
                ? {}
                : {
                      warning: `Expected ${doomed.join(', ')} to be removed; read back from Knack, ${removed ? removed.join(', ') || 'nothing' : 'the page tree could not be read'} was. Check the Builder.`,
                  }),
            linkedFrom: referrers,
            cacheNote: VIEW_CACHE_STALE_NOTE,
        });
    },
});

export const sceneMutationTools: AnyToolDef[] = [
    createPage,
    addPageRules,
    editPageRules,
    updatePageSettings,
    deletePage,
];
