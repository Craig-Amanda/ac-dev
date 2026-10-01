/**
 * What an app's structure gives away to anyone who has its application ID.
 *
 * Knack serves the application payload (objects, pages, views, rules, tasks) without a
 * key, because the live app loads itself from it. Two things in it matter most:
 *
 * - An email address typed into the app: into an email rule's recipients or text, a
 *   task's email, or any other text a view carries. It is readable by anyone, and an
 *   address in a rule is usually a person's.
 * - A form on a page anyone can reach. It accepts submissions from anyone, and its
 *   configuration names the table and fields it writes.
 *
 * Pure: no I/O. The tool in tools/analysis.ts feeds it the view map, the scenes and the
 * runtime metadata; the registry uses `typedEmailsInEmailRules` on a write's arguments.
 */
import { resolvePageAccess } from './page-access.js';
import { asRecord } from './util.js';
import type { CachedViewMap, SceneInfo } from '../types.js';

/** An address as a person would type it. Knack placeholders (`{field_1}`) never match. */
const EMAIL_ADDRESS = /[A-Z0-9._%+-]+@[A-Z0-9-]+(?:\.[A-Z0-9-]+)*\.[A-Z]{2,}/gi;

/** Keys under which Knack keeps an email's settings, on a rule or a task action. */
const EMAIL_KEYS = new Set(['email', 'emails']);

/** Walks stop here, so a malformed or cyclic payload cannot run away. */
const MAX_DEPTH = 40;

export type TypedEmail = {
    /** The address with its local part hidden: `j***@example.com`. */
    address: string;
    /** Dotted path from the scanned value to the string that holds it. */
    path: string;
    /** Whether the string sits inside an email's settings (a rule or a task action). */
    inEmail: boolean;
};

/** `jane.doe@example.com` → `j***@example.com`. The domain stays, since it says whose. */
export function maskEmailAddress(address: string): string {
    const at = address.lastIndexOf('@');
    if (at <= 0) return '***';
    return `${address[0]}***${address.slice(at)}`;
}

export type TypedEmailOptions = {
    /**
     * The app's own sender address (`settings.from_email`). The Builder pre-fills every
     * email rule's From with it, so a rule's `from_email` equal to it is the app default,
     * reported once with the settings, not a sender someone typed into the rule.
     */
    appDefaultSender?: string | null;
};

/**
 * Every typed email address in a value, with where it sits. Strings that hold JSON are
 * read as JSON first, so a caller's JSON argument is scanned by structure, not as text.
 */
export function findTypedEmails(
    value: unknown,
    basePath = '$',
    options: TypedEmailOptions = {},
): TypedEmail[] {
    const defaultSender =
        options.appDefaultSender?.trim().toLowerCase() || null;
    const out: TypedEmail[] = [];
    const seen = new WeakSet<object>();

    const walk = (
        node: unknown,
        path: string,
        inEmail: boolean,
        depth: number,
    ): void => {
        if (depth > MAX_DEPTH) return;
        if (typeof node === 'string') {
            const parsed = parseJsonObject(node);
            if (parsed !== null) {
                walk(parsed, path, inEmail, depth + 1);
                return;
            }
            const isSender = path.endsWith('.from_email');
            for (const match of node.match(EMAIL_ADDRESS) ?? []) {
                if (isSender && match.toLowerCase() === defaultSender) continue;
                out.push({ address: maskEmailAddress(match), path, inEmail });
            }
            return;
        }
        if (!node || typeof node !== 'object') return;
        if (seen.has(node)) return;
        seen.add(node);

        if (Array.isArray(node)) {
            node.forEach((item, index) =>
                walk(item, `${path}.${index}`, inEmail, depth + 1),
            );
            return;
        }
        const record = node as Record<string, unknown>;
        const isEmailAction =
            typeof record.action === 'string' &&
            record.action.toLowerCase() === 'email';
        for (const [key, child] of Object.entries(record)) {
            walk(
                child,
                `${path}.${key}`,
                inEmail || isEmailAction || EMAIL_KEYS.has(key.toLowerCase()),
                depth + 1,
            );
        }
    };

    walk(value, basePath, false, 0);
    return out;
}

/** Only the addresses inside an email's settings: what a write that adds an email types. */
export function typedEmailsInEmailRules(
    value: unknown,
    options: TypedEmailOptions = {},
): TypedEmail[] {
    return findTypedEmails(value, '$', options).filter((hit) => hit.inEmail);
}

/** The app's settings block (`application.settings`), where its own addresses live. */
export function readAppSettings(
    metadata: unknown,
): Record<string, unknown> | null {
    const root = asRecord(metadata);
    return (
        asRecord(asRecord(root?.application)?.settings) ??
        asRecord(root?.settings)
    );
}

/** The app's default sender address, when its settings carry one. */
export function readAppDefaultSender(metadata: unknown): string | null {
    const sender = readAppSettings(metadata)?.from_email;
    return typeof sender === 'string' && sender.trim() ? sender.trim() : null;
}

export type EmailExposure = TypedEmail & {
    where: 'view' | 'task';
    viewKey?: string;
    viewName?: string;
    sceneKey?: string;
    objectKey?: string;
    taskKey?: string;
    taskName?: string;
};

export type FormExposure = {
    viewKey: string;
    viewName: string | undefined;
    sceneKey: string;
    sceneName: string | undefined;
    sceneSlug: string | undefined;
    /**
     * `public`; `unknown` when the page's ancestry could not be walked; `account` when
     * the page is, or sits under, one of Knack's account pages (`type: "user"`).
     */
    access: 'public' | 'unknown' | 'account';
    reason: string;
    /** What the form does to a record: `insert`, `update` or whatever Knack stored. */
    action: string | null;
    /** The table the form writes to, when the view names one. */
    objectKey: string | null;
};

export type ExposureAudit = {
    /** Addresses in the app's own settings, such as `from_email` and `technical_contact`. */
    settingsEmails: TypedEmail[];
    /** Typed addresses in views and tasks; a rule's sender equal to the app default is left out. */
    typedEmails: EmailExposure[];
    publicForms: FormExposure[];
    /**
     * Forms on Knack's account pages (Account Settings and pages beneath it). The login
     * walk finds no login above them, but Knack shows an account page only to a
     * logged-in user, so they are listed apart from `publicForms` rather than as public.
     * Seen on NPS Test App on 30 September (Account Settings, Change Password); not
     * measured against a logged-out visitor.
     */
    accountForms: FormExposure[];
    truncated: boolean;
};

/** View types that accept a submission from whoever can see them. */
const SUBMITTING_VIEW_TYPES = new Set([
    'form',
    // Sign-up: public by construction, and writes a record to a user object.
    'registration',
    // E-commerce views that take a payment or a customer's details.
    'checkout',
    'customer',
]);

/**
 * Audit an app's structure for typed email addresses and forms on public pages.
 *
 * @param input.viewMap Every view's configuration, keyed by view key.
 * @param input.scenes The app's scenes, as parseRuntimeScenes returns them.
 * @param input.viewScenes Which scene each view sits on.
 * @param input.tasks Every scheduled task, with the object it runs on.
 * @param maxResults Cap on each list; `truncated` says when one was reached.
 */
export function auditExposure(
    input: {
        viewMap: CachedViewMap;
        scenes: SceneInfo[];
        viewScenes: Record<string, { sceneKey?: string }>;
        tasks: Array<Record<string, unknown>>;
        /** The app's settings block, as `readAppSettings` returns it. */
        settings?: Record<string, unknown> | null;
    },
    maxResults = 500,
): ExposureAudit {
    const defaultSender =
        typeof input.settings?.from_email === 'string'
            ? input.settings.from_email
            : null;
    const options: TypedEmailOptions = { appDefaultSender: defaultSender };
    // The app's own addresses (default sender, technical contact) are public in their
    // own right, so each is reported once here rather than once per rule using it.
    const settingsEmails = input.settings
        ? findTypedEmails(input.settings, '$.settings')
        : [];
    const typedEmails: EmailExposure[] = [];
    let truncated = false;
    const push = (hit: EmailExposure) => {
        if (typedEmails.length >= maxResults) {
            truncated = true;
            return;
        }
        typedEmails.push(hit);
    };

    for (const [viewKey, attrs] of Object.entries(input.viewMap)) {
        for (const hit of findTypedEmails(attrs, '$', options)) {
            push({
                ...hit,
                where: 'view',
                viewKey,
                viewName:
                    typeof attrs.name === 'string' ? attrs.name : undefined,
                sceneKey: input.viewScenes[viewKey]?.sceneKey,
            });
        }
    }
    for (const task of input.tasks) {
        for (const hit of findTypedEmails(task.action, '$', options)) {
            push({
                ...hit,
                path: hit.path.replace(/^\$/, '$.action'),
                where: 'task',
                objectKey:
                    typeof task.object_key === 'string'
                        ? task.object_key
                        : undefined,
                taskKey: typeof task.key === 'string' ? task.key : undefined,
                taskName: typeof task.name === 'string' ? task.name : undefined,
            });
        }
    }

    const publicForms: FormExposure[] = [];
    const accountForms: FormExposure[] = [];
    const sceneTypes = new Map(
        input.scenes.map((scene) => [scene.sceneKey, scene.sceneType]),
    );
    for (const scene of input.scenes) {
        const forms = scene.views.filter(
            (view) => view.viewType && SUBMITTING_VIEW_TYPES.has(view.viewType),
        );
        if (!forms.length) continue;
        const access = resolvePageAccess(scene.sceneKey, input.scenes);
        if (access.status === 'protected') continue;
        const onAccountPage = [scene.sceneKey, ...access.ancestry].some(
            (key) => sceneTypes.get(key) === 'user',
        );
        const list = onAccountPage ? accountForms : publicForms;
        for (const form of forms) {
            if (list.length >= maxResults) {
                truncated = true;
                break;
            }
            const attrs = input.viewMap[form.viewKey] ?? {};
            const source = asRecord(attrs.source);
            list.push({
                viewKey: form.viewKey,
                viewName: form.viewName,
                sceneKey: scene.sceneKey,
                sceneName: scene.sceneName,
                sceneSlug: scene.sceneSlug,
                access: onAccountPage ? 'account' : access.status,
                reason: onAccountPage
                    ? `On a Knack account page (type "user"), which Knack shows only to a logged-in user. The login walk found: ${access.reason}`
                    : access.reason,
                action: typeof attrs.action === 'string' ? attrs.action : null,
                objectKey:
                    typeof source?.object === 'string' ? source.object : null,
            });
        }
    }

    return {
        settingsEmails,
        typedEmails,
        publicForms,
        accountForms,
        truncated,
    };
}

function parseJsonObject(text: string): unknown {
    const trimmed = text.trim();
    if (!trimmed.startsWith('{') && !trimmed.startsWith('[')) return null;
    try {
        const parsed: unknown = JSON.parse(trimmed);
        return parsed && typeof parsed === 'object' ? parsed : null;
    } catch {
        return null;
    }
}
