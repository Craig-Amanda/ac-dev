import { fileURLToPath } from 'node:url';
import path from 'node:path';
import fs from 'node:fs';
import { readJsonFile } from './util.js';

/**
 * Which code this process is actually running.
 *
 * Three separate incidents traced back to the same blind spot: a client showing an
 * older response shape than the checkout it was pointed at. A branch that never
 * merged, a `dist/` that was never rebuilt, and a long-lived server process that
 * predated a `git checkout` all present identically — a missing key — and none of
 * them can be told apart from the response itself. So the server states its own
 * identity, and a stale build says so instead of leaving it to be inferred.
 */

/** Captured once at module load: the moment this process started running. */
export const SERVER_STARTED_AT = new Date().toISOString();

export const SERVER_MODULE_PATH = fileURLToPath(import.meta.url);
/** Root the server was loaded from — `src/` under tsx, `dist/` when compiled (this module sits in `lib/` beneath it). */
export const SERVER_MODULE_DIR = path.resolve(
    path.dirname(SERVER_MODULE_PATH),
    '..',
);

/**
 * Feature markers, so a caller can ask "does this build have X" without knowing
 * commit hashes. Hand-maintained: add a marker when a feature a caller could
 * reasonably check for lands, and never remove one without removing the feature.
 */
export const SERVER_FEATURES = [
    'cascade-delete-guard',
    'human-confirmation',
    'list-apps-banner',
    'mutation-snapshots',
    'server-build-identity',
];

/**
 * Read the package version without assuming a build layout.
 *
 * `package.json` sits one level above both `src/` and `dist/`, so the same relative
 * lookup works whether this is TypeScript under tsx or compiled JavaScript.
 *
 * @returns The declared version, or null if it cannot be read.
 */
export function readPackageVersion(): string | null {
    const pkg = readJsonFile<{ version?: unknown }>(
        path.resolve(SERVER_MODULE_DIR, '..', 'package.json'),
    );
    return typeof pkg?.version === 'string' ? pkg.version : null;
}

/**
 * Resolve the `.git` directory for a checkout, following the worktree indirection.
 *
 * A linked worktree or submodule has `.git` as a file containing `gitdir: <path>`
 * rather than a directory, so the plain existence check is not enough.
 *
 * @param startDir Directory to start walking up from.
 * @returns Absolute path to the git directory, or null if none is found.
 */
export function findGitDir(startDir: string): string | null {
    let current = path.resolve(startDir);

    for (let depth = 0; depth < 12; depth += 1) {
        const candidate = path.join(current, '.git');

        try {
            const stat = fs.statSync(candidate);
            if (stat.isDirectory()) return candidate;
            if (stat.isFile()) {
                const pointer = fs.readFileSync(candidate, 'utf8').trim();
                const match = /^gitdir:\s*(.+)$/.exec(pointer);
                if (match) return path.resolve(current, match[1].trim());
            }
        } catch {
            // Not here; keep walking up.
        }

        const parent = path.dirname(current);
        if (parent === current) break;
        current = parent;
    }

    return null;
}

/**
 * Explain a persist that was asked for and did not happen.
 *
 * `persistFiles` reads as an outcome and is only a request. Nothing can be written
 * until the metadata has been fetched, so the persist step lives inside the warm
 * branch — and with `warm: false` the caches are cleared, nothing is re-read, and
 * nothing reaches disk. The response echoed `persistFiles: true` beside `warm: false`
 * regardless, which claims files were written when none were. That is the failure this
 * server exists to prevent, in a diagnostic tool of all places: it cost a whole-app
 * referrer scan, which read a `viewMap.json` written before the fixture existed and
 * reported every count as zero.
 *
 * @param warm Whether the caller asked for the caches to be re-read.
 * @param persistFiles Whether the caller asked for the result to be written out.
 * @returns The reason nothing was written, or null when the question does not arise.
 */
export function describePersistOutcome(
    warm: boolean,
    persistFiles: boolean,
): string | null {
    if (!persistFiles || warm) return null;
    return 'Nothing was written. persistFiles only takes effect with warm: true — the caches were cleared, but no metadata was fetched to persist. Re-run with warm: true if you need the files on disk refreshed.';
}

/**
 * Report whether the running build is older than the checkout it sits in.
 *
 * `readGitIdentity` reads `.git` at call time, so on a compiled runtime the commit it
 * returns is the **checkout's**, not the build's — `dist/` can have been compiled from
 * an entirely different commit and nothing in the identity would say so. That gap is
 * not theoretical: a menu test was run against a build three commits behind the branch
 * it reported, and the reported commit is exactly what made it look current.
 *
 * Comparing modification times closes it without a build step. The running module file
 * is written by `tsc`; `.git/HEAD` and the ref it names are rewritten by checkout,
 * commit and pull. If either moved after the module was written, the source has
 * changed since this build and the commit above describes code that is not running.
 *
 * @param modulePath The file this process was loaded from.
 * @param runtime Whether that file is source or a build artefact.
 * @returns True when the checkout has moved on, false when it has not, null when it
 *     cannot be told — an unknown answer being better than a confident wrong one.
 */
export function detectStaleBuild(
    modulePath: string,
    runtime: 'typescript' | 'compiled',
): boolean | null {
    // Running the source directly means there is no build to be behind.
    if (runtime === 'typescript') return false;

    const gitDir = findGitDir(path.dirname(modulePath));
    if (!gitDir) return null;

    const mtime = (target: string): number | null => {
        try {
            return fs.statSync(target).mtimeMs;
        } catch {
            return null;
        }
    };

    const builtAt = mtime(modulePath);
    if (builtAt === null) return null;

    // HEAD alone is not enough. It changes when you switch branches, but a pull that
    // fast-forwards the branch you are already on rewrites the ref file instead — and
    // that is the common way to end up with a stale build.
    const candidates = [path.join(gitDir, 'HEAD')];
    try {
        const head = fs.readFileSync(path.join(gitDir, 'HEAD'), 'utf8').trim();
        const refMatch = /^ref:\s*(.+)$/.exec(head);
        if (refMatch) {
            candidates.push(
                path.join(gitDir, ...refMatch[1].trim().split('/')),
            );
        }
    } catch {
        // HEAD unreadable; the candidate list still holds its path, which will fail
        // its own stat below and leave the answer unknown rather than wrong.
    }

    const touched = candidates
        .map(mtime)
        .filter((value): value is number => value !== null);
    if (touched.length === 0) return null;

    return Math.max(...touched) > builtAt;
}

/**
 * Report the branch and commit this process's source was loaded from.
 *
 * Read from `.git` directly rather than by shelling out to `git`, so it cannot hang
 * startup or fail on a machine without the binary. Every failure degrades to null —
 * an unknown commit is a worse diagnostic than a known one, but it is not an error.
 *
 * @param startDir Directory to resolve the checkout from. Defaults to this module's.
 * @returns Branch and commit, or null when this is not a git checkout.
 */
export function readGitIdentity(
    startDir: string = SERVER_MODULE_DIR,
): { branch: string | null; commit: string | null } | null {
    const gitDir = findGitDir(startDir);
    if (!gitDir) return null;

    let head: string;
    try {
        head = fs.readFileSync(path.join(gitDir, 'HEAD'), 'utf8').trim();
    } catch {
        return null;
    }

    // Detached HEAD holds the commit itself rather than a ref to follow.
    if (/^[0-9a-f]{40}$/i.test(head)) {
        return { branch: null, commit: head.slice(0, 7) };
    }

    const refMatch = /^ref:\s*(.+)$/.exec(head);
    if (!refMatch) return null;

    const ref = refMatch[1].trim();
    const branch = ref.replace(/^refs\/heads\//, '');

    // A loose ref is a file; once packed, it only exists inside packed-refs.
    try {
        const loose = fs
            .readFileSync(path.join(gitDir, ...ref.split('/')), 'utf8')
            .trim();
        if (/^[0-9a-f]{40}$/i.test(loose)) {
            return { branch, commit: loose.slice(0, 7) };
        }
    } catch {
        // Fall through to packed-refs.
    }

    try {
        const packed = fs.readFileSync(
            path.join(gitDir, 'packed-refs'),
            'utf8',
        );
        for (const line of packed.split('\n')) {
            const [sha, name] = line.trim().split(/\s+/);
            if (name === ref && /^[0-9a-f]{40}$/i.test(sha ?? '')) {
                return { branch, commit: sha.slice(0, 7) };
            }
        }
    } catch {
        // No packed-refs either.
    }

    return { branch, commit: null };
}

export type ServerBuildIdentity = {
    name: string;
    version: string | null;
    mode: 'full' | 'readonly';
    runtime: 'typescript' | 'compiled';
    entryPath: string | null;
    moduleDir: string;
    git: { branch: string | null; commit: string | null } | null;
    /**
     * Whether `git` above describes code that is not actually running.
     *
     * True means the checkout moved after this build was compiled, so the commit is
     * the source tree's rather than the build's. Null means it could not be told.
     */
    sourceNewerThanBuild: boolean | null;
    startedAt: string;
    features: string[];
};

/**
 * Describe this process so a caller can tell a stale server from a current one.
 *
 * @param enforcedReadOnly Whether the server was started in enforced read-only mode.
 * @returns The build identity reported alongside every app listing.
 */
export function describeServerBuild(
    enforcedReadOnly: boolean,
): ServerBuildIdentity {
    return {
        name: 'knack-mcp',
        version: readPackageVersion(),
        mode: enforcedReadOnly ? 'readonly' : 'full',
        // The extension of this module is the only honest answer: a `dist/` build and
        // tsx running `src/` are exactly the confusion this field exists to settle.
        runtime: import.meta.url.endsWith('.ts') ? 'typescript' : 'compiled',
        entryPath: process.argv[1] ?? null,
        moduleDir: SERVER_MODULE_DIR,
        git: readGitIdentity(),
        sourceNewerThanBuild: detectStaleBuild(
            SERVER_MODULE_PATH,
            import.meta.url.endsWith('.ts') ? 'typescript' : 'compiled',
        ),
        startedAt: SERVER_STARTED_AT,
        features: [...SERVER_FEATURES],
    };
}

/**
 * Render the build identity as one line, for the banner and the startup log.
 *
 * @param build The identity to render.
 * @returns A single line naming version, mode, runtime, commit and start time.
 */
export function summariseServerBuild(build: ServerBuildIdentity): string {
    const parts = [
        `${build.name}${build.version ? ` ${build.version}` : ''}`,
        `${build.mode} mode`,
        build.runtime === 'typescript'
            ? 'TypeScript source'
            : 'compiled JavaScript',
    ];

    if (build.git) {
        const branch = build.git.branch ?? 'detached HEAD';
        parts.push(
            build.git.commit ? `${branch} @ ${build.git.commit}` : branch,
        );
    }

    parts.push(`started ${build.startedAt}`);

    // The commit is the checkout's, and on a stale build that is a different thing
    // from what is running. Saying so here matters more than in the payload: this
    // line is what goes to stderr at startup and leads the app listing.
    const staleWarning =
        build.sourceNewerThanBuild === true
            ? ` WARNING: the checkout has changed since this build was compiled, so ${
                  build.git?.commit ?? 'the commit above'
              } describes the source tree and not the code running. Rebuild before trusting it.`
            : '';

    return `Build: ${parts.join(', ')}. Loaded from ${build.moduleDir}.${staleWarning}`;
}
