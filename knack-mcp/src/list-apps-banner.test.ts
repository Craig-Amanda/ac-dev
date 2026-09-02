import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { after, describe, it } from 'node:test';

import {
    collectSceneViewLinks,
    describeAppListForHumans,
    findRawViewInMetadata,
    describeServerBuild,
    describePersistOutcome,
    detectStaleBuild,
    listAppNames,
    makeTextResponse,
    readGitIdentity,
    summariseServerBuild,
} from './server.js';
import { resolveViewAttributes } from './view-safety.js';

/**
 * These tests cover the plain-text banner that leads the knack_list_apps response. The
 * facts it states are already in the structured payload, so what matters here is that
 * the prose cannot say something the payload contradicts — in particular that it never
 * advertises writes on a server started in enforced read-only mode, and never claims a
 * human can be prompted on a client that did not advertise elicitation.
 */

const APPS = [
    {
        appKey: 'ARC',
        appName: 'ARC Beta 1.0',
        appId: 'a1',
        appFolder: '/k/ARC',
        readonly: true,
    },
    {
        appKey: 'GAP',
        appName: 'GAP-Track',
        appId: 'a2',
        appFolder: '/k/GAP',
        readonly: false,
        allowViewMutation: true,
    },
    {
        appKey: 'Spot',
        appName: 'Spot',
        appId: 'a3',
        appFolder: '/k/Spot',
        readonly: false,
    },
] as unknown as Parameters<typeof describeAppListForHumans>[0]['apps'];

const PROMPTS = {
    humanConfirmation: { available: true, client: 'codex-mcp-client 0.151.0' },
    cascadeDeleteBehaviour: {
        summary:
            'A mutation that would delete child pages is put to the user for confirmation.',
    },
};

const REFUSES = {
    humanConfirmation: { available: false, client: 'stub-client 1.0' },
    cascadeDeleteBehaviour: {
        summary:
            'No human can be prompted, so a mutation that would delete child pages is refused outright.',
    },
};

function banner(
    overrides: Partial<Parameters<typeof describeAppListForHumans>[0]> = {},
) {
    return describeAppListForHumans({
        knackAppsDir: '/k',
        activeAppKey: null,
        apps: APPS,
        enforcedReadOnly: false,
        buildSummary: 'Build: knack-mcp 1.0.0, full mode, TypeScript source.',
        ...PROMPTS,
        ...overrides,
    });
}

describe('describeAppListForHumans', () => {
    it('names the writable apps and the view-mutable subset', () => {
        const text = banner();
        assert.match(text, /Writable: GAP-Track, Spot\./);
        assert.match(text, /View mutation allowed: GAP-Track\./);
        assert.doesNotMatch(text, /Writable:[^.]*ARC Beta/);
    });

    it('never advertises writes in enforced read-only mode', () => {
        const text = banner({ enforcedReadOnly: true });
        assert.match(text, /Writes: none\./);
        assert.match(text, /enforced read-only mode/);
        assert.doesNotMatch(text, /Writable:/);
        assert.doesNotMatch(text, /View mutation allowed:/);
    });

    it('says a human is prompted when the client advertised elicitation', () => {
        const text = banner();
        assert.match(text, /Cascade deletes: a human is prompted\./);
        assert.match(
            text,
            /Client "codex-mcp-client 0\.151\.0" advertised MCP elicitation\./,
        );
        assert.ok(text.includes(PROMPTS.cascadeDeleteBehaviour.summary));
    });

    it('says refused when the client did not advertise elicitation', () => {
        const text = banner(REFUSES);
        assert.match(text, /Cascade deletes: refused\./);
        assert.match(text, /did not advertise MCP elicitation\./);
        assert.doesNotMatch(text, /a human is prompted/);
        assert.ok(text.includes(REFUSES.cascadeDeleteBehaviour.summary));
    });

    it('reuses the structured summary verbatim so the prose cannot drift', () => {
        const summary = 'Something entirely different happens here.';
        const text = banner({ cascadeDeleteBehaviour: { summary } });
        assert.ok(text.includes(summary));
    });

    it('falls back to a generic client label when the client is unknown', () => {
        const text = banner({
            humanConfirmation: { available: true, client: null },
        });
        assert.match(text, /This client advertised MCP elicitation\./);
    });

    it('reports the app count, folder and active app', () => {
        const text = banner({ activeAppKey: 'GAP' });
        assert.match(
            text,
            /Knack apps: 3 discovered in \/k\. Active app: GAP\./,
        );
        assert.match(banner(), /Active app: none\./);
    });

    it('reports no writable apps without claiming otherwise', () => {
        const readonlyOnly = [APPS[0]] as typeof APPS;
        const text = banner({ apps: readonlyOnly });
        assert.match(text, /Writable: none\. View mutation allowed: none\./);
    });
});

describe('listAppNames', () => {
    it('lists every name up to the limit', () => {
        assert.equal(listAppNames(['a', 'b', 'c'], 3), 'a, b, c');
    });

    it('truncates with a count of what was dropped', () => {
        assert.equal(listAppNames(['a', 'b', 'c', 'd'], 2), 'a, b +2 more');
    });

    it('reports an empty list as none', () => {
        assert.equal(listAppNames([]), 'none');
    });
});

describe('describeAppListForHumans build line', () => {
    it('ends with the build summary it was given', () => {
        const buildSummary =
            'Build: knack-mcp 9.9.9, readonly mode, compiled JavaScript.';
        const lines = banner({ buildSummary }).split('\n');
        assert.equal(lines.at(-1), buildSummary);
    });

    it('keeps the cascade rule above the build line', () => {
        const lines = banner().split('\n');
        assert.match(lines.at(-2) ?? '', /Cascade deletes:/);
    });
});

describe('describeServerBuild', () => {
    it('reports the mode it was started in', () => {
        assert.equal(describeServerBuild(false).mode, 'full');
        assert.equal(describeServerBuild(true).mode, 'readonly');
    });

    it('reports the runtime it is actually executing', () => {
        const build = describeServerBuild(false);
        assert.ok(
            build.runtime === 'typescript' || build.runtime === 'compiled',
        );
        // These tests run the TypeScript directly under tsx, so nothing else is honest.
        assert.equal(build.runtime, 'typescript');
    });

    it('names itself and reads its own package version', () => {
        const build = describeServerBuild(false);
        assert.equal(build.name, 'knack-mcp');
        assert.match(build.version ?? '', /^\d+\.\d+\.\d+/);
    });

    it('records where the module was loaded from', () => {
        const build = describeServerBuild(false);
        assert.ok(path.isAbsolute(build.moduleDir));
        assert.equal(path.basename(build.moduleDir), 'src');
    });

    it('stamps a stable process start time', () => {
        const first = describeServerBuild(false).startedAt;
        assert.match(first, /^\d{4}-\d{2}-\d{2}T/);
        assert.equal(describeServerBuild(true).startedAt, first);
    });

    it('lists feature markers a caller can check for', () => {
        const { features } = describeServerBuild(false);
        for (const marker of [
            'cascade-delete-guard',
            'human-confirmation',
            'list-apps-banner',
            'server-build-identity',
        ]) {
            assert.ok(features.includes(marker), `missing marker: ${marker}`);
        }
    });

    it('hands out a copy of the feature list, not the shared array', () => {
        const first = describeServerBuild(false).features;
        first.push('not-a-real-feature');
        assert.ok(
            !describeServerBuild(false).features.includes('not-a-real-feature'),
        );
    });

    it('reports git identity as a branch and short commit, or null', () => {
        const { git } = describeServerBuild(false);
        if (git === null) return;
        if (git.commit !== null) assert.match(git.commit, /^[0-9a-f]{7}$/);
        if (git.branch !== null)
            assert.doesNotMatch(git.branch, /^refs\/heads\//);
    });
});

describe('summariseServerBuild', () => {
    const BASE = {
        name: 'knack-mcp',
        version: '1.0.0',
        mode: 'full' as const,
        runtime: 'typescript' as const,
        entryPath: '/repo/knack-mcp/src/server.ts',
        moduleDir: '/repo/knack-mcp/src',
        git: { branch: 'main', commit: 'abc1234' },
        sourceNewerThanBuild: false,
        startedAt: '2026-08-31T10:00:00.000Z',
        features: ['human-confirmation'],
    };

    it('names version, mode, runtime, commit, start time and load path', () => {
        const text = summariseServerBuild(BASE);
        assert.match(text, /knack-mcp 1\.0\.0/);
        assert.match(text, /full mode/);
        assert.match(text, /TypeScript source/);
        assert.match(text, /main @ abc1234/);
        assert.match(text, /started 2026-08-31T10:00:00\.000Z/);
        assert.match(text, /Loaded from \/repo\/knack-mcp\/src/);
    });

    it('says compiled JavaScript when running from dist', () => {
        const text = summariseServerBuild({ ...BASE, runtime: 'compiled' });
        assert.match(text, /compiled JavaScript/);
        assert.doesNotMatch(text, /TypeScript source/);
    });

    it('warns when the checkout has moved past the build', () => {
        // The commit is read from `.git` at call time, so on a stale build it names
        // the source tree rather than the code running — which is how a live test was
        // once run against a build three commits behind the branch it reported. The
        // warning goes in this line specifically: it is what reaches stderr at
        // startup and leads the app listing.
        const text = summariseServerBuild({
            ...BASE,
            runtime: 'compiled',
            sourceNewerThanBuild: true,
        });
        assert.match(text, /WARNING/);
        assert.match(text, /abc1234/);
        assert.match(text, /Rebuild/);
    });

    it('stays quiet when the build is current or cannot be told', () => {
        for (const state of [false, null] as const) {
            assert.doesNotMatch(
                summariseServerBuild({ ...BASE, sourceNewerThanBuild: state }),
                /WARNING/,
            );
        }
    });

    it('survives a checkout with no git, no version and a detached HEAD', () => {
        assert.doesNotMatch(
            summariseServerBuild({ ...BASE, git: null, version: null }),
            /undefined|null/,
        );
        assert.match(
            summariseServerBuild({
                ...BASE,
                git: { branch: null, commit: 'abc1234' },
            }),
            /detached HEAD @ abc1234/,
        );
        assert.match(
            summariseServerBuild({
                ...BASE,
                git: { branch: 'main', commit: null },
            }),
            /main, started/,
        );
    });
});

/**
 * These drive the git reader against synthetic checkouts rather than this repository,
 * because the shapes that break it — a packed ref, a detached HEAD, a worktree whose
 * `.git` is a file, no checkout at all — are not the shape a developer happens to be
 * sitting in. Every one of them runs at server startup, where a throw is a server
 * that will not start.
 */
describe('readGitIdentity', () => {
    const roots: string[] = [];
    const SHA = 'a1b2c3d4e5f60718293a4b5c6d7e8f9012345678';

    function makeCheckout(build: (gitDir: string) => void): string {
        const root = fs.mkdtempSync(path.join(os.tmpdir(), 'knack-git-'));
        roots.push(root);
        const gitDir = path.join(root, '.git');
        fs.mkdirSync(gitDir, { recursive: true });
        build(gitDir);
        const nested = path.join(root, 'knack-mcp', 'src');
        fs.mkdirSync(nested, { recursive: true });
        return nested;
    }

    after(() => {
        for (const root of roots)
            fs.rmSync(root, { recursive: true, force: true });
    });

    it('reads a loose ref and walks up from a nested directory', () => {
        const dir = makeCheckout((gitDir) => {
            fs.writeFileSync(
                path.join(gitDir, 'HEAD'),
                'ref: refs/heads/feature/x\n',
            );
            fs.mkdirSync(path.join(gitDir, 'refs', 'heads', 'feature'), {
                recursive: true,
            });
            fs.writeFileSync(
                path.join(gitDir, 'refs', 'heads', 'feature', 'x'),
                SHA + '\n',
            );
        });
        assert.deepEqual(readGitIdentity(dir), {
            branch: 'feature/x',
            commit: SHA.slice(0, 7),
        });
    });

    it('falls back to packed-refs when the ref has no loose file', () => {
        const dir = makeCheckout((gitDir) => {
            fs.writeFileSync(
                path.join(gitDir, 'HEAD'),
                'ref: refs/heads/main\n',
            );
            fs.writeFileSync(
                path.join(gitDir, 'packed-refs'),
                `# pack-refs with: peeled fully-peeled sorted\n${SHA} refs/heads/main\ndeadbeef00000000000000000000000000000000 refs/heads/other\n`,
            );
        });
        assert.deepEqual(readGitIdentity(dir), {
            branch: 'main',
            commit: SHA.slice(0, 7),
        });
    });

    it('reports a detached HEAD as a commit with no branch', () => {
        const dir = makeCheckout((gitDir) => {
            fs.writeFileSync(path.join(gitDir, 'HEAD'), SHA + '\n');
        });
        assert.deepEqual(readGitIdentity(dir), {
            branch: null,
            commit: SHA.slice(0, 7),
        });
    });

    it('follows the gitdir pointer when .git is a file, as in a worktree', () => {
        const root = fs.mkdtempSync(path.join(os.tmpdir(), 'knack-wt-'));
        roots.push(root);
        const realGitDir = path.join(root, 'actual-git');
        fs.mkdirSync(path.join(realGitDir, 'refs', 'heads'), {
            recursive: true,
        });
        fs.writeFileSync(path.join(realGitDir, 'HEAD'), 'ref: refs/heads/wt\n');
        fs.writeFileSync(
            path.join(realGitDir, 'refs', 'heads', 'wt'),
            SHA + '\n',
        );
        const work = path.join(root, 'work');
        fs.mkdirSync(work, { recursive: true });
        fs.writeFileSync(path.join(work, '.git'), `gitdir: ${realGitDir}\n`);
        assert.deepEqual(readGitIdentity(work), {
            branch: 'wt',
            commit: SHA.slice(0, 7),
        });
    });

    it('returns the branch with a null commit when the ref cannot be resolved', () => {
        const dir = makeCheckout((gitDir) => {
            fs.writeFileSync(
                path.join(gitDir, 'HEAD'),
                'ref: refs/heads/orphan\n',
            );
        });
        assert.deepEqual(readGitIdentity(dir), {
            branch: 'orphan',
            commit: null,
        });
    });

    it('returns null rather than throwing when there is no checkout', () => {
        const bare = fs.mkdtempSync(path.join(os.tmpdir(), 'knack-nogit-'));
        roots.push(bare);
        assert.equal(readGitIdentity(bare), null);
    });

    it('returns null rather than throwing on an unreadable HEAD', () => {
        const dir = makeCheckout(() => {
            // .git exists, HEAD does not.
        });
        assert.equal(readGitIdentity(dir), null);
    });

    it('returns null rather than throwing on a HEAD it cannot parse', () => {
        const dir = makeCheckout((gitDir) => {
            fs.writeFileSync(path.join(gitDir, 'HEAD'), 'this is not a ref\n');
        });
        assert.equal(readGitIdentity(dir), null);
    });
});

/**
 * A server that fails to start is the one case that never reaches a tool call, so the
 * build identity has to reach stderr before anything that can throw. This regression
 * exists because it originally did not: the line was logged after createServer, which
 * is what throws on a missing KNACK_APPS_DIR — so it printed whenever it was not
 * needed and was absent exactly when someone was trying to work out which code was
 * failing.
 */
describe('startup diagnostics', () => {
    it('states the build before a startup failure, not after', () => {
        const entry = fileURLToPath(
            new URL('./server-full.ts', import.meta.url),
        );
        const env = { ...process.env };
        delete env.KNACK_APPS_DIR;

        const result = spawnSync(process.execPath, ['--import', 'tsx', entry], {
            env,
            encoding: 'utf8',
            timeout: 60_000,
        });

        const stderr = result.stderr ?? '';
        const buildAt = stderr.indexOf('[knack-mcp] Build:');
        const failureAt = stderr.indexOf('Missing env var KNACK_APPS_DIR');

        assert.notEqual(buildAt, -1, `no build line on stderr:\n${stderr}`);
        assert.notEqual(
            failureAt,
            -1,
            `expected the startup failure:\n${stderr}`,
        );
        assert.ok(
            buildAt < failureAt,
            'the build line must precede the failure it is meant to explain',
        );
        // stdout is reserved for JSON-RPC; a diagnostic there would corrupt the stream.
        assert.doesNotMatch(result.stdout ?? '', /\[knack-mcp\]/);
    });
});

/**
 * The guard's preflight is sourced from the application payload because Knack serves no
 * per-view route to a REST API key: every candidate host answers
 * `scenes/<scene>/views/<view>` with a web-server HTML 404. These cover the payload
 * shapes that would otherwise turn a legitimate mutation into a refusal, or — worse —
 * hand the guard the wrong view to reason about.
 */
describe('findRawViewInMetadata', () => {
    const VIEW = {
        key: 'view_4',
        attributes: {
            name: 'Clients',
            type: 'table',
            columns: [{ type: 'link', scene: 'client-details' }],
        },
    };

    const NESTED = {
        application: {
            scenes: [
                { key: 'scene_1', slug: 'home', views: [] },
                {
                    key: 'scene_3',
                    slug: 'clients',
                    views: [{ key: 'view_9' }, VIEW],
                },
            ],
        },
    };

    it('finds a view nested under application.scenes', () => {
        assert.deepEqual(
            findRawViewInMetadata(NESTED, 'scene_3', 'view_4'),
            VIEW,
        );
    });

    it('finds a view when scenes sit at the top level', () => {
        const flat = { scenes: NESTED.application.scenes };
        assert.deepEqual(
            findRawViewInMetadata(flat, 'scene_3', 'view_4'),
            VIEW,
        );
    });

    it('returns the raw record, so resolveViewAttributes can unwrap attributes', () => {
        const found = findRawViewInMetadata(NESTED, 'scene_3', 'view_4');
        assert.equal(resolveViewAttributes(found)?.type, 'table');
        assert.ok(Array.isArray(resolveViewAttributes(found)?.columns));
    });

    it('returns null for an unknown view, so the guard refuses rather than guessing', () => {
        assert.equal(
            findRawViewInMetadata(NESTED, 'scene_3', 'view_999'),
            null,
        );
    });

    it('returns null for an unknown scene', () => {
        assert.equal(
            findRawViewInMetadata(NESTED, 'scene_404', 'view_4'),
            null,
        );
    });

    it('keeps scanning past a duplicate scene key that lacks the view', () => {
        // A wrong "not found" here becomes a refusal on a legitimate mutation, so a
        // first scene entry without the view must not mask a later one that has it.
        const duplicated = {
            application: {
                scenes: [
                    { key: 'scene_3', slug: 'clients', views: [] },
                    { key: 'scene_3', slug: 'clients', views: [VIEW] },
                ],
            },
        };
        assert.deepEqual(
            findRawViewInMetadata(duplicated, 'scene_3', 'view_4'),
            VIEW,
        );
    });

    it('survives payloads with nothing to walk', () => {
        for (const payload of [
            null,
            undefined,
            {},
            { application: {} },
            { application: { scenes: 'not-an-array' } },
            { scenes: [] },
            { scenes: [null, 'x', 42] },
            { scenes: [{ key: 'scene_3' }] },
            { scenes: [{ key: 'scene_3', views: 'not-an-array' }] },
            { scenes: [{ key: 'scene_3', views: [null, 7] }] },
        ]) {
            assert.equal(
                findRawViewInMetadata(payload, 'scene_3', 'view_4'),
                null,
                `threw or matched on: ${JSON.stringify(payload)}`,
            );
        }
    });

    it('does not match a view key belonging to a different scene', () => {
        const crossed = {
            application: {
                scenes: [
                    { key: 'scene_1', views: [VIEW] },
                    { key: 'scene_3', views: [] },
                ],
            },
        };
        assert.equal(findRawViewInMetadata(crossed, 'scene_3', 'view_4'), null);
    });
});

describe('collectSceneViewLinks', () => {
    /**
     * Two pages, three views. view_232 and view_233 both link to the same child page,
     * which is the whole reason the referrer count exists — the mutating view's own
     * definition says nothing about the second link.
     */
    const METADATA = {
        application: {
            scenes: [
                {
                    key: 'scene_90',
                    slug: 'dashboard',
                    views: [
                        {
                            key: 'view_232',
                            attributes: {
                                type: 'table',
                                columns: [{ type: 'link', scene: 'detail' }],
                            },
                        },
                        {
                            key: 'view_240',
                            attributes: { type: 'rich_text' },
                        },
                    ],
                },
                {
                    key: 'scene_91',
                    slug: 'reports',
                    views: [
                        {
                            key: 'view_233',
                            attributes: {
                                type: 'details',
                                columns: [
                                    {
                                        groups: [
                                            {
                                                columns: [
                                                    [
                                                        {
                                                            type: 'scene_link',
                                                            scene: 'detail',
                                                        },
                                                    ],
                                                ],
                                            },
                                        ],
                                    },
                                ],
                            },
                        },
                    ],
                },
            ],
        },
    };

    it('reads every view link in the app, whatever the link is typed', () => {
        // A details view stores `scene_link` where a table stores `link`. A collector
        // that saw only one of them would miss half the referrers, and a missed
        // referrer is a page reported as doomed that is not.
        const links = collectSceneViewLinks(METADATA);
        assert.deepEqual(links.get('scene_90'), [
            { viewKey: 'view_232', childSceneRefs: ['detail'] },
            { viewKey: 'view_240', childSceneRefs: [] },
        ]);
        assert.deepEqual(links.get('scene_91'), [
            { viewKey: 'view_233', childSceneRefs: ['detail'] },
        ]);
    });

    it('reads the bare scenes shape as well as the wrapped one', () => {
        const links = collectSceneViewLinks({
            scenes: METADATA.application.scenes,
        });
        assert.equal(links.size, 2);
    });

    it('keeps both halves when a scene key appears twice', () => {
        // findRawViewInMetadata already scans past a duplicate scene key rather than
        // stopping at the first. Dropping one here would lose its views, and a lost
        // view is a lost referrer.
        const links = collectSceneViewLinks({
            scenes: [
                {
                    key: 'scene_90',
                    views: [
                        {
                            key: 'view_1',
                            attributes: {
                                type: 'table',
                                columns: [{ type: 'link', scene: 'a' }],
                            },
                        },
                    ],
                },
                {
                    key: 'scene_90',
                    views: [
                        {
                            key: 'view_2',
                            attributes: {
                                type: 'table',
                                columns: [{ type: 'link', scene: 'b' }],
                            },
                        },
                    ],
                },
            ],
        });
        assert.deepEqual(
            links.get('scene_90')?.map((view) => view.viewKey),
            ['view_1', 'view_2'],
        );
    });

    it('lists navigation links only, not rule redirects', () => {
        // The wiring this fix turns on. collectLinkTargets is broad by design, which
        // is right for the view being mutated and wrong for counting who else links
        // to a page: a form's submit-rule redirect carries a `scene`, and counting it
        // made a singly-linked page look shared and skip its confirmation.
        const links = collectSceneViewLinks({
            scenes: [
                {
                    key: 'scene_5',
                    views: [
                        {
                            key: 'view_form',
                            attributes: {
                                type: 'form',
                                groups: [
                                    {
                                        columns: [
                                            {
                                                inputs: [
                                                    {
                                                        type: 'link',
                                                        field: { key: 'f1' },
                                                    },
                                                ],
                                            },
                                        ],
                                    },
                                ],
                                rules: {
                                    submits: [
                                        { action: 'scene', scene: 'kid' },
                                    ],
                                },
                            },
                        },
                        {
                            key: 'view_nav',
                            attributes: {
                                type: 'table',
                                columns: [{ type: 'link', scene: 'kid' }],
                            },
                        },
                    ],
                },
            ],
        });

        assert.deepEqual(links.get('scene_5'), [
            { viewKey: 'view_form', childSceneRefs: [] },
            { viewKey: 'view_nav', childSceneRefs: ['kid'] },
        ]);
    });

    it('returns an empty map for a payload carrying no scenes', () => {
        assert.equal(collectSceneViewLinks({ nope: true }).size, 0);
        assert.equal(collectSceneViewLinks(null).size, 0);
    });
});

describe('detectStaleBuild', () => {
    const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'knack-stale-'));
    after(() => fs.rmSync(tempRoot, { recursive: true, force: true }));

    /** A checkout with a loose ref, and a build artefact beside it. */
    const makeCheckout = (name: string, buildFirst: boolean) => {
        const root = path.join(tempRoot, name);
        const gitDir = path.join(root, '.git');
        fs.mkdirSync(path.join(gitDir, 'refs', 'heads'), { recursive: true });
        fs.mkdirSync(path.join(root, 'dist'), { recursive: true });
        const modulePath = path.join(root, 'dist', 'server.js');
        const refPath = path.join(gitDir, 'refs', 'heads', 'main');

        const writeRepo = () => {
            fs.writeFileSync(
                path.join(gitDir, 'HEAD'),
                'ref: refs/heads/main\n',
            );
            fs.writeFileSync(refPath, `${'a'.repeat(40)}\n`);
        };

        if (buildFirst) {
            writeRepo();
            fs.writeFileSync(modulePath, '// built after the checkout');
            fs.utimesSync(
                modulePath,
                new Date(),
                new Date(Date.now() + 10_000),
            );
        } else {
            fs.writeFileSync(modulePath, '// built before the checkout');
            writeRepo();
            fs.utimesSync(refPath, new Date(), new Date(Date.now() + 10_000));
        }

        return modulePath;
    };

    it('reports a build compiled before the checkout moved', () => {
        // The ref file, not HEAD: a pull that fast-forwards the branch you are already
        // on rewrites the ref and leaves HEAD alone, and that is the common way to end
        // up running code you did not build.
        assert.equal(
            detectStaleBuild(makeCheckout('stale', false), 'compiled'),
            true,
        );
    });

    it('reports a build compiled after the checkout as current', () => {
        assert.equal(
            detectStaleBuild(makeCheckout('fresh', true), 'compiled'),
            false,
        );
    });

    it('never calls the TypeScript runtime stale, since it runs the source', () => {
        assert.equal(
            detectStaleBuild(makeCheckout('tsx', false), 'typescript'),
            false,
        );
    });

    it('answers null outside a checkout rather than guessing', () => {
        const loose = path.join(tempRoot, 'loose.js');
        fs.writeFileSync(loose, '// no git anywhere above this');
        assert.equal(detectStaleBuild(loose, 'compiled'), null);
    });

    it('answers null when the module file is gone', () => {
        assert.equal(
            detectStaleBuild(path.join(tempRoot, 'missing.js'), 'compiled'),
            null,
        );
    });
});

describe('describePersistOutcome', () => {
    it('explains a persist that was asked for and could not happen', () => {
        // The response used to echo `persistFiles: true` beside `warm: false`, which
        // reads as "files written" and is the opposite of what occurred. It cost a
        // referrer scan that read a stale viewMap.json and reported every count as 0.
        const reason = describePersistOutcome(false, true);
        assert.match(reason ?? '', /Nothing was written/);
        assert.match(reason ?? '', /warm: true/);
    });

    it('says nothing when the persist actually ran', () => {
        assert.equal(describePersistOutcome(true, true), null);
    });

    it('says nothing when no persist was asked for', () => {
        assert.equal(describePersistOutcome(false, false), null);
        assert.equal(describePersistOutcome(true, false), null);
    });
});

describe('tool response block order', () => {
    it('keeps the JSON payload at content[0] when a note is attached', () => {
        // The banner was added as a *leading* block, which silently moved the payload
        // to content[1] and broke anything parsing content[0].text. A second block is
        // read either way; the parse position is a contract worth keeping.
        const response = makeTextResponse(
            { ok: true, apps: [] },
            'Knack apps: 0.',
        );
        assert.equal(response.content.length, 2);
        assert.deepEqual(JSON.parse(response.content[0].text), {
            ok: true,
            apps: [],
        });
        assert.match(response.content[1].text, /^Knack apps: 0\.$/);
    });

    it('emits a single payload block when there is no note', () => {
        const response = makeTextResponse({ ok: true });
        assert.equal(response.content.length, 1);
        assert.deepEqual(JSON.parse(response.content[0].text), { ok: true });
    });
});
