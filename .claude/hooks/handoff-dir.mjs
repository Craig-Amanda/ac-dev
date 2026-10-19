// Where hand-off notes live: <git common dir>/claude-handoff/. The common dir
// is shared by every worktree of this clone, so a note saved in one worktree
// is found from another, and being inside .git it is never committed. Shared
// by the handoff skill's state script and the SessionStart/SessionEnd hooks.
// AC_DEV_HANDOFF_DIR overrides it, for testing the hooks by hand.

import { execFileSync } from 'node:child_process';
import { isAbsolute, join, resolve } from 'node:path';

export function handoffDir(cwd = process.cwd()) {
    if (process.env.AC_DEV_HANDOFF_DIR) return process.env.AC_DEV_HANDOFF_DIR;
    try {
        const common = execFileSync(
            'git',
            ['-C', cwd, 'rev-parse', '--git-common-dir'],
            {
                encoding: 'utf8',
                stdio: ['ignore', 'pipe', 'ignore'],
            },
        ).trim();
        if (!common) return null;
        return join(
            isAbsolute(common) ? common : resolve(cwd, common),
            'claude-handoff',
        );
    } catch {
        return null;
    }
}
