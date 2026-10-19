#!/usr/bin/env node
// PreToolUse hook on Bash and PowerShell. Refuses file content written through
// the shell: sed -i, perl -pi, a heredoc into a file, an inline node/python
// script that writes, or PowerShell's Get-Content | Set-Content rewrite. A rewrite like that nests shell, script and regex
// quoting, mangles silently and costs a retry; a rule in CLAUDE.md does not
// stop it, because auto mode's own instructions suggest shell edits. Reading,
// searching and redirecting command output (`npm test > log`) stay allowed.
// Fails open (exit 0) on anything it cannot parse.

let input = '';
process.stdin.on('data', (chunk) => {
    input += chunk;
});
process.stdin.on('end', () => {
    try {
        const command = JSON.parse(input)?.tool_input?.command;
        if (typeof command !== 'string') process.exit(0);

        // The start of the command, or after a shell operator, optionally behind
        // VAR=value or a wrapper. Unanchored, `grep -rn "sed -i"` would be refused.
        const COMMAND_POSITION = String.raw`(?:^|[\n;&|(]|\$\()\s*(?:[A-Za-z_]\w*=\S*\s+|(?:sudo|env|npx|command|exec)\s+)*`;
        const atCommand = (tool) =>
            new RegExp(`${COMMAND_POSITION}(?:[\\w./~+-]*/)?${tool}`, 'i');

        const inlineScript = atCommand(
            '(?:node|python3?|bun|deno)\\s+(?:-e|-p|--eval|-c|-)(?:\\s|$)',
        );
        const inPlace = atCommand(
            '(?:sed|perl)(?:\\s+-{1,2}[\\w=.-]+)*\\s+(?:-[a-zA-Z]*i\\b|--in-place\\b)',
        );
        const scriptWrites =
            inlineScript.test(command) &&
            /\b(?:writeFileSync|appendFileSync|writeFile|write_text|open\([^)]*['"][wa])/.test(
                command,
            );
        const heredocLine =
            command
                .split('\n')
                .find((line) => /<<-?\s*['"]?[A-Za-z_]/.test(line)) ?? '';
        const heredocToFile =
            /(?:^|[^0-9&>])>{1,2}\s*(?!\/dev\/null|&)[^\s|;&]/.test(
                heredocLine,
            ) || /\btee\s+(?!-)\S/.test(heredocLine);

        // PowerShell's equivalents: (Get-Content f) -replace … | Set-Content f, a
        // here-string piped into a file, or a .NET File.WriteAllText. Piping a
        // command's output to Out-File (`npm test | Out-File log`) stays allowed.
        // Anchored, so a commit message that names these cmdlets passes.
        const psRewrite =
            atCommand('(?:Set-Content|Add-Content|Out-File)\\b').test(
                command,
            ) &&
            (atCommand('Get-Content\\b').test(command) ||
                /@['"]\s*$/m.test(command));
        const dotnetWrite =
            /\[(?:System\.)?IO\.File\]::(?:WriteAll|AppendAll)/i.test(command);

        if (
            inPlace.test(command) ||
            scriptWrites ||
            heredocToFile ||
            psRewrite ||
            dotnetWrite
        ) {
            console.error(
                'Blocked: file content goes through the Edit tool (a change) or the Write tool (a new file, the scratchpad included), never sed -i, a heredoc into a file, a Get-Content | Set-Content rewrite or an inline script that writes. ' +
                    'Shell rewrites nest three quoting layers and mangle silently.\nCommand: ' +
                    command.split('\n')[0],
            );
            process.exit(2);
        }
        process.exit(0);
    } catch {
        process.exit(0);
    }
});
