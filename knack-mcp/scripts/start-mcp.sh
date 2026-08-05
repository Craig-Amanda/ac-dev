#!/usr/bin/env bash
set -euo pipefail

required_node_major=24
entrypoint="${1:-server.js}"
script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
nvm_dir="${NVM_DIR:-$HOME/.nvm}"

case "$entrypoint" in
    server.js|server-readonly.js|server-full.js) ;;
    *)
        echo "[knack-mcp] Unsupported server entrypoint: $entrypoint" >&2
        exit 1
        ;;
esac

if [[ ! -s "$nvm_dir/nvm.sh" ]]; then
    echo "[knack-mcp] nvm was not found at $nvm_dir. Install Node.js $required_node_major or set NVM_DIR." >&2
    exit 1
fi

# nvm's normal status output must not enter MCP stdout, which is reserved for JSON-RPC.
# shellcheck disable=SC1090
source "$nvm_dir/nvm.sh"
if ! nvm use --silent "$required_node_major" >/dev/null; then
    echo "[knack-mcp] Node.js $required_node_major is not installed. Run: nvm install $required_node_major" >&2
    exit 1
fi

exec node "$script_dir/../dist/$entrypoint"
