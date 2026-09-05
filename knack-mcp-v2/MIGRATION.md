# Migrating from knack-mcp

Same capabilities, fewer tools. Point your client at `knack-mcp-v2/dist/index.js`
instead of `knack-mcp/dist/server.js`; `server-readonly.js` becomes the `--readonly`
flag. The apps folder, `app.json` permissions, secrets file and cache files are unchanged.

<!-- MAPPING -->
