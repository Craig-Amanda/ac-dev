export {};

process.env.KNACK_MCP_ENABLE_MUTATION_TOOLS = 'false';
process.env.KNACK_MCP_ENABLE_VIEW_MUTATION_TOOLS = 'false';
process.env.KNACK_MCP_ENABLE_DIAGNOSTIC_TOOLS = 'false';

const { main: startReadonlyServer } = await import('./server.js');

await startReadonlyServer();