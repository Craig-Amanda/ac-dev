export {};

process.env.KNACK_MCP_ENABLE_MUTATION_TOOLS = 'true';
process.env.KNACK_MCP_ENABLE_DIAGNOSTIC_TOOLS = 'true';

const { main: startFullServer } = await import('./server.js');

await startFullServer();