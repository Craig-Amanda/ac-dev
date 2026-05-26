export {};

const { main: startFullServer } = await import('./server.js');

await startFullServer();