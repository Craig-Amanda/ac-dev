export {};

const { main: startReadonlyServer } = await import('./server.js');

await startReadonlyServer({ readOnly: true });
