// End-to-end check of the whole system: real Kafka, real Postgres, all four services.
// Run after `docker compose up -d --build`:   node tests/e2e/e2e.mjs
const ORDER = 'http://localhost:3001',
  INVENTORY = 'http://localhost:3002',
  QUERY = 'http://localhost:3004';
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function waitFor(what, fn, timeoutMs = 120_000) {
  const start = Date.now();
  for (;;) {
    try {
      const v = await fn();
      if (v) return v;
    } catch {}
    if (Date.now() - start > timeoutMs) throw new Error(`timed out waiting for ${what}`);
    await sleep(1000);
  }
}
const json = async (res) => {
  if (!res.ok) throw new Error(`${res.status} ${res.url}`);
  return res.json();
};
const stock = async () =>
  Object.fromEntries((await json(await fetch(`${INVENTORY}/products`))).map((p) => [p.sku, p.available]));
async function placeOrder(items) {
  return json(
    await fetch(`${ORDER}/orders`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ customerId: 'e2e-customer', items }),
    }),
  );
}
const finalView = (orderId) =>
  waitFor(`order ${orderId} to settle`, async () => {
    const res = await fetch(`${QUERY}/orders/${orderId}`);
    if (!res.ok) return null;
    const view = await res.json();
    // wait for the final status AND (for cancels with a reservation) the release to be projected
    return view.status !== 'PENDING' ? view : null;
  });
function expectEq(label, actual, expected) {
  if (actual !== expected) throw new Error(`${label}: expected ${expected}, got ${actual}`);
  console.log(`  ✓ ${label} = ${actual}`);
}

console.log('waiting for services...');
await waitFor('inventory', () => stock());
await waitFor('query', async () => (await fetch(`${QUERY}/stats`)).ok);
await waitFor(
  'order',
  async () => (await fetch(`${ORDER}/orders/00000000-0000-0000-0000-000000000000`)).status === 404,
);
const before = await stock();

console.log('happy path: BOOK x1');
const happy = await finalView(
  (await placeOrder([{ sku: 'BOOK', quantity: 1, unitPriceCents: 4500 }])).orderId,
);
expectEq('status', happy.status, 'CONFIRMED');
expectEq('payment', happy.paymentStatus, 'SUCCEEDED');
expectEq('timeline length', happy.timeline.length, 4);

console.log('payment declined: LAPTOP x1 at $1,500');
const declined = await finalView(
  (await placeOrder([{ sku: 'LAPTOP', quantity: 1, unitPriceCents: 150000 }])).orderId,
);
expectEq('status', declined.status, 'CANCELLED');
expectEq('payment', declined.paymentStatus, 'FAILED');

console.log('out of stock: KEYBOARD x99');
const rejected = await finalView(
  (await placeOrder([{ sku: 'KEYBOARD', quantity: 99, unitPriceCents: 9000 }])).orderId,
);
expectEq('status', rejected.status, 'CANCELLED');
expectEq('inventory', rejected.inventoryStatus, 'REJECTED');

console.log('compensation: stock after the saga');
const after = await waitFor(
  'laptop stock to be released',
  async () => {
    const s = await stock();
    return s.LAPTOP === before.LAPTOP ? s : null;
  },
  30_000,
);
expectEq('BOOK stock went down by 1', after.BOOK, before.BOOK - 1);
expectEq('LAPTOP stock restored', after.LAPTOP, before.LAPTOP);
expectEq('KEYBOARD stock untouched', after.KEYBOARD, before.KEYBOARD);

console.log('\nall end-to-end checks passed');
