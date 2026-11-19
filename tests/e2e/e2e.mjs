// End-to-end check of the whole system: real Kafka, real Postgres, all four services.
// It is a black-box test: it only talks to the services over HTTP, like a client would.
//
//   docker compose up -d --build --wait
//   node tests/e2e/e2e.mjs            (or: npm run test:e2e)
//
// Written to the same JPL rules as the services: every wait is a for loop with a fixed
// maximum number of attempts (no while, no recursion), and every HTTP call has a timeout.

const ORDER = 'http://localhost:3001';
const INVENTORY = 'http://localhost:3002';
const PAYMENT = 'http://localhost:3003';
const QUERY = 'http://localhost:3004';
const SERVICES = [
  ['order-service', ORDER],
  ['inventory-service', INVENTORY],
  ['payment-service', PAYMENT],
  ['order-query-service', QUERY],
];

const POLL_INTERVAL_MS = 1_000;
const MAX_HEALTH_POLLS = 180; // services may still be starting: up to 3 minutes
const MAX_SETTLE_POLLS = 60; // an order goes through the saga in a few seconds: up to 1 minute
const REQUEST_TIMEOUT_MS = 5_000;
const CUSTOMER_ID = `e2e-${Date.now()}`; // fresh per run, so the customer-orders check is exact

const sleep = (ms) =>
  new Promise((resolve) => {
    setTimeout(resolve, ms);
  });

/**
 * Calls `probe` up to `maxAttempts` times, one second apart, until it returns { done: true }.
 * Errors count as "not yet" (a service may still be starting). On timeout, the error message
 * includes the last value seen, which is usually all you need to see what went wrong.
 */
async function poll(what, probe, maxAttempts) {
  let last = 'nothing yet';
  for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
    try {
      const result = await probe();
      if (result.done) return result.value;
      last = result.value;
    } catch (err) {
      last = `error: ${err instanceof Error ? err.message : String(err)}`;
    }
    await sleep(POLL_INTERVAL_MS);
  }
  throw new Error(
    `timed out waiting for ${what} (${maxAttempts} attempts).\nLast seen: ${JSON.stringify(last, null, 2)}`,
  );
}

async function request(method, url, body) {
  const res = await fetch(url, {
    method,
    headers: body === undefined ? {} : { 'content-type': 'application/json' },
    body: body === undefined ? undefined : JSON.stringify(body),
    signal: AbortSignal.timeout(REQUEST_TIMEOUT_MS),
  });
  const text = await res.text();
  return { status: res.status, body: text.length > 0 ? JSON.parse(text) : null };
}

async function getJson(url) {
  const res = await request('GET', url);
  if (res.status !== 200) throw new Error(`GET ${url} -> HTTP ${res.status}`);
  return res.body;
}

function check(label, actual, expected) {
  if (actual !== expected) {
    throw new Error(`${label}: expected ${JSON.stringify(expected)}, got ${JSON.stringify(actual)}`);
  }
  console.log(`  ✓ ${label} = ${JSON.stringify(actual)}`);
}

async function waitForServices() {
  console.log('waiting for services to be healthy...');
  for (const [name, base] of SERVICES) {
    await poll(
      `${name} GET /health`,
      async () => ({ done: (await request('GET', `${base}/health`)).status === 200, value: name }),
      MAX_HEALTH_POLLS,
    );
    console.log(`  ✓ ${name} is healthy`);
  }
}

/** Available stock per sku, from the inventory service's catalog. */
async function readStock() {
  const products = await getJson(`${INVENTORY}/products`);
  return Object.fromEntries(products.map((p) => [p.sku, p.available]));
}

async function placeOrder(items) {
  return await request('POST', `${ORDER}/orders`, { customerId: CUSTOMER_ID, items });
}

/** Places an order that passes validation, and checks the immediate 202 answer. */
async function placeValidOrder(items, expectedTotalCents) {
  const res = await placeOrder(items);
  check('POST /orders HTTP status', res.status, 202);
  check('initial status', res.body.status, 'PENDING');
  check('totalCents', res.body.totalCents, expectedTotalCents);
  return res.body.orderId;
}

/** The fields of a query-side view that the scenarios check. */
function summarize(view) {
  return {
    status: view.status,
    paymentStatus: view.paymentStatus,
    inventoryStatus: view.inventoryStatus,
    timelineLength: view.timeline.length,
  };
}

/**
 * Waits until the query-side view shows EVERY expected field, not just a final status.
 * The query service reads three topics, and Kafka does not order messages across topics,
 * so a view can already say CANCELLED before PaymentFailed or InventoryRejected arrives.
 */
async function waitForView(orderId, expected) {
  return await poll(
    `order ${orderId} to reach ${JSON.stringify(expected)}`,
    async () => {
      const res = await request('GET', `${QUERY}/orders/${orderId}`);
      if (res.status !== 200) return { done: false, value: `HTTP ${res.status}` };
      const summary = summarize(res.body);
      const done = Object.entries(expected).every(([key, value]) => summary[key] === value);
      return { done, value: res.body };
    },
    MAX_SETTLE_POLLS,
  );
}

/** Places an order, waits for the saga to finish, and prints a check line per expected field. */
async function runSaga(items, expectedTotalCents, expected) {
  const orderId = await placeValidOrder(items, expectedTotalCents);
  const view = await waitForView(orderId, expected);
  const summary = summarize(view);
  for (const [key, value] of Object.entries(expected)) check(key, summary[key], value);
  // The write side (order-service) must agree with the read side.
  const order = await getJson(`${ORDER}/orders/${orderId}`);
  check('order-service status', order.status, expected.status);
  return view;
}

async function happyPath() {
  console.log('\nhappy path: BOOK x1 at 4500');
  await runSaga([{ sku: 'BOOK', quantity: 1, unitPriceCents: 4500 }], 4500, {
    status: 'CONFIRMED',
    paymentStatus: 'SUCCEEDED',
    inventoryStatus: 'RESERVED',
    timelineLength: 4, // OrderCreated, InventoryReserved, PaymentSucceeded, OrderConfirmed
  });
}

async function paymentDeclined() {
  console.log('\npayment declined: LAPTOP x1 at 150000 (over the 100000 limit)');
  await runSaga([{ sku: 'LAPTOP', quantity: 1, unitPriceCents: 150000 }], 150000, {
    status: 'CANCELLED',
    paymentStatus: 'FAILED',
    inventoryStatus: 'RELEASED',
    timelineLength: 4, // OrderCreated, InventoryReserved, PaymentFailed, OrderCancelled
  });
}

async function outOfStock() {
  console.log('\nout of stock: KEYBOARD x99');
  await runSaga([{ sku: 'KEYBOARD', quantity: 99, unitPriceCents: 9000 }], 99 * 9000, {
    status: 'CANCELLED',
    inventoryStatus: 'REJECTED',
    paymentStatus: null, // never charged
    timelineLength: 3, // OrderCreated, InventoryRejected, OrderCancelled
  });
}

async function priceTampering() {
  console.log('\nprice tampering: BOOK x1 at 1 cent (the catalog says 4500)');
  const view = await runSaga([{ sku: 'BOOK', quantity: 1, unitPriceCents: 1 }], 1, {
    status: 'CANCELLED',
    inventoryStatus: 'REJECTED',
    paymentStatus: null,
    timelineLength: 3,
  });
  check('cancelReason mentions the price', /price/i.test(view.cancelReason ?? ''), true);
}

async function inputValidation() {
  console.log('\ninput validation: bad orders are refused before anything happens');
  const zero = await placeOrder([{ sku: 'BOOK', quantity: 0, unitPriceCents: 4500 }]);
  check('quantity 0 -> HTTP status', zero.status, 400);
  const tooMany = Array.from({ length: 51 }, () => ({ sku: 'MUG', quantity: 1, unitPriceCents: 1200 }));
  const big = await placeOrder(tooMany);
  check('51 items -> HTTP status', big.status, 400);
}

/** Compensation: the declined LAPTOP must be released, and only the confirmed BOOK is gone. */
async function compensation(before) {
  console.log('\ncompensation: stock after all the sagas');
  const after = await poll(
    'stock to settle (LAPTOP released, BOOK down by 1)',
    async () => {
      const stock = await readStock();
      return { done: stock.LAPTOP === before.LAPTOP && stock.BOOK === before.BOOK - 1, value: stock };
    },
    MAX_SETTLE_POLLS,
  );
  check('LAPTOP stock restored', after.LAPTOP, before.LAPTOP);
  check('BOOK stock down by exactly 1', after.BOOK, before.BOOK - 1);
  check('KEYBOARD stock untouched', after.KEYBOARD, before.KEYBOARD);
}

/** The query side lists this run's customer orders: the four valid ones, none of the 400s. */
async function customerOrders() {
  console.log('\nquery side: customer order list');
  const orders = await poll(
    `GET /customers/${CUSTOMER_ID}/orders to list 4 orders`,
    async () => {
      const list = await getJson(`${QUERY}/customers/${CUSTOMER_ID}/orders`);
      return { done: list.length === 4, value: list };
    },
    MAX_SETTLE_POLLS,
  );
  check('orders for this customer', orders.length, 4);
}

async function main() {
  await waitForServices();
  const before = await readStock();
  console.log(`\nstarting stock: ${JSON.stringify(before)}`);
  if (!(before.BOOK >= 2 && before.LAPTOP >= 1)) {
    throw new Error('not enough BOOK/LAPTOP stock left; reset with: docker compose down -v');
  }
  await happyPath();
  await paymentDeclined();
  await outOfStock();
  await priceTampering();
  await inputValidation();
  await compensation(before);
  await customerOrders();
}

try {
  await main();
  console.log('\nall end-to-end checks passed');
} catch (err) {
  console.error(`\nEND-TO-END CHECK FAILED: ${err instanceof Error ? err.message : String(err)}`);
  console.error(
    'Service logs: docker compose logs order-service inventory-service payment-service order-query-service',
  );
  process.exitCode = 1;
}
