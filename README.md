# @sulthonzh/promise-pool

Zero-dep async concurrency control — run N promises at a time, not all at once.

## Why?

`Promise.all()` fires everything simultaneously. That's fine for 5 promises. It's not fine for 5,000 API calls. This library limits how many promises are in flight at once.

## Install

```bash
npm install @sulthonzh/promise-pool
```

## Quick start

```js
import { pool } from '@sulthonzh/promise-pool';

// Fetch 100 URLs, 10 at a time
const responses = await pool.map(urls, fetchUrl, { concurrency: 10 });
```

## API

### `pool.map(items, mapper, options?)` → `Promise<Array>`

Run mapper on each item with concurrency limit. Results in order.

```js
const doubled = await pool.map([1, 2, 3, 4, 5], async (n) => n * 2, {
  concurrency: 2,
});
// [2, 4, 6, 8, 10]
```

### `pool.filter(items, predicate, options?)` → `Promise<Array>`

Filter items through an async predicate.

```js
const reachable = await pool.filter(urls, async (url) => {
  const res = await fetch(url);
  return res.ok;
}, { concurrency: 20 });
```

### `pool.each(items, iterator, options?)` → `Promise<void>`

Side-effect iteration. Returns nothing.

```js
await pool.each(records, async (record) => {
  await db.insert(record);
}, { concurrency: 5 });
```

### `PromisePool` class

Reusable instance — create once, use many times.

```js
const pp = new PromisePool({ concurrency: 5 });

await pp.map(batch1, processItem);
await pp.map(batch2, processItem); // reuses the pool
```

Override options per call:

```js
const pp = new PromisePool({ concurrency: 10 });
await pp.map(items, fn, { concurrency: 1 }); // this call runs serial
```

## Options

| Option | Default | Description |
|--------|---------|-------------|
| `concurrency` | `8` | Max concurrent promises |
| `timeout` | `0` | Per-task timeout in ms (0 = none) |
| `signal` | `null` | `AbortSignal` to cancel the pool |
| `stopOnError` | `false` | Throw on first error (default: continue) |
| `scheduler` | `'fifo'` | `'fifo'` or `'lifo'` |
| `onProgress` | `null` | Callback with `{ completed, failed, total, percent, etaMs }` |
| `collectErrors` | `false` | Attach `.errors` array to results |

## Progress tracking

```js
const results = await pool.map(files, uploadFile, {
  concurrency: 3,
  onProgress(info) {
    console.log(`${info.completed}/${info.total} (${info.percent}%) ETA: ${info.etaMs}ms`);
  },
});
```

## Timeout

```js
// Kill any task that takes > 5 seconds
const results = await pool.map(urls, fetchUrl, {
  concurrency: 10,
  timeout: 5000,
});
```

## Abort

```js
const ac = new AbortController();

// Cancel after 10 seconds
setTimeout(() => ac.abort(), 10_000);

const results = await pool.map(items, slowTask, {
  concurrency: 4,
  signal: ac.signal,
});
// Throws PoolAbortError if cancelled
```

## Error handling

By default, failures are swallowed (result is `undefined` for that index):

```js
const results = await pool.map([1, 2, 3], async (n) => {
  if (n === 2) throw new Error('boom');
  return n;
});
// [1, undefined, 3]
```

Use `stopOnError` to throw on first failure:

```js
try {
  await pool.map(items, fn, { stopOnError: true });
} catch (err) {
  console.log(err.index);  // which item failed
  console.log(err.item);   // the original item
  console.log(err.cause);  // the original error
}
```

Or collect all errors:

```js
const results = await pool.map(items, fn, { collectErrors: true });
if (results.errors?.length) {
  console.log(`${results.errors.length} tasks failed`);
}
```

## CLI

```bash
# Map items through an expression
echo '[1,2,3,4,5]' | promise-pool map --concurrency 2 --expr "async (x) => x * 2"

# JSON output
echo '["a","b","c"]' | promise-pool map --concurrency 3 --expr "async (s) => s.toUpperCase()" --json
```

## Zero dependencies

No deps. Just Node.js ≥18.

## License

MIT
