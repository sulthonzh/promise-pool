import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { pool, PromisePool, PoolTimeoutError, PoolAbortError, TaskError } from '../src/index.js';

// Helper: delay that can be cancelled by abort
const delay = (ms, val) => new Promise(resolve => setTimeout(() => resolve(val), ms));
const rejectAfter = (ms, err) => new Promise((_, reject) => setTimeout(() => reject(err), ms));

describe('pool.map — basic', () => {
  it('should run all items through the mapper', async () => {
    const items = [1, 2, 3, 4, 5];
    const results = await pool.map(items, async (x) => x * 2);
    assert.deepEqual(results, [2, 4, 6, 8, 10]);
  });

  it('should respect concurrency limit', async () => {
    let active = 0;
    let maxActive = 0;
    const items = Array.from({ length: 20 }, (_, i) => i);
    await pool.map(items, async (x) => {
      active++;
      maxActive = Math.max(maxActive, active);
      await delay(10);
      active--;
      return x;
    }, { concurrency: 3 });
    assert.ok(maxActive <= 3, `maxActive was ${maxActive}, expected <= 3`);
  });

  it('should return results in order regardless of completion', async () => {
    const items = [1, 2, 3, 4, 5];
    const results = await pool.map(items, async (x) => {
      await delay(50 - x * 10); // later items finish first
      return x;
    }, { concurrency: 2 });
    assert.deepEqual(results, [1, 2, 3, 4, 5]);
  });

  it('should handle empty array', async () => {
    const results = await pool.map([], async (x) => x);
    assert.deepEqual(results, []);
  });

  it('should handle single item', async () => {
    const results = await pool.map([42], async (x) => x);
    assert.deepEqual(results, [42]);
  });

  it('should pass index to mapper', async () => {
    const indices = await pool.map(['a', 'b', 'c'], async (_, i) => i);
    assert.deepEqual(indices, [0, 1, 2]);
  });
});

describe('pool.map — concurrency = 1 (serial)', () => {
  it('should run one at a time', async () => {
    let active = 0;
    let maxActive = 0;
    const items = Array.from({ length: 5 }, (_, i) => i);
    await pool.map(items, async (x) => {
      active++;
      maxActive = Math.max(maxActive, active);
      await delay(10);
      active--;
    }, { concurrency: 1 });
    assert.equal(maxActive, 1);
  });
});

describe('pool.map — errors', () => {
  it('should not throw by default on task failure', async () => {
    const results = await pool.map([1, 2, 3], async (x) => {
      if (x === 2) throw new Error('boom');
      return x;
    }, { concurrency: 1 });
    assert.equal(results[0], 1);
    assert.equal(results[1], undefined);
    assert.equal(results[2], 3);
  });

  it('should throw TaskError when stopOnError is true', async () => {
    await assert.rejects(
      pool.map([1, 2, 3], async (x) => {
        if (x === 2) throw new Error('boom');
        return x;
      }, { concurrency: 1, stopOnError: true }),
      (err) => {
        assert.equal(err.name, 'TaskError');
        assert.equal(err.index, 1);
        return true;
      }
    );
  });

  it('should allow continuation after errors', async () => {
    let processed = [];
    const results = await pool.map([1, 2, 3, 4], async (x) => {
      processed.push(x);
      if (x === 2) throw new Error('skip');
      return x * 10;
    }, { concurrency: 1 });
    assert.deepEqual(processed, [1, 2, 3, 4]);
    assert.deepEqual(results, [10, undefined, 30, 40]);
  });
});

describe('pool.map — timeout', () => {
  it('should timeout slow tasks', async () => {
    await assert.rejects(
      pool.map([1], async (x) => {
        await delay(500);
        return x;
      }, { timeout: 50 }),
      PoolTimeoutError
    );
  });

  it('should not timeout fast tasks', async () => {
    const results = await pool.map([1, 2, 3], async (x) => {
      await delay(5);
      return x;
    }, { timeout: 1000 });
    assert.deepEqual(results, [1, 2, 3]);
  });
});

describe('pool.map — abort', () => {
  it('should abort via AbortSignal', async () => {
    const ac = new AbortController();
    setTimeout(() => ac.abort(), 20);

    await assert.rejects(
      pool.map([1, 2, 3, 4, 5], async (x) => {
        await delay(100);
        return x;
      }, { concurrency: 2, signal: ac.signal }),
      PoolAbortError
    );
  });

  it('should throw immediately if already aborted', async () => {
    const ac = new AbortController();
    ac.abort();
    await assert.rejects(
      pool.map([1, 2], async (x) => x, { signal: ac.signal }),
      PoolAbortError
    );
  });
});

describe('pool.filter', () => {
  it('should filter items by predicate', async () => {
    const items = [1, 2, 3, 4, 5, 6];
    const evens = await pool.filter(items, async (x) => x % 2 === 0, { concurrency: 3 });
    assert.deepEqual(evens, [2, 4, 6]);
  });

  it('should preserve order', async () => {
    const items = Array.from({ length: 20 }, (_, i) => i);
    const filtered = await pool.filter(items, async (x) => {
      await delay(Math.random() * 20);
      return x > 10;
    }, { concurrency: 5 });
    assert.deepEqual(filtered, items.filter(x => x > 10));
  });

  it('should handle empty', async () => {
    assert.deepEqual(await pool.filter([], async () => true), []);
  });
});

describe('pool.each', () => {
  it('should iterate all items', async () => {
    const seen = [];
    await pool.each(['a', 'b', 'c'], async (item) => {
      seen.push(item);
    }, { concurrency: 2 });
    assert.deepEqual(seen.sort(), ['a', 'b', 'c']);
  });

  it('should not return results', async () => {
    const result = await pool.each([1, 2], async () => {});
    assert.equal(result, undefined);
  });
});

describe('PromisePool class', () => {
  it('should work as instance', async () => {
    const pp = new PromisePool({ concurrency: 2 });
    const results = await pp.map([1, 2, 3], async (x) => x * 2);
    assert.deepEqual(results, [2, 4, 6]);
  });

  it('should allow reusing instance', async () => {
    const pp = new PromisePool({ concurrency: 3 });
    const r1 = await pp.map([1, 2], async (x) => x + 10);
    const r2 = await pp.map([3, 4], async (x) => x + 10);
    assert.deepEqual(r1, [11, 12]);
    assert.deepEqual(r2, [13, 14]);
  });

  it('should allow per-call option overrides', async () => {
    const pp = new PromisePool({ concurrency: 10 });
    let maxActive = 0;
    let active = 0;
    await pp.map([1, 2, 3, 4, 5], async (x) => {
      active++;
      maxActive = Math.max(maxActive, active);
      await delay(10);
      active--;
      return x;
    }, { concurrency: 1 });
    assert.equal(maxActive, 1);
  });

  it('should reject invalid concurrency', () => {
    assert.throws(() => new PromisePool({ concurrency: 0 }), RangeError);
    assert.throws(() => new PromisePool({ concurrency: -1 }), RangeError);
  });

  it('should get/set concurrency', () => {
    const pp = new PromisePool({ concurrency: 5 });
    assert.equal(pp.concurrency, 5);
    pp.concurrency = 10;
    assert.equal(pp.concurrency, 10);
    assert.throws(() => { pp.concurrency = 0; }, RangeError);
  });
});

describe('progress callback', () => {
  it('should call onProgress with correct info', async () => {
    const progress = [];
    await pool.map([1, 2, 3, 4], async (x) => {
      await delay(5);
      return x;
    }, {
      concurrency: 2,
      onProgress(info) {
        progress.push({ ...info });
      }
    });
    assert.equal(progress.length, 4);
    assert.equal(progress[0].completed, 1);
    assert.equal(progress[0].total, 4);
    assert.equal(progress[0].percent, 25);
    assert.equal(progress[3].completed, 4);
    assert.equal(progress[3].percent, 100);
    assert.ok(progress[3].elapsedMs >= 0);
  });
});

describe('scheduling', () => {
  it('should support LIFO scheduling', async () => {
    const order = [];
    await pool.map([1, 2, 3, 4, 5, 6], async (x) => {
      order.push(x);
      await delay(10);
      return x;
    }, { concurrency: 3, scheduler: 'lifo' });
    // With LIFO, the first batch starts from the end
    // At least verify all items processed
    assert.equal(order.length, 6);
    assert.deepEqual(order.slice().sort(), [1, 2, 3, 4, 5, 6]);
  });

  it('should support FIFO scheduling (default)', async () => {
    const order = [];
    await pool.map([1, 2, 3], async (x) => {
      order.push(x);
      await delay(5);
      return x;
    }, { concurrency: 1, scheduler: 'fifo' });
    assert.deepEqual(order, [1, 2, 3]);
  });
});

describe('edge cases', () => {
  it('should handle items faster than concurrency', async () => {
    const results = await pool.map([1, 2, 3], async (x) => x, { concurrency: 10 });
    assert.deepEqual(results, [1, 2, 3]);
  });

  it('should handle mapper returning promises', async () => {
    const results = await pool.map([1, 2], (x) => Promise.resolve(x * 3));
    assert.deepEqual(results, [3, 6]);
  });

  it('should handle mapper returning values', async () => {
    const results = await pool.map([1, 2], (x) => x * 3);
    assert.deepEqual(results, [3, 6]);
  });

  it('should handle collectErrors option', async () => {
    const results = await pool.map([1, 2, 3], async (x) => {
      if (x === 2) throw new Error('err');
      return x;
    }, { concurrency: 1, collectErrors: true });
    assert.equal(results[0], 1);
    assert.equal(results[2], 3);
    assert.ok(results.errors);
    assert.equal(results.errors.length, 1);
  });
});

describe('integration — realistic', () => {
  it('should simulate API fetching with concurrency', async () => {
    const urls = Array.from({ length: 10 }, (_, i) => `https://api.example.com/${i}`);
    const results = await pool.map(urls, async (url) => {
      await delay(5);
      return { url, ok: true };
    }, { concurrency: 3 });
    assert.equal(results.length, 10);
    assert.equal(results[0].url, 'https://api.example.com/0');
    assert.equal(results[9].url, 'https://api.example.com/9');
  });

  it('should simulate mixed success/failure', async () => {
    const items = Array.from({ length: 10 }, (_, i) => i);
    const results = await pool.map(items, async (x) => {
      await delay(2);
      if (x % 3 === 0) throw new Error(`bad: ${x}`);
      return x;
    }, { concurrency: 4 });
    // Indices 0, 3, 6, 9 should be undefined
    assert.equal(results[0], undefined);
    assert.equal(results[1], 1);
    assert.equal(results[2], 2);
    assert.equal(results[3], undefined);
    assert.equal(results[4], 4);
  });

  it('should handle large array with small concurrency', async () => {
    const n = 100;
    const items = Array.from({ length: n }, (_, i) => i);
    const results = await pool.map(items, async (x) => {
      await delay(1);
      return x;
    }, { concurrency: 5 });
    assert.equal(results.length, n);
    assert.equal(results[0], 0);
    assert.equal(results[n - 1], n - 1);
  });
});
