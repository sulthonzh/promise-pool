/**
 * @sulthonzh/promise-pool
 * Zero-dep async concurrency control.
 *
 * Run async operations with a concurrency limit — never more than N promises
 * in flight at once. Results come back in order, errors are collected or thrown,
 * and you get progress callbacks, per-task timeouts, and AbortSignal support.
 *
 * @example
 * import { pool } from '@sulthonzh/promise-pool';
 *
 * // Process 1000 items, 10 at a time
 * const results = await pool.map(urls, fetchUrl, { concurrency: 10 });
 *
 * @example
 * import { PromisePool } from '@sulthonzh/promise-pool';
 *
 * const pp = new PromisePool({ concurrency: 5 });
 * const urls = ['https://api.example.com/1', 'https://api.example.com/2'];
 * const bodies = await pp.map(urls, async (url) => {
 *   const res = await fetch(url);
 *   return res.text();
 * });
 */

/** Custom error for pool-level timeouts. */
export class PoolTimeoutError extends Error {
  constructor(message) {
    super(message);
    this.name = 'PoolTimeoutError';
  }
}

/** Custom error for aborted pools. */
export class PoolAbortError extends Error {
  constructor(message) {
    super(message);
    this.name = 'PoolAbortError';
  }
}

/**
 * Per-task error with context about which item failed.
 */
export class TaskError extends Error {
  constructor(message, { index, item, cause }) {
    super(message);
    this.name = 'TaskError';
    this.index = index;
    this.item = item;
    if (cause) this.cause = cause;
  }
}

/**
 * Default options for the pool.
 */
const DEFAULT_OPTIONS = {
  concurrency: 8,
  timeout: 0, // 0 = no timeout
  signal: null, // AbortSignal
  stopOnError: false,
  scheduler: 'fifo', // 'fifo' | 'lifo'
};

/**
 * A reusable promise pool that limits concurrency.
 *
 * Create once, use many times. Each map/filter/each call is independent
 * and waits for its own batch to complete.
 *
 * @example
 * const pp = new PromisePool({ concurrency: 3 });
 * await pp.each(files, processFile);
 * await pp.each(images, resizeImage);
 */
export class PromisePool {
  constructor(options = {}) {
    this.options = { ...DEFAULT_OPTIONS, ...options };
    if (this.options.concurrency < 1) {
      throw new RangeError('concurrency must be >= 1');
    }
  }

  /**
   * Get or set the concurrency limit.
   */
  get concurrency() { return this.options.concurrency; }
  set concurrency(n) {
    if (n < 1) throw new RangeError('concurrency must be >= 1');
    this.options.concurrency = n;
  }

  /**
   * Map items through an async mapper with concurrency limit.
   * Results are returned in the same order as items.
   *
   * @param {Array} items - Items to process
   * @param {Function} mapper - async (item, index) => result
   * @param {Object} [callOptions] - Override pool options for this call
   * @returns {Promise<Array>} Results in order
   *
   * @example
   * const pp = new PromisePool({ concurrency: 5 });
   * const results = await pp.map(urls, async (url) => {
   *   const res = await fetch(url);
   *   return res.json();
   * });
   */
  async map(items, mapper, callOptions = {}) {
    const opts = { ...this.options, ...callOptions };
    return runPool(items, mapper, opts, 'map');
  }

  /**
   * Filter items through an async predicate with concurrency limit.
   *
   * @param {Array} items - Items to filter
   * @param {Function} predicate - async (item, index) => boolean
   * @param {Object} [callOptions]
   * @returns {Promise<Array>} Filtered items (original order)
   *
   * @example
   * const pp = new PromisePool({ concurrency: 10 });
   * const valid = await pp.filter(urls, async (url) => {
   *   const res = await fetch(url);
   *   return res.ok;
   * });
   */
  async filter(items, predicate, callOptions = {}) {
    const opts = { ...this.options, ...callOptions };
    const results = await runPool(items, predicate, opts, 'filter');
    return items.filter((_, i) => results[i]);
  }

  /**
   * Iterate over items with concurrency limit. Returns nothing.
   *
   * @param {Array} items
   * @param {Function} iterator - async (item, index) => void
   * @param {Object} [callOptions]
   *
   @example
   * const pp = new PromisePool({ concurrency: 3 });
   * await pp.each(records, async (record) => {
   *   await db.insert(record);
   * });
   */
  async each(items, iterator, callOptions = {}) {
    const opts = { ...this.options, ...callOptions };
    await runPool(items, iterator, opts, 'each');
  }
}

/**
 * Functional API — use without creating a PromisePool instance.
 *
 * @example
 * import { pool } from '@sulthonzh/promise-pool';
 * const results = await pool.map(items, mapper, { concurrency: 5 });
 */
export const pool = {
  /**
   * Map items with concurrency limit.
   * @param {Array} items
   * @param {Function} mapper
   * @param {Object} [options] - { concurrency, timeout, signal, stopOnError }
   * @returns {Promise<Array>}
   */
  map(items, mapper, options = {}) {
    const opts = { ...DEFAULT_OPTIONS, ...options };
    return runPool(items, mapper, opts, 'map');
  },

  /**
   * Filter items with concurrency limit.
   * @param {Array} items
   * @param {Function} predicate
   * @param {Object} [options]
   * @returns {Promise<Array>}
   */
  filter(items, predicate, options = {}) {
    const opts = { ...DEFAULT_OPTIONS, ...options };
    return runPool(items, predicate, opts, 'filter').then(results =>
      items.filter((_, i) => results[i])
    );
  },

  /**
   * Iterate items with concurrency limit.
   * @param {Array} items
   * @param {Function} iterator
   * @param {Object} [options]
   * @returns {Promise<void>}
   */
  each(items, iterator, options = {}) {
    const opts = { ...DEFAULT_OPTIONS, ...options };
    return runPool(items, iterator, opts, 'each');
  },
};

/**
 * Progress callback info.
 * @typedef {Object} ProgressInfo
 * @property {number} completed - Tasks finished
 * @property {number} failed - Tasks that threw
 * @property {number} total - Total tasks
 * @property {number} pending - Tasks not yet started
 * @property {number} active - Currently running
 * @property {number} percent - 0-100
 * @property {number} elapsedMs - Time since start
 * @property {number} [etaMs] - Estimated time to completion
 */

/**
 * Internal pool runner.
 *
 * @param {Array} items
 * @param {Function} fn - mapper/predicate/iterator
 * @param {Object} opts
 * @param {string} mode - 'map' | 'filter' | 'each'
 * @returns {Promise<Array|undefined>}
 */
async function runPool(items, fn, opts, mode) {
  const n = items.length;
  if (n === 0) return mode === 'map' ? [] : mode === 'filter' ? [] : undefined;

  const results = new Array(n);
  const errors = new Array(n).fill(null);
  const concurrency = Math.min(opts.concurrency, n);

  // Execution order: build an index queue
  const indices = Array.from({ length: n }, (_, i) => i);
  if (opts.scheduler === 'lifo') indices.reverse();

  let nextIdx = 0;
  let completed = 0;
  let failed = 0;
  const startTime = Date.now();

  // Check abort signal
  if (opts.signal?.aborted) {
    throw new PoolAbortError('Pool aborted before start');
  }

  function getNextIndex() {
    return nextIdx < indices.length ? indices[nextIdx++] : null;
  }

  async function worker() {
    while (true) {
      const idx = getNextIndex();
      if (idx === null) break;

      // Check abort
      if (opts.signal?.aborted) {
        throw new PoolAbortError(`Pool aborted (at task ${idx})`);
      }

      const item = items[idx];

      try {
        const value = await runTask(fn, item, idx, opts);
        if (mode === 'map' || mode === 'filter') {
          results[idx] = value;
        }
        completed++;
      } catch (err) {
        // Timeouts and aborts always propagate, regardless of stopOnError
        if (err instanceof PoolTimeoutError || err instanceof PoolAbortError) throw err;

        failed++;
        errors[idx] = err;

        if (opts.stopOnError) {
          throw new TaskError(`Task ${idx} failed: ${err.message}`, { index: idx, item, cause: err });
        }

        if (mode === 'map' || mode === 'filter') {
          // Store undefined for failed tasks when not stopOnError
          results[idx] = undefined;
        }
      }

      // Progress callback
      if (opts.onProgress) {
        const elapsedMs = Date.now() - startTime;
        const done = completed + failed;
        const percent = Math.round((done / n) * 100);
        const active = Math.min(concurrency, n - done) - (nextIdx < indices.length ? 0 : 0);
        const info = {
          completed,
          failed,
          total: n,
          pending: Math.max(0, n - done - Math.min(concurrency - 1, n - done)),
          active: done + (indices.length - nextIdx) > 0 ? Math.min(concurrency, n - done) : 0,
          percent,
          elapsedMs,
          etaMs: done > 0 ? Math.round((elapsedMs / done) * (n - done)) : undefined,
        };
        opts.onProgress(info);
      }
    }
  }

  // Spawn workers
  const workers = [];
  for (let i = 0; i < concurrency; i++) {
    workers.push(worker());
  }

  await Promise.all(workers);

  // Handle abort signal listener cleanup
  // (handled by runTask)

  // If stopOnError is false and there were errors, attach them
  if (!opts.stopOnError && failed > 0 && opts.collectErrors) {
    const collectedErrors = errors.filter(e => e !== null);
    if (collectedErrors.length > 0) {
      results.errors = collectedErrors;
    }
  }

  if (mode === 'each') return undefined;
  return results;
}

/**
 * Run a single task with optional timeout and abort handling.
 */
async function runTask(fn, item, index, opts) {
  let timeoutId;
  let abortHandler;

  const taskPromise = Promise.resolve().then(() => fn(item, index));

  // No timeout or signal → just run
  if (!opts.timeout && !opts.signal) {
    return taskPromise;
  }

  const race = [];

  // Timeout
  if (opts.timeout > 0) {
    const timeoutPromise = new Promise((_, reject) => {
      timeoutId = setTimeout(() => {
        reject(new PoolTimeoutError(`Task ${index} timed out after ${opts.timeout}ms`));
      }, opts.timeout);
    });
    race.push(timeoutPromise);
  }

  // AbortSignal
  if (opts.signal) {
    const abortPromise = new Promise((_, reject) => {
      if (opts.signal.aborted) {
        reject(new PoolAbortError(`Task ${index} aborted`));
        return;
      }
      abortHandler = () => reject(new PoolAbortError(`Task ${index} aborted`));
      opts.signal.addEventListener('abort', abortHandler, { once: true });
    });
    race.push(abortPromise);
  }

  race.push(taskPromise);

  try {
    return await Promise.race(race);
  } finally {
    if (timeoutId) clearTimeout(timeoutId);
    if (abortHandler && opts.signal) {
      opts.signal.removeEventListener('abort', abortHandler);
    }
  }
}

export default pool;
