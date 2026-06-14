#!/usr/bin/env node
/**
 * promise-pool CLI
 *
 * Demonstrate concurrency control from the command line.
 * Reads JSON array from stdin, maps through a JS expression, prints results.
 *
 * Usage:
 *   echo '[1,2,3,4,5]' | promise-pool map --concurrency 2 --expr "async (x) => x * 2"
 *   echo '[{"url":"https://example.com"}]' | promise-pool each --concurrency 5 --expr "async (item) => console.log(item.url)"
 *   promise-pool info
 */
import { pool, PromisePool } from './index.js';

const args = process.argv.slice(2);

function usage() {
  console.log(`
promise-pool CLI — async concurrency control

Commands:
  map        Map items through an async expression
  filter     Filter items through an async predicate
  each       Iterate over items (side effects)
  info       Show package info

Options:
  --concurrency <n>   Max concurrent tasks (default: 8)
  --timeout <ms>      Per-task timeout in milliseconds
  --stop-on-error     Stop on first error
  --expr <code>       JS expression: async (item, index) => ...
  --json              Output results as JSON
  --no-color          Disable colored output

Examples:
  echo '[1,2,3]' | promise-pool map --concurrency 2 --expr "async (x) => x * x"
  echo '["a","b","c"]' | promise-pool each --expr "async (s) => console.log(s.toUpperCase())"
  promise-pool info
`);
}

function parseArgs(argv) {
  const opts = { concurrency: 8, timeout: 0, stopOnError: false, expr: null, json: false, color: true };
  let command = null;
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === 'map' || a === 'filter' || a === 'each' || a === 'info') {
      command = a;
    } else if (a === '--concurrency') {
      opts.concurrency = parseInt(argv[++i], 10);
    } else if (a === '--timeout') {
      opts.timeout = parseInt(argv[++i], 10);
    } else if (a === '--stop-on-error') {
      opts.stopOnError = true;
    } else if (a === '--expr') {
      opts.expr = argv[++i];
    } else if (a === '--json') {
      opts.json = true;
    } else if (a === '--no-color') {
      opts.color = false;
    } else if (a === '--help' || a === '-h') {
      usage();
      process.exit(0);
    }
  }
  return { command, opts };
}

async function readStdin() {
  const chunks = [];
  for await (const chunk of process.stdin) chunks.push(chunk);
  return Buffer.concat(chunks).toString();
}

async function main() {
  const { command, opts } = parseArgs(args);

  if (!command || command === 'info') {
    console.log(`promise-pool v1.0.0 — zero-dep async concurrency control
  • map / filter / each with limited concurrency
  • Per-task timeouts
  • AbortSignal support
  • Progress tracking
  • stopOnError or collect
  • FIFO / LIFO scheduling
`);
    return;
  }

  // Read items from stdin
  const stdin = await readStdin();
  if (!stdin.trim()) {
    console.error('Error: no input on stdin. Pipe a JSON array.');
    process.exit(1);
  }

  let items;
  try {
    items = JSON.parse(stdin);
    if (!Array.isArray(items)) {
      console.error('Error: input must be a JSON array');
      process.exit(1);
    }
  } catch (e) {
    console.error('Error: invalid JSON input:', e.message);
    process.exit(1);
  }

  // Build mapper from expr
  if (!opts.expr) {
    console.error('Error: --expr is required for map/filter/each');
    process.exit(1);
  }

  let fn;
  try {
    fn = eval(opts.expr);
  } catch (e) {
    console.error('Error: invalid --expr:', e.message);
    process.exit(1);
  }

  if (typeof fn !== 'function') {
    console.error('Error: --expr must evaluate to a function');
    process.exit(1);
  }

  const poolOpts = {
    concurrency: opts.concurrency,
    timeout: opts.timeout,
    stopOnError: opts.stopOnError,
    onProgress: (info) => {
      if (!opts.json) {
        process.stderr.write(`\r${info.completed + info.failed}/${info.total} (${info.percent}%)`);
      }
    },
  };

  try {
    let result;
    if (command === 'map') {
      result = await pool.map(items, fn, poolOpts);
    } else if (command === 'filter') {
      result = await pool.filter(items, fn, poolOpts);
    } else if (command === 'each') {
      await pool.each(items, fn, poolOpts);
      if (!opts.json) console.log('\ndone.');
      return;
    }

    if (opts.json) {
      console.log(JSON.stringify(result, null, 2));
    } else {
      console.log('\n');
      console.log(result);
    }
  } catch (err) {
    console.error('\nError:', err.message);
    process.exit(1);
  }
}

main();
