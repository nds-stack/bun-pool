import { Pool } from "../src/index.ts";

const iterations = 500;
const samples = 5;

let globalCounter = 0;

function makeFactory() {
  return {
    create: () => ++globalCounter,
    destroy: (_n: number) => {},
  };
}

function makeValidatingFactory() {
  return {
    create: () => ++globalCounter,
    destroy: (_n: number) => {},
    validate: (n: number) => n > 0,
  };
}

function bench(fn: () => void | Promise<void>): number {
  const totalOps = iterations * samples;
  const start = performance.now();
  for (let s = 0; s < samples; s++) {
    for (let i = 0; i < iterations; i++) {
      fn();
    }
  }
  const elapsed = performance.now() - start;
  return Math.round(totalOps / (elapsed / 1000));
}

function benchAsync(fn: () => Promise<void>): Promise<number> {
  return (async () => {
    const totalOps = iterations * samples;
    const start = performance.now();
    for (let s = 0; s < samples; s++) {
      for (let i = 0; i < iterations; i++) {
        await fn();
      }
    }
    const elapsed = performance.now() - start;
    return Math.round(totalOps / (elapsed / 1000));
  })();
}

function format(ops: number): string {
  if (ops > 1_000_000) return `${(ops / 1_000_000).toFixed(1)}M ops/s`;
  if (ops > 1_000) return `${(ops / 1_000).toFixed(0)}K ops/s`;
  return `${ops} ops/s`;
}

// ── Baseline: raw factory create/destroy ───────────────────────
const rawFn = () => {
  const f = makeFactory();
  const c = f.create();
  f.destroy(c);
};

// ── Pool instances (reused across bench runs) ──────────────────
const poolAcq = new Pool<number>({ min: 0, max: iterations, factory: makeFactory() });
const poolWith = new Pool<number>({ min: 0, max: iterations, factory: makeFactory() });
const poolVal = new Pool<number>({ min: 0, max: iterations, factory: makeValidatingFactory() });

// Prime pools with one idle connection for fair "reuse" comparison
async function primePool(p: Pool<number>) {
  const c = await p.acquire();
  p.release(c);
}

await primePool(poolAcq);
await primePool(poolWith);
await primePool(poolVal);

// ── Results ────────────────────────────────────────────────────
interface BenchResult {
  name: string;
  ops: number;
}

const results: BenchResult[] = [];

results.push({
  name: "raw factory create/destroy",
  ops: bench(rawFn),
});

results.push({
  name: "Pool acquire/release",
  ops: await benchAsync(async () => {
    const c = await poolAcq.acquire();
    poolAcq.release(c);
  }),
});

results.push({
  name: "Pool withPool",
  ops: await benchAsync(async () => {
    await poolWith.withPool(async () => {});
  }),
});

results.push({
  name: "Pool acquire/release with validation",
  ops: await benchAsync(async () => {
    const c = await poolVal.acquire();
    poolVal.release(c);
  }),
});

// ── Print table ────────────────────────────────────────────────
const rawOps = results.find(r => r.name === "raw factory create/destroy")!.ops;

const base = 2;
const opPad = results.reduce((m, r) => Math.max(m, r.name.length), 0);

console.log(`\n--- bun-pool Benchmark ---`);
console.log(`Bun ${Bun.version}, ${iterations} iterations \u00d7 ${samples} samples\n`);

const padRight = (s: string, n: number) => s.padEnd(n);

console.log(
  `${padRight("Operation", opPad + base)} | ${padRight("Throughput", 14)} | ${padRight("Overhead", 10)}`
);
console.log(
  `${"-".repeat(opPad + base)}-|-${"-".repeat(14)}-|-${"-".repeat(10)}`
);

for (const r of results) {
  const opsStr = format(r.ops);
  if (r.name === "raw factory create/destroy") {
    console.log(
      `${padRight(r.name, opPad + base)} | ${padRight(opsStr, 14)} | ${padRight("(baseline)", 10)}`
    );
  } else {
    const overhead = ((r.ops - rawOps) / rawOps * 100).toFixed(1);
    const sign = +overhead >= 0 ? "+" : "";
    console.log(
      `${padRight(r.name, opPad + base)} | ${padRight(opsStr, 14)} | ${padRight(`${sign}${overhead}%`, 10)}`
    );
  }
}

console.log("");

await poolAcq.drain();
await poolWith.drain();
await poolVal.drain();
