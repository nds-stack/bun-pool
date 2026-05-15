import { describe, test, expect, afterEach } from "bun:test";
import { Pool } from "../src/index.ts";

describe("Pool", () => {
  let pool: Pool<string>;

  afterEach(async () => {
    if (pool) await pool.drain();
  });

  test("acquire creates new connection", async () => {
    pool = new Pool({
      min: 0, max: 3,
      factory: { create: () => "conn", destroy: () => {} },
    });
    const conn = await pool.acquire();
    expect(conn).toBe("conn");
  });

  test("release returns to pool", async () => {
    pool = new Pool({
      min: 0, max: 3,
      factory: { create: () => "conn", destroy: () => {} },
    });
    const conn = await pool.acquire();
    pool.release(conn);
    const c2 = await pool.acquire();
    expect(c2).toBe("conn");
  });

  test("withPool auto releases", async () => {
    pool = new Pool({
      min: 0, max: 3,
      factory: { create: () => "conn", destroy: () => {} },
    });
    await pool.withPool(async (c) => `got:${c}`);
    expect(pool.available).toBe(1);
  });

  test("limits concurrent connections", async () => {
    pool = new Pool({
      min: 0, max: 2,
      factory: { create: () => "conn", destroy: () => {} },
    });
    await pool.acquire();
    await pool.acquire();
    expect(pool.size).toBe(2);
    let flag = false;
    pool.acquire().then(() => { flag = true; });
    expect(flag).toBe(false);
  });

  test("pending count with waiters", async () => {
    pool = new Pool({
      min: 0, max: 1,
      factory: { create: () => "conn", destroy: () => {} },
    });
    await pool.acquire();
    pool.acquire();
    expect(pool.pending).toBe(1);
  });

  test("drain destroys all connections", async () => {
    let destroyed = 0;
    pool = new Pool({
      min: 2, max: 5,
      factory: { create: () => "conn", destroy: () => { destroyed++; } },
    });
    await Bun.sleep(10);
    await pool.drain();
    expect(destroyed).toBe(2);
  });

  test("acquire timeout rejects", async () => {
    pool = new Pool({
      min: 0, max: 1, acquireTimeoutMs: 50,
      factory: { create: () => "conn", destroy: () => {} },
    });
    await pool.acquire();
    await expect(pool.acquire(10)).rejects.toThrow();
  });

  test("throws on invalid options", () => {
    expect(() => new Pool({ min: -1, max: 5, factory: { create: () => "x", destroy: () => {} } })).toThrow();
    expect(() => new Pool({ min: 10, max: 5, factory: { create: () => "x", destroy: () => {} } })).toThrow();
  });
});
