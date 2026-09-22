import { mkdir, mkdtemp, readdir, readFile, rm, stat, utimes, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, test } from "bun:test";
import { CredentialPool, fingerprint, retryAfterMs } from "../src/pool.ts";
import { readPools, SerializedPools, writePools } from "../src/storage.ts";
import { createPooledStream } from "../extensions/pi-credential-pool.ts";
import credentialPoolExtension from "../extensions/pi-credential-pool.ts";
import { opencodeGoProvider } from "@earendil-works/pi-ai/providers/opencode-go";
import { createAssistantMessageEventStream } from "@earendil-works/pi-ai";
import { requestSmoke } from "../scripts/live-smoke.ts";

describe("CredentialPool", () => {
  test("rotates three credentials and rejects duplicates", () => {
    const pool = new CredentialPool(["one", "two", "three"]);
    expect([pool.select()?.key, pool.select()?.key, pool.select()?.key]).toEqual(["one", "two", "three"]);
    expect(() => new CredentialPool(["one", "one"])).toThrow("distinct");
  });

  test("uses stable session affinity and safe concurrent selection", () => {
    const pool = new CredentialPool(["one", "two", "three"]);
    expect(pool.select("stable")?.key).toBe(pool.select("stable")?.key);
    const selected = Array.from({ length: 30 }, () => pool.select()?.key);
    expect(new Set(selected)).toEqual(new Set(["one", "two", "three"]));
  });

  test("cools 429s, disables 401s, then falls back", () => {
    const pool = new CredentialPool(["one", "two"]);
    const first = pool.select()!;
    pool.fail(first, { status: 429, retryAfterMs: 1_000 }, 10);
    expect(pool.select(undefined, 11)?.key).toBe("two");
    const second = pool.select(undefined, 11)!;
    pool.fail(second, { status: 401 }, 11);
    expect(pool.select(undefined, 12)).toBeUndefined();
    expect(pool.select(undefined, 1_010)?.key).toBe("one");
  });

  test("exposes fingerprints and parses Retry-After without secrets", () => {
    const pool = new CredentialPool(["super-secret"]);
    expect(pool.entries()[0]).toMatchObject({ fingerprint: fingerprint("super-secret"), health: "ready" });
    expect(JSON.stringify(pool.entries())).not.toContain("super-secret");
    expect(retryAfterMs("2")).toBe(2_000);
  });
});

test("stores credentials atomically with 0600 permissions", async () => {
  const directory = await mkdtemp(join(tmpdir(), "pi-credential-pool-"));
  const path = join(directory, "credential-pools.json");
  await writePools({ version: 1, pools: { "opencode-go": ["secret"] } }, path);
  expect((await stat(path)).mode & 0o777).toBe(0o600);
  expect(await readPools(path)).toEqual({ version: 1, pools: { "opencode-go": ["secret"] } });
});

test("serializes mutations from independent pools", async () => {
  const directory = await mkdtemp(join(tmpdir(), "pi-credential-pool-"));
  const path = join(directory, "credential-pools.json");
  const firstPool = new SerializedPools();
  const secondPool = new SerializedPools();
  let startFirst!: () => void;
  let releaseFirst!: () => void;
  let startSecond!: () => void;
  const firstStarted = new Promise<void>((resolve) => { startFirst = resolve; });
  const firstReleased = new Promise<void>((resolve) => { releaseFirst = resolve; });
  const secondStarted = new Promise<void>((resolve) => { startSecond = resolve; });

  const first = firstPool.mutate(path, async (pools) => {
    startFirst();
    await firstReleased;
    pools.pools.first = ["first"];
  });
  await firstStarted;
  const second = secondPool.mutate(path, (pools) => {
    startSecond();
    pools.pools.second = ["second"];
  });

  expect(await Promise.race([secondStarted.then(() => true), Bun.sleep(25).then(() => false)])).toBe(false);
  releaseFirst();
  await Promise.all([first, second]);
  expect(await readPools(path)).toEqual({ version: 1, pools: { first: ["first"], second: ["second"] } });
});

test("leaves a replacement lock alone when a superseded owner releases late", async () => {
  const directory = await mkdtemp(join(tmpdir(), "pi-credential-pool-"));
  const path = join(directory, "credential-pools.json");
  const lockPath = `${path}.lock`;
  let startFirst!: () => void;
  let releaseFirst!: () => void;
  const firstStarted = new Promise<void>((resolve) => { startFirst = resolve; });
  const firstReleased = new Promise<void>((resolve) => { releaseFirst = resolve; });

  const first = new SerializedPools().mutate(path, async (pools) => {
    startFirst();
    await firstReleased;
    pools.pools.first = ["first"];
  });
  await firstStarted;
  await rm(lockPath, { recursive: true, force: true });
  await mkdir(lockPath, { mode: 0o700 });
  await writeFile(join(lockPath, "owner"), "replacement", { mode: 0o600 });
  releaseFirst();
  await first;

  expect(await readFile(join(lockPath, "owner"), "utf8")).toBe("replacement");
  expect(await readPools(path)).toEqual({ version: 1, pools: { first: ["first"] } });
  await rm(lockPath, { recursive: true, force: true });
});

test("never detaches the lock a waiter can grab while an owner is inside its critical section", async () => {
  const directory = await mkdtemp(join(tmpdir(), "pi-credential-pool-"));
  const path = join(directory, "credential-pools.json");
  const lockName = "credential-pools.json.lock";
  const lockPath = join(directory, lockName);
  const pools = new SerializedPools();
  let held = false;
  let collisions = 0;
  let sampling = true;

  const sample = async () => {
    while (sampling) {
      try {
        await mkdir(lockPath, { mode: 0o700 });
      } catch {
        await Bun.sleep(0);
        continue;
      }
      const stray = (await readdir(directory)).filter((entry) => entry.startsWith(lockName) && entry !== lockName);
      if (held && stray.length > 0) collisions += 1;
      await rm(lockPath, { recursive: true, force: true });
      await Bun.sleep(0);
    }
  };
  const samplers = Array.from({ length: 5 }, () => sample());

  for (let iteration = 0; iteration < 200; iteration += 1) {
    await pools.mutate(path, (stored) => {
      held = true;
      stored.pools.keys = [`key-${iteration}`];
    });
    held = false;
  }
  sampling = false;
  await Promise.all(samplers);

  expect(collisions).toBe(0);
  expect(await readdir(directory)).toEqual(["credential-pools.json"]);
  expect(await readPools(path)).toEqual({ version: 1, pools: { keys: ["key-199"] } });
}, 60_000);

test("waits for a paused live holder instead of taking its lock over", async () => {
  const directory = await mkdtemp(join(tmpdir(), "pi-credential-pool-"));
  const path = join(directory, "credential-pools.json");
  const lockPath = `${path}.lock`;
  let startHolder!: () => void;
  let releaseHolder!: () => void;
  const holderStarted = new Promise<void>((resolve) => { startHolder = resolve; });
  const holderReleased = new Promise<void>((resolve) => { releaseHolder = resolve; });

  const holder = new SerializedPools().mutate(path, async (pools) => {
    startHolder();
    await holderReleased;
    pools.pools.holder = ["holder"];
  });
  await holderStarted;
  const paused = new Date(Date.now() - 31_000);
  await utimes(join(lockPath, "owner"), paused, paused);
  await utimes(lockPath, paused, paused);

  let entered = false;
  const waiter = new SerializedPools().mutate(path, (pools) => {
    entered = true;
    pools.pools.waiter = ["waiter"];
  });
  await Bun.sleep(3_000);
  expect(entered).toBe(false);

  releaseHolder();
  await Promise.all([holder, waiter]);
  expect(await readPools(path)).toEqual({ version: 1, pools: { holder: ["holder"], waiter: ["waiter"] } });
}, 20_000);

test("recovers an ownerless lock a crashed holder left behind", async () => {
  const directory = await mkdtemp(join(tmpdir(), "pi-credential-pool-"));
  const path = join(directory, "credential-pools.json");
  const lockPath = `${path}.lock`;
  await mkdir(lockPath, { mode: 0o700 });
  const crashed = new Date(Date.now() - 31_000);
  await utimes(lockPath, crashed, crashed);

  await new SerializedPools().mutate(path, (pools) => { pools.pools.recovered = ["key"]; });
  expect(await readPools(path)).toEqual({ version: 1, pools: { recovered: ["key"] } });
}, 15_000);

test("waits out a holder that fell silent instead of timing out", async () => {
  const directory = await mkdtemp(join(tmpdir(), "pi-credential-pool-"));
  const path = join(directory, "credential-pools.json");
  const lockPath = `${path}.lock`;
  await mkdir(lockPath, { mode: 0o700 });
  await writeFile(join(lockPath, "owner"), "crashed", { mode: 0o600 });
  const silent = new Date(Date.now() - 23_000);
  await utimes(join(lockPath, "owner"), silent, silent);
  await utimes(lockPath, silent, silent);

  const started = Date.now();
  await new SerializedPools().mutate(path, (pools) => { pools.pools.recovered = ["key"]; });

  expect(Date.now() - started).toBeGreaterThan(5_000);
  expect(await readPools(path)).toEqual({ version: 1, pools: { recovered: ["key"] } });
}, 30_000);

test("paces the waiting recoverer instead of spinning on one core", async () => {
  const directory = await mkdtemp(join(tmpdir(), "pi-credential-pool-"));
  const path = join(directory, "credential-pools.json");
  const lockPath = `${path}.lock`;
  const recoveryPath = join(lockPath, "recovery");
  await mkdir(lockPath, { mode: 0o700 });
  await writeFile(join(lockPath, "owner"), "abandoned", { mode: 0o600 });
  await writeFile(recoveryPath, "other", { mode: 0o600 });
  const stale = new Date(Date.now() - 31_000);
  await utimes(join(lockPath, "owner"), stale, stale);
  await utimes(lockPath, stale, stale);

  const before = process.cpuUsage();
  const mutation = new SerializedPools().mutate(path, (pools) => { pools.pools.recovered = ["key"]; });
  await Bun.sleep(500);
  const spent = process.cpuUsage(before);
  await rm(lockPath, { recursive: true, force: true });
  await mutation;

  expect(spent.user + spent.system).toBeLessThan(300_000);
  expect(await readPools(path)).toEqual({ version: 1, pools: { recovered: ["key"] } });
}, 15_000);

test("serializes mutations across separate processes", async () => {
  const directory = await mkdtemp(join(tmpdir(), "pi-credential-pool-"));
  const path = join(directory, "credential-pools.json");
  const storage = join(import.meta.dir, "../src/storage.ts");
  const keys = Array.from({ length: 8 }, (_, index) => `key-${index}`);
  const children = keys.map((key) => Bun.spawn(["bun", "-e", `import { SerializedPools } from ${JSON.stringify(storage)};\nawait new SerializedPools().mutate(${JSON.stringify(path)}, async (pools) => { pools.pools.keys = [...(pools.pools.keys ?? []), ${JSON.stringify(key)}]; await Bun.sleep(25); });`], { stdout: "ignore", stderr: "pipe" }));
  const results = await Promise.all(children.map(async (child) => ({ code: await child.exited, stderr: await new Response(child.stderr).text() })));
  expect(results.filter((result) => result.code !== 0)).toEqual([]);
  expect((await readPools(path)).pools.keys?.slice().sort()).toEqual([...keys].sort());
});

test("smoke request sends Pi OpenCode headers with a unique session per credential", async () => {
  const requests: RequestInit[] = [];
  const expectedBody = { model: "deepseek-v4-flash", messages: [{ role: "user", content: "Reply with OK." }], max_tokens: 4 };
  const fetchMock = async (_input: string | URL | Request, init?: RequestInit) => {
    requests.push(init!);
    expect(init?.body).toBeDefined();
    expect(JSON.parse(init?.body as string)).toEqual(expectedBody);
    return new Response(null, { status: 204 });
  };
  await Promise.all([requestSmoke("first-key", fetchMock), requestSmoke("second-key", fetchMock)]);
  expect(requests).toHaveLength(2);
  const headers = requests.map((request) => new Headers(request.headers));
  expect(headers.map((value) => Object.fromEntries(value))).toEqual([
    expect.objectContaining({ authorization: "Bearer first-key", "content-type": "application/json", "x-opencode-client": "pi" }),
    expect.objectContaining({ authorization: "Bearer second-key", "content-type": "application/json", "x-opencode-client": "pi" }),
  ]);
  const sessions = headers.map((value) => value.get("x-opencode-session"));
  expect(sessions.every((value) => value && /^[0-9a-f-]{36}$/.test(value))).toBe(true);
  expect(new Set(sessions).size).toBe(2);
});

const model = opencodeGoProvider().getModels()[0]!;

function events(...items: unknown[]) {
  const stream = createAssistantMessageEventStream();
  for (const item of items) stream.push(item as never);
  return stream;
}

test("retries three distinct siblings before visible output and terminates", async () => {
  const pool = new CredentialPool(["one", "two", "three"]);
  const expected = pool.select("stable")!.key;
  const attempts: string[] = [];
  const output = createPooledStream(pool, () => "stable", model, (key) => {
    attempts.push(key);
    return attempts.length < 3 ? events({ type: "error", error: { errorMessage: "429 quota" } }) : events({ type: "done", message: { role: "assistant" } });
  });
  const received = [];
  for await (const event of output) received.push(event);
  expect(attempts).toHaveLength(3);
  expect(attempts[0]).toBe(expected);
  expect(new Set(attempts).size).toBe(3);
  expect(received).toHaveLength(1);
  expect(received[0]?.type).toBe("done");
});

test("does not replay after visible output and preserves its provider error", async () => {
  const pool = new CredentialPool(["one", "two"]);
  const attempts: string[] = [];
  const output = createPooledStream(pool, () => undefined, model, (key) => {
    attempts.push(key);
    return events({ type: "text_delta", delta: "partial" }, { type: "error", error: { errorMessage: "429 quota" } });
  });
  const received = [];
  for await (const event of output) received.push(event);
  expect(attempts).toEqual(["one"]);
  expect(received.map((event) => event.type)).toEqual(["text_delta", "error"]);
  expect((received[1] as { error: { errorMessage: string } }).error.errorMessage).toBe("429 quota");
});

test("bounds exhausted retries and closes with the final provider error", async () => {
  const pool = new CredentialPool(["one", "two", "three"]);
  const attempts: string[] = [];
  const output = createPooledStream(pool, () => undefined, model, (key) => {
    attempts.push(key);
    return events({ type: "error", error: { errorMessage: `429 quota ${key}` } });
  });
  const received = [];
  for await (const event of output) received.push(event);
  expect(new Set(attempts)).toEqual(new Set(["one", "two", "three"]));
  expect(received).toHaveLength(1);
  expect((received[0] as { error: { errorMessage: string } }).error.errorMessage).toBe(`429 quota ${attempts.at(-1)}`);
});

test("native sidecar auth exposes models and serializes immediate command mutations", async () => {
  const home = await mkdtemp(join(tmpdir(), "pi-credential-pool-home-"));
  const previousHome = process.env.HOME;
  process.env.HOME = home;
  const path = join(home, ".pi", "agent", "credential-pools.json");
  await writePools({ version: 1, pools: { "opencode-go": ["fake-sidecar-key"] } }, path);
  let provider: any;
  let command: any;
  let next = 0;
  const notifications: string[] = [];
  await credentialPoolExtension({
    on: () => undefined,
    registerProvider: (value: unknown) => { provider = value; },
    registerCommand: (_name: string, value: unknown) => { command = value; },
  } as any);
  try {
    expect(provider.getModels().length).toBeGreaterThan(0);
    expect(await provider.auth.apiKey.check({ signal: new AbortController().signal })).toMatchObject({ source: "credential-pool" });
    const context = { ui: { input: async () => `added-${next++}`, select: async () => undefined, notify: (message: string) => notifications.push(message) } };
    await Promise.all([command.handler("add", context), command.handler("add", context)]);
    expect((await readPools(path)).pools["opencode-go"]).toEqual(["fake-sidecar-key", "added-0", "added-1"]);
    await command.handler("reset", context);
    await command.handler("list", context);
    expect(notifications.join("\n")).not.toContain("fake-sidecar-key");
  } finally {
    if (previousHome === undefined) delete process.env.HOME;
    else process.env.HOME = previousHome;
  }
});
