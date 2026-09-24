import { mkdir, mkdtemp, readdir, readFile, rm, stat, utimes, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, test } from "bun:test";
import { CredentialPool, credentialIdentity, fingerprint, retryAfterMs, type HealthRecord } from "../src/pool.ts";
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

  test("keeps a 401 disabled state across a pool replacement", () => {
    const pool = new CredentialPool(["one", "two"]);
    const selected = pool.select()!;
    pool.replace(["one", "two"], {});
    expect(pool.fail(selected, { status: 401 }, 10)).toBe(true);
    expect(pool.entries(11).map((entry) => entry.health)).toEqual(["disabled", "ready"]);
    expect(pool.select(undefined, 11)?.key).toBe("two");
  });

  test("applies stored health across a pool replacement", () => {
    const pool = new CredentialPool(["one", "two"]);
    pool.fail(pool.select()!, { status: 429, retryAfterMs: 1_000 }, 10);
    pool.replace(["one", "two"], { [credentialIdentity("one")]: { state: "cooling", retryAt: 5_000 } });
    expect(pool.entries(11)[0]).toMatchObject({ health: "cooling", retryAt: 5_000 });
    expect(pool.select(undefined, 11)?.key).toBe("two");
    expect(pool.select(undefined, 5_000)?.key).toBe("one");
    pool.replace(["one", "two"], {});
    expect(pool.entries(11).map((entry) => entry.health)).toEqual(["ready", "ready"]);
  });

  test("applies stored health by identity when a replacement reorders the keys", () => {
    const pool = new CredentialPool(["one", "two"]);
    pool.replace(["two", "one"], { [credentialIdentity("one")]: { state: "cooling", retryAt: 1_010 } });
    expect(pool.entries(11)).toMatchObject([
      { fingerprint: fingerprint("two"), health: "ready" },
      { fingerprint: fingerprint("one"), health: "cooling", retryAt: 1_010 },
    ]);
    expect(pool.select(undefined, 11)?.key).toBe("two");
    expect(pool.entries(1_010).map((entry) => entry.health)).toEqual(["ready", "ready"]);
  });

  test("starts a fresh pool from stored cooling and disabled health", () => {
    const pool = new CredentialPool(["one", "two", "three"], {
      [credentialIdentity("one")]: { state: "cooling", retryAt: 5_000 },
      [credentialIdentity("two")]: { state: "disabled" },
    });
    expect(pool.entries(11)).toMatchObject([
      { fingerprint: fingerprint("one"), health: "cooling", retryAt: 5_000 },
      { fingerprint: fingerprint("two"), health: "disabled" },
      { fingerprint: fingerprint("three"), health: "ready" },
    ]);
    expect(pool.select(undefined, 11)?.key).toBe("three");
    expect(pool.select(undefined, 5_000)?.key).toBe("one");
  });

  test("cools a 429 without Retry-After for the default minute", () => {
    const pool = new CredentialPool(["one", "two"]);
    const selected = pool.select()!;
    pool.fail(selected, { status: 429 }, 10);
    expect(pool.entries(60_009)[0]?.health).toBe("cooling");
    expect(pool.select(undefined, 60_009)?.key).toBe("two");
    expect(pool.entries(60_010)[0]?.health).toBe("ready");
    expect(pool.select(undefined, 60_010)?.key).toBe("one");
  });

  test("cools a credential whose account cannot pay for an hour whatever Retry-After says", () => {
    const pool = new CredentialPool(["one", "two"]);
    expect(pool.fail(pool.select(undefined, 10)!, { status: 402, retryAfterMs: 1_000 }, 10)).toBe(true);
    expect(pool.fail(pool.select(undefined, 10)!, { insufficientFunds: true }, 10)).toBe(true);
    expect(pool.entries(3_600_009).map((entry) => entry.health)).toEqual(["cooling", "cooling"]);
    expect(pool.entries(3_600_010).map((entry) => entry.health)).toEqual(["ready", "ready"]);
  });

  test("clearing health through a replacement returns disabled and cooling credentials to ready", () => {
    const pool = new CredentialPool(["one", "two"]);
    const first = pool.select()!;
    pool.fail(first, { status: 401 }, 10);
    const second = pool.select(undefined, 11)!;
    pool.fail(second, { status: 429, retryAfterMs: 5_000 }, 11);
    expect(pool.entries(12).map((entry) => entry.health)).toEqual(["disabled", "cooling"]);
    pool.replace(["one", "two"], {});
    expect(pool.entries(12)).toMatchObject([{ health: "ready" }, { health: "ready" }]);
    expect(pool.entries(12).every((entry) => entry.retryAt === undefined)).toBe(true);
  });

  test("snapshots cooling and disabled health without an expired cooldown", () => {
    const pool = new CredentialPool(["one", "two"]);
    pool.fail(pool.select(undefined, 10)!, { status: 429, retryAfterMs: 1_000 }, 10);
    pool.fail(pool.select(undefined, 10)!, { status: 401 }, 10);
    expect(pool.healthRecord(11)).toEqual({
      [credentialIdentity("one")]: { state: "cooling", retryAt: 1_010 },
      [credentialIdentity("two")]: { state: "disabled" },
    });
    expect(pool.healthRecord(1_010)).toEqual({ [credentialIdentity("two")]: { state: "disabled" } });
  });

  test("publishes each health change to its listener", () => {
    const published: HealthRecord[] = [];
    const pool = new CredentialPool(["one", "two"], {}, (health) => published.push(health));
    pool.fail(pool.select(undefined, 10)!, { status: 429, retryAfterMs: 1_000 }, 10);
    pool.fail(pool.select(undefined, 10)!, { status: 401 }, 10);
    expect(published).toEqual([
      { [credentialIdentity("one")]: { state: "cooling", retryAt: 1_010 } },
      { [credentialIdentity("one")]: { state: "cooling", retryAt: 1_010 }, [credentialIdentity("two")]: { state: "disabled" } },
    ]);
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

test("reads a v1 store written before health was stored and writes health back as v1", async () => {
  const directory = await mkdtemp(join(tmpdir(), "pi-credential-pool-v1-"));
  const path = join(directory, "credential-pools.json");
  await writeFile(path, JSON.stringify({ version: 1, pools: { "opencode-go": ["legacy"] } }));
  expect(await readPools(path)).toEqual({ version: 1, pools: { "opencode-go": ["legacy"] } });
  await new SerializedPools().mutate(path, (stored) => { stored.health = { [credentialIdentity("legacy")]: { state: "disabled" } }; });
  const raw = JSON.parse(await readFile(path, "utf8"));
  expect(raw.version).toBe(1);
  expect(raw.pools["opencode-go"]).toEqual(["legacy"]);
  expect(raw.health).toEqual({ [credentialIdentity("legacy")]: { state: "disabled" } });
});

test("drops a malformed health map without losing keys", async () => {
  const directory = await mkdtemp(join(tmpdir(), "pi-credential-pool-health-shape-"));
  const path = join(directory, "credential-pools.json");
  await writeFile(path, JSON.stringify({ version: 1, pools: { "opencode-go": ["kept"] }, health: { [credentialIdentity("kept")]: { state: "melting" }, orphan: "nonsense" } }));
  expect(await readPools(path)).toEqual({ version: 1, pools: { "opencode-go": ["kept"] } });
});

test("keeps an empty health map out of the store", async () => {
  const directory = await mkdtemp(join(tmpdir(), "pi-credential-pool-health-empty-"));
  const path = join(directory, "credential-pools.json");
  await new SerializedPools().mutate(path, (stored) => { stored.health = { [credentialIdentity("one")]: { state: "disabled" } }; });
  await new SerializedPools().mutate(path, (stored) => { stored.health = {}; });
  expect(JSON.parse(await readFile(path, "utf8"))).toEqual({ version: 1, pools: {} });
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
  await expect(first).rejects.toThrow("Lost the credential pool storage lock");

  expect(await readFile(join(lockPath, "owner"), "utf8")).toBe("replacement");
  expect(await readPools(path)).toEqual({ version: 1, pools: {} });
  await rm(lockPath, { recursive: true, force: true });
});

test("fails the late write of a frozen holder whose lock was taken over", async () => {
  const directory = await mkdtemp(join(tmpdir(), "pi-credential-pool-"));
  const path = join(directory, "credential-pools.json");
  const lockPath = `${path}.lock`;
  const readyPath = join(directory, "held");
  const storage = join(import.meta.dir, "../src/storage.ts");
  const holder = Bun.spawn(["bun", "-e", `import { SerializedPools } from ${JSON.stringify(storage)};\nawait new SerializedPools().mutate(${JSON.stringify(path)}, async (pools) => { pools.pools.holder = ["holder"]; await Bun.write(${JSON.stringify(readyPath)}, "ready"); await Bun.sleep(500); });`], { stdout: "ignore", stderr: "pipe" });
  while (!await Bun.file(readyPath).exists()) await Bun.sleep(10);

  process.kill(holder.pid, "SIGSTOP");
  const frozen = new Date(Date.now() - 31_000);
  await utimes(join(lockPath, "owner"), frozen, frozen);
  await utimes(lockPath, frozen, frozen);
  await new SerializedPools().mutate(path, (pools) => { pools.pools.waiter = ["waiter"]; });
  process.kill(holder.pid, "SIGCONT");

  const code = await holder.exited;
  const stderr = await new Response(holder.stderr).text();
  expect(code).not.toBe(0);
  expect(stderr).toContain("Lost the credential pool storage lock");
  expect(await readPools(path)).toEqual({ version: 1, pools: { waiter: ["waiter"] } });
}, 20_000);

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

test("completes a recovery whose claim a rival keeps deleting underneath it", async () => {
  const directory = await mkdtemp(join(tmpdir(), "pi-credential-pool-"));
  const path = join(directory, "credential-pools.json");
  const lockPath = `${path}.lock`;
  const recoveryPath = join(lockPath, "recovery");
  const stale = new Date(Date.now() - 31_000);
  await mkdir(lockPath, { mode: 0o700 });
  await utimes(lockPath, stale, stale);

  let clearing = true;
  const rival = (async () => {
    while (clearing) {
      await rm(recoveryPath, { force: true });
      await mkdir(lockPath, { mode: 0o700 }).catch(() => undefined);
      await utimes(lockPath, stale, stale).catch(() => undefined);
    }
  })();
  const outcome = new SerializedPools().mutate(path, (pools) => { pools.pools.recovered = ["key"]; }).then(() => "resolved", (error: Error) => error.message);
  await Bun.sleep(750);
  clearing = false;
  await rival;

  expect(await outcome).toBe("resolved");
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

function throwingEvents(message: string, ...items: unknown[]) {
  return Object.assign(createAssistantMessageEventStream(), {
    async *[Symbol.asyncIterator]() {
      for (const item of items) yield item as never;
      throw new Error(message);
    },
  });
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

test("rotates on a provider message that says the account cannot pay", async () => {
  for (const message of ["Upstream request failed: Insufficient account funds", "Insufficient Balance", "402 Payment Required"]) {
    const pool = new CredentialPool(["one", "two"]);
    const attempts: string[] = [];
    const output = createPooledStream(pool, () => undefined, model, (key) => {
      attempts.push(key);
      return attempts.length === 1 ? events({ type: "error", error: { errorMessage: message } }) : events({ type: "done", message: { role: "assistant" } });
    });
    const received = [];
    for await (const event of output) received.push(event);
    expect(attempts).toEqual(["one", "two"]);
    expect(received.map((event) => event.type)).toEqual(["done"]);
    expect(pool.entries()[0]).toMatchObject({ health: "cooling", lastOutcome: "insufficient-funds" });
  }
});

test("still disables a 401 or 403 whose message also says the account cannot pay", async () => {
  for (const [message, outcome] of [["401 Insufficient Balance", "unauthorized"], ["403 Insufficient account funds", "forbidden"]]) {
    const pool = new CredentialPool(["one", "two"]);
    const output = createPooledStream(pool, () => undefined, model, (key) => (key === "one" ? events({ type: "error", error: { errorMessage: message } }) : events({ type: "done", message: { role: "assistant" } })));
    for await (const _event of output) { /* drain */ }
    expect(pool.entries()[0]).toMatchObject({ health: "disabled", lastOutcome: outcome });
  }
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

test("does not rotate once the provider has forwarded its start event", async () => {
  const pool = new CredentialPool(["one", "two"]);
  const attempts: string[] = [];
  const output = createPooledStream(pool, () => undefined, model, (key) => {
    attempts.push(key);
    return events({ type: "start", partial: { role: "assistant" } }, { type: "error", error: { errorMessage: "429 quota" } });
  });
  const received = [];
  for await (const event of output) received.push(event);
  expect(received.map((event) => event.type)).toEqual(["start", "error"]);
  expect(attempts).toEqual(["one"]);
});

test("does not rotate when a classifiable exception follows its forwarded start event", async () => {
  const pool = new CredentialPool(["one", "two"]);
  const attempts: string[] = [];
  const output = createPooledStream(pool, () => undefined, model, (key) => {
    attempts.push(key);
    return throwingEvents("429 quota exceeded", { type: "start", partial: { role: "assistant" } }, { type: "text_delta", delta: "partial" });
  });
  const received = [];
  for await (const event of output) received.push(event);
  expect(attempts).toEqual(["one"]);
  expect(received.map((event) => event.type)).toEqual(["start", "text_delta", "error"]);
  expect((received[2] as { error: { errorMessage: string } }).error.errorMessage).toBe("429 quota exceeded");
  expect(pool.entries()[0]).toMatchObject({ health: "ready", lastOutcome: "error" });
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

test("caps one turn at three upstream attempts regardless of pool size", async () => {
  const pool = new CredentialPool(["one", "two", "three", "four", "five"]);
  const attempts: string[] = [];
  const output = createPooledStream(pool, () => undefined, model, (key) => {
    attempts.push(key);
    return events({ type: "error", error: { errorMessage: `429 quota ${key}` } });
  });
  for await (const _event of output) { /* drain */ }
  expect(attempts).toHaveLength(3);
  expect(new Set(attempts).size).toBe(3);
});

test("classifies the upstream status and honors Retry-After before rotating", async () => {
  const pool = new CredentialPool(["one", "two"]);
  const attempts: string[] = [];
  const started = Date.now();
  const output = createPooledStream(pool, () => undefined, model, (key, fetch) => {
    attempts.push(key);
    const result = createAssistantMessageEventStream();
    void (async () => {
      const response = await fetch("https://opencode.ai/zen/go/v1/messages", { method: "POST" });
      result.push((response.status === 429 ? { type: "error", error: { errorMessage: "Too Many Requests" } } : { type: "done", message: { role: "assistant" } }) as never);
    })();
    return result;
  }, async () => new Response(null, { status: 429, headers: { "retry-after": "2" } }));
  for await (const _event of output) { /* drain */ }
  expect(attempts).toEqual(["one", "two"]);
  expect(pool.entries(started)[0]).toMatchObject({ health: "cooling", lastOutcome: "rate-limited" });
  expect(pool.entries(started + 1_500)[0]!.health).toBe("cooling");
  expect(pool.entries(started + 2_500)[0]!.health).toBe("ready");
});

test("classifies the failing response when the attempt keeps fetching afterwards", async () => {
  const pool = new CredentialPool(["one", "two"]);
  const attempts: string[] = [];
  const started = Date.now();
  const output = createPooledStream(pool, () => undefined, model, (key, fetch) => {
    attempts.push(key);
    const result = createAssistantMessageEventStream();
    void (async () => {
      const response = await fetch("https://opencode.ai/zen/go/v1/messages", { method: "POST" });
      if (response.status === 429) await fetch("https://opencode.ai/zen/go/v1/models");
      result.push((response.status === 429 ? { type: "error", error: { errorMessage: "Too Many Requests" } } : { type: "done", message: { role: "assistant" } }) as never);
    })();
    return result;
  }, async (input) => (attempts.length === 1 && String(input).endsWith("/messages") ? new Response(null, { status: 429, headers: { "retry-after": "30" } }) : new Response(null, { status: 200 })));
  for await (const _event of output) { /* drain */ }
  expect(attempts).toEqual(["one", "two"]);
  expect(pool.entries(started)[0]).toMatchObject({ health: "cooling", lastOutcome: "rate-limited" });
  expect(pool.entries(started + 29_000)[0]!.health).toBe("cooling");
  expect(pool.entries(started + 31_000)[0]!.health).toBe("ready");
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

async function loadExtension() {
	type Handler = (event: unknown, ctx: unknown) => void | Promise<void>;
	const handlers = new Map<string, Handler[]>();
	let command: { handler: (args: string, ctx: unknown) => Promise<void> } | undefined;
	const providers: unknown[] = [];
	await credentialPoolExtension({
		on: (name: string, handler: Handler) => handlers.set(name, [...(handlers.get(name) ?? []), handler]),
		registerProvider: (value: unknown) => providers.push(value),
		unregisterProvider: () => undefined,
		registerCommand: (_name: string, value: unknown) => { command = value as typeof command; },
	} as any);
	return { handlers, command: command!, provider: providers.at(-1)! };
}

async function routedKeys(provider: any): Promise<string[]> {
	const model = opencodeGoProvider().getModels()[0]!;
	const used: string[] = [];
	const output = provider.streamSimple(model, { messages: [{ role: "user", content: "hi" }] }, {
		fetch: async (_input: string | URL | Request, init?: RequestInit) => {
			const headers = new Headers(init?.headers);
			used.push(headers.get("x-api-key") ?? (headers.get("authorization") ?? "").replace(/^Bearer /, ""));
			return new Response(null, { status: 401 });
		},
	});
	for await (const _event of output) { /* drain */ }
	return used;
}

async function expectPoolMirrorsStore(provider: any, path: string): Promise<void> {
	const stored = (await readPools(path)).pools["opencode-go"] ?? [];
	const routed = [...await routedKeys(provider)].sort();
	expect(routed).toEqual([...stored].sort());
	const identities = routed.map(credentialIdentity);
	const states = async () => {
		const health = (await readPools(path)).health ?? {};
		return identities.map((identity) => health[identity]?.state);
	};
	const deadline = Date.now() + 2_000;
	while ((await states()).some((state) => state !== "disabled") && Date.now() < deadline) await Bun.sleep(10);
	expect(await states()).toEqual(identities.map(() => "disabled"));
}

async function listing(session: Awaited<ReturnType<typeof loadExtension>>): Promise<string> {
	const notifications: string[] = [];
	await session.command.handler("list", { ui: { notify: (message: string) => notifications.push(message), input: async () => undefined, select: async () => undefined } });
	return notifications.join("\n");
}

async function rateLimited(provider: any, retryAfter = "300"): Promise<void> {
	const model = opencodeGoProvider().getModels()[0]!;
	const output = provider.streamSimple(model, { messages: [{ role: "user", content: "hi" }] }, {
		fetch: async () => new Response(null, { status: 429, headers: { "retry-after": retryAfter } }),
	});
	for await (const _event of output) { /* drain */ }
}

async function waitForHealth(path: string, matches: (health: HealthRecord) => boolean): Promise<HealthRecord> {
	const deadline = Date.now() + 2_000;
	let health = (await readPools(path)).health ?? {};
	while (!matches(health) && Date.now() < deadline) {
		await Bun.sleep(10);
		health = (await readPools(path)).health ?? {};
	}
	return health;
}

test("re-reads the store on turn start so a key removed elsewhere stops routing", async () => {
	const home = await mkdtemp(join(tmpdir(), "pi-credential-pool-sync-"));
	const previousHome = process.env.HOME;
	process.env.HOME = home;
	const path = join(home, ".pi", "agent", "credential-pools.json");
	const poolKeys = ["sync-key-one", "sync-key-two", "sync-key-three"];
	await writePools({ version: 1, pools: { "opencode-go": poolKeys } }, path);
	try {
		const first = await loadExtension();
		const stale = await loadExtension();
		const second = await loadExtension();
		const choices = poolKeys.map((key) => `${fingerprint(key)} (${credentialIdentity(key).slice(-8)})`);
		await first.command.handler("remove", { ui: { select: async () => choices[1], input: async () => undefined, notify: () => undefined } });
		expect((await readPools(path)).pools["opencode-go"]).toEqual([poolKeys[0], poolKeys[2]]);
		expect(await routedKeys(stale.provider)).toContain(poolKeys[1]);
		for (const handler of second.handlers.get("turn_start") ?? []) await handler({ type: "turn_start", turnIndex: 0, timestamp: 0 }, {});
		const used = await routedKeys(second.provider);
		expect(used).not.toContain(poolKeys[1]);
		expect(used).toEqual([poolKeys[0], poolKeys[2]]);
	} finally {
		if (previousHome === undefined) delete process.env.HOME;
		else process.env.HOME = previousHome;
	}
});

test("a v1 store written before health was stored still loads and routes", async () => {
	const home = await mkdtemp(join(tmpdir(), "pi-credential-pool-v1-load-"));
	const previousHome = process.env.HOME;
	process.env.HOME = home;
	const directory = join(home, ".pi", "agent");
	const path = join(directory, "credential-pools.json");
	await mkdir(directory, { recursive: true });
	await writeFile(path, JSON.stringify({ version: 1, pools: { "opencode-go": ["legacy-key"] } }));
	try {
		const session = await loadExtension();
		expect(await routedKeys(session.provider)).toEqual(["legacy-key"]);
		const health = await waitForHealth(path, (value) => Object.keys(value).length === 1);
		expect(Object.values(health).map((entry) => entry.state)).toEqual(["disabled"]);
		const raw = JSON.parse(await readFile(path, "utf8"));
		expect(raw.version).toBe(1);
		expect(raw.pools["opencode-go"]).toEqual(["legacy-key"]);
	} finally {
		if (previousHome === undefined) delete process.env.HOME;
		else process.env.HOME = previousHome;
	}
});

test("persists a cooldown to the store when a request is rate limited", async () => {
	const home = await mkdtemp(join(tmpdir(), "pi-credential-pool-flush-"));
	const previousHome = process.env.HOME;
	process.env.HOME = home;
	const path = join(home, ".pi", "agent", "credential-pools.json");
	const poolKeys = ["flush-key"];
	await writePools({ version: 1, pools: { "opencode-go": poolKeys } }, path);
	try {
		const session = await loadExtension();
		await rateLimited(session.provider);
		const health = await waitForHealth(path, (value) => Object.keys(value).length === poolKeys.length);
		expect(health).toEqual({ [credentialIdentity(poolKeys[0]!)]: { state: "cooling", retryAt: expect.any(Number) } });
		expect((await readPools(path)).pools["opencode-go"]).toEqual(poolKeys);
		expect(JSON.parse(await readFile(path, "utf8")).version).toBe(1);
	} finally {
		if (previousHome === undefined) delete process.env.HOME;
		else process.env.HOME = previousHome;
	}
});

function successSse(): string {
	return [
		{ type: "message_start", message: { id: "flush", type: "message", role: "assistant", model: model.id, content: [], stop_reason: null, stop_sequence: null, usage: { input_tokens: 1, output_tokens: 0 } } },
		{ type: "content_block_start", index: 0, content_block: { type: "text", text: "" } },
		{ type: "content_block_delta", index: 0, delta: { type: "text_delta", text: "ok" } },
		{ type: "content_block_stop", index: 0 },
		{ type: "message_delta", delta: { stop_reason: "end_turn", stop_sequence: null }, usage: { output_tokens: 1 } },
		{ type: "message_stop" },
	].map((event) => `event: ${event.type}\ndata: ${JSON.stringify(event)}\n\n`).join("");
}

// The session, not the test runner, has to survive the rejected flush, so the run
// happens in its own process and the exit code is what proves it stayed alive.
test("a health flush the store rejects keeps serving the request and outlives it", async () => {
	const home = await mkdtemp(join(tmpdir(), "pi-credential-pool-flush-reject-"));
	const path = join(home, ".pi", "agent", "credential-pools.json");
	await writePools({ version: 1, pools: { "opencode-go": ["reject-key-one", "reject-key-two"] } }, path);
	const session = join(home, "session.ts");
	await Bun.write(session, [
		`import credentialPoolExtension from ${JSON.stringify(join(import.meta.dir, "../extensions/pi-credential-pool.ts"))};`,
		"let provider; let command;",
		"await credentialPoolExtension({ on: () => undefined, registerProvider: (value) => { provider = value; }, unregisterProvider: () => undefined, registerCommand: (_name, value) => { command = value; } });",
		"await Bun.write(process.env.STORE, '{ truncated');",
		"let attempts = 0;",
		"const output = provider.streamSimple(JSON.parse(process.env.MODEL), { messages: [{ role: 'user', content: 'hi' }] }, { fetch: async () => (attempts++ === 0 ? new Response(null, { status: 429, headers: { 'retry-after': '300' } }) : new Response(process.env.SSE, { status: 200, headers: { 'content-type': 'text/event-stream' } })) });",
		"const received = []; for await (const event of output) received.push(event);",
		"const ui = { ui: { input: async () => undefined, select: async () => undefined, notify: () => undefined } };",
		"await command.handler('reset', ui).catch(() => undefined);",
		"console.log(received.at(-1)?.type, attempts);",
	].join("\n"));
	const child = Bun.spawn(["bun", session], { env: { ...process.env, HOME: home, STORE: path, MODEL: JSON.stringify(model), SSE: successSse() }, stdout: "pipe", stderr: "pipe" });
	const [code, stdout, stderr] = await Promise.all([child.exited, new Response(child.stdout).text(), new Response(child.stderr).text()]);
	expect([code, stderr.trim()]).toEqual([0, ""]);
	expect(stdout.trim()).toBe("done 2");
});

test("a pool started against a store with health adopts it", async () => {
	const home = await mkdtemp(join(tmpdir(), "pi-credential-pool-store-health-"));
	const previousHome = process.env.HOME;
	process.env.HOME = home;
	const path = join(home, ".pi", "agent", "credential-pools.json");
	const poolKeys = ["cool-key", "dead-key", "live-key"];
	await writePools({ version: 1, pools: { "opencode-go": poolKeys }, health: { [credentialIdentity(poolKeys[0]!)]: { state: "cooling", retryAt: Date.now() + 300_000 }, [credentialIdentity(poolKeys[1]!)]: { state: "disabled" } } }, path);
	try {
		const session = await loadExtension();
		expect(await routedKeys(session.provider)).toEqual([poolKeys[2]]);
	} finally {
		if (previousHome === undefined) delete process.env.HOME;
		else process.env.HOME = previousHome;
	}
});

test("two sessions over one store share a cooldown", async () => {
	const home = await mkdtemp(join(tmpdir(), "pi-credential-pool-shared-"));
	const previousHome = process.env.HOME;
	process.env.HOME = home;
	const path = join(home, ".pi", "agent", "credential-pools.json");
	const poolKeys = ["shared-key-one", "shared-key-two"];
	await writePools({ version: 1, pools: { "opencode-go": poolKeys } }, path);
	try {
		const first = await loadExtension();
		const second = await loadExtension();
		await rateLimited(first.provider);
		const health = await waitForHealth(path, (value) => Object.keys(value).length === poolKeys.length);
		expect(Object.keys(health).sort()).toEqual(poolKeys.map(credentialIdentity).sort());
		expect(await listing(second)).toContain("cooling");
		expect(await routedKeys(second.provider)).toEqual([]);
		expect(await listing(first)).toContain("cooling");
	} finally {
		if (previousHome === undefined) delete process.env.HOME;
		else process.env.HOME = previousHome;
	}
});

const unpaidBody = JSON.stringify({ error: { type: "server_error", message: "Upstream request failed: Insufficient account funds" } });

function completionSse(): string {
	const chunk = (delta: { role?: "assistant"; content?: string }, finishReason: "stop" | null) => ({ id: "paid", object: "chat.completion.chunk", created: 0, model: "deepseek-v4-flash", choices: [{ index: 0, delta, finish_reason: finishReason }] });
	return [chunk({ role: "assistant", content: "ok" }, null), chunk({}, "stop")].map((value) => `data: ${JSON.stringify(value)}\n\n`).join("") + "data: [DONE]\n\n";
}

async function startSession(session: Awaited<ReturnType<typeof loadExtension>>, sessionId: string): Promise<void> {
	for (const handler of session.handlers.get("session_start") ?? []) await handler({ type: "session_start" }, { sessionManager: { getSessionId: () => sessionId }, modelRegistry: { getProvider: () => undefined } });
}

async function unpaidRequest(provider: any, unpaidKey: string) {
	const deepseek = opencodeGoProvider().getModels().find((entry) => entry.id === "deepseek-v4-flash")!;
	const used: string[] = [];
	const output = provider.streamSimple(deepseek, { messages: [{ role: "user", content: "hi" }] }, {
		fetch: async (_input: string | URL | Request, init?: RequestInit) => {
			const key = new Headers(init?.headers).get("authorization")!.replace(/^Bearer /, "");
			used.push(key);
			return key === unpaidKey
				? new Response(unpaidBody, { status: 402, headers: { "content-type": "application/json" } })
				: new Response(completionSse(), { status: 200, headers: { "content-type": "text/event-stream" } });
		},
	});
	const received: { type: string; error?: { errorMessage?: string } }[] = [];
	for await (const event of output) received.push(event);
	return { used, received };
}

test("rotates a request away from a key whose account cannot pay", async () => {
	const home = await mkdtemp(join(tmpdir(), "pi-credential-pool-unpaid-"));
	const previousHome = process.env.HOME;
	process.env.HOME = home;
	const path = join(home, ".pi", "agent", "credential-pools.json");
	const [unpaid, paid] = ["unpaid-key", "paid-key"];
	await writePools({ version: 1, pools: { "opencode-go": [unpaid, paid] } }, path);
	try {
		expect(new CredentialPool([unpaid, paid]).select("first-session")?.key).toBe(unpaid);
		const session = await loadExtension();
		await startSession(session, "first-session");
		const { used, received } = await unpaidRequest(session.provider, unpaid);
		expect(used).toEqual([unpaid, paid]);
		expect(received.map((event) => event.type)).not.toContain("error");
		expect(received.at(-1)?.type).toBe("done");
		expect(await listing(session)).toMatch(new RegExp(`${fingerprint(unpaid)} cooling attempts=1 last-used=\\S+ outcome=insufficient-funds`));
	} finally {
		if (previousHome === undefined) delete process.env.HOME;
		else process.env.HOME = previousHome;
	}
});

test("keeps later requests from other sessions and processes off a key whose account cannot pay", async () => {
	const home = await mkdtemp(join(tmpdir(), "pi-credential-pool-unpaid-later-"));
	const previousHome = process.env.HOME;
	process.env.HOME = home;
	const path = join(home, ".pi", "agent", "credential-pools.json");
	const [unpaid, paid] = ["unpaid-key", "paid-key"];
	await writePools({ version: 1, pools: { "opencode-go": [unpaid, paid] } }, path);
	try {
		expect(new CredentialPool([unpaid, paid]).select("second-session")?.key).toBe(unpaid);
		const session = await loadExtension();
		await startSession(session, "first-session");
		const before = Date.now();
		await unpaidRequest(session.provider, unpaid);
		const after = Date.now();
		await startSession(session, "second-session");
		expect((await unpaidRequest(session.provider, unpaid)).used).toEqual([paid]);

		const health = await waitForHealth(path, (value) => credentialIdentity(unpaid) in value);
		expect(health[credentialIdentity(unpaid)]).toEqual({ state: "cooling", retryAt: expect.any(Number) });
		expect(health[credentialIdentity(unpaid)]!.retryAt).toBeGreaterThanOrEqual(before + 3_600_000);
		expect(health[credentialIdentity(unpaid)]!.retryAt).toBeLessThanOrEqual(after + 3_600_000);
		const other = await loadExtension();
		await startSession(other, "second-session");
		expect((await unpaidRequest(other.provider, unpaid)).used).toEqual([paid]);
	} finally {
		if (previousHome === undefined) delete process.env.HOME;
		else process.env.HOME = previousHome;
	}
});

test("forwards the provider's insufficient funds error when no other credential can pay", async () => {
	const home = await mkdtemp(join(tmpdir(), "pi-credential-pool-unpaid-only-"));
	const previousHome = process.env.HOME;
	process.env.HOME = home;
	const path = join(home, ".pi", "agent", "credential-pools.json");
	const unpaid = "unpaid-key";
	await writePools({ version: 1, pools: { "opencode-go": [unpaid] } }, path);
	try {
		const session = await loadExtension();
		const { used, received } = await unpaidRequest(session.provider, unpaid);
		expect(used).toEqual([unpaid]);
		expect(received.map((event) => event.type)).toEqual(["error"]);
		expect(received[0]?.error?.errorMessage).toBe('402: {"type":"server_error","message":"Upstream request failed: Insufficient account funds"}');
		expect(await listing(session)).toContain(`${fingerprint(unpaid)} cooling`);
	} finally {
		if (previousHome === undefined) delete process.env.HOME;
		else process.env.HOME = previousHome;
	}
});

test("reset clears the shared health every session sees", async () => {
	const home = await mkdtemp(join(tmpdir(), "pi-credential-pool-reset-"));
	const previousHome = process.env.HOME;
	process.env.HOME = home;
	const path = join(home, ".pi", "agent", "credential-pools.json");
	const poolKeys = ["reset-key-one", "reset-key-two"];
	await writePools({ version: 1, pools: { "opencode-go": poolKeys } }, path);
	try {
		const first = await loadExtension();
		await rateLimited(first.provider);
		await waitForHealth(path, (value) => Object.keys(value).length === poolKeys.length);
		const second = await loadExtension();
		await second.command.handler("reset", { ui: { input: async () => undefined, select: async () => undefined, notify: () => undefined } });
		expect((await readPools(path)).health).toBeUndefined();
		expect(await listing(first)).not.toContain("cooling");
		expect(await listing(await loadExtension())).not.toContain("cooling");
		expect([...await routedKeys(first.provider)].sort()).toEqual([...poolKeys].sort());
	} finally {
		if (previousHome === undefined) delete process.env.HOME;
		else process.env.HOME = previousHome;
	}
});

test("a re-added credential does not inherit the health of the removed one", async () => {
	const home = await mkdtemp(join(tmpdir(), "pi-credential-pool-readd-"));
	const previousHome = process.env.HOME;
	process.env.HOME = home;
	const path = join(home, ".pi", "agent", "credential-pools.json");
	const key = "dead-key";
	await writePools({ version: 1, pools: { "opencode-go": [key] }, health: { [credentialIdentity(key)]: { state: "disabled" } } }, path);
	try {
		const session = await loadExtension();
		const choice = `${fingerprint(key)} (${credentialIdentity(key).slice(-8)})`;
		await session.command.handler("remove", { ui: { select: async () => choice, input: async () => undefined, notify: () => undefined } });
		expect((await readPools(path)).health).toBeUndefined();
		await session.command.handler("add", { ui: { input: async () => key, select: async () => undefined, notify: () => undefined } });
		expect(await routedKeys(session.provider)).toEqual([key]);
	} finally {
		if (previousHome === undefined) delete process.env.HOME;
		else process.env.HOME = previousHome;
	}
});

test("keeps the in-memory pool unchanged when the store write fails", async () => {
	const home = await mkdtemp(join(tmpdir(), "pi-credential-pool-write-"));
	const previousHome = process.env.HOME;
	process.env.HOME = home;
	const directory = join(home, ".pi", "agent");
	const path = join(directory, "credential-pools.json");
	await writePools({ version: 1, pools: { "opencode-go": ["kept-key"] } }, path);
	try {
		const session = await loadExtension();
		const frozenNow = 1_900_000_000_000;
		await mkdir(join(directory, `credential-pools.json.${process.pid}.${frozenNow}.tmp`));
		const realNow = Date.now;
		Date.now = () => frozenNow;
		let failure: unknown;
		try {
			await session.command.handler("add", { ui: { input: async () => "unwritten-key", select: async () => undefined, notify: () => undefined } }).catch((error) => { failure = error; });
		} finally {
			Date.now = realNow;
		}
		expect(failure).toBeInstanceOf(Error);
		expect((await readPools(path)).pools["opencode-go"]).toEqual(["kept-key"]);
		const notifications: string[] = [];
		await session.command.handler("list", { ui: { notify: (message: string) => notifications.push(message), input: async () => undefined, select: async () => undefined } });
		const listing = notifications.join("\n");
		expect(listing).toContain(fingerprint("kept-key"));
		expect(listing).not.toContain(fingerprint("unwritten-key"));
		await expectPoolMirrorsStore(session.provider, path);
	} finally {
		if (previousHome === undefined) delete process.env.HOME;
		else process.env.HOME = previousHome;
	}
});

test("refuses a whitespace-only key without touching the store", async () => {
	const home = await mkdtemp(join(tmpdir(), "pi-credential-pool-blank-"));
	const previousHome = process.env.HOME;
	process.env.HOME = home;
	const path = join(home, ".pi", "agent", "credential-pools.json");
	await writePools({ version: 1, pools: { "opencode-go": ["seed-key"] } }, path);
	try {
		const session = await loadExtension();
		let failure: unknown;
		await session.command.handler("add", { ui: { input: async () => "   ", select: async () => undefined, notify: () => undefined } }).catch((error) => { failure = error; });
		expect(failure).toBeInstanceOf(Error);
		expect((await readPools(path)).pools["opencode-go"]).toEqual(["seed-key"]);
		await expect(loadExtension()).resolves.toBeDefined();
	} finally {
		if (previousHome === undefined) delete process.env.HOME;
		else process.env.HOME = previousHome;
	}
});

test("re-reads the store at the command boundary with no turn start in between", async () => {
	const home = await mkdtemp(join(tmpdir(), "pi-credential-pool-command-sync-"));
	const previousHome = process.env.HOME;
	process.env.HOME = home;
	const path = join(home, ".pi", "agent", "credential-pools.json");
	const poolKeys = ["command-key-one", "command-key-two"];
	await writePools({ version: 1, pools: { "opencode-go": poolKeys } }, path);
	try {
		const reader = await loadExtension();
		const mutator = await loadExtension();
		const choices = poolKeys.map((key) => `${fingerprint(key)} (${credentialIdentity(key).slice(-8)})`);
		await mutator.command.handler("remove", { ui: { select: async () => choices[1], input: async () => undefined, notify: () => undefined } });
		const notifications: string[] = [];
		await reader.command.handler("list", { ui: { notify: (message: string) => notifications.push(message), input: async () => undefined, select: async () => undefined } });
		const listing = notifications.join("\n");
		expect(listing).toContain(fingerprint(poolKeys[0]!));
		expect(listing).not.toContain(fingerprint(poolKeys[1]!));
	} finally {
		if (previousHome === undefined) delete process.env.HOME;
		else process.env.HOME = previousHome;
	}
});

test("refuses a duplicate key without poisoning the store", async () => {
	const home = await mkdtemp(join(tmpdir(), "pi-credential-pool-duplicate-"));
	const previousHome = process.env.HOME;
	process.env.HOME = home;
	const path = join(home, ".pi", "agent", "credential-pools.json");
	await writePools({ version: 1, pools: { "opencode-go": ["seed-key"] } }, path);
	try {
		const session = await loadExtension();
		let failure: unknown;
		await session.command.handler("add", { ui: { input: async () => "seed-key", select: async () => undefined, notify: () => undefined } }).catch((error) => { failure = error; });
		expect(failure).toBeInstanceOf(Error);
		expect((await readPools(path)).pools["opencode-go"]).toEqual(["seed-key"]);
		await expect(loadExtension()).resolves.toBeDefined();
	} finally {
		if (previousHome === undefined) delete process.env.HOME;
		else process.env.HOME = previousHome;
	}
});

test("adds onto the store as another session left it while the prompt was open", async () => {
	const home = await mkdtemp(join(tmpdir(), "pi-credential-pool-prompt-race-"));
	const previousHome = process.env.HOME;
	process.env.HOME = home;
	const path = join(home, ".pi", "agent", "credential-pools.json");
	await writePools({ version: 1, pools: { "opencode-go": ["seed-key"] } }, path);
	try {
		const session = await loadExtension();
		const input = async () => {
			await writePools({ version: 1, pools: { "opencode-go": ["seed-key", "other-session-key"] } }, path);
			return "added-key";
		};
		await session.command.handler("add", { ui: { input, select: async () => undefined, notify: () => undefined } });
		expect((await readPools(path)).pools["opencode-go"]).toEqual(["seed-key", "other-session-key", "added-key"]);
		await expectPoolMirrorsStore(session.provider, path);
	} finally {
		if (previousHome === undefined) delete process.env.HOME;
		else process.env.HOME = previousHome;
	}
});

test("treats a cancelled add prompt as a no-op", async () => {
	const home = await mkdtemp(join(tmpdir(), "pi-credential-pool-cancel-"));
	const previousHome = process.env.HOME;
	process.env.HOME = home;
	const path = join(home, ".pi", "agent", "credential-pools.json");
	await writePools({ version: 1, pools: { "opencode-go": ["seed-key"] } }, path);
	try {
		const session = await loadExtension();
		const notifications: string[] = [];
		await session.command.handler("add", { ui: { input: async () => undefined, select: async () => undefined, notify: (message: string) => notifications.push(message) } });
		expect(notifications).toEqual([]);
		expect((await readPools(path)).pools["opencode-go"]).toEqual(["seed-key"]);
	} finally {
		if (previousHome === undefined) delete process.env.HOME;
		else process.env.HOME = previousHome;
	}
});

test("keeps the in-memory pool unchanged when the remove write fails", async () => {
	const home = await mkdtemp(join(tmpdir(), "pi-credential-pool-remove-write-"));
	const previousHome = process.env.HOME;
	process.env.HOME = home;
	const directory = join(home, ".pi", "agent");
	const path = join(directory, "credential-pools.json");
	const poolKeys = ["kept-one", "doomed-two"];
	await writePools({ version: 1, pools: { "opencode-go": poolKeys } }, path);
	try {
		const session = await loadExtension();
		const choices = poolKeys.map((key) => `${fingerprint(key)} (${credentialIdentity(key).slice(-8)})`);
		const frozenNow = 1_900_000_000_000;
		await mkdir(join(directory, `credential-pools.json.${process.pid}.${frozenNow}.tmp`));
		const realNow = Date.now;
		Date.now = () => frozenNow;
		let failure: unknown;
		try {
			await session.command.handler("remove", { ui: { select: async () => choices[1], input: async () => undefined, notify: () => undefined } }).catch((error) => { failure = error; });
		} finally {
			Date.now = realNow;
		}
		expect(failure).toBeInstanceOf(Error);
		expect((await readPools(path)).pools["opencode-go"]).toEqual(poolKeys);
		const notifications: string[] = [];
		await session.command.handler("list", { ui: { notify: (message: string) => notifications.push(message), input: async () => undefined, select: async () => undefined } });
		const listing = notifications.join("\n");
		expect(listing).toContain(fingerprint(poolKeys[0]!));
		expect(listing).toContain(fingerprint(poolKeys[1]!));
		await expectPoolMirrorsStore(session.provider, path);
	} finally {
		if (previousHome === undefined) delete process.env.HOME;
		else process.env.HOME = previousHome;
	}
});

test("removes from the store as another session left it while the prompt was open", async () => {
	const home = await mkdtemp(join(tmpdir(), "pi-credential-pool-remove-race-"));
	const previousHome = process.env.HOME;
	process.env.HOME = home;
	const path = join(home, ".pi", "agent", "credential-pools.json");
	const poolKeys = ["race-kept-one", "race-doomed-two"];
	await writePools({ version: 1, pools: { "opencode-go": poolKeys } }, path);
	try {
		const session = await loadExtension();
		const choices = poolKeys.map((key) => `${fingerprint(key)} (${credentialIdentity(key).slice(-8)})`);
		const select = async () => {
			await writePools({ version: 1, pools: { "opencode-go": [...poolKeys, "other-session-key"] } }, path);
			return choices[1];
		};
		await session.command.handler("remove", { ui: { select, input: async () => undefined, notify: () => undefined } });
		expect((await readPools(path)).pools["opencode-go"]).toEqual(["race-kept-one", "other-session-key"]);
		await expectPoolMirrorsStore(session.provider, path);
	} finally {
		if (previousHome === undefined) delete process.env.HOME;
		else process.env.HOME = previousHome;
	}
});

test("adds onto an empty store and routes the credential it just persisted", async () => {
	const home = await mkdtemp(join(tmpdir(), "pi-credential-pool-empty-add-"));
	const previousHome = process.env.HOME;
	process.env.HOME = home;
	const path = join(home, ".pi", "agent", "credential-pools.json");
	try {
		const session = await loadExtension();
		await session.command.handler("add", { ui: { input: async () => "first-key", select: async () => undefined, notify: () => undefined } });
		expect((await readPools(path)).pools["opencode-go"]).toEqual(["first-key"]);
		await expectPoolMirrorsStore(session.provider, path);
	} finally {
		if (previousHome === undefined) delete process.env.HOME;
		else process.env.HOME = previousHome;
	}
});

test("stops routing entirely once the last credential is removed", async () => {
	const home = await mkdtemp(join(tmpdir(), "pi-credential-pool-empty-remove-"));
	const previousHome = process.env.HOME;
	process.env.HOME = home;
	const path = join(home, ".pi", "agent", "credential-pools.json");
	await writePools({ version: 1, pools: { "opencode-go": ["only-key"] } }, path);
	try {
		const session = await loadExtension();
		const choice = `${fingerprint("only-key")} (${credentialIdentity("only-key").slice(-8)})`;
		await session.command.handler("remove", { ui: { select: async () => choice, input: async () => undefined, notify: () => undefined } });
		expect((await readPools(path)).pools["opencode-go"]).toEqual([]);
		await expectPoolMirrorsStore(session.provider, path);
	} finally {
		if (previousHome === undefined) delete process.env.HOME;
		else process.env.HOME = previousHome;
	}
});

test("adopts neither side of a raced add whose store write failed", async () => {
	const home = await mkdtemp(join(tmpdir(), "pi-credential-pool-raced-add-fail-"));
	const previousHome = process.env.HOME;
	process.env.HOME = home;
	const directory = join(home, ".pi", "agent");
	const path = join(directory, "credential-pools.json");
	await writePools({ version: 1, pools: { "opencode-go": ["kept-key"] } }, path);
	const realNow = Date.now;
	try {
		const session = await loadExtension();
		const frozenNow = 1_900_000_000_000;
		await mkdir(join(directory, `credential-pools.json.${process.pid}.${frozenNow}.tmp`));
		const input = async () => {
			await writePools({ version: 1, pools: { "opencode-go": ["kept-key", "other-session-key"] } }, path);
			Date.now = () => frozenNow;
			return "unwritten-key";
		};
		let failure: unknown;
		await session.command.handler("add", { ui: { input, select: async () => undefined, notify: () => undefined } }).catch((error) => { failure = error; });
		Date.now = realNow;
		expect(failure).toBeInstanceOf(Error);
		expect((await readPools(path)).pools["opencode-go"]).toEqual(["kept-key", "other-session-key"]);
		expect(await routedKeys(session.provider)).toEqual(["kept-key"]);
	} finally {
		Date.now = realNow;
		if (previousHome === undefined) delete process.env.HOME;
		else process.env.HOME = previousHome;
	}
});

test("adopts neither side of a raced remove whose store write failed", async () => {
	const home = await mkdtemp(join(tmpdir(), "pi-credential-pool-raced-remove-fail-"));
	const previousHome = process.env.HOME;
	process.env.HOME = home;
	const directory = join(home, ".pi", "agent");
	const path = join(directory, "credential-pools.json");
	const poolKeys = ["raced-kept", "raced-doomed"];
	await writePools({ version: 1, pools: { "opencode-go": poolKeys } }, path);
	const realNow = Date.now;
	try {
		const session = await loadExtension();
		const choices = poolKeys.map((key) => `${fingerprint(key)} (${credentialIdentity(key).slice(-8)})`);
		const frozenNow = 1_900_000_000_000;
		await mkdir(join(directory, `credential-pools.json.${process.pid}.${frozenNow}.tmp`));
		const select = async () => {
			await writePools({ version: 1, pools: { "opencode-go": [...poolKeys, "other-session-key"] } }, path);
			Date.now = () => frozenNow;
			return choices[1];
		};
		let failure: unknown;
		await session.command.handler("remove", { ui: { select, input: async () => undefined, notify: () => undefined } }).catch((error) => { failure = error; });
		Date.now = realNow;
		expect(failure).toBeInstanceOf(Error);
		expect((await readPools(path)).pools["opencode-go"]).toEqual([...poolKeys, "other-session-key"]);
		expect(await routedKeys(session.provider)).toEqual(poolKeys);
	} finally {
		Date.now = realNow;
		if (previousHome === undefined) delete process.env.HOME;
		else process.env.HOME = previousHome;
	}
});
