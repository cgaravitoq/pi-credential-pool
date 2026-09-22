import { mkdtemp, stat, utimes } from "node:fs/promises";
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

test("preserves a replacement lock when a stale owner releases late", async () => {
  const directory = await mkdtemp(join(tmpdir(), "pi-credential-pool-"));
  const path = join(directory, "credential-pools.json");
  const firstPool = new SerializedPools();
  const secondPool = new SerializedPools();
  const thirdPool = new SerializedPools();
  let startFirst!: () => void;
  let releaseFirst!: () => void;
  let releaseSecond!: () => void;
  let startSecond!: () => void;
  let startThird!: () => void;
  const firstReleased = new Promise<void>((resolve) => { releaseFirst = resolve; });
  const secondReleased = new Promise<void>((resolve) => { releaseSecond = resolve; });
  const firstStarted = new Promise<void>((resolve) => { startFirst = resolve; });
  const secondStarted = new Promise<void>((resolve) => { startSecond = resolve; });
  const thirdStarted = new Promise<void>((resolve) => { startThird = resolve; });

  const first = firstPool.mutate(path, async () => {
    startFirst();
    await firstReleased;
  });
  await firstStarted;
  const stale = new Date(Date.now() - 31_000);
  await utimes(`${path}.lock`, stale, stale);
  const second = secondPool.mutate(path, async (pools) => {
    startSecond();
    await secondReleased;
    pools.pools.second = ["second"];
  });
  await secondStarted;
  releaseFirst();
  await first;
  const third = thirdPool.mutate(path, (pools) => {
    startThird();
    pools.pools.third = ["third"];
  });

  expect(await Promise.race([thirdStarted.then(() => true), Bun.sleep(25).then(() => false)])).toBe(false);
  releaseSecond();
  await Promise.all([second, third]);
  expect(await readPools(path)).toEqual({ version: 1, pools: { second: ["second"], third: ["third"] } });
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
