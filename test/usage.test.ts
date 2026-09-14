import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, test } from "bun:test";
import { InMemoryCredentialStore, InMemoryModelsStore, createAssistantMessageEventStream, type Api, type Model, type Provider } from "@earendil-works/pi-ai";
import { opencodeGoProvider } from "@earendil-works/pi-ai/providers/opencode-go";
import { ModelRegistry, ModelRuntime, type ExtensionAPI } from "@earendil-works/pi-coding-agent";
import credentialPoolExtension, { createPooledStream, renderUsage } from "../extensions/pi-credential-pool.ts";
import { CredentialPool, fingerprint } from "../src/pool.ts";
import { writePools } from "../src/storage.ts";
import { usageUrl, UsageCache, parseUsageReport, type FetchLike } from "../src/usage.ts";

const keys = ["usage-key-one", "usage-key-two", "usage-key-three"];
const resetsAt = "2026-09-13T18:00:00.000Z";

function usageBody(rolling: number, weekly: number, monthly: number, status = "ok") {
  const window = (percent: number) => ({ percent, status, resetsAt });
  return { usage: { rolling: window(rolling), weekly: window(weekly), monthly: window(monthly) } };
}

function json(body: unknown, status = 200) {
  return new Response(JSON.stringify(body), { status, headers: { "content-type": "application/json" } });
}

type Request = { url: string; init?: RequestInit };

async function harness(options: { keys?: string[]; now?: () => number; respond: (key: string) => Response | Promise<Response> }) {
  const home = await mkdtemp(join(tmpdir(), "pi-credential-pool-usage-"));
  const previousHome = process.env.HOME;
  process.env.HOME = home;
  const path = join(home, ".pi", "agent", "credential-pools.json");
  await writePools({ version: 1, pools: { "opencode-go": options.keys ?? keys } }, path);
  const requests: Request[] = [];
  const fetcher: FetchLike = async (input, init) => {
    requests.push({ url: String(input), init });
    const authorization = new Headers(init?.headers).get("authorization") ?? "";
    return options.respond(authorization.replace(/^Bearer /, ""));
  };
  let command: { handler: (args: string, ctx: unknown) => Promise<void> } | undefined;
  const extension = {
    on: () => undefined,
    registerProvider: () => undefined,
    registerCommand: (_name: string, value: unknown) => { command = value as typeof command; },
  } as Partial<ExtensionAPI>;
  try {
    await credentialPoolExtension(extension as ExtensionAPI, { fetch: fetcher, now: options.now });
  } finally {
    if (previousHome === undefined) delete process.env.HOME;
    else process.env.HOME = previousHome;
  }
  const notifications: string[] = [];
  const context = { ui: { notify: (message: string) => notifications.push(message), input: async () => undefined, select: async () => undefined } };
  return { path, requests, notifications, run: (action: string) => command!.handler(action, context), output: () => notifications.join("\n") };
}

describe("usage report parsing", () => {
  test("decodes every window and rejects malformed payloads", () => {
    const report = parseUsageReport(usageBody(10, 20, 30), 1_000);
    expect(report?.windows.map((window) => [window.name, window.percent, window.status, window.resetsAt])).toEqual([
      ["rolling", 10, "ok", Date.parse(resetsAt)],
      ["weekly", 20, "ok", Date.parse(resetsAt)],
      ["monthly", 30, "ok", Date.parse(resetsAt)],
    ]);
    expect(parseUsageReport({ usage: { rolling: { percent: 1 }, weekly: { percent: 1 } } })).toBeUndefined();
    expect(parseUsageReport({ usage: { rolling: { percent: "10" }, weekly: { percent: 1 }, monthly: { percent: 1 } } })).toBeUndefined();
    expect(parseUsageReport({ usage: { rolling: { percent: 1, resetsAt: "not-a-date" }, weekly: { percent: 1 }, monthly: { percent: 1 } } })).toBeUndefined();
    expect(parseUsageReport({ usage: { rolling: null, weekly: { percent: 1 }, monthly: { percent: 1 } } })).toBeUndefined();
    expect(parseUsageReport(null)).toBeUndefined();
  });

  test("keeps a report fresh for five minutes and retains it as last good", () => {
    const cache = new UsageCache();
    const report = parseUsageReport(usageBody(10, 20, 30), 1_000)!;
    cache.record("identity", report);
    expect(cache.fresh("identity", 1_000 + 299_999)).toBe(report);
    expect(cache.fresh("identity", 1_000 + 300_000)).toBeUndefined();
    expect(cache.lastGood("identity")).toBe(report);
    cache.clear("identity");
    expect(cache.lastGood("identity")).toBeUndefined();
  });
});

describe("credential-pool usage command", () => {
  test("queries the usage endpoint once per stored key with the exact wire shape", async () => {
    const session = await harness({ respond: (key) => json(usageBody(keys.indexOf(key) * 10 + 10, 20, 30)) });
    await session.run("usage");
    expect(session.requests).toHaveLength(3);
    const headers = session.requests.map((request) => new Headers(request.init?.headers));
    expect(session.requests.every((request) => request.url === usageUrl)).toBe(true);
    expect(session.requests.every((request) => request.init?.method === "GET")).toBe(true);
    expect(session.requests.every((request) => request.init?.body === undefined)).toBe(true);
    expect(headers.map((value) => value.get("accept"))).toEqual(["application/json", "application/json", "application/json"]);
    expect(headers.map((value) => value.get("x-opencode-client"))).toEqual(["pi", "pi", "pi"]);
    expect(new Set(headers.map((value) => value.get("authorization")))).toEqual(new Set(keys.map((key) => `Bearer ${key}`)));
    const sessions = headers.map((value) => value.get("x-opencode-session"));
    expect(sessions.every((value) => value !== null && /^[0-9a-f-]{36}$/.test(value))).toBe(true);
    expect(new Set(sessions).size).toBe(1);
    expect(sessions).not.toContain(keys[0]);
    const output = session.output();
    for (const [index, key] of keys.entries()) {
      expect(output).toContain(`Account ${index + 1} ${fingerprint(key)} [ready]`);
      expect(output).toContain(`rolling ${index * 10 + 10}% ok resets ${new Date(resetsAt).toISOString()}`);
    }
    expect(output).not.toContain(keys[0]);
    expect(output).not.toContain(keys[1]);
    expect(output).not.toContain(keys[2]);
    expect(output).not.toContain(sessions[0]!);
  });

  test("keeps the three accounts separate and warns when the monthly window is exhausted", async () => {
    const session = await harness({
      respond: (key) => key === keys[1]
        ? json({ usage: { rolling: { percent: 100, status: "rate-limited", resetsAt }, weekly: { percent: 100, status: "rate-limited", resetsAt }, monthly: { percent: 100, status: "rate-limited", resetsAt } } })
        : json(usageBody(10, 20, 30)),
    });
    await session.run("usage");
    const output = session.output();
    expect(output).toContain(`Account 2 ${fingerprint(keys[1])} [ready]`);
    expect(output).toContain(`monthly 100% rate-limited resets ${new Date(resetsAt).toISOString()}`);
    expect(output.match(/monthly window exhausted/g)).toHaveLength(1);
    expect(output).toContain("rolling 10% ok");
    await session.run("list");
    expect(session.notifications.at(-1)).not.toContain("cooling");
    expect(session.notifications.at(-1)?.match(/ready/g)).toHaveLength(3);
    expect(session.notifications.at(-1)).toContain("attempts=0 last-used=never outcome=none");
  });

  test("serves cache within five minutes, refreshes after, and retains last good on transient failures", async () => {
    let now = 1_000_000;
    let failing = false;
    const session = await harness({ now: () => now, respond: () => failing ? json({ error: "boom" }, 503) : json(usageBody(10, 20, 30)) });
    await session.run("usage");
    expect(session.requests).toHaveLength(3);
    now += 60_000;
    await session.run("usage");
    expect(session.requests).toHaveLength(3);
    now += 300_000;
    failing = true;
    await session.run("usage");
    expect(session.requests).toHaveLength(6);
    const output = session.output();
    expect(output).toContain("stale: last good data kept after HTTP 503");
    expect(output).toContain("monthly 30% ok");
  });

  test("surfaces a definitive auth failure instead of cached success", async () => {
    let now = 1_000_000;
    let status = 200;
    const session = await harness({ now: () => now, respond: () => status === 200 ? json(usageBody(10, 20, 30)) : new Response("nope", { status }) });
    await session.run("usage");
    expect(session.output()).toContain("monthly 30% ok");
    now += 300_000;
    status = 401;
    session.notifications.length = 0;
    await session.run("usage");
    expect(session.output()).toContain("usage unavailable: authentication failed (401)");
    expect(session.output()).not.toContain("monthly 30% ok");
    status = 200;
    session.notifications.length = 0;
    await session.run("usage");
    expect(session.output()).toContain("monthly 30% ok");
    expect(session.requests).toHaveLength(9);
  });

  test("skips the fetch entirely when no credential is stored", async () => {
    const session = await harness({ keys: [], respond: () => json(usageBody(1, 2, 3)) });
    await session.run("usage");
    expect(session.requests).toHaveLength(0);
    expect(session.output()).toBe("No credentials configured");
  });
});

describe("routing activity", () => {
  test("attributes attempts and outcomes across a retry and a success", async () => {
    const pool = new CredentialPool(keys);
    const model = opencodeGoProvider().getModels()[0] as Model<Api>;
    const events = (...items: unknown[]) => {
      const stream = createAssistantMessageEventStream();
      for (const item of items) stream.push(item as never);
      return stream;
    };
    const attempts: string[] = [];
    const output = createPooledStream(pool, () => "stable", model, (key) => {
      attempts.push(key);
      return attempts.length === 1 ? events({ type: "error", error: { errorMessage: "429 quota" } }) : events({ type: "done", message: { role: "assistant" } });
    });
    for await (const _event of output) { /* drain */ }
    expect(attempts).toHaveLength(2);
    const entries = pool.entries();
    const first = entries.find((entry) => entry.fingerprint === fingerprint(attempts[0]!))!;
    const second = entries.find((entry) => entry.fingerprint === fingerprint(attempts[1]!))!;
    expect(first).toMatchObject({ attempts: 1, lastOutcome: "quota", lastSelected: false });
    expect(second).toMatchObject({ attempts: 1, lastOutcome: "ok", lastSelected: true });
    const rendered = entries.map((entry, index) => renderUsage(entry, index, { report: parseUsageReport(usageBody(1, 2, 3), 1)! }));
    expect(rendered.join("\n")).toContain("routing: attempts 1, last used");
    expect(rendered.join("\n")).toContain("outcome quota");
    expect(rendered.join("\n")).toContain("outcome ok, last selected");
    for (const key of keys) expect(rendered.join("\n")).not.toContain(key);
  });

  test("drops activity for credentials removed from the pool", () => {
    const pool = new CredentialPool(keys);
    pool.markAttempt(pool.select()!, 1_000);
    pool.replace([keys[0]!, keys[2]!]);
    expect(pool.entries()).toHaveLength(2);
    expect(pool.entries().reduce((total, entry) => total + entry.attempts, 0)).toBe(1);
    pool.replace([keys[2]!]);
    expect(pool.entries().every((entry) => entry.attempts === 0 && !entry.lastSelected)).toBe(true);
  });
});

describe("provider catalog", () => {
  test("keeps a refreshed runtime catalog model selectable and delegates refresh to it", async () => {
    const store = new InMemoryModelsStore();
    const base = opencodeGoProvider().getModels()[0]!;
    await store.write("opencode-go", {
      models: [{ ...base, id: "deepseek-refreshed" }],
      checkedAt: Date.now(),
      lastModified: Date.now() + 1_000_000_000,
    });
    const runtime = await ModelRuntime.create({ credentials: new InMemoryCredentialStore(), modelsPath: null, modelsStore: store, allowModelNetwork: false });
    const registry = new ModelRegistry(runtime);
    expect(registry.find("opencode-go", "deepseek-refreshed")).toBeDefined();

    const home = await mkdtemp(join(tmpdir(), "pi-credential-pool-catalog-"));
    const previousHome = process.env.HOME;
    process.env.HOME = home;
    await writePools({ version: 1, pools: { "opencode-go": ["catalog-key"] } }, join(home, ".pi", "agent", "credential-pools.json"));
    const handlers: Array<(event: unknown, ctx: unknown) => void> = [];
    const pi = {
      on: (_name: string, handler: (event: unknown, ctx: unknown) => void) => handlers.push(handler),
      registerProvider: (provider: unknown) => registry.registerProvider(provider as Provider),
      unregisterProvider: (name: string) => registry.unregisterProvider(name),
      registerCommand: () => undefined,
    } as Partial<ExtensionAPI>;
    try {
      await credentialPoolExtension(pi as ExtensionAPI, { fetch: async () => json(usageBody(1, 2, 3)) });
      await runtime.refresh({ allowNetwork: false });
      expect(registry.find("opencode-go", "deepseek-refreshed")).toBeDefined();
      for (const handler of handlers) await handler({ type: "session_start", reason: "startup" }, { sessionManager: { getSessionId: () => "session" }, modelRegistry: registry });
      expect(registry.find("opencode-go", "deepseek-refreshed")).toBeDefined();
      const registered = registry.getRegisteredNativeProvider("opencode-go");
      expect(registered?.getModels().some((model) => model.id === "deepseek-refreshed")).toBe(true);
      expect(registered?.refreshModels).toBeDefined();
    } finally {
      if (previousHome === undefined) delete process.env.HOME;
      else process.env.HOME = previousHome;
      await rm(home, { recursive: true, force: true });
    }
  });

  test("adopts the composed runtime provider as its streaming base", async () => {
    const refreshed = { ...opencodeGoProvider().getModels()[0]!, id: "someone-elses-refresh" };
    let refreshCalls = 0;
    const live = { ...opencodeGoProvider(), getModels: () => [refreshed], refreshModels: async () => { refreshCalls += 1; } } as Provider;
    const home = await mkdtemp(join(tmpdir(), "pi-credential-pool-catalog-"));
    const previousHome = process.env.HOME;
    process.env.HOME = home;
    await writePools({ version: 1, pools: { "opencode-go": ["catalog-key"] } }, join(home, ".pi", "agent", "credential-pools.json"));
    const handlers: Array<(event: unknown, ctx: unknown) => void> = [];
    const registered: Provider[] = [];
    const pi = {
      on: (_name: string, handler: (event: unknown, ctx: unknown) => void) => handlers.push(handler),
      registerProvider: (provider: unknown) => registered.push(provider as Provider),
      unregisterProvider: () => undefined,
      registerCommand: () => undefined,
    } as Partial<ExtensionAPI>;
    try {
      await credentialPoolExtension(pi as ExtensionAPI, { fetch: async () => json(usageBody(1, 2, 3)) });
      expect(registered[0]!.getModels().some((model) => model.id === "someone-elses-refresh")).toBe(false);
      for (const handler of handlers) handler({ type: "session_start", reason: "startup" }, { sessionManager: { getSessionId: () => "session" }, modelRegistry: { getProvider: () => live } });
      const wrapped = registered.at(-1)!;
      expect(wrapped.getModels()).toEqual([refreshed]);
      await wrapped.refreshModels!({ allowNetwork: false, signal: new AbortController().signal, publish: async () => true });
      expect(refreshCalls).toBe(1);
    } finally {
      if (previousHome === undefined) delete process.env.HOME;
      else process.env.HOME = previousHome;
      await rm(home, { recursive: true, force: true });
    }
  });
});
