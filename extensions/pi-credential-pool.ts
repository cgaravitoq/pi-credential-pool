import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { createAssistantMessageEventStream, type Api, type AssistantMessageEvent, type AssistantMessageEventStream, type Model, type ModelsStoreEntry, type Provider } from "@earendil-works/pi-ai";
import { builtinProviders, getBuiltinModelDataGeneratedAt } from "@earendil-works/pi-ai/providers/all";
import { attemptOutcome, credentialIdentity, CredentialPool, retryAfterMs, type CredentialEntry, type Failure } from "../src/pool.ts";
import { defaultStorePath, readPools, SerializedPools } from "../src/storage.ts";
import { fetchUsage, UsageCache, type FetchLike, type UsageReport } from "../src/usage.ts";

const poolName = "opencode-go";

export type PoolExtensionDeps = { fetch?: FetchLike; now?: () => number };
type ProviderStream = (key: string, fetch: typeof globalThis.fetch) => AssistantMessageEventStream;
type StreamMetadata = { status?: number; retryAfterMs?: number };
type CatalogRef = { current: GoProvider };
type RestoredModelRef = { models: readonly Model<GoApi>[] };

function statusFrom(message: string): number | undefined {
	const match = message.match(/\b(401|403|429)\b/);
	return match ? Number(match[1]) : undefined;
}

function failureFrom(response: StreamMetadata, message = ""): Failure {
	return { status: response.status ?? statusFrom(message), retryAfterMs: response.retryAfterMs, quota: /quota|rate limit/i.test(message) };
}

function errorEvent(model: Model<Api>, error: unknown): Extract<AssistantMessageEvent, { type: "error" }> {
	return { type: "error", reason: "error", error: { role: "assistant", content: [], api: model.api, provider: model.provider, model: model.id, usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, totalTokens: 0, cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 } }, stopReason: "error", errorMessage: error instanceof Error ? error.message : "Credential pool failed", timestamp: Date.now() } } as Extract<AssistantMessageEvent, { type: "error" }>;
}

export function createPooledStream(pool: CredentialPool, sessionId: () => string | undefined, model: Model<Api>, stream: ProviderStream, fetcher: FetchLike = globalThis.fetch): AssistantMessageEventStream {
	const output = createAssistantMessageEventStream();
	void (async () => {
		const attempted = new Set<string>();
		let finalFailure: AssistantMessageEvent | undefined;
		while (attempted.size < Math.min(pool.size, 3)) {
			const at = Date.now();
			const credential = pool.select(sessionId(), at, attempted);
			if (!credential) break;
			attempted.add(credential.identity);
			pool.markAttempt(credential, at);
			let forwarded = false;
			let response: StreamMetadata = {};
			const trackedFetch = (async (input: RequestInfo | URL, init?: RequestInit) => {
				const result = await fetcher(input, init);
				if (result.status >= 400) response = { status: result.status, retryAfterMs: retryAfterMs(result.headers.get("retry-after")) };
				return result;
			}) as typeof globalThis.fetch;
			try {
				const input = stream(credential.key, trackedFetch);
				let rotate = false;
				for await (const event of input) {
					if (event.type === "error" && !forwarded) {
						const failure = event.error;
						const attempt = failureFrom(response, failure.errorMessage);
						if (pool.fail(credential, attempt)) {
							pool.markOutcome(credential, attemptOutcome(attempt));
							finalFailure = event;
							rotate = true;
							break;
						}
					}
					if (event.type === "done") pool.markOutcome(credential, "ok");
					else if (event.type === "error") pool.markOutcome(credential, "error");
					forwarded = true;
					output.push(event);
					if (event.type === "done" || event.type === "error") return;
				}
				if (rotate) continue;
				pool.markOutcome(credential, "error");
				output.push(finalFailure ?? errorEvent(model, new Error("Provider stream ended without a terminal event")));
				return;
			} catch (error) {
				const failure = errorEvent(model, error);
				const attempt = failureFrom(response, failure.error.errorMessage);
				if (!forwarded && pool.fail(credential, attempt)) {
					pool.markOutcome(credential, attemptOutcome(attempt));
					finalFailure = failure;
					continue;
				}
				pool.markOutcome(credential, "error");
				output.push(failure);
				return;
			}
		}
		output.push(finalFailure ?? errorEvent(model, new Error("No eligible credential remains in the pool")));
	})().catch((error) => output.push(errorEvent(model, error)));
	return output;
}

type GoApi = "anthropic-messages" | "openai-completions" | "openai-responses";
type GoProvider = Provider<GoApi>;

function mergeCatalog<T extends Api>(base: readonly Model<T>[], overlay: readonly Model<T>[]): readonly Model<T>[] {
	const merged = [...base];
	for (const model of overlay) {
		const index = merged.findIndex((entry) => entry.id === model.id);
		if (index >= 0) merged[index] = model;
		else merged.push(model);
	}
	return merged;
}

// Pi restores the persisted catalog only when it is newer than the bundled one.
// Mirroring that rule keeps the startup window (before the runtime's composed
// provider can be adopted as this provider's base) from resurrecting stale models.
function storedCatalog(stored: Readonly<ModelsStoreEntry> | undefined, builtinGeneratedAt: number | undefined): readonly Model<GoApi>[] {
	if (!stored || builtinGeneratedAt === undefined || stored.lastModified === undefined || stored.lastModified <= builtinGeneratedAt) return [];
	return stored.models.filter((model) => model.provider === poolName) as readonly Model<GoApi>[];
}
export type UsageOutcome = { report: UsageReport; stale?: string } | { error: string };

function formatTime(at: number): string {
	return new Date(at).toISOString();
}

export function renderUsage(entry: CredentialEntry, index: number, outcome: UsageOutcome): string {
	const lines = [`Account ${index + 1} ${entry.fingerprint} [${entry.health}]`];
	if ("report" in outcome) {
		for (const item of outcome.report.windows) lines.push(`  ${item.name} ${item.percent}% ${item.status}${item.resetsAt === undefined ? "" : ` resets ${formatTime(item.resetsAt)}`}`);
		const monthly = outcome.report.windows.find((item) => item.name === "monthly");
		if (monthly && (monthly.percent >= 100 || monthly.status === "rate-limited")) lines.push("  warning: monthly window exhausted; further requests may consume account balance");
		if (outcome.stale) lines.push(`  stale: last good data kept after ${outcome.stale}`);
	} else {
		lines.push(`  usage unavailable: ${outcome.error}`);
	}
	lines.push(`  routing: attempts ${entry.attempts}${entry.lastUsedAt === undefined ? "" : `, last used ${formatTime(entry.lastUsedAt)}`}${entry.lastOutcome === undefined ? "" : `, outcome ${entry.lastOutcome}`}${entry.lastSelected ? ", last selected" : ""}`);
	return lines.join("\n");
}

export default async function credentialPoolExtension(pi: ExtensionAPI, deps: PoolExtensionDeps = {}): Promise<void> {
	const path = defaultStorePath();
	const stored = await readPools(path);
	const pool = new CredentialPool(stored.pools[poolName] ?? []);
	const builtin = builtinProviders().find((provider) => provider.id === poolName) as GoProvider | undefined;
	if (!builtin) throw new Error("OpenCode Go provider is unavailable");
	const catalog: CatalogRef = { current: builtin };
	const restoredCatalog: RestoredModelRef = { models: [] };
	const builtinGeneratedAt = getBuiltinModelDataGeneratedAt();
	const mutations = new SerializedPools();
	const usageCache = new UsageCache();
	const usageSessionId = crypto.randomUUID();
	let activeSessionId: string | undefined;
	const provider: GoProvider = {
		...builtin,
		getModels: () => mergeCatalog(catalog.current.getModels(), restoredCatalog.models),
		refreshModels: async (context) => {
			if (catalog.current === builtin) restoredCatalog.models = storedCatalog(context.stored, builtinGeneratedAt);
			await catalog.current.refreshModels?.(context);
		},
		auth: { apiKey: {
			name: "OpenCode Go credential pool",
			check: async (input) => pool.size ? { type: "api_key", source: "credential-pool" } : builtin.auth.apiKey?.check?.(input),
			resolve: async (input) => pool.size ? { auth: { apiKey: "" }, source: "credential-pool" } : builtin.auth.apiKey?.resolve(input),
		} },
		streamSimple: (model, context, options) => createPooledStream(pool, () => activeSessionId, model, (key, fetch) => builtin.streamSimple(model, context, { ...options, apiKey: key, fetch }), options?.fetch ?? deps.fetch ?? globalThis.fetch),
	};
	pi.registerProvider(provider);
	pi.on("session_start", (_event, ctx) => {
		activeSessionId = ctx.sessionManager.getSessionId();
		// Pi's persisted/remote catalog lives in the provider the runtime already composed.
		// Restoring it as this provider's base keeps refreshed models selectable.
		pi.unregisterProvider(poolName);
		const live = ctx.modelRegistry.getProvider(poolName) as GoProvider | undefined;
		if (live && live !== provider) { catalog.current = live; restoredCatalog.models = []; pi.registerProvider(provider); }
	});
	pi.registerCommand("credential-pool", {
		description: "Manage the local OpenCode Go credential pool",
		handler: async (args, ctx) => {
			const action = args.trim();
			if (action === "list") { ctx.ui.notify(pool.entries().map((entry) => `${entry.fingerprint} ${entry.health} attempts=${entry.attempts} last-used=${entry.lastUsedAt === undefined ? "never" : formatTime(entry.lastUsedAt)} outcome=${entry.lastOutcome ?? "none"}${entry.lastSelected ? " last-selected" : ""}`).join("\n") || "No credentials configured"); return; }
			if (action === "usage") {
				const now = deps.now ?? Date.now;
				const fetcher = deps.fetch ?? globalThis.fetch;
				const entries = pool.entries(now());
				const keys = pool.keys();
				const outcomes = await Promise.all(entries.map(async (entry, index): Promise<UsageOutcome> => {
					const fresh = usageCache.fresh(entry.identity, now());
					if (fresh) return { report: fresh };
					const result = await fetchUsage(keys[index]!, usageSessionId, fetcher, now());
					if (result.kind === "ok") { usageCache.record(entry.identity, result.report); return { report: result.report }; }
					if (result.kind === "auth") { usageCache.clear(entry.identity); return { error: `authentication failed (${result.status})${result.message ? `: ${result.message}` : ""}` }; }
					const lastGood = usageCache.lastGood(entry.identity);
					return lastGood ? { report: lastGood, stale: result.message } : { error: result.message };
				}));
				ctx.ui.notify(entries.length ? entries.map((entry, index) => renderUsage(entry, index, outcomes[index]!)).join("\n\n") : "No credentials configured");
				return;
			}
			if (action === "add") {
				const key = await ctx.ui.input("Add OpenCode Go credential", "Paste a credential");
				if (!key) return;
				await mutations.mutate(path, (current) => { const keys = [...(current.pools[poolName] ?? []), key]; new CredentialPool(keys); current.pools[poolName] = keys; pool.replace(keys); });
				ctx.ui.notify("Credential added");
				return;
			}
			if (action === "remove") {
				const entries = pool.entries();
				const choices = entries.map((entry) => `${entry.fingerprint} (${entry.identity.slice(-8)})`);
				const selected = await ctx.ui.select("Remove credential", choices);
				if (!selected) return;
				const identity = entries[choices.indexOf(selected)]?.identity;
				if (!identity) return;
				await mutations.mutate(path, (current) => { const keys = (current.pools[poolName] ?? []).filter((key) => credentialIdentity(key) !== identity); current.pools[poolName] = keys; pool.replace(keys); });
				ctx.ui.notify("Credential removed");
				return;
			}
			if (action === "reset") {
				await mutations.mutate(path, (current) => { pool.replace(current.pools[poolName] ?? []); pool.reset(); });
				ctx.ui.notify("Credential health reset");
				return;
			}
			ctx.ui.notify("Use /credential-pool add, list, remove, reset, or usage", "info");
		},
	});
}
