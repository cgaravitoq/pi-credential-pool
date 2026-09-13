import { CredentialPool } from "../src/pool.ts";

const keys = process.env.PI_CREDENTIAL_POOL_KEYS?.split(",").filter(Boolean) ?? [];
type SmokeFetch = (input: string | URL | Request, init?: RequestInit) => Promise<Response>;

export async function requestSmoke(key: string, fetchImpl: SmokeFetch = fetch) {
	return fetchImpl("https://opencode.ai/zen/go/v1/chat/completions", {
		method: "POST",
		headers: {
			authorization: `Bearer ${key}`,
			"content-type": "application/json",
			"x-opencode-client": "pi",
			"x-opencode-session": crypto.randomUUID(),
		},
		body: JSON.stringify({ model: "deepseek-v4-flash", messages: [{ role: "user", content: "Reply with OK." }], max_tokens: 4 }),
	});
}

if (import.meta.main) {
	if (!keys.length) throw new Error("Set PI_CREDENTIAL_POOL_KEYS to run the OpenCode Go smoke test");
	const pool = new CredentialPool(keys);
	const results = await Promise.all(pool.entries().map(async (entry, index) => {
		const key = keys[index]!;
		try {
			const response = await requestSmoke(key);
			return { fingerprint: entry.fingerprint, passed: response.ok, status: response.status };
		} catch {
			return { fingerprint: entry.fingerprint, passed: false, status: "network-error" };
		}
	}));
	for (const result of results) console.log(`${result.fingerprint} ${result.passed ? "pass" : "fail"} ${result.status}`);
	if (results.some((result) => !result.passed)) process.exitCode = 1;
}
