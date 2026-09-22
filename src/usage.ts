export const usageUrl = "https://opencode.ai/zen/go/v1/usage";
const usageCacheTtlMs = 5 * 60_000;

export type UsageWindowName = "rolling" | "weekly" | "monthly";

export type UsageWindow = {
  name: UsageWindowName;
  percent: number;
  status: string;
  resetsAt?: number;
};

export type UsageReport = { fetchedAt: number; windows: UsageWindow[] };

export type FetchLike = (input: string | URL | Request, init?: RequestInit) => Promise<Response>;

export type UsageResult =
  | { kind: "ok"; report: UsageReport }
  | { kind: "auth"; status: number; message: string }
  | { kind: "transient"; message: string };

const usageWindowNames: readonly UsageWindowName[] = ["rolling", "weekly", "monthly"];

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function parseWindow(name: UsageWindowName, value: unknown): UsageWindow | undefined {
  if (!isRecord(value)) return undefined;
  const percent = value.percent;
  if (typeof percent !== "number" || !Number.isFinite(percent) || percent < 0 || percent > 100) return undefined;
  const status = value.status;
  if (typeof status !== "string" || !status) return undefined;
  const parsed = typeof value.resetsAt === "string" ? Date.parse(value.resetsAt) : typeof value.resetsAt === "number" ? value.resetsAt : Number.NaN;
  if (!Number.isFinite(parsed)) return undefined;
  return { name, percent, status, resetsAt: parsed };
}

// All-or-nothing: a partial response must not replace a complete last-good
// report, so any malformed window makes the whole payload a transient failure.
export function parseUsageReport(payload: unknown, fetchedAt = Date.now()): UsageReport | undefined {
  if (!isRecord(payload) || !isRecord(payload.usage)) return undefined;
  const usage = payload.usage;
  const windows: UsageWindow[] = [];
  for (const name of usageWindowNames) {
    const window = parseWindow(name, usage[name]);
    if (!window) return undefined;
    windows.push(window);
  }
  return { fetchedAt, windows };
}

async function upstreamMessage(response: Response): Promise<string> {
  try {
    const payload: unknown = await response.json();
    if (isRecord(payload) && isRecord(payload.error) && typeof payload.error.message === "string") return payload.error.message;
  } catch {
    // Body is optional detail; the status is the contract.
  }
  return response.statusText;
}

export async function fetchUsage(key: string, sessionId: string, fetcher: FetchLike, now = Date.now(), signal?: AbortSignal): Promise<UsageResult> {
  let response: Response;
  try {
    response = await fetcher(usageUrl, {
      method: "GET",
      headers: {
        accept: "application/json",
        authorization: `Bearer ${key}`,
        "x-opencode-client": "pi",
        "x-opencode-session": sessionId,
      },
      signal,
    });
  } catch (error) {
    return { kind: "transient", message: error instanceof Error ? error.message : "network error" };
  }
  if (response.status === 401 || response.status === 403) return { kind: "auth", status: response.status, message: await upstreamMessage(response) };
  if (!response.ok) return { kind: "transient", message: `HTTP ${response.status}` };
  let payload: unknown;
  try {
    payload = await response.json();
  } catch {
    return { kind: "transient", message: "invalid JSON" };
  }
  const report = parseUsageReport(payload, now);
  return report ? { kind: "ok", report } : { kind: "transient", message: "malformed usage response" };
}

export class UsageCache {
  #reports = new Map<string, UsageReport>();

  fresh(identity: string, now: number): UsageReport | undefined {
    const report = this.#reports.get(identity);
    return report && now - report.fetchedAt < usageCacheTtlMs ? report : undefined;
  }

  lastGood(identity: string): UsageReport | undefined {
    return this.#reports.get(identity);
  }

  record(identity: string, report: UsageReport): void {
    this.#reports.set(identity, report);
  }

  clear(identity: string): void {
    this.#reports.delete(identity);
  }
}
