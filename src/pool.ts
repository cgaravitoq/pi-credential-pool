import { createHash } from "node:crypto";

export type Health = "ready" | "cooling" | "disabled";

export interface HealthRecord {
  [identity: string]: { state: Health; retryAt?: number };
}

export type Credential = {
  key: string;
  fingerprint: string;
  identity: string;
  health: Health;
  retryAt?: number;
};

export type Failure = { status?: number; retryAfterMs?: number; quota?: boolean; insufficientFunds?: boolean };

export type AttemptOutcome = "ok" | "unauthorized" | "forbidden" | "rate-limited" | "quota" | "insufficient-funds" | "error";

type RoutingActivity = {
  attempts: number;
  lastUsedAt?: number;
  lastOutcome?: AttemptOutcome;
  lastSelected: boolean;
};

export type CredentialEntry = Omit<Credential, "key"> & RoutingActivity;

export function attemptOutcome(failure: Failure): AttemptOutcome {
  if (failure.status === 402 || failure.insufficientFunds) return "insufficient-funds";
  if (failure.quota) return "quota";
  if (failure.status === 401) return "unauthorized";
  if (failure.status === 403) return "forbidden";
  if (failure.status === 429) return "rate-limited";
  return "error";
}

export function fingerprint(key: string): string {
  return credentialIdentity(key).slice(0, 10);
}

export function credentialIdentity(key: string): string {
  return createHash("sha256").update(key).digest("hex");
}

export function retryAfterMs(value: string | null, now = Date.now()): number | undefined {
  if (!value) return undefined;
  const seconds = Number(value);
  if (Number.isFinite(seconds) && seconds >= 0) return seconds * 1000;
  const date = Date.parse(value);
  return Number.isFinite(date) ? Math.max(0, date - now) : undefined;
}

export class CredentialPool {
  #items: Credential[];
  #cursor = 0;
  #activity = new Map<string, { attempts: number; lastUsedAt?: number; lastOutcome?: AttemptOutcome }>();
  #lastSelected?: string;
  #onHealthChange?: (health: HealthRecord) => void;

  constructor(keys: readonly string[], health: HealthRecord = {}, onHealthChange?: (health: HealthRecord) => void) {
    this.#onHealthChange = onHealthChange;
    const seen = new Set<string>();
    this.#items = keys.map((key) => {
      if (!key.trim() || seen.has(key)) throw new Error("Credential keys must be distinct and non-empty");
      seen.add(key);
      const identity = credentialIdentity(key);
      const stored = health[identity];
      const item: Credential = { key, fingerprint: fingerprint(key), identity, health: stored?.state ?? "ready" };
      if (stored?.retryAt !== undefined) item.retryAt = stored.retryAt;
      return item;
    });
  }

  get size(): number { return this.#items.length; }

  entries(now = Date.now()): ReadonlyArray<CredentialEntry> {
    this.#wake(now);
    return this.#items.map(({ key: _key, ...entry }) => {
      const activity = this.#activity.get(entry.identity);
      return {
        ...entry,
        attempts: activity?.attempts ?? 0,
        lastUsedAt: activity?.lastUsedAt,
        lastOutcome: activity?.lastOutcome,
        lastSelected: entry.identity === this.#lastSelected,
      };
    });
  }

  keys(): readonly string[] {
    return this.#items.map((item) => item.key);
  }

  markAttempt(item: Credential, now = Date.now()): void {
    const activity = this.#activity.get(item.identity) ?? { attempts: 0 };
    activity.attempts += 1;
    activity.lastUsedAt = now;
    this.#activity.set(item.identity, activity);
    this.#lastSelected = item.identity;
  }

  markOutcome(item: Credential, outcome: AttemptOutcome): void {
    const activity = this.#activity.get(item.identity);
    if (activity) activity.lastOutcome = outcome;
  }

  select(sessionId?: string, now = Date.now(), excluded = new Set<string>()): Credential | undefined {
    this.#wake(now);
    const eligible = this.#items.filter((item) => item.health === "ready" && !excluded.has(item.identity));
    if (!eligible.length) return undefined;
    if (sessionId) return eligible[this.#hash(sessionId) % eligible.length];
    const selected = eligible[this.#cursor % eligible.length];
    this.#cursor = (this.#cursor + 1) % eligible.length;
    return selected;
  }

  fail(item: Credential, failure: Failure, now = Date.now()): boolean {
    const target = this.#items.find((candidate) => candidate.identity === item.identity);
    if (failure.status === 401 || failure.status === 403) {
      if (target) {
        target.health = "disabled";
        delete target.retryAt;
        this.#publish(now);
      }
      return true;
    }
    const unpaid = failure.status === 402 || failure.insufficientFunds;
    if (unpaid || failure.status === 429 || failure.quota) {
      if (target) {
        target.health = "cooling";
        target.retryAt = now + (unpaid ? 3_600_000 : failure.retryAfterMs ?? 60_000);
        this.#publish(now);
      }
      return true;
    }
    return false;
  }

  healthRecord(now = Date.now()): HealthRecord {
    this.#wake(now);
    const record: HealthRecord = {};
    for (const item of this.#items) {
      if (item.health === "disabled") record[item.identity] = { state: "disabled" };
      else if (item.health === "cooling" && item.retryAt !== undefined) record[item.identity] = { state: "cooling", retryAt: item.retryAt };
    }
    return record;
  }

  replace(keys: readonly string[], health: HealthRecord = {}): void {
    this.#items = new CredentialPool(keys, health).#items;
    this.#cursor %= Math.max(this.#items.length, 1);
    const identities = new Set(this.#items.map((item) => item.identity));
    for (const identity of this.#activity.keys()) if (!identities.has(identity)) this.#activity.delete(identity);
    if (this.#lastSelected !== undefined && !identities.has(this.#lastSelected)) this.#lastSelected = undefined;
  }

  #publish(now: number): void {
    this.#onHealthChange?.(this.healthRecord(now));
  }

  #wake(now: number): void {
    for (const item of this.#items) if (item.health === "cooling" && item.retryAt !== undefined && item.retryAt <= now) {
      item.health = "ready";
      delete item.retryAt;
    }
  }

  #hash(value: string): number {
    let hash = 2166136261;
    for (let i = 0; i < value.length; i++) hash = Math.imul(hash ^ value.charCodeAt(i), 16777619);
    return hash >>> 0;
  }
}
