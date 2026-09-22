import { mkdir, readFile, rename, rm, rmdir, stat, utimes, writeFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import { randomUUID } from "node:crypto";
import type { HealthRecord } from "./pool.ts";

export type StoredPools = { version: 1; pools: Record<string, string[]>; health?: HealthRecord };

const empty = (): StoredPools => ({ version: 1, pools: {} });
const lockWaitMs = 5_000;
const staleLockMs = 30_000;
const heartbeatMs = 1_000;
const recoveryConfirmMs = 2_000;
const pollMs = 10;

type LockSignal = { mtimeMs: number; owned: boolean };
type MutationLock = { assertOwned: () => Promise<void>; release: () => Promise<void> };

const sleep = (milliseconds: number) => new Promise<void>((resolve) => setTimeout(resolve, milliseconds));
const errorCode = (error: unknown) => (error as NodeJS.ErrnoException).code;
const missing = (error: unknown) => errorCode(error) === "ENOENT";

async function readIfPresent(target: string): Promise<string | undefined> {
  try {
    return await readFile(target, "utf8");
  } catch (error) {
    if (missing(error)) return undefined;
    throw error;
  }
}

const ownedBy = async (ownerPath: string, owner: string) => await readIfPresent(ownerPath) === owner;

async function mtimeOf(target: string): Promise<number | undefined> {
  try {
    return (await stat(target)).mtimeMs;
  } catch (error) {
    if (missing(error)) return undefined;
    throw error;
  }
}

async function lockSignal(lockPath: string, ownerPath: string): Promise<LockSignal | undefined> {
  const owned = await mtimeOf(ownerPath);
  if (owned !== undefined) return { mtimeMs: owned, owned: true };
  const created = await mtimeOf(lockPath);
  return created === undefined ? undefined : { mtimeMs: created, owned: false };
}

async function dropRecovery(recoveryPath: string, recovery: string): Promise<void> {
  if (await readIfPresent(recoveryPath) === recovery) await rm(recoveryPath, { force: true });
}

async function recoverStaleLock(lockPath: string, ownerPath: string, recoveryPath: string, signal: LockSignal): Promise<void> {
  const recovery = randomUUID();
  try {
    await writeFile(recoveryPath, recovery, { encoding: "utf8", flag: "wx", mode: 0o600 });
  } catch (error) {
    if (missing(error)) return;
    if (errorCode(error) !== "EEXIST") throw error;
    const claimed = await mtimeOf(recoveryPath);
    if (claimed !== undefined && Date.now() - claimed > recoveryConfirmMs + lockWaitMs) await rm(recoveryPath, { force: true });
    return;
  }
  if (await readIfPresent(recoveryPath) !== recovery) return;
  if (signal.owned) {
    await sleep(recoveryConfirmMs);
    const current = await lockSignal(lockPath, ownerPath);
    if (current?.owned !== true || current.mtimeMs !== signal.mtimeMs) {
      await dropRecovery(recoveryPath, recovery);
      return;
    }
  }
  await rm(ownerPath, { force: true });
  await dropRecovery(recoveryPath, recovery);
  try {
    await rmdir(lockPath);
  } catch (error) {
    if (!missing(error) && errorCode(error) !== "ENOTEMPTY") throw error;
  }
}

async function acquireMutationLock(path: string): Promise<MutationLock> {
  const lockPath = `${path}.lock`;
  const ownerPath = join(lockPath, "owner");
  const recoveryPath = join(lockPath, "recovery");
  const owner = randomUUID();
  const deadline = Date.now() + lockWaitMs + staleLockMs + recoveryConfirmMs;
  await mkdir(dirname(path), { recursive: true, mode: 0o700 });

  while (true) {
    let created = false;
    try {
      await mkdir(lockPath, { mode: 0o700 });
      created = true;
      await writeFile(ownerPath, owner, { encoding: "utf8", flag: "wx", mode: 0o600 });
      const heartbeat = setInterval(() => {
        const beat = new Date();
        utimes(ownerPath, beat, beat).catch(() => undefined);
      }, heartbeatMs);
      heartbeat.unref();
      return {
        assertOwned: async () => {
          if (!await ownedBy(ownerPath, owner)) throw new Error("Lost the credential pool storage lock before writing");
        },
        release: async () => {
          clearInterval(heartbeat);
          if (!await ownedBy(ownerPath, owner)) return;
          await rm(ownerPath, { force: true });
          try {
            await rmdir(lockPath);
          } catch (error) {
            if (!missing(error) && errorCode(error) !== "ENOTEMPTY") throw error;
          }
        },
      };
    } catch (error) {
      if (created && !missing(error)) throw error;
      if (!created && errorCode(error) !== "EEXIST") throw error;
    }

    const signal = await lockSignal(lockPath, ownerPath);
    if (!signal) continue;
    if (Date.now() - signal.mtimeMs > staleLockMs) {
      await recoverStaleLock(lockPath, ownerPath, recoveryPath, signal);
      await sleep(pollMs);
      continue;
    }
    if (Date.now() >= deadline) throw new Error("Timed out waiting for credential pool storage lock");
    await sleep(pollMs);
  }
}

export function defaultStorePath(home = process.env.HOME ?? process.env.USERPROFILE ?? "."): string {
  return join(home, ".pi", "agent", "credential-pools.json");
}

function parseHealth(value: unknown): HealthRecord {
  const health: HealthRecord = {};
  if (!value || typeof value !== "object" || Array.isArray(value)) return health;
  for (const [identity, entry] of Object.entries(value)) {
    if (!entry || typeof entry !== "object" || Array.isArray(entry)) continue;
    const { state, retryAt } = entry as { state?: unknown; retryAt?: unknown };
    if (state === "disabled") health[identity] = { state };
    else if (state === "cooling" && typeof retryAt === "number" && Number.isFinite(retryAt)) health[identity] = { state, retryAt };
  }
  return health;
}

export async function readPools(path = defaultStorePath()): Promise<StoredPools> {
  try {
    const parsed: unknown = JSON.parse(await readFile(path, "utf8"));
    if (!parsed || typeof parsed !== "object" || (parsed as { version?: unknown }).version !== 1) throw new Error("Invalid credential pool store");
    const pools = (parsed as { pools?: unknown }).pools;
    if (!pools || typeof pools !== "object" || Array.isArray(pools)) throw new Error("Invalid credential pool store");
    for (const keys of Object.values(pools)) if (!Array.isArray(keys) || keys.some((key) => typeof key !== "string")) throw new Error("Invalid credential pool store");
    const stored: StoredPools = { version: 1, pools: pools as Record<string, string[]> };
    const health = parseHealth((parsed as { health?: unknown }).health);
    if (Object.keys(health).length) stored.health = health;
    return stored;
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return empty();
    throw error;
  }
}

export async function writePools(value: StoredPools, path = defaultStorePath()): Promise<void> {
  const directory = dirname(path);
  await mkdir(directory, { recursive: true, mode: 0o700 });
  const temporary = `${path}.${process.pid}.${Date.now()}.tmp`;
  await writeFile(temporary, JSON.stringify(value), { mode: 0o600 });
  await rename(temporary, path);
}

export class SerializedPools {
  #chain = Promise.resolve();

  mutate(path: string, change: (pools: StoredPools) => void | Promise<void>): Promise<StoredPools> {
    const run = this.#chain.then(async () => {
      const lock = await acquireMutationLock(path);
      try {
        const pools = await readPools(path);
        await change(pools);
        if (pools.health && Object.keys(pools.health).length === 0) delete pools.health;
        await lock.assertOwned();
        await writePools(pools, path);
        return pools;
      } finally {
        await lock.release();
      }
    });
    this.#chain = run.then(() => undefined, () => undefined);
    return run;
  }
}
