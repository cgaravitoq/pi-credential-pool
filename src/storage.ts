import { mkdir, readFile, rename, rm, rmdir, stat, utimes, writeFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import { randomUUID } from "node:crypto";

export type StoredPools = { version: 1; pools: Record<string, string[]> };

const empty = (): StoredPools => ({ version: 1, pools: {} });
const lockWaitMs = 5_000;
const staleLockMs = 30_000;
const heartbeatMs = 1_000;
const recoveryConfirmMs = 2_000;
const pollMs = 10;

type LockSignal = { mtimeMs: number; owned: boolean };

const sleep = (milliseconds: number) => new Promise<void>((resolve) => setTimeout(resolve, milliseconds));
const errorCode = (error: unknown) => (error as NodeJS.ErrnoException).code;
const missing = (error: unknown) => errorCode(error) === "ENOENT";

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
  try {
    if (await readFile(recoveryPath, "utf8") === recovery) await rm(recoveryPath, { force: true });
  } catch (error) {
    if (!missing(error)) throw error;
  }
}

async function recoverStaleLock(lockPath: string, ownerPath: string, recoveryPath: string, signal: LockSignal): Promise<void> {
  const recovery = randomUUID();
  try {
    await writeFile(recoveryPath, recovery, { encoding: "utf8", flag: "wx", mode: 0o600 });
  } catch (error) {
    if (errorCode(error) !== "EEXIST") throw error;
    const claimed = await mtimeOf(recoveryPath);
    if (claimed !== undefined && Date.now() - claimed > recoveryConfirmMs + lockWaitMs) await rm(recoveryPath, { force: true });
    return;
  }
  if (await readFile(recoveryPath, "utf8") !== recovery) return;
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

async function acquireMutationLock(path: string): Promise<() => Promise<void>> {
  const lockPath = `${path}.lock`;
  const ownerPath = join(lockPath, "owner");
  const recoveryPath = join(lockPath, "recovery");
  const owner = randomUUID();
  const deadline = Date.now() + lockWaitMs + staleLockMs + recoveryConfirmMs;
  await mkdir(dirname(path), { recursive: true, mode: 0o700 });

  while (true) {
    try {
      await mkdir(lockPath, { mode: 0o700 });
      await writeFile(ownerPath, owner, { encoding: "utf8", flag: "wx", mode: 0o600 });
      const heartbeat = setInterval(() => {
        const beat = new Date();
        utimes(ownerPath, beat, beat).catch(() => undefined);
      }, heartbeatMs);
      heartbeat.unref();
      return async () => {
        clearInterval(heartbeat);
        try {
          if (await readFile(ownerPath, "utf8") !== owner) return;
        } catch (error) {
          if (missing(error)) return;
          throw error;
        }
        await rm(ownerPath, { force: true });
        try {
          await rmdir(lockPath);
        } catch (error) {
          if (!missing(error) && errorCode(error) !== "ENOTEMPTY") throw error;
        }
      };
    } catch (error) {
      if (errorCode(error) !== "EEXIST") throw error;
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

export async function readPools(path = defaultStorePath()): Promise<StoredPools> {
  try {
    const parsed: unknown = JSON.parse(await readFile(path, "utf8"));
    if (!parsed || typeof parsed !== "object" || (parsed as { version?: unknown }).version !== 1) throw new Error("Invalid credential pool store");
    const pools = (parsed as { pools?: unknown }).pools;
    if (!pools || typeof pools !== "object" || Array.isArray(pools)) throw new Error("Invalid credential pool store");
    for (const keys of Object.values(pools)) if (!Array.isArray(keys) || keys.some((key) => typeof key !== "string")) throw new Error("Invalid credential pool store");
    return parsed as StoredPools;
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
      const release = await acquireMutationLock(path);
      try {
        const pools = await readPools(path);
        await change(pools);
        await writePools(pools, path);
        return pools;
      } finally {
        await release();
      }
    });
    this.#chain = run.then(() => undefined, () => undefined);
    return run;
  }
}
