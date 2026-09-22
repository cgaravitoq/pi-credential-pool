import { mkdir, readFile, rename, rm, rmdir, stat, writeFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import { randomUUID } from "node:crypto";

export type StoredPools = { version: 1; pools: Record<string, string[]> };

const empty = (): StoredPools => ({ version: 1, pools: {} });
const lockWaitMs = 5_000;
const staleLockMs = 30_000;

const sleep = (milliseconds: number) => new Promise<void>((resolve) => setTimeout(resolve, milliseconds));
const missing = (error: unknown) => (error as NodeJS.ErrnoException).code === "ENOENT";

async function acquireMutationLock(path: string): Promise<() => Promise<void>> {
  const lockPath = `${path}.lock`;
  const ownerPath = join(lockPath, "owner");
  const recoveryPath = join(lockPath, "recovery");
  const owner = randomUUID();
  let deadline = Date.now() + lockWaitMs;
  await mkdir(dirname(path), { recursive: true, mode: 0o700 });

  while (true) {
    try {
      await mkdir(lockPath, { mode: 0o700 });
      await writeFile(ownerPath, owner, { encoding: "utf8", flag: "wx", mode: 0o600 });
      return async () => {
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
          if (!missing(error) && (error as NodeJS.ErrnoException).code !== "ENOTEMPTY") throw error;
        }
      };
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error;
    }

    try {
      const held = await stat(lockPath);
      deadline = Math.max(deadline, held.mtimeMs + staleLockMs + 1_000);
      if (Date.now() - held.mtimeMs > staleLockMs) {
        const recovery = randomUUID();
        try {
          await writeFile(recoveryPath, recovery, { encoding: "utf8", flag: "wx", mode: 0o600 });
        } catch (error) {
          if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error;
          if (Date.now() - (await stat(recoveryPath)).mtimeMs > staleLockMs) await rm(recoveryPath, { force: true });
          await sleep(10);
          continue;
        }

        if (await readFile(recoveryPath, "utf8") !== recovery) {
          await sleep(10);
          continue;
        }
        await rm(ownerPath, { force: true });
        try {
          if (await readFile(recoveryPath, "utf8") === recovery) await rm(recoveryPath, { force: true });
        } catch (error) {
          if (!missing(error)) throw error;
        }
        try {
          await rmdir(lockPath);
        } catch (error) {
          if (!missing(error) && (error as NodeJS.ErrnoException).code !== "ENOTEMPTY") throw error;
        }
        await sleep(10);
        continue;
      }
    } catch (error) {
      if (!missing(error)) throw error;
      continue;
    }

    if (Date.now() >= deadline) throw new Error("Timed out waiting for credential pool storage lock");
    await sleep(10);
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
