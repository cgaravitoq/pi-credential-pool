import { mkdir, mkdtemp, readdir, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, test } from "bun:test";

async function run(command: string[], env = process.env) {
  const child = Bun.spawn(command, { env, stdout: "pipe", stderr: "pipe" });
  const [stdout, stderr, exitCode] = await Promise.all([
    new Response(child.stdout).text(),
    new Response(child.stderr).text(),
    child.exited,
  ]);
  return { stdout, stderr, exitCode };
}

test("the packed extension loads in Pi without package-local peer dependencies", async () => {
  const root = await mkdtemp(join(tmpdir(), "pi-credential-pool-package-"));
  try {
    const packed = await run(["npm", "pack", "--pack-destination", root]);
    expect(packed.exitCode, packed.stderr).toBe(0);
    const tarball = (await readdir(root)).find((entry) => entry.endsWith(".tgz"));
    expect(tarball).toBeDefined();

    const install = join(root, "install");
    const installed = await run(["npm", "install", "--prefix", install, "--legacy-peer-deps", "--no-audit", "--no-fund", join(root, tarball!)]);
    expect(installed.exitCode, installed.stderr).toBe(0);

    const extension = join(install, "node_modules", "pi-credential-pool", "extensions", "pi-credential-pool.ts");
    const home = join(root, "home");
    const agent = join(home, ".pi", "agent");
    await mkdir(agent, { recursive: true });
    await writeFile(join(agent, "credential-pools.json"), JSON.stringify({ version: 1, pools: { "opencode-go": ["package-test-key"] } }));
    const loaded = await run([join(process.cwd(), "node_modules", ".bin", "pi"), "-e", extension, "--list-models", "opencode-go"], {
      ...process.env,
      HOME: home,
    });
    expect(loaded.exitCode, loaded.stderr).toBe(0);
    expect(loaded.stderr).not.toContain("Failed to load extension");
    expect(loaded.stdout).toContain("opencode-go");
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});
