import { mkdir, mkdtemp, readFile, readdir, rm, writeFile } from "node:fs/promises";
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

function gates(source: string): Set<string> {
  return new Set([...source.matchAll(/bun run ([\w:-]+)/g)].map((match) => match[1]!));
}

function branchPatterns(source: string): string[] | undefined {
  return /pull_request:\s*\n\s*branches:\s*\[([^\]]*)\]/.exec(source)?.[1]?.split(",").map((pattern) => pattern.trim().replace(/^["']|["']$/g, ""));
}

test("the packed tarball ships every declared runtime file", async () => {
  const packed = await run(["npm", "pack", "--dry-run", "--json"]);
  expect(packed.exitCode, packed.stderr).toBe(0);
  const [report] = JSON.parse(packed.stdout) as [{ files: { path: string }[] }];
  const paths = report!.files.map((file) => file.path);
  const required = ["extensions/pi-credential-pool.ts", "src/pool.ts", "src/storage.ts", "src/usage.ts", "scripts/live-smoke.ts"];
  expect(required.filter((path) => !paths.includes(path))).toEqual([]);
});

test("the release workflow runs every gate the CI workflow runs", async () => {
  const workflow = async (name: string) => readFile(join(import.meta.dir, "..", ".github", "workflows", name), "utf8");
  const [ci, release] = await Promise.all([workflow("ci.yml"), workflow("release.yml")]);
  expect([...gates(ci)].filter((gate) => !gates(release).has(gate))).toEqual([]);
});

test("reads gates from inline and block workflow steps", () => {
  expect([...gates("      - name: Check\n        run: bun run check\n")]).toEqual(["check"]);
  expect([...gates("      - name: Test\n        run: |\n          bun run test\n")]).toEqual(["test"]);
});

test("the CI workflow covers pull requests into base branches containing a slash", async () => {
  const source = await readFile(join(import.meta.dir, "..", ".github", "workflows", "ci.yml"), "utf8");
  expect(branchPatterns(source)).toEqual(["**"]);
});

test("reads branch filters as whole patterns instead of a substring", () => {
  expect(branchPatterns('  pull_request:\n    branches: ["**"]\n')).toEqual(["**"]);
  expect(branchPatterns('  pull_request:\n    branches: ["no**pe"]\n')).not.toEqual(["**"]);
});

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
