# pi-credential-pool

`pi-credential-pool` is a Pi package for a local OpenCode Go credential pool.
It wraps the `opencode-go` provider the runtime already composed, including models from Pi's persisted and remote catalog, and routes requests with keys from its local sidecar store.

## Installation

Install from a package source with `pi install npm:pi-credential-pool`, or test a checkout with `pi install /absolute/path/to/pi-credential-pool`.
Pi loads the package extension automatically.

## Configuration and commands

Credentials are stored only in `~/.pi/agent/credential-pools.json`.
Use `/credential-pool add` and paste a key in Pi's input dialog.
Use `/credential-pool list` to see short fingerprints, health, and routing activity, `/credential-pool remove` to select a fingerprint, `/credential-pool reset` to clear temporary health state, and `/credential-pool usage` for per-account usage.
Commands never accept a secret as an argument.

`/credential-pool usage` calls `GET https://opencode.ai/zen/go/v1/usage` once per stored key, in parallel, with `Authorization: Bearer <key>`, `accept: application/json`, `x-opencode-client: pi`, and one stable non-secret session identifier for the run.
Each credential is reported as `Account N` with its ten-character fingerprint, the rolling, weekly, and monthly percentages, statuses, and reset times.
Successful responses are cached in memory per credential for five minutes and the last good report is kept through transient failures.
A 401 or 403 is reported for that credential instead of a cached report, because the key or subscription is no longer valid.
An exhausted monthly window prints a warning, because the account can still serve requests by consuming its balance, so selection policy does not change.
The report also lists routing activity observed from real provider attempts: attempt count, last-used time, latest outcome, and which credential was selected last.
Keys and session identifiers are never displayed, logged, or persisted.

The OpenCode Go preset uses the `opencode-go` provider name, so its built-in models are available when the sidecar store contains a key even if `OPENCODE_API_KEY` is unset.
Models refreshed by Pi into `models-store.json` stay selectable while the pool is active.
The package never reads existing provider credentials and does not run a proxy, server, or child process.

## Security

The store is created with mode `0600` and written by rename from a `0600` temporary file.
Keys are not logged or displayed; status uses a stable ten-character SHA-256 fingerprint and health metadata only.
Keep the file out of backups or sharing systems that do not meet your credential-handling requirements.

## Behavior and limitations

For a stable Pi session ID, selection is deterministic.
Otherwise selection is round-robin across eligible keys.
Only pre-output 401, 403, 429, and quota failures can rotate a request, Retry-After is honored, and a request is never replayed after text, thinking, or tool output begins.
Attempts never exceed pool size.
Routing activity is in memory only and disappears when Pi exits.

This package only supports the OpenCode Go sidecar flow described above.

## Optional live smoke test

`PI_CREDENTIAL_POOL_KEYS='key-one,key-two' bun run smoke:opencode-go` sends one minimal request per supplied key and prints only each fingerprint, pass/fail, and status.
It is intentionally not run by the test suite.
