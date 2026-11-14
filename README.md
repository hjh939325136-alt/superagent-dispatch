# SuperAgent Dispatch

SuperAgent Dispatch is a Python task coordinator for projects with multiple tools or execution devices. It keeps a SQLite ledger of versioned tasks, capability matching, leases, idempotency keys, and SHA-256 evidence. A content-operations adapter and small synthetic fixtures show how a project can integrate its own tool.

## Run from source

Use Python 3.11 or later from the checkout root. The runtime uses only the Python standard library.

```sh
python -B -m sa_dispatch selftest
python -B -m unittest discover -s tests -v
```

The self-test creates a separate temporary ledger, runs three synthetic jobs, exports completed task history, and confirms the new ledger can read the same receipts without replaying component calls. The tests also exercise the foreground coordinator, authorization, and process restart.

## Deploy a synthetic coordinator

The foreground service runs on IPv4 loopback. Its dedicated ledger serves synthetic tasks and fixtures only. The [Linux deployment guide](deploy/README.md) covers setup on a systemd host, generating separate dispatcher and worker credentials, a smoke check across service restart, and offline backup verification.

The source CLI and synthetic tests are portable. Linux systemd deployment has a separate checklist. Use a local filesystem for the SQLite ledger, with one coordinator per ledger. Keep real project data, component credentials, and live configuration in the systems that own them. Host identities and fixture credentials are operator configuration, separate from released code.

## Project structure

| Directory | Contents |
| --- | --- |
| `sa_dispatch/` | Ledger, task contracts, artifact store, CLI and synthetic transport |
| `sa_dispatch_hotspot/` | Example content-operations adapter |
| `deploy/` | Linux service, deployment checks, backup verification, and synthetic smoke client |
| `tests/` | Regression tests and source-based fixture |

The release scope is single-host coordination and synthetic handoff. Real-device access, data transfer for live business tasks, and host reboot recovery have separate deployment acceptance criteria.

## Use a project adapter

Create a Python entry point that invokes `sa_dispatch.cli.main(plugins={"my-project": KINDS})`, with a mapping from task kind names to trusted adapter objects. Set `plugins = ["my-project"]` in your TOML and configure the corresponding component executable there. A task kind provides `name`, `version`, `enabled`, `side_effect`, `component`, `validate(params)`, `scope(params)`, `request(task)`, and `result(response, task)`. Its optional `allow_no_new` defaults to false. The [included adapter](sa_dispatch_hotspot/__init__.py) shows the request and response shape. The worker gives the configured executable a generated JSON request file and expects a JSON response with the original `request_id`. Test task validation, response binding and duplicate requests using synthetic fixtures before using a component on real records.

For the local executor, the request includes `idempotency_key`, which can be null for reads. Synthetic reconciliation uses the included fixture's `fake.write.get` contract. Other write adapters can leave uncertain results in `unknown` until their operator has enough evidence to reconcile them.
