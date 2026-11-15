# M0 Preview Guide

[简体中文](M0_PREVIEW.md) · **English** · [Quick start](README.en.md) · [Development updates](DEVELOPMENT_UPDATES.en.md)

The [quick start](README.en.md) shows how to run task coordination, sample research, and simulated content review on one computer. If you only want to try task coordination, use [v0.1.0](https://github.com/hjh939325136-alt/superagent-dispatch/tree/v0.1.0) and run `python -B -m sa_dispatch` from its repository root. Both versions require Python 3.11 or later.

## What you can try

- **Task coordination.** Run `python -I -B superagent.py dispatch --help` from the preview root to list the available commands.
- **Research and content.** Run the examples to read a query, find sample references, and record simulated review. Manager reads one JSON request from standard input. The role in a request selects a local query scope. Cross-device identity needs separate verification.
- **Inventory, retention, and backup.** Run `python -B superagent.py inventory --help` to see how to export an inventory, or `python -B superagent.py retention --help` to see how to make a retention plan. The [synthetic backup instructions](https://github.com/hjh939325136-alt/superagent-dispatch/blob/562daf314fe0f822a1eb04b646010369a3661735/README.md#bound-synthetic-backup) describe how to use a dedicated local destination and set a size limit.

## Manager queries and content events

Manager requests include `op`, `role`, and `args`, with an optional `"schema_version":"0.1"`. Existing requests can omit the version. Successful queries and handled JSON errors both include a top-level `schema_version`, with exit codes `0` and `2` respectively. Unsupported explicit versions are rejected before the binding is loaded. Strict response consumers should allow the added field. The version inside a `sources` entry still describes that source's data format.

After running the content example, check `event_sync` and `event_readback` in `report.json` for the initial sync, repeat sync, and query results. `events.query` returns pages of summaries from the Dispatch ledger. Use `trace.get` with the content source and a `trace_id` to see related content events. Those events are not automatically linked to a particular Dispatch attempt.

Run the Manager compatibility tests from the preview root:

```sh
python -B -m unittest tools.m0_manager.test_protocol
```

## Collector command versions

Collector `sync` and `adopt`, including calls through `manager sync` and `manager adopt`, accept the optional `"schema_version":"0.1"` field. Existing requests are still supported. The CLI checks fields and version before it reads the trusted local configuration. Successful requests and handled JSON errors include the version, with exit codes `0` and `1` respectively. The command-line parser handles usage errors separately.

`ok: true` means the command was processed. Check `result.state` for the actual sync outcome. If the stream is `frozen`, the operator should inspect the source and gap before deciding whether to explicitly resume with `adopt`. Source authorization and login identity are separate from the version field.

## Isolated and mixed execution

Ledger schema `0012` adds optional mixed mode, and the export format moves to `bundle.v7`. In mixed mode, local research and content commands and a loopback HTTP coordinator can use one ledger while claiming their own routed tasks. Manager can query compatible synthetic `0012` ledgers read-only. Activation is an explicit maintenance action that checks evidence against the ledger's current state. The regular examples remain in isolated mode.

Mixed mode supports binding rules and recording approvals for individual tasks on the caller's route, reserving uncertain usage, and settling final usage against evidence. Existing task constraints cannot be weakened. Approvals stay with their original task and settlement stays within a route. Status is read-only, shows tasks for the selected route, and labels budget totals as ledger-wide.

Global budget configuration, handoff administration, and automatic host settlement remain unavailable in mixed mode. Settling usage leaves an uncertain business outcome pending and does not replay the write. Final business settlement of uncertain tasks, domain recovery, migration of real data, and switching the running service still require validation. This update publishes development preview source, and the running service has yet to adopt it.

## Verify Dispatch

In an environment without a custom `PYTHONPATH` or `PYTHONHOME`, run from the preview root:

```sh
cd dispatch
python -B -m unittest discover -s tests -v
python -B -m sa_dispatch selftest
```

The checks use Python and synthetic data, including fixtures for older task records. The [earlier compatibility notes](https://github.com/hjh939325136-alt/superagent-dispatch/blob/562daf314fe0f822a1eb04b646010369a3661735/M0_PREVIEW.md#reproduce-the-dispatcher-checks) describe the fixture coverage. Linux templates live under [dispatch/deploy](dispatch/deploy/README.md).

This public source preview was tested with local examples on Windows and isolated Debian. Production rollout, server installation, continuous operation, cross-device execution, and migration of real databases require separate validation. The running service has yet to adopt these updates. See [MIT license](LICENSE), `SHA256SUMS` for file checksums, and `SOURCE_PROVENANCE.json` for file origins.
