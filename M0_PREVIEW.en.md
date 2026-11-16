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

### Continuing collection from 0011 to 0012

The synthetic test starts at the actual `0011` migration prefix. After the `0012` upgrade, the old binding freezes, and replacing the trusted binding leaves the stream frozen. Use read-only Manager queries to check backlog and prior anchors before explicitly adopting each stream with `accept_schema`. `projection_lag=0` on a frozen response does not establish an empty backlog. This update adds upgrade coverage. Collector runtime code stays the same. An older `0011` ledger alone lacks the material and evidence needed for complete domain recovery.

## Isolated and mixed execution

Ledger schema `0012` adds optional mixed mode, and the export format moves to `bundle.v7`. In mixed mode, local research and content commands and a loopback HTTP coordinator can use one ledger while claiming their own routed tasks. Manager can query compatible synthetic `0012` ledgers read-only. Activation is an explicit maintenance action that checks evidence against the ledger's current state. The regular examples remain in isolated mode.

Mixed mode supports binding rules and recording approvals for individual tasks on the caller's route, reserving uncertain usage, and settling final usage against evidence. Existing task constraints cannot be weakened. Approvals stay with their original task and settlement stays within a route. Status is read-only, shows tasks for the selected route, and labels budget totals as ledger-wide.

Shared budgets and synthetic handoffs can be managed through the [central maintenance entry point](dispatch/docs/design/central_control.en.md). A trusted local caller must select it explicitly and supply the expected activation evidence. Status is read-only, and expiry requires an explicit handoff ID. Execution-route permissions stay the same, and automatic host settlement remains unavailable. Usage settlement leaves the business outcome for the evidence review described below. Switching the running service to mixed mode and migrating real data still require validation.

## Historical reconciliation and tool compatibility

Before executing a mixed-mode local task, the system records the original request under a valid lease. For an uncertain `research_content.v1` write, a read-only lookup checks the original request, historical approval, component instance, and recorded result. Complete evidence can confirm historical completion even if the original journal is unavailable. Later denial, expiry, or source withdrawal still applies. Historical confirmation leaves current availability unchanged. HTTP and manual settlement, declaring a write incomplete, and replay remain disabled. Tasks from unsupported components or without an original record stay pending review.

**Use `0.1.1-m0.6` or a later compatible implementation for export, verify, and import.** `0.1.1-m0.5` and earlier v7 tools can omit supporting evidence and still report their own incomplete export as valid. The new verifier rejects missing or mismatched evidence. The v7 label describes the bundle format. Compatibility depends on the tool version. Stop old writers before using the new ledger. Keep writer versions separate and avoid in-place downgrades. The maintenance procedure enforces this boundary since the runtime has yet to enforce a minimum version automatically.

Content hashes verify evidence bytes. When one local OS user can forge a consistent set of component receipts, hashes cannot authenticate the receipts' origin. The original workflow binding digest includes absolute locations. Offline recovery maps new locations separately and preserves the original bindings and task identities. Central maintenance has been tested locally with synthetic data. Cross-machine management and switching the running service to mixed mode still require validation.

## Offline synthetic domain recovery

The checkpoint preserves actual synthetic research, content, and ledger data with the original bindings and explicitly referenced CAS evidence. The operator must own the source, stop its writers, and use a new destination. Active tasks, pending schedule reservations, unfinished external calls, and SQLite sidecar files are rejected. A separate recovery configuration maps the new locations while preserving the original v1 identity. Ordinary v7 still rejects ledgers with unknown tasks or held usage. The offline checkpoint uses its own format.

Historical access is read-only by default. An explicit synthetic rehearsal can check only listed unknown tasks and record historical findings without rerunning them. Held usage and revocations captured in the snapshot are preserved. Later revocations are unverified. Pinning file identity does not prevent a local operator from editing the database directly. The scope is offline synthetic workspaces, with live backup, real data, and continuous operation still awaiting validation.

Starting in `0.1.1-m0.8`, checkpoints preserve settled schedule history and pinned references used only by current or frozen templates. `schedule-status` uses a read-only connection to show plans, slots, and events across all captured routes. Original enabled flags, versions, and task rows are preserved. The flags describe history and grant no permission to run. Recovery rejects `schedule-tick` and `schedule-upsert`. Schedule events are deduplicated aggregates with limited evidence about individual slots.

This scope covers settled standard read-only `hotspot.topic_fetch@1` plans on the complete current schema. Pending reservations, custom kinds, incomplete older schemas, active work, and restarting production scheduling remain unsupported. Use `0.1.1-m0.8` or a later compatible tool to read domain checkpoints with schedule history. Ordinary v7 eligibility stays the same.

Starting in `0.1.1-m0.10`, an explicit per-row review can preserve two synthetic `ignored` audit profiles: `runtime_fixture` for a terminal read-only task's old fence, and `recovery_ignored_claim` for a guarded unknown task's current fence. Original rows, classifications, and CAS objects are preserved. They cannot be used as reconciliation evidence. Ordinary v7 continues to reject all late receipts.

The [offline recovery guide](tools/m0_backup/README.md#offline-synthetic-recovery) contains the demo command.

## Verify Dispatch

In an environment without a custom `PYTHONPATH` or `PYTHONHOME`, run from the preview root:

```sh
cd dispatch
python -B -m unittest discover -s tests -v
python -B -m sa_dispatch selftest
```

The checks use Python and synthetic data, including fixtures for older task records. The [earlier compatibility notes](https://github.com/hjh939325136-alt/superagent-dispatch/blob/562daf314fe0f822a1eb04b646010369a3661735/M0_PREVIEW.md#reproduce-the-dispatcher-checks) describe the fixture coverage. Linux templates live under [dispatch/deploy](dispatch/deploy/README.md).

This public source preview was tested with local synthetic examples on Windows and isolated Debian.

## Experimental mixed-mode maintenance

The [maintenance toolkit](tools/m0_native_mixed/README.en.md) stages a separate synthetic domain and coordinates one mixed-mode activation through persistent phases. Tests use real SQLite, CAS, and activation transactions with synthetic service and recovery adapters. Exceptions simulate interruption at 24 points. The public toolkit covers simulated scenarios. Physical power loss, actual systemd start guards, and live Collector adoption still need validation.

The preview includes a [one-shot model task example](tools/m0_agent/README.en.md). Developers can bind approved synthetic text to a time slot, let a local worker claim it, and inspect its result and token usage. A simulated timeout leaves the outcome unknown to prevent automatic resubmission.

The one-shot worker can resume grants and time slots recorded by `0.1.1-m0.12`. Replaying a terminal task returns its original result or unknown status without another submission.

The preview includes an [R1 installation rehearsal](tools/m0_installer/README.en.md) and a [local read-only status command](tools/m0_agent/READONLY_STATUS.en.md). Build a source-only rehearsal package to simulate one authorized slot in an isolated Debian environment and read task status, usage, and the result reference.

## Sandbox and delivery progress

An isolated Debian VM upgraded its shared ledger from 0012 to 0013. The same ledger recorded synthetic HTTP and local tasks and one authorized live DeepSeek task. After a VM reboot and a repeat installation, the model task still had one attempt and used quota was preserved.

A rehearsal restored the checkpoint to a quarantine directory with execution disabled. Independent-disk disaster recovery and long-running multi-task operation still need validation.

The development candidate includes a common-source builder and offline verifier for research, content, and synthetic HTTP tasks that use a shared Dispatch ledger. The verifier checks the file set and digests without Git, extraction, or execution of package contents.

The distribution is `source_candidate` and `installer_entry` is `null`. The common installer entry point is still in development. Target validation compares the expected manifest digest and machine identity. Fresh VM installation and real systemd startup still need verification.

A systemd timer completed one synthetic task in an isolated Debian sandbox. Reinstalling and starting the service again kept one task and one attempt in the ledger. Debian passed 46 tests. Windows passed 45 and skipped one POSIX-only check. A simulator supplied the model response. Real model calls: 0.

Development now includes a mixed-mode adapter and recovery-set capture for a fixed Debian layout. Synthetic checks used the same 147 frozen files. Windows passed 51 checks and skipped one Unix ownership check. Debian passed all 52. The tests simulated service control. Live mixed-mode activation and Collector adoption still need validation.

A separate Debian installation rehearsal used a test credential to verify first installation, repeat installation, and credential loading after a service restart. The installation image, machine reboot, and long-running operation still need validation.

A dedicated synthetic Debian sandbox has been upgraded from `0011` to `0012` in isolated mode. Post-upgrade checks verified historical tasks, ignored late audit, unified events, Collector cursors, and evidence objects. Mixed-mode activation, native research and content integration, and explicit adoption of each Collector stream are still pending. Full M0 is still in progress.

The delivery plan is to use an automated installation USB drive on-site with a verified new blank SSD. The installation image and virtual-machine validation are still pending, so the USB drive is not ready for delivery.

Continuous operation, cross-device execution, and migration of real databases still require separate validation. See [MIT license](LICENSE), `SHA256SUMS` for file checksums, and `SOURCE_PROVENANCE.json` for file origins.
