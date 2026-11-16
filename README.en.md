# SuperAgent Development Preview

[阅读简体中文版](README.md) · **English** · [Agent guide](AGENTS.md) · [Documentation index](llms.txt)

**SuperAgent helps several tools work on one job and lets you check what happened at each step.** It records who picked up a task, how far it got, and where its results are stored on your computer.

For example, to make a short video, you can find relevant references, generate a sample script, and simulate a human review. After the demo, open one report to see which references the script used and how far review progressed.

| What you want to try | Where to start |
| --- | --- |
| Give local scripts tasks and check progress and results | [Stable Dispatch v0.1.0](https://github.com/hjh939325136-alt/superagent-dispatch/blob/main/README.en.md) |
| Try searching references, generating a sample script, and simulating review | The examples below use the project's built-in synthetic data |

This is the first development milestone (M0). The demos run on one computer. For how the components work together and what is available today, see the [technical preview guide](M0_PREVIEW.en.md).

## Try an example

Use Python 3.11 or later. From the root of the preview checkout, try the Dispatch self-test:

```sh
python -I -B superagent.py dispatch selftest
```

For the query or content workflow examples, go back to the parent directory. Replace `superagent-dispatch` with the name of your checkout and use a fresh local output directory on each run:

```sh
python -B "superagent-dispatch/superagent.py" manager-fixture --output "M0 manager fixture"
python -B "superagent-dispatch/superagent.py" pipeline-fixture --output "M0 pipeline fixture"
```

Once the query example is ready, send the included JSON request to Manager. `$PWD` supplies the required absolute path. In Bash:

```sh
python -B "superagent-dispatch/superagent.py" manager --binding "$PWD/M0 manager fixture/manager-binding.json" < "superagent-dispatch/examples/manager-query.json"
```

In PowerShell:

```powershell
Get-Content -Raw -Encoding utf8 "superagent-dispatch/examples/manager-query.json" | python -B "superagent-dispatch/superagent.py" manager --binding "$PWD/M0 manager fixture/manager-binding.json"
```

After the content workflow runs, open `report.json` in its output directory to see the source references, sample draft, simulated review, and which events reached the task ledger.

## What this update adds

A single task ledger can now serve both local commands and a loopback HTTP coordinator, with each side claiming its own tasks. Mixed mode requires explicit activation by an operator. The regular examples still run in isolated mode. The [preview guide](M0_PREVIEW.en.md#isolated-and-mixed-execution) lists the remaining limits.

Mixed mode can bind approvals and settle synthetic usage per task. For a supported local research-to-content write with complete evidence, this version can also confirm the historical result through a read-only lookup. It leaves withdrawn material and approvals unavailable and performs no automatic retry.

**Use `0.1.1-m0.6` or a later compatible build for export, verification, and import of these evidence bundles.** Earlier v7 tools can omit supporting evidence. See [historical reconciliation and tool compatibility](M0_PREVIEW.en.md#historical-reconciliation-and-tool-compatibility).

This preview can also restore an owned, stopped synthetic workspace to a new directory while preserving its original task identities. Restored data is read-only by default. An explicit rehearsal can check listed historical tasks and cannot run them again. Try the [offline recovery demo](tools/m0_backup/README.md#offline-synthetic-recovery) in a temporary workspace.

Offline recovery also preserves settled scheduling history. Restored schedules and slots can be inspected read-only. An old enabled flag remains historical, and the recovery tools do not run overdue slots.

A separate central maintenance entry point can configure shared budgets, record handoffs, and inspect status on an activated local synthetic mixed ledger. Task claims, approvals, and final settlement stay with their execution routes. See the [central maintenance guide](dispatch/docs/design/central_control.en.md).

Offline checkpoints can now preserve supported ignored late receipts after an explicit per-row review. They remain audit history and cannot establish task completion. A synthetic test also verifies freezing and explicitly resuming each Collector stream across `0011` to `0012`.

This preview also includes an [experimental maintenance toolkit](tools/m0_native_mixed/README.en.md) for staging synthetic research and content directories and rehearsing interrupted mixed-mode activation. Live Collector adoption still needs validation.

The preview includes a [one-shot model task example](tools/m0_agent/README.en.md). Developers can bind approved synthetic text to a time slot, let a local worker claim it, and inspect its result and token usage. A simulated timeout leaves the outcome unknown to prevent automatic resubmission.

The one-shot worker can resume grants and time slots recorded by `0.1.1-m0.12`. Replaying a terminal task returns its original result or unknown status without another submission.

The preview includes an [R1 installation rehearsal](tools/m0_installer/README.en.md) and a [local read-only status command](tools/m0_agent/READONLY_STATUS.en.md). Build a source-only rehearsal package to simulate one authorized slot in an isolated Debian environment and read task status, usage, and the result reference.

The shared ledger 0013 preview includes source for [timed producer and reviewer tasks](tools/m0_agent/CAMPAIGN.en.md). The roles hand off work within each slot, with tasks, usage limits, and result references in one ledger.

The preview adds a [resident Collector for Debian](tools/m0_agent/COLLECTOR.en.md). A dedicated timer collects the three shared-ledger streams in bounded batches and reports recent health observations.

Manager adds [operational and handoff queries](tools/m0_manager/README.en.md) for service health and single-handoff ledger evidence on an explicitly bound 0013 shared ledger.

The preview adds [operational observation of the shared coordinator and Collector](tools/m0_manager/SOAK.en.md). The development VM started a 24-hour window, with full-window validation pending.

The preview adds [unified offline installation](tools/m0_installer/COMPOSED.en.md) on a fresh Debian system. It prepares the 0013 shared ledger and collection service with model execution blocked.

The preview adds an [isolated Hermes comparison](tools/m0_compare/README.en.md) for a fixed worker and a tool-capable host, using synthetic material, bounded requests, and known-response reconciliation.

## Current status

One live model slot has been verified with the shared ledger in an isolated VM. See the [development updates](DEVELOPMENT_UPDATES.en.md) for scope.

The common-source package is still in development validation. See the [development updates](DEVELOPMENT_UPDATES.en.md).

The public preview includes local examples and experimental maintenance tools. See [development updates](DEVELOPMENT_UPDATES.en.md) for the latest adapter and installation rehearsal progress.

The public source has been tested with local examples on Windows and isolated Debian. The demos use synthetic material and simulated review. A dedicated synthetic Debian sandbox has been upgraded from `0011` to `0012` in isolated mode. Post-upgrade checks verified historical tasks, ignored late audit, unified events, Collector cursors, and evidence objects. Mixed-mode activation, native research and content integration, and explicit adoption of each Collector stream are still pending. Full M0 is still in progress.

The delivery plan is to use an automated installation USB drive on-site with a verified new blank SSD. The installation image and virtual-machine validation are still pending, so the USB drive is not ready for delivery.

[Technical preview guide](M0_PREVIEW.en.md) · [Development updates](DEVELOPMENT_UPDATES.en.md)

If you find this project useful, consider [starring it on GitHub](https://github.com/hjh939325136-alt/superagent-dispatch/tree/preview/m0-source) or [forking it](https://github.com/hjh939325136-alt/superagent-dispatch/fork) to adapt it to your workflow.
