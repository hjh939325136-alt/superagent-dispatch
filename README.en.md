# SuperAgent M0 Source Preview

[简体中文](README.md) · **English**

**SuperAgent aims to make work traceable when it spans several tools.** Imagine making a short video from a collection of references. You find relevant material, produce a draft, send it for review, and later check which references it used and where review stopped.

This M0 preview walks through that workflow on one computer with sample data. Dispatch tracks tasks and status, Manager finds material and answers queries, and Collector adds summaries and references for events such as sample script generation and simulated review to the task ledger. Syncing again does not duplicate events. For single-machine task coordination alone, start with the [Dispatch v0.1.0 overview](https://github.com/hjh939325136-alt/superagent-dispatch/blob/main/README.en.md).

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

## Current status

The public source has been tested with local examples on Windows and isolated Debian. The demos use synthetic material and simulated review. The running service has yet to adopt these updates. Production rollout, cross-device execution, continuous operation, and migration of real databases still need separate validation.

[Technical preview guide](M0_PREVIEW.en.md) · [Development updates](DEVELOPMENT_UPDATES.en.md)
