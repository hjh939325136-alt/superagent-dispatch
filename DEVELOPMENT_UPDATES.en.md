# Development updates

[简体中文](DEVELOPMENT_UPDATES.md) · **English** · [Quick start](README.en.md)

## 0.1.1-m0.1 (2026-11-15)

The first public M0 preview includes instructions for trying the source on your own computer.

- Try task coordination, research queries, simulated content review, inventory, retention, and backup with built-in examples.
- The repository includes compatibility fixtures and Linux service templates. This milestone covers sample workflows. Continuous operation, cross-device execution, and migration of real databases remain to be validated.
- Start with the [M0 quick start](README.en.md) or try single-machine task coordination with [Dispatch v0.1.0](https://github.com/hjh939325136-alt/superagent-dispatch/blob/main/README.en.md).

## 0.1.1-m0.2 (2026-11-15)

The M0 preview can show sample content events, and Manager query replies identify their format version.

- Manager still accepts existing JSON requests. Callers can specify version `0.1`, and JSON replies carry a version on success and on handled query errors.
- The content example adds summaries and references for events such as draft generation and simulated review to the task ledger. Syncing again does not duplicate the batch.
- The quick start gains query and content-event instructions. Validation uses local examples. The running service has yet to adopt these changes.

## 0.1.1-m0.3 (2026-11-15)

The M0 Collector commands also support versioned requests and continue to accept existing requests.

- Collector `sync` and `adopt` accept version `0.1` and validate the request before reading the local configuration. The same format applies when syncing through Manager.
- A gap still freezes sync for operator review. Check the reported state to see whether sync actually finished.
- The preview guide explains requests, replies, and frozen states. The running service has yet to adopt these changes.

## 0.1.1-m0.4 (2026-11-15)

One task ledger can now explicitly enable mixed mode so local commands and a loopback HTTP coordinator each claim their own work.

- The existing demos stay in isolated mode. Manager can query compatible new ledgers read-only, and the export format moves to v7.
- In the synthetic scenario, a lost acknowledgement leaves the write task pending review without an automatic retry.
- Approval administration, budget settlement, and final settlement of uncertain tasks remain unavailable in mixed mode. Domain recovery and switching the running service still need validation.
