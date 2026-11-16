# Offline shared stack installation on Debian

[简体中文](COMPOSED.md) · **English**

Use a fresh Debian systemd environment. The composed installer prepares the 0012 mixed baseline, upgrades the same ledger to 0013, and enables the loopback HTTP coordinator and Collector. The observer starts paused. Model grants, credentials, jobs, and budgets require no provisioning. The offline gate refuses model execution.

## Install and resume

Use the [common source package builder](COMMON_PACKAGE.en.md) to build and verify the baseline and addon ZIPs separately. Freeze the verified addon sources in a root-managed read-only directory named by the full addon manifest SHA-256. Run `tools/m0_installer/composed.py install` from those sources with:

- `--baseline-zip`, `--baseline-manifest-sha256`, and `--baseline-zip-sha256`
- `--addon-zip`, `--addon-manifest-sha256`, and `--addon-zip-sha256`
- `--expected-machine-id` and `--expected-hostname`

Use the same arguments with `status` to read progress. Retry an interruption with the same package and target binding. Completed steps use their verified receipts. Check the affected component's evidence when an internal phase remains incomplete. Re-running a completed installation reads back its current status.

Acceptance covered a fresh offline installation, seven component receipt boundaries, the offline sealing boundary, task execution, collection, and a normal reboot. The supported resume path uses the same package. OS installation, arbitrary in-place upgrade, interruption at arbitrary migration steps, and conversion to a paid grant require separate validation.
