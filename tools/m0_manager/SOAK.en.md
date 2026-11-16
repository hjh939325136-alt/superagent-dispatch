# Observe the shared coordinator and Collector

[简体中文](SOAK.md) · **English**

The observer samples the bound 0013 shared coordinator and Collector through Manager `ops.snapshot` about every five minutes. It observes existing jobs and budgets. Model timers and model execution are managed separately.

On an isolated, bound Debian validation system, use the [common source package](../m0_installer/COMMON_PACKAGE.en.md) to verify and freeze the source. Run `soak_install.py install` from the frozen source with `--expected-machine` and `--manifest-sha256`. It starts paused. The `enable` command records a window after a successful read-only probe. Retrying enable reuses the same deadline. Use `status` to inspect the window and `pause` to stop an unfinished observation while retaining the record.

The service writes only to its observation directory and is isolated from the network and model credentials. A run is limited to 20 seconds and 128 MiB of memory. The record allows up to 512 samples and 512 KiB. The journal rate limit controls write rate.

A complete observation requires one boot, consistent clocks, at least 145 good samples, and gaps no longer than 600 seconds. The development VM started the window and recorded two good samples. The 24-hour result is pending.
