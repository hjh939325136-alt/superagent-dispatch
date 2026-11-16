# Resident Collector for Debian

[简体中文](COLLECTOR.md) · **English**

The Collector uses a bound 0013 shared ledger. A dedicated systemd timer first wakes about 30 seconds after boot, then waits about 60 seconds after each run ends. A run reads at most 100 entries from each stream. It checks the maintenance and shared-ledger locks and skips writes when either lock is busy or related maintenance is incomplete.

## Install and observe

Use the [shared-ledger package builder](../m0_installer/COMMON_PACKAGE.en.md) to build and verify source with both the Collector and producer/reviewer addons. On a bound, isolated Debian test system, run `collector_install.py install` from the frozen source and supply `--expected-machine` and `--manifest-sha256`. Installation starts paused. Use `enable` to activate, `pause` to pause, and `status` to inspect the timer and recent health observation.

Run `upgrade` from the new frozen source with `--expected-previous-manifest`. The upgrade preserves the binding and health file and leaves the service paused. Retry an interruption with the same arguments.

The service has network access disabled and cannot see the model credential directory. Each run has a 45-second limit and a 128 MiB memory limit. The default batch admission threshold is 32 MiB for the ledger and SQLite sidecars, with more than 256 MiB of free filesystem space. Logging has a rate limit. The deployment manages cumulative log storage. Health observations become stale after 180 seconds by default.

The isolated VM validation covered timed collection, lock contention, cold recovery, and a normal reboot. A 24-hour soak and long-term storage behavior still need validation.
