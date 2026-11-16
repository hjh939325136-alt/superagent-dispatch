# Experimental mixed-mode maintenance

[简体中文](README.md) · **English**

This toolkit is for synthetic environments. `domains.stage_domains(...)` creates synthetic research and content databases, pinned bindings, configuration, and a journal directory in a new empty directory owned by the caller. It references the existing ledger and CAS locations. It checks configuration, bindings, and source hashes before declaring success. A failed staging directory is marked incomplete.

`maintenance.Coordinator(..., adapter)` plans and coordinates one mixed-mode activation. A plan pins configuration bytes, prior state, and evidence from the adapter. Persistent phases cover stopping writers, checking the recovery set, activating, replacing configuration, checking the service, and resuming collection. After an interruption, the coordinator checks prior activation evidence before continuing. New writes or identity drift after exposure trigger a stop for reconciliation. `guard-check` reads the configuration and phase to check whether startup is allowed.

## Run the synthetic tests

Use Python 3.11 or later from the preview source root:

```sh
python -X utf8 -B -m unittest tools.m0_native_mixed.test_domains tools.m0_native_mixed.test_maintenance
```

Tests use temporary directories, real SQLite/CAS and activation transactions, and synthetic service and recovery adapters. Exceptions simulate interruption at 24 points. Physical power loss still needs testing. Windows skips the Unix ownership case.

The toolkit is for synthetic rehearsals. See [development updates](../../DEVELOPMENT_UPDATES.en.md) for the adapter status.
