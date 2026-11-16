# R1 local read-only status

[简体中文](READONLY_STATUS.md) · **English**

On a dedicated R1 rehearsal system, an administrator runs the installer-generated `manager-readonly` entry point. It uses the installed release and service account, takes zero arguments, and returns one bounded JSON line.

The query uses the fixed ledger and returns `not_submitted` if the ledger is absent, leaving it absent. A successful response contains task state, scheduled time, requested model, execution mode, token usage, and a result reference. Errors return fixed codes. Result text, raw configuration, credentials, and logs stay on the local system.

See the [R1 installation rehearsal](../m0_installer/README.en.md) for test commands and scope.
