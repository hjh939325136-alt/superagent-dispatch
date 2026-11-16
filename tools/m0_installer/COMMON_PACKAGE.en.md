# Shared ledger source packages

[简体中文](COMMON_PACKAGE.md) · **English**

Use Python 3.11 or later in a clean Git checkout. Build the 0012 common installation baseline and the 0013 external-task source package:

```sh
python -B tools/m0_installer/build_common.py build --include-installer --destination common-baseline-output
python -B tools/m0_agent/common_package.py build . common-agent-output
```

Use each tool's `verify` command with the complete manifest and ZIP SHA-256 digests printed by its builder. Verification checks files and digests offline. Omitting `--include-installer` builds a source-only baseline package.

The verified sequence binds an isolated Debian target, installs the 0012 mixed baseline, then installs the 0013 external-task entry point. Execution stays closed until explicitly enabled. Verify the package digests and target identity before setup or configuration changes. The pinned QEMU proxy is a validation setting. Other network setups require separate configuration and validation.

[Producer and reviewer tasks and tests](../m0_agent/CAMPAIGN.en.md)
