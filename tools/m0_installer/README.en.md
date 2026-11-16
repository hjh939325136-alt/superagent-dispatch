# R1 single-task installation rehearsal

[简体中文](README.md) · **English**

R1 uses a systemd timer to run one fixed synthetic task in an isolated Debian environment and read its state locally. This preview provides source. With Python 3.11 or later, build from a clean checkout into a fresh local directory:

```sh
python -B -m tools.m0_installer.build_r1 --destination rehearsal-output
```

The builder prints SHA-256 digests for the archive and the manifest. Verify the archive before extracting it on the dedicated test system, then provide the manifest digest to the extracted `installer/r1.py install`. For this rehearsal, use `--mode simulation` and specify `--package`, `--manifest-sha256`, `--model`, and `--secret-file`. The credential argument points to a separate synthetic input file readable only by local root. Installation requires Debian systemd, system account tools, and Python. Run as administrator in a fresh installation namespace.

Use `installer/r1.py check` to verify the service and task, or use the [local read-only entry point](../m0_agent/READONLY_STATUS.en.md). Repeat installation requires the same manifest, execution mode, and model. The existing task is reused for the same slot.

## Local tests

```sh
python -X utf8 -B -c "import sys, unittest; sys.path[:0] = ['dispatch', 'dispatch/tests']; unittest.main(module=None)" tools.m0_agent.test_runtime tools.m0_agent.test_readonly_status tools.m0_installer.test_r1
```

Tests use synthetic credentials and simulated model responses. They cover the isolated runtime, package verification, and local read-only status. SSH access requires separate receiver software and authorization. Full installation, real model calls, and long-running operation have separate acceptance requirements.
