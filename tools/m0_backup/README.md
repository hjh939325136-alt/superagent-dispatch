# Offline recovery / 离线恢复

[简体中文](#离线合成恢复) · [English](#offline-synthetic-recovery)

## 离线合成恢复

用 Python 3.11 或更高版本，在预览源码根目录运行。请为报告选择一个新文件名。

```sh
python -B tools/m0_backup/run_domain_recovery_lab.py --report recovery-demo.json
```

演练会创建临时合成工作区，模拟写入后回执丢失，等待写入进程退出，再保存研究资料、内容、账本和已知证据。随后把原目录移开，在新位置用独立进程核对历史结果。报告包含本机临时位置，保留在本机即可。

实际捕获仅用于调用者独占、已经停写的合成源，新目标必须可安全创建。恢复时须使用从可信来源保管的清单哈希。默认历史只读。显式演练只核对准入的历史任务，保留未结预算和捕获前撤权。后续撤权未知。普通 v7 继续拒绝待核实的任务。 [中文预览说明](../../M0_PREVIEW.md#离线合成领域恢复)

演练还会实际运行三个合成时槽。恢复后即使第四个时槽已到期，`schedule-status` 也只显示原历史，调度命令仍被拒绝。带调度历史的检查点须用 `0.1.1-m0.8` 或后续兼容工具读取。

## Offline synthetic recovery

Use Python 3.11 or later from the preview source root and choose a new report filename.

```sh
python -B tools/m0_backup/run_domain_recovery_lab.py --report recovery-demo.json
```

The demo creates a temporary synthetic workspace, simulates a lost write acknowledgement, and waits for its writers to exit. It captures the research, content, ledger, and known evidence, moves the original directory aside, then starts a separate process to check the historical result from the restored location. The report contains local temporary locations. Keep it local.

Capture requires an owned, offline synthetic source and a new destination. Restore requires a trusted copy of the manifest hash. Historical access is read-only by default. An explicit rehearsal checks only eligible historical tasks and preserves held usage and revocations recorded in the snapshot. Later revocations remain unverified. Ordinary v7 continues to reject unknown tasks. [English preview guide](../../M0_PREVIEW.en.md#offline-synthetic-domain-recovery)

The demo also runs three synthetic schedule slots before capture. Once the fourth slot is due, `schedule-status` reads historical state and scheduling commands are refused. Use `0.1.1-m0.8` or a later compatible build for checkpoints with schedule history.
