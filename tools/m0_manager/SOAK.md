# 共同协调器与 Collector 运行观察

**简体中文** · [English](SOAK.en.md)

观察器约每五分钟用 Manager `ops.snapshot` 采样一次已绑定的 0013 共同协调器和 Collector。本流程只观察既有任务和预算。模型定时器和模型任务由独立流程管理。

在独立、已绑定的 Debian 验证环境，用[共同源码包](../m0_installer/COMMON_PACKAGE.md)校验并冻结源码。运行冻结源码里的 `soak_install.py install`，显式提供 `--expected-machine` 和 `--manifest-sha256`。初始状态为暂停。`enable` 要求首次只读探测合格，再登记窗口。重试启用会复用同一截止时间。`status` 读取当前窗口，`pause` 停止尚未完成的观察并保留记录。

服务只写观察目录，网络和模型凭据均隔离。每次运行上限 20 秒，内存上限 128 MiB。观察记录最多 512 个样本、512 KiB。journal 限速只控制写入速率。

完整观察要求同一次启动周期、连续的时钟、至少 145 个有效样本，且样本间隔不超过 600 秒。当前开发 VM 已启动并取得两个合格样本，24 小时结果仍待验证。
