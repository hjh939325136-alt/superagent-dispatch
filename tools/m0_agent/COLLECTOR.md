# Debian 常驻 Collector

**简体中文** · [English](COLLECTOR.en.md)

Collector 使用已绑定的 0013 共同账本。独立 systemd 定时器开机后约 30 秒首次唤醒，之后在上次运行结束约 60 秒后再唤醒。每次最多从每条流读取 100 条。任务先检查维护锁和共享账本锁，锁繁忙或关联维护尚未完成时跳过写入。

## 安装与观测

用[共同账本打包工具](../m0_installer/COMMON_PACKAGE.md)构建和验证包含完整 Collector 及双角色模块的源码包。在已绑定的独立 Debian 测试环境，从冻结源码运行 `collector_install.py install`，明确提供 `--expected-machine` 与 `--manifest-sha256`。安装后从暂停状态开始。使用 `enable` 启用、`pause` 暂停、`status` 查看定时器和最近健康观测。

如需升级，从新冻结源码运行 `upgrade`，再提供 `--expected-previous-manifest`。升级保留绑定和健康文件，完成后保持暂停。中断后使用同样的参数重试。

服务禁止网络，运行时看不到模型凭据目录。单次运行上限 45 秒，内存上限 128 MiB。账本连同 SQLite 边车文件默认以 32 MiB 为批次准入水位，文件系统可用空间须超过 256 MiB。日志按速率限制，累计日志大小由部署环境管理。健康状态超过默认 180 秒未更新会标为过期。

当前隔离 VM 验收覆盖定时采集、锁冲突、冷恢复和正常重启。24 小时稳态与长期存储容量仍待验证。
