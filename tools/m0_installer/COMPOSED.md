# Debian 共同运行栈离线安装

**简体中文** · [English](COMPOSED.en.md)

面向全新的 Debian systemd 环境。统一入口先建立 0012 mixed 基线，再将同一账本升级到 0013，启用回环 HTTP 协调器和 Collector。观察器安装后暂停。模型授权、凭据、任务和预算无需配置，离线执行门拒绝启动模型任务。

## 安装和续跑

用[共同源码包工具](COMMON_PACKAGE.md)构建并分别校验基线与扩展 ZIP。将已验扩展源码冻结在 root 管理的只读目录，末级目录名使用完整的扩展清单 SHA-256。从该源码运行 `tools/m0_installer/composed.py install`，提供下列参数：

- `--baseline-zip`、`--baseline-manifest-sha256`、`--baseline-zip-sha256`
- `--addon-zip`、`--addon-manifest-sha256`、`--addon-zip-sha256`
- `--expected-machine-id`、`--expected-hostname`

相同参数下，`status` 可只读回报进度。中断后用同一包和绑定重试。已核验组件按回执续接，阶段不完整时须先依照组件证据核对。已完成安装重复执行只回读现状。

验收覆盖离线新装、七个组件回执确认边界、离线封存边界、任务执行、采集及正常重启。支持范围是同包续装。操作系统安装、任意原地升级、任意内部迁移中断及离线转付费授权另行验证。
