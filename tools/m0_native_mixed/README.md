# 实验混合运行维护工具

**简体中文** · [English](README.en.md)

本模块用于合成环境。`domains.stage_domains(...)` 在调用方拥有的新空目录创建合成研究与内容数据库、固定绑定、配置和日志目录。它只引用已有账本及 CAS 的位置。启动前会回读配置和绑定并核对源码。失败的暂存目录保留未完成状态。

`maintenance.Coordinator(..., adapter)` 计划并协调一次混合模式激活。计划固定配置字节、原状态和现场证据。协调器按持久阶段处理停写、核对恢复集、激活、配置替换、服务检查和采集接续。中断后先核对原激活凭证，再向前补齐；启动后的新增写入或身份漂移会触发停写待核对。`guard-check` 可以只读检查配置与阶段是否允许启动。

## 运行合成测试

用 Python 3.11 或更新版本，从预览源码根目录运行：

```sh
python -X utf8 -B -m unittest tools.m0_native_mixed.test_domains tools.m0_native_mixed.test_maintenance
```

测试使用临时目录、真实 SQLite/CAS 与激活事务，以及合成服务和恢复适配器。24 个异常中断位置模拟运行中断，实机断电仍待验证。Unix 文件所有权用例在 Windows 按平台跳过。

工具包用于合成演练。开发环境中的适配器进度请见[开发更新](../../DEVELOPMENT_UPDATES.md)。
