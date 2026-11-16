# SuperAgent M0 源码预览

**简体中文** · [English](README.en.md)

**SuperAgent 想让分散在不同工具里的工作有迹可查。** 比如你要用一批资料做一条短视频，可以先查找相关资料，接着生成草稿、交给人审核，回头再看草稿用了哪些资料、审核停在哪一步。

这份 M0 预览可以在一台电脑上用样例跑通这条流程。调度器负责任务和状态，查询工具负责找资料和查结果，采集器把样例脚本生成、模拟审核等事件的摘要和引用汇入同一本任务账。重复同步不会追加重复事件。只想看单机任务调度时，从 [Dispatch v0.1.0 中文首页](https://github.com/hjh939325136-alt/superagent-dispatch/blob/main/README.md) 开始。

## 选一个样例试用

需要 Python 3.11 或更新版本。进入这个预览版的源码目录，先试跑调度器：

```sh
python -I -B superagent.py dispatch selftest
```

想看资料查询或内容流程，再回到源码的上一级目录。把 `superagent-dispatch` 换成实际的源码目录名，每次运行请选择新的本地输出目录：

```sh
python -B "superagent-dispatch/superagent.py" manager-fixture --output "M0 manager fixture"
python -B "superagent-dispatch/superagent.py" pipeline-fixture --output "M0 pipeline fixture"
```

查询样例生成后，可把附带的 JSON 交给查询工具。命令里的 `$PWD` 会提供当前目录的绝对路径。Bash 用法如下：

```sh
python -B "superagent-dispatch/superagent.py" manager --binding "$PWD/M0 manager fixture/manager-binding.json" < "superagent-dispatch/examples/manager-query.json"
```

PowerShell 用法如下：

```powershell
Get-Content -Raw -Encoding utf8 "superagent-dispatch/examples/manager-query.json" | python -B "superagent-dispatch/superagent.py" manager --binding "$PWD/M0 manager fixture/manager-binding.json"
```

跑完内容流程后，打开输出目录中的 `report.json`，可以看到资料引用、样例草稿、模拟审核结果，以及哪些事件已汇入任务账。

## 这一版增加了什么

同一个任务账本现在可以同时给本机命令和本机 HTTP 调度器使用。两边各领自己的任务。混合模式需要维护人员明确启用，普通演示仍走隔离模式。[预览技术说明](M0_PREVIEW.md#隔离与混合执行)列出了尚未开放的功能。

混合模式可以按任务绑定审批和结清合成用量。对于有完整证据的本机研究到内容写入，新版还可以只读查询并确认历史结果。它不会自动重跑写入，也不会恢复已经撤回的资料或批准。

**证据包从 `0.1.1-m0.6` 起须用本版或后续兼容版本导出、验证和导入。** 早期 v7 工具可能漏掉子证据。详见[历史结果核对与工具兼容性](M0_PREVIEW.md#历史结果核对与工具兼容性)。

这版还可以把自己拥有、已经停写的合成工作区恢复到新目录。它保留原任务身份，日常查看默认只读。显式演练只能回查清单内待核实的历史任务，不能重新执行。[离线恢复试用](tools/m0_backup/README.md#离线合成恢复)可以用临时样例跑一遍。

离线恢复现在也保留已结清的调度历史。恢复后可以只读查看原计划和时槽，即使历史上标为启用，已经到期的时槽也不会补跑。

这版新增独立的中央维护入口，可在已激活的本机合成混合账本上设置共享预算、记录交接并查看状态。任务的领取、审批和最终结算继续走各自的执行路由。[中央维护用法](dispatch/docs/design/central_control.md)

离线检查点现在可以经逐条审查保留受支持的未采信迟到回执。它们仍是历史审计记录，不能用来确认任务完成。合成测试还验证了 Collector 从 `0011` 到 `0012` 时如何冻结旧游标并逐流接续。

## 当前进度

公开版在 Windows 和隔离 Debian 上跑过本机样例。演示用合成资料和模拟审核。现役服务尚未采用这些更新，完整系统上线、跨设备执行、24 小时连续运行和真实数据库迁移仍待验证。

[预览技术说明](M0_PREVIEW.md) · [中文开发更新](DEVELOPMENT_UPDATES.md)
