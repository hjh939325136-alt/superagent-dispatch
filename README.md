# SuperAgent 开发预览

**简体中文** · [Read in English](README.en.md) · [Agent 指南](AGENTS.md) · [文档索引](llms.txt)

**SuperAgent 帮你把几个工具串起来做一件事，并能回查每一步。** 它在本机记录任务由谁领取、进展如何、结果放在哪里。

比如做一条短视频，你可以先找参考资料，再生成一份示例脚本，最后模拟人工审核。跑完后打开一份报告，就能核对脚本用了哪些资料、审核到了哪一步。

| 想试什么 | 从哪里开始 |
| --- | --- |
| 给本机脚本派活、查进度和结果 | [稳定版 Dispatch v0.1.0](https://github.com/hjh939325136-alt/superagent-dispatch/blob/main/README.md) |
| 跑一遍资料查询、示例脚本和模拟审核 | 本页的样例，使用项目自带的合成数据 |

这页是第一阶段的源码预览（M0）。下面的演示在一台电脑上运行。调用关系和当前实现范围见[预览技术说明](M0_PREVIEW.md)。

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

这版预览还包含一套[实验维护工具](tools/m0_native_mixed/README.md)，用于暂存合成研究与内容目录，以及演练混合模式切换中断后的接续。现场采集接续仍待验收。

预览新增[单次模型任务示例](tools/m0_agent/README.md)。开发者可以演练如何把一份获准的合成文本绑定到一个时间槽，由本机工作器领取，再回查结果与 token 用量。模拟超时会把任务标成待核实，以避免自动重复发送。

单次模型任务工作器可以继续使用 `0.1.1-m0.12` 登记的授权和时间槽。回查已结束的任务会返回原结果或待核实状态，避免重复提交。

新增 [R1 安装演练源码](tools/m0_installer/README.md) 和[本机只读状态查询](tools/m0_agent/READONLY_STATUS.md)。可准备一份纯源码演练包，在独立 Debian 环境模拟执行一个预先授权的时间槽，并读取任务状态、用量和结果引用。

## 当前进度

共同源码包仍处于开发验证阶段。详情见[开发更新](DEVELOPMENT_UPDATES.md)。

目前公开的是单机样例和实验维护工具。最新的适配器与安装演练进度见[开发更新](DEVELOPMENT_UPDATES.md)。

公开源码已在 Windows 和隔离 Debian 上跑过本机样例。演示使用合成资料和模拟审核。一套专属合成 Debian 沙盒已实际从 `0011` 升级到 `0012` 隔离模式。升级后核对了原历史任务、未采信迟到审计、统一事件、采集游标和证据对象。混合模式激活、原生研究与内容接线、Collector 逐流显式接续仍待完成。完整 M0 尚未就绪。

交付方向是使用自动安装 U 盘，在现场把系统安装到已核对的新空 SSD。安装镜像和虚拟机验收还待完成，可交付的安装 U 盘尚未就绪。

[预览技术说明](M0_PREVIEW.md) · [中文开发更新](DEVELOPMENT_UPDATES.md)

如果这个项目帮到了你，欢迎[在 GitHub 点 Star](https://github.com/hjh939325136-alt/superagent-dispatch/tree/preview/m0-source)，或 [Fork 仓库](https://github.com/hjh939325136-alt/superagent-dispatch/fork) 来适配自己的流程。
