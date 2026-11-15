# SuperAgent M0 源码预览

[English](https://github.com/hjh939325136-alt/superagent-dispatch/blob/562daf314fe0f822a1eb04b646010369a3661735/README.md) · [预览说明](M0_PREVIEW.md) · [开发更新](DEVELOPMENT_UPDATES.md) · [稳定版 v0.1.0 中文说明](https://github.com/hjh939325136-alt/superagent-dispatch#readme)

M0 是 SuperAgent 的早期源码预览。在任务调度之外，它还提供几组本机演示。你可以查询样例项目记录、试走从资料检索到内容审核的流程，或者查看文件整理计划、试做备份和恢复。

## 用样例跑一遍

需要 Python 3.11 或更新版本及其 SQLite 模块。先进入预览仓库根目录，试跑调度器：

```sh
python -I -B superagent.py dispatch selftest
```

再回到仓库的上一级目录运行下方命令。将 `superagent-dispatch` 换成你解压或克隆得到的目录名。每次运行请选新的本地输出目录。

```sh
python -B "superagent-dispatch/superagent.py" manager-fixture --output "M0 manager fixture"
python -B "superagent-dispatch/superagent.py" pipeline-fixture --output "M0 pipeline fixture"
```

生成查询演示后，可以用附带的 JSON 提问。查询命令需要配置文件的绝对路径，`$PWD` 会带入当前目录。在 Bash 等支持输入重定向的 shell 中运行：

```sh
python -B "superagent-dispatch/superagent.py" manager --binding "$PWD/M0 manager fixture/manager-binding.json" < "superagent-dispatch/examples/manager-query.json"
```

PowerShell 请使用 UTF-8 管道：

```powershell
Get-Content -Raw -Encoding utf8 "superagent-dispatch/examples/manager-query.json" | python -B "superagent-dispatch/superagent.py" manager --binding "$PWD/M0 manager fixture/manager-binding.json"
```

内容流程的 `report.json` 可以看到用了哪些资料、样例脚本从哪一步生成、模拟审核是否通过和撤销。演示使用程序生成的数据和模拟审核，接入真实资料、执行正式审核或发布需要单独验证。

## 目前的进度

当前公开源码可以在本机试跑。完整系统上线、服务器安装、24 小时连续运行、跨设备执行和真实数据库迁移仍待验证。演示配置里有本机生成的绝对路径，换机器或移动目录后请重新生成。真实凭据和业务数据请留在原系统。

[预览说明](M0_PREVIEW.md)列出调度器测试、文件整理和部署模板。备份的目标配置、预检及恢复参数见 [英文进阶说明](https://github.com/hjh939325136-alt/superagent-dispatch/blob/562daf314fe0f822a1eb04b646010369a3661735/README.md#bound-synthetic-backup)。
