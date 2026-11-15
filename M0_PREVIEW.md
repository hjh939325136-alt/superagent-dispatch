# M0 预览说明

**简体中文** · [English](M0_PREVIEW.en.md) · [本机试用](README.md) · [开发更新](DEVELOPMENT_UPDATES.md)

如果你想看调度、资料查询和内容流程如何在一台电脑上串起来，可以从这个预览开始。只想试任务调度，可以下载 [稳定版 v0.1.0](https://github.com/hjh939325136-alt/superagent-dispatch/tree/v0.1.0)，在它的仓库根目录运行 `python -B -m sa_dispatch`。两个版本都需要 Python 3.11 或更新版本。

## 可以试什么

- **任务调度**。从预览根目录运行 `python -I -B superagent.py dispatch --help`，可以查看调度器有哪些命令。
- **查询和内容流程**。按 [本机试用步骤](README.md) 生成演示，查看如何读入一条查询、找回样例资料、记录模拟审核结果。查询入口从标准输入读取一条 JSON。请求里的角色字段仅供本机演示选择查询范围，访问身份需要另行验证。
- **文件整理和备份**。运行 `python -B superagent.py retention --help` 查看整理命令，或运行 `python -B superagent.py inventory --help` 查看如何导出清单。备份前需要设置独立的本地目标目录和空间上限，详见[备份实验步骤（英文）](https://github.com/hjh939325136-alt/superagent-dispatch/blob/562daf314fe0f822a1eb04b646010369a3661735/README.md#bound-synthetic-backup)。

## 查询与内容事件

Manager 的查询 JSON 包含 `op`、`role` 和 `args`。可以加上 `"schema_version":"0.1"`，省略版本的旧请求仍可使用。成功与查询错误都会在回复顶部带上 `schema_version`，退出码分别为 `0` 和 `2`。不支持的显式版本会先被拒绝。依赖固定回复字段的外部调用方需要验证自己能读到新增字段。`sources` 条目里的版本仍表示各自的数据格式。

试跑 [内容流程](README.md) 后，查看 `report.json` 的 `event_sync` 和 `event_readback`，可以看到本次同步、重复同步和查询验证结果。调度账本里的 `events.query` 可分页查看事件摘要。用内容来源和 `trace_id` 请求 `trace.get` 可以查看同一组内容事件。当前内容事件还没有自动关联到具体一次派单执行。

从预览仓库根目录运行 Manager 查询兼容测试：

```sh
python -B -m unittest tools.m0_manager.test_protocol
```

## 采集命令的版本

独立的 Collector `sync` 和 `adopt`，包括通过 `manager sync` 或 `manager adopt` 转发的调用，都接受可选的 `"schema_version":"0.1"`。旧请求仍兼容。请求会先检查字段和版本，再读取本机的可信配置。成功与受控的 JSON 错误都会带上版本，退出码分别为 `0` 和 `1`。使用命令行时，如果参数写错，帮助和报错仍由命令行解析器处理。

`ok: true` 表示命令处理完毕。同步是否实际完成要看 `result.state`。如果状态为 `frozen`，先检查来源和缺口，再决定是否通过 `adopt` 由操作员明确接续。版本字段与来源许可或登录身份无关。

## 复查调度器

从预览根目录进入 `dispatch/`。请用未额外配置 `PYTHONPATH` 和 `PYTHONHOME` 的环境运行：

```sh
cd dispatch
python -B -m unittest discover -s tests -v
python -B -m sa_dispatch selftest
```

测试使用 Python 和样例数据，包括读取几种旧版任务记录。具体的历史样例覆盖范围见 [原英文说明](https://github.com/hjh939325136-alt/superagent-dispatch/blob/562daf314fe0f822a1eb04b646010369a3661735/M0_PREVIEW.md#reproduce-the-dispatcher-checks)。Linux systemd 模板和操作说明在 [dispatch/deploy](dispatch/deploy/README.md)，运行上述测试不会安装或启动系统服务。

本版是公开源码预览，验收范围是 Windows 和隔离 Debian 上的本机样例。现有运行服务尚未升级到这版。完整系统上线、服务器安装、24 小时连续运行、跨设备操作和真实数据库迁移仍待验证。源码以 [MIT 协议](LICENSE)公开，`SHA256SUMS` 可用于校验下载到的文件，`SOURCE_PROVENANCE.json` 记录各文件的来源。
