# SuperAgent Dispatch

**简体中文** · [English](README.en.md)

**SuperAgent 想解决的是：一项工作交给几个工具去做时，怎么知道谁接了、做到哪一步、结果在哪里？** Dispatch 是它的任务调度器。你的项目提交一项任务，能处理它的工具领取并执行，Dispatch 把状态和结果留在本机，方便你回查和接着处理。

比如你在整理一批素材，可以让一个脚本生成候选清单，再让另一个工具处理选中的条目。如果中间卡住，可以回看哪个任务已完成、哪个还需要处理，以及已有结果在哪里。

## 从哪里开始

| 你想看什么 | 从这里开始 |
| --- | --- |
| 一台电脑上怎么派任务、记状态、查结果 | 本页的 Dispatch v0.1.0 自检 |
| 怎么串起资料查询、样例脚本生成和模拟审核 | [M0 预览试用](https://github.com/hjh939325136-alt/superagent-dispatch/blob/preview/m0-source/README.md) |

这个 GitHub 仓库提供源码和可以本机运行的样例。v0.1.0 已验证单机任务调度和样例交接。M0 预览展示更长的流程，完整系统上线、跨设备执行、24 小时运行和真实数据库迁移仍待验证。

## 在本机试用

用 Python 3.11 或更新版本。在终端进入解压或克隆后的目录，运行下面第一条命令即可。第二条是可选的回归测试。

```sh
python -B -m sa_dispatch selftest
python -B -m unittest discover -s tests -v
```

自检会用内置样例执行任务，并检查能否从导出的任务记录找回结果。它在临时目录使用 SQLite，无需额外运行数据库服务。

## 继续了解

- [v0.1.0 源码](https://github.com/hjh939325136-alt/superagent-dispatch/tree/v0.1.0)
- [接入自己的脚本（英文技术说明）](README.en.md#connect-your-own-script)
- [Linux 单机示例部署（英文）](deploy/README.md)
- [中文开发更新](https://github.com/hjh939325136-alt/superagent-dispatch/blob/preview/m0-source/DEVELOPMENT_UPDATES.md)
