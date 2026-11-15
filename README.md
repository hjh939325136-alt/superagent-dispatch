# SuperAgent Dispatch

[English](https://github.com/hjh939325136-alt/superagent-dispatch/blob/v0.1.0/README.md) · [稳定版 v0.1.0](https://github.com/hjh939325136-alt/superagent-dispatch/tree/v0.1.0) · [M0 预览](https://github.com/hjh939325136-alt/superagent-dispatch/tree/preview/m0-source) · [开发更新](https://github.com/hjh939325136-alt/superagent-dispatch/blob/preview/m0-source/DEVELOPMENT_UPDATES.md)

SuperAgent Dispatch 是一个用 Python 写的任务调度工具。一个项目要调用多个脚本或工具时，可以把任务交给 Dispatch，由它记录谁接了任务、任务执行到哪一步、最后拿到了什么结果。记录保存在本机的 SQLite 数据库里，方便事后查验。仓库附带一个内容处理示例，可以用来了解怎么接入自己的脚本。

## 在本机试用

下载源码，用 Python 3.11 或更新版本，在仓库根目录运行。用 Python 是为了让同一套命令能在 Windows 和 Linux 上运行。SQLite 随 Python 一起使用，本机就能保存和查询任务记录，无需额外安装数据库服务。

```sh
python -B -m sa_dispatch selftest
python -B -m unittest discover -s tests -v
```

第一条命令在临时目录创建数据库，用示例数据跑完任务，再检查导出的任务记录能否在新库中读回。第二条运行回归测试，包括权限检查和进程重启后的行为。

## 现在能用到哪一步

稳定版 v0.1.0 覆盖单机任务调度和样例任务交接。Linux 上可以按 [systemd 部署指南（英文）](deploy/README.md) 启动仅本机可访问的示例服务，检查服务重启以及备份能否恢复。SQLite 数据放在本地文件系统，每个数据库由一个调度进程管理。真实业务数据、访问凭据和运行配置由各自系统保管。

真实设备接入、跨设备执行、整机重启后的恢复和真实数据库迁移仍待验证。更多查询、内容流程和备份演示见 [M0 预览](https://github.com/hjh939325136-alt/superagent-dispatch/blob/preview/m0-source/M0_PREVIEW.md)。

## 接入自己的脚本

可以从 [内容处理接入示例](sa_dispatch_hotspot/__init__.py) 开始。脚本读取 Dispatch 生成的 JSON 请求文件，完成后返回含原 `request_id` 的 JSON 响应。适配器如何注册、如何测试重复请求，以及如何处理执行结果暂时无法确认的写操作，见 [英文技术说明](https://github.com/hjh939325136-alt/superagent-dispatch/blob/v0.1.0/README.md#use-a-project-adapter)。
