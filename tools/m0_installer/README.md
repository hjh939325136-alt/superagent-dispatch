# R1 单任务安装演练

**简体中文** · [English](README.en.md)

R1 在独立 Debian 环境里用 systemd 定时器执行一条预先确定的合成任务，并从本机读取状态。公开预览提供源码。使用 Python 3.11 或更新版本，从干净的预览源码检出目录构建到新的本地目录：

```sh
python -B -m tools.m0_installer.build_r1 --destination rehearsal-output
```

构建输出包含软件包与清单的 SHA-256。在专用测试系统解包前核对软件包哈希，安装时将清单哈希传给解包后的 `installer/r1.py install`。本次演练使用 `--mode simulation`，并明确指定 `--package`、`--manifest-sha256`、`--model` 和 `--secret-file`。凭据参数指向单独创建、仅本机 root 可读的合成输入文件。正常安装依赖 Debian 的 systemd、系统账户管理工具和 Python，须在全新安装命名空间中由管理员运行。

安装后，使用 `installer/r1.py check` 核对服务和任务结果，或通过[本机只读入口](../m0_agent/READONLY_STATUS.md)查询。重复安装要求相同清单、执行模式和模型。同一个时间槽继续沿用原任务。

## 本地测试

```sh
python -X utf8 -B -c "import sys, unittest; sys.path[:0] = ['dispatch', 'dispatch/tests']; unittest.main(module=None)" tools.m0_agent.test_runtime tools.m0_agent.test_readonly_status tools.m0_installer.test_r1
```

测试使用合成凭据和模拟模型响应。它们覆盖隔离运行时、软件包校验以及本机只读状态。SSH 访问需要另行提供接收端和授权配置。完整系统安装、真实模型调用和长期运行另行验收。
