# 共同账本源码包

**简体中文** · [English](COMMON_PACKAGE.en.md)

从干净的 Git 检出目录使用 Python 3.11 或更新版本。构建 0012 共同安装基线和 0013 外部任务源码包：

```sh
python -B tools/m0_installer/build_common.py build --include-installer --destination common-baseline-output
python -B tools/m0_agent/common_package.py build . common-agent-output
```

分别使用同一工具的 `verify` 子命令，传入构建返回的完整清单和 ZIP SHA-256。验证器离线核对文件和散列。省略 `--include-installer` 得到只有源码的基线包。

已验收的顺序是在独立 Debian 环境显式绑定目标，安装 0012 mixed 基线，再安装 0013 外部任务入口。执行门在明确启用前关闭。安装、启用或更换已有配置前核对包散列和目标身份。QEMU 代理是固定验证配置，其他网络须另行配置和验证。

[双角色任务与测试](../m0_agent/CAMPAIGN.md)
