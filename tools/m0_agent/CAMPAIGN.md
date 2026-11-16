# 限时双角色任务

**简体中文** · [English](CAMPAIGN.en.md)

在已就绪的 0013 共同账本上登记限时授权。生产者交付结果后，审阅者只能领取同一时槽的交接。各角色以不同服务用户运行，预算和任务结果仍可在同一本账里查询。

安装程序核对既有基线、目标机器与源码清单，离线验证恢复副本。使用 `status` 查询。用 `pause` 暂停唤醒，在同一授权窗口内用 `enable` 恢复。用 `stop` 永久关闭当前授权。设置授权时需明确时间窗口、时槽数、总请求上限、模型与提示引用。供应商凭据只在指定运行时通道提供。

## 升级已安装授权的源码

从新冻结源码执行 `upgrade`，填写 `--expected-machine`、新 `--manifest-sha256` 和 `--expected-previous-manifest`。维护入口会等待当前工作结束，验证冷恢复副本，再切换服务单元。原授权、任务身份、交接和已用额度继续保留，原启用、暂停或永久停止状态会恢复。中断后可用相同参数重试。若账本或单元发生漂移，启动门会保持关闭。

## 切换到下一批授权

先用 `rotate-check` 只读核对候选。审核文件的确切字节后，从当前冻结源码运行 `rotate`，提供 `--expected-machine`、`--manifest-sha256`、`--candidate-config` 和 `--candidate-sha256`。轮换验证冷恢复副本、保留旧授权历史与预算，并登记新授权。新授权以暂停状态落地，由单独的 `enable` 开启执行。

进程退出后用相同参数续跑。若候选文件丢失或窗口已过期，只有在候选**尚未登记**时才可用 `rotate-abort`，仍须提供原候选路径和审核散列。取消保留当前配置和账本记录，同一候选身份不能复用。已登记的候选应以原审核字节续跑 `rotate`。

在源码根目录跑合成测试：

```sh
python -X utf8 -B -c "import sys,unittest; sys.path[:0]=['dispatch','dispatch/tests']; unittest.main(module=None)" tools.m0_agent.test_campaign_install tools.m0_agent.test_campaign_runtime tools.m0_agent.test_campaign_worker tools.m0_agent.test_campaign_recovery
```

开发验收在隔离 Debian VM 中覆盖三个时槽、六次实际 DeepSeek 请求、三次交接、重启和冷恢复。当前验证窗口为 20 分钟以上。长时间稳定性、现役多 Agent 接管和真实业务数据迁移仍待验收。

[构建和校验共同源码包](../m0_installer/COMMON_PACKAGE.md)
