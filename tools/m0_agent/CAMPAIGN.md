# 限时双角色任务

**简体中文** · [English](CAMPAIGN.en.md)

在已就绪的 0013 共同账本上登记限时授权。生产者交付结果后，审阅者只能领取同一时槽的交接。各角色以不同服务用户运行，预算和任务结果仍可在同一本账里查询。

安装程序核对既有基线、目标机器与源码清单，离线验证恢复副本。使用 `status` 查询。用 `pause` 暂停唤醒，在同一授权窗口内用 `enable` 恢复。用 `stop` 永久关闭当前授权。设置授权时需明确时间窗口、时槽数、总请求上限、模型与提示引用。供应商凭据只在指定运行时通道提供。

在源码根目录跑合成测试：

```sh
python -X utf8 -B -c "import sys,unittest; sys.path[:0]=['dispatch','dispatch/tests']; unittest.main(module=None)" tools.m0_agent.test_campaign_install tools.m0_agent.test_campaign_runtime tools.m0_agent.test_campaign_worker tools.m0_agent.test_campaign_recovery
```

开发验收在隔离 Debian VM 中覆盖三个时槽、六次实际 DeepSeek 请求、三次交接、重启和冷恢复。当前验证窗口为 20 分钟以上。长时间稳定性、现役多 Agent 接管和真实业务数据迁移仍待验收。

[构建和校验共同源码包](../m0_installer/COMMON_PACKAGE.md)
