# 单次模型任务

**简体中文** · [English](README.en.md)

这组 Python 模块演示怎样给一个获准的模型任务绑定单个时间槽，在本机任务账本里查看状态、结果引用和 token 用量。测试使用合成输入和模拟响应。

- `worker.run_once` 对照 `OperatorGrant` 核对输入引用、内容散列、模型、输出上限和 UTC 时间槽。它使用隔离运行的 Dispatch 账本。同一授权和时间槽的重复触发会复用原任务。
- 工作器调用由调用方传入的 provider 函数。`provider_process.complete_bounded` 提供 DeepSeek HTTPS 实现，给整个子进程设定期限。真实调用只应使用明确获准外发的公开或合成文本。凭据由调用方在运行时提供。
- 收到完整结果后记录引用与用量。输出截断时保留部分结果并标记失败。遇到可能已经发出的超时，任务进入待核实状态，避免自动重试。

## 运行合成测试

使用 Python 3.11 或更新版本，从预览源码根目录运行：

```sh
python -X utf8 -B -c "import sys, unittest; sys.path[:0] = ['dispatch', 'dispatch/tests']; unittest.main(module=None)" tools.m0_agent.test_deepseek_client tools.m0_agent.test_provider_process tools.m0_agent.test_worker test_scheduler test_scheduler_bounds test_scheduler_process test_scheduler_routing
```

测试覆盖单槽竞争、重复触发、成功与超时、凭据通过子进程管道传递，以及原调度行为。连接定时服务和长期运行仍待验证。
