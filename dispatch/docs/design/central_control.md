# 混合账本的中央管理入口

**简体中文** · [English](central_control.en.md)

这个入口用于已有的、完成 mixed 激活的本机合成账本。它允许总管查询全部控制记录、配置共享预算、记录交接及显式过期。执行端继续使用原来的 `control_cli` 和 `route_task` 权限；中央入口不会代理任务领取、审批、结算、执行或向其他对话发送消息。

## 调用

在 Dispatch 源码目录运行，路径替换为已核验的本机绝对路径：

```sh
python -B -m sa_dispatch.central_control_cli --central-maintenance --expected-activation-ref "sha256:ACTIVATION_HASH" --ledger "LEDGER_PATH" --artifacts "CAS_PATH" --request "REQUEST_PATH"
```

查询请求：

```json
{"schema":"sa_dispatch.central_control_request.v1","synthetic":true,"operation":"status","params":{}}
```

必须显式选择中央维护入口和预期激活引用。账本、控制表及 CAS 必须已经存在；入口不初始化、不迁移、不修复数据库。仅支持本地普通文件、rollback journal 模式的 SQLite 和无符号链接/联接点的路径。Windows 本地路径和 Debian 临时目录分别测试；这不是网络共享盘支持声明。

`status` 查询不修改过期交接。结果中的 `expiry_due` 只表示该交接已到期；只有对指定 ID 调用 `handoff_expire` 才记录过期事件。结果中的 `messages_sent: 0` 表明本入口没有发送消息，并不证明任何外部接收方读过交接。

## 预算

`budget_upsert` 的参数是 `budget_id`、`period`、`unit`、`limit_units`、`starts_at`、`ends_at` 和 `expected_config_hash`。

- 新建时传 `expected_config_hash: null`。已存在同名配置会拒绝，避免覆盖。
- 修改时先读取当前 `config_hash`，再提交该值。其他管理者已改配置时必须重新核对。
- 预算周期、单位及起止时间不可修改。上限不能低于当前已用额度与预留额度之和。
- 配置散列不包含动态用量；执行端并发领取仍在同一 SQLite 写事务中检查、预留共享额度。

中央配置不会清除任务预留，也不会把 `unknown` 任务变为成功。最终用量仍由对应执行路由提交证据结算。

## 交接及证据

`handoff_offer` 参数包括 `handoff_id`、`from_conv`、`to_conv`、`project_id`、`scope`、`scope_refs`、`expires_at`、`actor` 和 `authorization_ref`。`actor` 必须等于发送方；范围限 `read_status`、`instruct`、`takeover`。本入口中的这些名字是合成标签，不能据此取得真实对话操控权限。

使用 `handoff_offer_hash` 计算规范化 offer 散列：包含上述交接身份、双方、项目、范围、按顺序排列的引用及 UTC 到期时间。授权 CAS 文档必须恰好包含：

```json
{"schema":"sa_dispatch.central_handoff_authorization.v1","synthetic":true,"offer_hash":"sha256:<offer散列>","authorized_by":"synthetic-reviewer"}
```

`handoff_transition` 参数为 `handoff_id`、`state`、`actor`、`receipt_ref`。只有接收方可以确认。支持 offered→accepted/rejected、accepted→completed/rejected。回执必须恰好包含：

```json
{"schema":"sa_dispatch.central_handoff_receipt.v1","synthetic":true,"offer_hash":"sha256:<offer散列>","recipient":"synthetic-recipient","decision":"accepted"}
```

完成回执使用 `decision: completed` 并增加非空 `evidence_refs` 数组，每个输出必须能从 CAS 读取且散列正确。重复提交同一 offer 或同一状态回执不会重复记录事件；更换授权、延长到期时间、替换完成回执或输出均被拒绝。失败请求不顺带修改其他过期交接。

历史上未绑定该授权契约的交接可查询，不能用新入口静默补授权后接受或完成。中央维护者可以显式使其中指定的已到期交接过期，以解除旧活动记录对迁移的阻塞；这只撤销活动状态，不授予新权限。交接状态与对应事件在同一事务提交。

## 信任范围

这是可信本机程序的合成管理边界，不是操作系统隔离、人类身份认证或防范同账户恶意程序的机制。合成授权和回执不代表真实用户许可。真实跨机管理、长驻 Agent、业务数据和安装盘部署仍需分别验收。

集成测试 `tests/test_central_control_cli.py` 使用实际子进程调用入口，覆盖两路共享额度、只读状态、证据不可替换及权限边界；HTTP 路由的已授权交接标记是测试夹具，不声称启动或测试了远程服务器。
