# Manager 运行状态与交接回读

**简体中文** · [English](README.en.md)

在已绑定的 0013 `common_metadata_0013` 来源上，使用原有的 [Manager 标准输入入口](../../README.md) 查询。以下是两条示例请求，请换成绑定中的来源 ID 和 `handoff.list` 返回的交接 ID。

```json
{"op":"ops.snapshot","role":"technical","args":{"source_id":"example-dispatch"}}
{"op":"handoff.get","role":"manager","args":{"source_id":"example-dispatch","handoff_id":"<handoff-id>"}}
```

- `ops.snapshot` 观察已安装共同账本、当前启动会话、白名单服务单元、Collector 和活动授权。权限不足、信息缺失或证据不一致会显示为未知。按需本地服务当前未运行属于正常状态，永久服务缺失仍记为未知。各项观测分别记录时间。
- `handoff.get` 回读一条终态交接，核对生产、接受、审阅、结束和预算结清记录的账本哈希关联。回复标注 `coverage: partial` 与 `content_integrity: not_checked`，表示 CAS 文件内容尚未在此请求核验。证据不完整时可继续用 `handoff.list` 与 `task.get` 查原状态。

已验证的离线安装中，未配置的模型单元显示为 `unconfigured / not_granted`。其他缺失及异常仍为未知。

两种请求都要求显式来源。查询只读取相关状态，结果中省略提示和模型正文。旧请求继续使用原协议。
