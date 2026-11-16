# Manager operational and handoff queries

[简体中文](README.md) · **English**

Use the existing [Manager stdin entry point](../../README.en.md) with a bound 0013 `common_metadata_0013` source. Replace the example source ID and the handoff ID with values from the binding and `handoff.list`.

```json
{"op":"ops.snapshot","role":"technical","args":{"source_id":"example-dispatch"}}
{"op":"handoff.get","role":"manager","args":{"source_id":"example-dispatch","handoff_id":"<handoff-id>"}}
```

- `ops.snapshot` observes the installed shared ledger, current boot, allowed units, Collector, and active grant. Missing, inaccessible, or inconsistent observations remain unknown. An idle on-demand local unit is expected. A missing permanent unit stays unknown. Each observation carries its own timestamp.
- `handoff.get` reads one terminal handoff and checks the ledger hash linkage across producer, acceptance, review, terminal receipt, and budget settlement. The reply uses `coverage: partial` and `content_integrity: not_checked` because this query leaves CAS content verification to a separate check. Use `handoff.list` or `task.get` when the evidence is incomplete.

A verified offline installation reports unconfigured model units as `unconfigured / not_granted`. Other missing or invalid observations remain unknown.

Both queries require an explicit source. They read status and omit prompts and model response text. Existing requests keep their protocol.
