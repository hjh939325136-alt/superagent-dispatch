# Isolated Hermes comparison

[简体中文](README.md) · **English**

The module compares a fixed worker with a pinned upstream Hermes 0.16.0 candidate using synthetic material. Hermes communicates with a trusted broker over Unix sockets. The broker checks requests, tools, and actual receipts and allows only fixture search and retrieval. The experiment ledger limits requests and keeps reservations for unknown outcomes.

Given a complete response and trusted forwarding receipt, reconciliation checks the original task, request digest, and lease before settling the same unknown request. It preserves the original audit event and sends zero provider replays. Host conversation recovery requires separate validation.

Run the offline tests from the repository root:

```sh
python -X utf8 -B -m unittest discover -s tools/m0_compare -t . -v
```

Native tests need a separately prepared, pinned Linux Hermes environment. The public modules use synthetic configuration and receipts. A real provider connection requires a separately configured trusted forwarder and authorization. The experiment is separate from the default installation packages.

Development testing used two live worker calls and 18 live Hermes calls. Both passed two summary rounds. Ordinary retrieval did not deliver. After a transient failure, two current-version reads were verified, and the final output hit the 512-token limit. The server denied both restricted requests. The denial wording still needs work. Conservative prediction gated some later requests. These results cover the small trial.
