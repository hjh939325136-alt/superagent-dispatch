# One-shot model task

[简体中文](README.md) · **English**

These Python modules demonstrate how to bind one approved model task to a single time slot and inspect its state, result reference, and token usage in the local task ledger. The tests use synthetic inputs and simulated responses.

- `worker.run_once` checks the input reference, content hash, model, output limit, and UTC time slot against an `OperatorGrant`. It uses an isolated Dispatch ledger. Repeated triggers for the same grant and slot reuse the existing task.
- The worker calls a provider function supplied by the caller. `provider_process.complete_bounded` provides a DeepSeek HTTPS implementation with a deadline for the subprocess. Real calls require explicit approval to send the public or synthetic input. The caller supplies the credential at runtime.
- A complete response records a result reference and token usage. A truncated response preserves the partial result and marks the task failed. A timeout after a possible send leaves the outcome unknown to avoid an automatic retry.

## Run the synthetic tests

Use Python 3.11 or later. Run this from the preview source root:

```sh
python -X utf8 -B -c "import sys, unittest; sys.path[:0] = ['dispatch', 'dispatch/tests']; unittest.main(module=None)" tools.m0_agent.test_deepseek_client tools.m0_agent.test_provider_process tools.m0_agent.test_worker test_scheduler test_scheduler_bounds test_scheduler_process test_scheduler_routing
```

The tests cover concurrent claims for one slot, repeated triggers, success and timeout, credential transfer over a subprocess pipe, and existing scheduling behavior. See the [development updates](../../DEVELOPMENT_UPDATES.en.md) for the isolated Debian timer rehearsal. Long-running operation still needs validation.
