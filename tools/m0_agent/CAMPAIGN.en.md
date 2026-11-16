# Timed producer and reviewer tasks

[简体中文](CAMPAIGN.md) · **English**

Register a time-bounded grant on a ready 0013 shared ledger. Once the producer finishes, the reviewer can claim the handoff for that slot. The roles use separate service users. The ledger records task results and request budgets together.

The installer verifies the existing baseline, target, and source manifest, and checks an offline recovery copy. Use `status` to inspect progress. Use `pause` to stop wakeups, `enable` to resume within the same authorization window, and `stop` to close the grant permanently. Configure the window, slot count, request limit, model, and prompt references explicitly. Provide the provider credential through the designated runtime channel.

## Upgrade the source for an installed grant

Run `upgrade` from the new frozen release with `--expected-machine`, the new `--manifest-sha256`, and `--expected-previous-manifest`. The installer drains current work, verifies a cold recovery copy, and switches the service units. It preserves the grant, task identities, handoffs, and consumed quota, then restores the prior enabled, paused, or stopped phase. Use the same arguments to resume an interrupted upgrade. Ledger or unit drift keeps the startup gate closed.

## Rotate to the next grant

Use `rotate-check` to inspect a candidate. After reviewing its exact bytes, run `rotate` from the current frozen source with `--expected-machine`, `--manifest-sha256`, `--candidate-config`, and `--candidate-sha256`. Rotation verifies a cold recovery copy, preserves the old grant history and budget, and registers the next grant. The next grant starts paused. A separate `enable` starts execution.

Resume an interrupted operation with the same arguments. If the candidate file is missing or its window has expired, `rotate-abort` is available while the candidate is **unregistered**, using the original candidate path and reviewed digest. Cancellation preserves the current configuration and ledger rows and retires the candidate identity. Resume `rotate` with the originally reviewed bytes for a registered candidate.

Run the synthetic tests from the source root:

```sh
python -X utf8 -B -c "import sys,unittest; sys.path[:0]=['dispatch','dispatch/tests']; unittest.main(module=None)" tools.m0_agent.test_campaign_install tools.m0_agent.test_campaign_runtime tools.m0_agent.test_campaign_worker tools.m0_agent.test_campaign_recovery
```

Development acceptance on an isolated Debian VM covered three slots, six live DeepSeek requests, three handoffs, a reboot, and cold recovery. The observation window exceeded 20 minutes. Long-running stability, integration with production agents, and real-data migration still need validation.

[Build and verify the packages](../m0_installer/COMMON_PACKAGE.en.md)
