# Timed producer and reviewer tasks

[简体中文](CAMPAIGN.md) · **English**

Register a time-bounded grant on a ready 0013 shared ledger. Once the producer finishes, the reviewer can claim the handoff for that slot. The roles use separate service users. The ledger records task results and request budgets together.

The installer verifies the existing baseline, target, and source manifest, and checks an offline recovery copy. Use `status` to inspect progress. Use `pause` to stop wakeups, `enable` to resume within the same authorization window, and `stop` to close the grant permanently. Configure the window, slot count, request limit, model, and prompt references explicitly. Provide the provider credential through the designated runtime channel.

## Upgrade the source for an installed grant

Run `upgrade` from the new frozen release with `--expected-machine`, the new `--manifest-sha256`, and `--expected-previous-manifest`. The installer drains current work, verifies a cold recovery copy, and switches the service units. It preserves the grant, task identities, handoffs, and consumed quota, then restores the prior enabled, paused, or stopped phase. Use the same arguments to resume an interrupted upgrade. Ledger or unit drift keeps the startup gate closed.

`rotate-check` takes `--expected-machine`, the current `--manifest-sha256`, and `--candidate-config`. It checks the next grant while the current grant is paused or expired and no requests are held. It validates the candidate configuration, prompts, and identities. Changing or enabling the grant requires a separate operation.

Run the synthetic tests from the source root:

```sh
python -X utf8 -B -c "import sys,unittest; sys.path[:0]=['dispatch','dispatch/tests']; unittest.main(module=None)" tools.m0_agent.test_campaign_install tools.m0_agent.test_campaign_runtime tools.m0_agent.test_campaign_worker tools.m0_agent.test_campaign_recovery
```

Development acceptance on an isolated Debian VM covered three slots, six live DeepSeek requests, three handoffs, a reboot, and cold recovery. The observation window exceeded 20 minutes. Long-running stability, integration with production agents, and real-data migration still need validation.

[Build and verify the packages](../m0_installer/COMMON_PACKAGE.en.md)
