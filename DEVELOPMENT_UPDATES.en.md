# Development updates

[简体中文](DEVELOPMENT_UPDATES.md) · **English** · [Quick start](README.en.md)

## 0.1.1-m0.1 (2026-11-15)

The first public M0 preview includes instructions for trying the source on your own computer.

- Try task coordination, research queries, simulated content review, inventory, retention, and backup with built-in examples.
- The repository includes compatibility fixtures and Linux service templates. This milestone covers sample workflows. Continuous operation, cross-device execution, and migration of real databases remain to be validated.
- Start with the [M0 quick start](README.en.md) or try single-machine task coordination with [Dispatch v0.1.0](https://github.com/hjh939325136-alt/superagent-dispatch/blob/main/README.en.md).

## 0.1.1-m0.2 (2026-11-15)

The M0 preview can show sample content events, and Manager query replies identify their format version.

- Manager still accepts existing JSON requests. Callers can specify version `0.1`, and JSON replies carry a version on success and on handled query errors.
- The content example adds summaries and references for events such as draft generation and simulated review to the task ledger. Syncing again does not duplicate the batch.
- The quick start gains query and content-event instructions. Validation uses local examples. The running service has yet to adopt these changes.

## 0.1.1-m0.3 (2026-11-15)

The M0 Collector commands also support versioned requests and continue to accept existing requests.

- Collector `sync` and `adopt` accept version `0.1` and validate the request before reading the local configuration. The same format applies when syncing through Manager.
- A gap still freezes sync for operator review. Check the reported state to see whether sync actually finished.
- The preview guide explains requests, replies, and frozen states. The running service has yet to adopt these changes.

## 0.1.1-m0.4 (2026-11-15)

One task ledger can now explicitly enable mixed mode so local commands and a loopback HTTP coordinator each claim their own work.

- The existing demos stay in isolated mode. Manager can query compatible new ledgers read-only, and the export format moves to v7.
- In the synthetic scenario, a lost acknowledgement leaves the write task pending review without an automatic retry.
- Approval administration, budget settlement, and final settlement of uncertain tasks remain unavailable in mixed mode. Domain recovery and switching the running service still need validation.

## 0.1.1-m0.5 (2026-11-15)

Mixed mode can record per-task approvals and settle each task’s usage against evidence.

- Approvals stay with their original task, new rules cannot weaken existing constraints, and usage settlement stays within the route. Read-only status shows tasks by route and labels shared totals as ledger-wide.
- Settling usage leaves an uncertain business outcome pending review, with no automatic replay of the write.
- Global budget configuration, handoff administration, and automatic host settlement remain unavailable. The existing demos stay in isolated mode. The running service has yet to adopt the update.

## 0.1.1-m0.6 (2026-11-15)

The local mixed workflow can now confirm the historical result of an uncertain write through read-only lookup when evidence is complete.

- Supported research-to-content writes record the original request before invocation. Historical approval and component results can be checked without the original journal. Later withdrawal or denial still applies, and the lookup never replays the write.
- Use 0.1.1-m0.6 or a later compatible implementation to export, verify, and import these bundles. Earlier v7 tools can omit supporting evidence and still report a pass. Stop old writers, keep writer versions separate, and avoid in-place downgrades.
- Validation covers trusted local synthetic workflows. Cross-machine rebinding, central control, and upgrades to the running service still need validation.

## 0.1.1-m0.7 (2026-11-15)

An offline synthetic workspace can now be restored to a new location while preserving its original tasks, bindings, and evidence.

- The checkpoint preserves research, content, the ledger, and known evidence. Use an owned, stopped source and a new destination. Ordinary v7 still rejects unknown tasks.
- Historical access is read-only by default. An explicit rehearsal can check only listed historical tasks and never reruns the write. Original identity, held usage, and revocations captured in the snapshot are preserved.
- A checkpoint created on Windows was restored in isolated Debian. Later revocations remain unverified. Live backup, real data, and continuous operation still need validation.

## 0.1.1-m0.8 (2026-11-15)

Offline recovery now preserves settled scheduling history and evidence referenced only by schedule templates.

- Read-only schedule-status shows the original enabled flags, versions, slots, and task records. The demo captures three completed synthetic slots. Recovery refuses scheduling commands even after a fourth slot becomes due.
- Support covers settled standard read-only hotspot.topic_fetch@1 plans on the complete current schema. Legitimate retry diagnostics and deduplicated event summaries are preserved. The summaries have limited evidence about individual slots.
- Use 0.1.1-m0.8 or a later compatible tool for domain checkpoints with schedule history. Ordinary v7 eligibility is unchanged. Pending reservations, custom kinds, and restarting production schedules remain unsupported.

## 0.1.1-m0.9 (2026-11-15)

Activated local synthetic mixed ledgers now have a dedicated central maintenance entry point for shared budgets and handoffs.

- Trusted local callers explicitly select central maintenance and supply the expected activation evidence. Both routes continue to share the ledger budget. A limit update preserves spent and held usage.
- Status is read-only. Explicit expiry affects just the named handoff. A due legacy record can expire without granting authority to accept or complete it.
- Authorization, receipts, and outputs stay bound to their original handoff. Completed handoffs passed export, verification, and restore tests. This entry point runs no tasks and sends no messages.

## 0.1.1-m0.10 (2026-11-16)

Offline checkpoints can preserve two reviewed kinds of ignored late audit. The preview also adds Collector upgrade coverage from 0011 to 0012.

- Original audit rows and evidence objects are preserved as ignored. A late claim cannot establish completion. The rehearsal confirms history through the original invocation’s read-only lookup.
- Collector freezes the old binding after upgrade. Check backlog and anchors before explicitly adopting each stream. The tests confirm existing projections stay intact and repeat sync adds no duplicates. Collector runtime code stays the same.
- Ordinary v7 still rejects every late receipt. An older 0011 ledger alone remains insufficient for complete domain recovery. At publication time, the dedicated sandbox had yet to be upgraded.

## 2026-11-16 sandbox and delivery progress

A dedicated synthetic Debian sandbox has been upgraded from `0011` to `0012` in isolated mode. Post-upgrade checks verified historical tasks, ignored late audit, unified events, Collector cursors, and evidence objects. Mixed-mode activation, native research and content integration, and explicit adoption of each Collector stream are still pending. Full M0 is still in progress.

The delivery plan is to use an automated installation USB drive on-site with a verified new blank SSD. The installation image and virtual-machine validation are still pending, so the USB drive is not ready for delivery.

The sandbox uses the already published `0.1.1-m0.10` source.

## 0.1.1-m0.11 (2026-11-16)

The preview adds experimental tools for staging synthetic domains and resuming interrupted mixed-mode activation.

- Staging prepares a separate synthetic research and content directory and checks its configuration and bindings. The source ledger and evidence store stay untouched.
- Persistent phases coordinate stopping writes, activation, and configuration replacement. Tests use real SQLite/CAS with synthetic adapters and simulate interruption by raising exceptions at 24 points.
- A real Debian adapter, a complete recovery set, systemd start guards, and live Collector adoption are still pending. The dedicated sandbox still runs in isolated mode.

## 2026-11-16 Adapter and installation rehearsal progress

Development now includes a mixed-mode adapter and recovery-set capture for a fixed Debian layout. Synthetic checks used the same 147 frozen files. Windows passed 51 checks and skipped one Unix ownership check. Debian passed all 52. The tests simulated service control. Live mixed-mode activation and Collector adoption still need validation.

A separate Debian installation rehearsal used a test credential to verify first installation, repeat installation, and credential loading after a service restart. The installation image, machine reboot, and long-running operation still need validation.

## 0.1.1-m0.12 (2026-11-16)

This release adds a one-shot model task example. Use synthetic text to rehearse scheduling, claiming a task, and reading its result and token usage.

- The worker runs against an explicitly approved text reference and hash. Repeated triggers for the same grant and time slot reuse the existing task.
- The DeepSeek HTTPS client can run in a subprocess with a deadline for the whole call. A simulated timeout leaves the outcome unknown to avoid an automatic resubmission.
- Scheduling and task claims can filter by schedule or task ID. The public package passed 62 tests on each of Windows and Debian, with zero real model calls. Timer service integration and long-running operation still need validation.

## 2026-11-16 Scheduled task installation rehearsal

A systemd timer completed one synthetic task in an isolated Debian sandbox. Reinstalling and starting the service again kept one task and one attempt in the ledger. Debian passed 46 tests. Windows passed 45 and skipped one POSIX-only check. A simulator supplied the model response. Real model calls: 0.

The public preview continues to use the one-shot model task example. Compatibility with existing preview ledgers, the installation image, and long-running operation still need validation.

## 0.1.1-m0.13 (2026-11-16)

The one-shot worker can resume grants and tasks recorded by 0.1.1-m0.12 and return their original results.

- Legacy calls keep their original grant and result formats. Explicit null mode is rejected, and a registered time slot cannot switch modes.
- The tests use the original worker source to create synthetic ledgers and exercise queued work, a lost acknowledgement after commit, lease expiry, and terminal replay. Existing tasks retain their identity and attempt count.
- The public package passed 69 tests on each of Windows and Debian with zero real model calls. Validation covers the portable worker and scheduler. The installer package has its own validation.

## 0.1.1-m0.14 (2026-11-16)

This release provides source for the R1 single-task simulation installer and a local read-only status command.

- Build a rehearsal package from a clean checkout. On a dedicated Debian system, a separate systemd timer can rehearse one authorized time slot.
- The local query returns task state, schedule time, usage, and the result reference. It leaves a missing ledger absent.
- Remote credential delivery and the SSH query client are outside this release. They require separate receiver software and authorization.
- Public source passed 93 tests on Debian. Windows passed 91 and skipped two POSIX-only cases. Both platforms rebuilt and verified the package offline. System installation, live SSH access, and real model calls have separate acceptance requirements.

## 2026-11-16 Common-source package progress

The development candidate includes a common-source builder and offline verifier for research, content, and synthetic HTTP tasks that use a shared Dispatch ledger. The verifier checks the file set and digests without Git, extraction, or execution of package contents.

The distribution is `source_candidate` and `installer_entry` is `null`. The common installer entry point is still in development. Target validation compares the expected manifest digest and machine identity. Fresh VM installation and real systemd startup still need verification.

The same development candidate passed 45 tests on Windows and 35 on Debian. The 10 developer-side build tests ran only on Windows because the Debian test environment had no Git. Target-side offline verification passed on Debian. This update documents the progress while the public build dependencies are being completed.

## 0.1.1-m0.14-docs.1 (2026-11-16)

The 0013 shared ledger completed an isolated VM milestone. This documentation update describes the results. Public runtime source remains at 0.1.1-m0.14.

- An isolated Debian VM upgraded its shared ledger from 0012 to 0013. The same ledger recorded synthetic HTTP and local tasks and one authorized live DeepSeek task. After a VM reboot and a repeat installation, the model task still had one attempt and used quota was preserved.
- A rehearsal restored the checkpoint to a quarantine directory with execution disabled. Independent-disk disaster recovery and long-running multi-task operation still need validation.
- The development source depends on a common-install baseline outside the public tree and network settings specific to the test environment. A public deployable package requires separate validation.

## 0.1.1-m0.15 (2026-11-16)

This preview publishes shared ledger 0013, the common install baseline, and timed producer and reviewer source. Each handoff is scoped to one slot in the shared ledger.

- Development acceptance on an isolated Debian VM completed three slots, six live DeepSeek requests using 644 tokens, and three handoffs. Pause and resume and a full reboot preserved prior attempts and budgets.
- A cold recovery copy was restored to an isolated directory with execution disabled. Explicit Collector adoption replayed 30 events, preserved the original gap record, and left zero lag on three streams.
- The public source packages built and verified offline. Windows passed 228 tests with two POSIX skips. Debian passed 217 tests. The 13 Git-dependent build tests ran on Windows.
- The VM observation lasted at least 20 minutes. A 24-hour run, native agent integration, production data migration, and recovery onto independent hardware still need validation.

## 0.1.1-m0.16 (2026-11-16)

Timed producer and reviewer jobs can upgrade frozen source while preserving the grant, tasks, handoffs, and consumed quota.

- The upgrade drains work and verifies a cold recovery copy before switching service units. Ledger or unit drift keeps the startup gate closed.
- On an isolated Debian VM, the upgrade preserved six units of consumed request quota and three handoffs. Cold recovery, replay with the same arguments, and another normal reboot passed, with zero additional model calls.
- The rotate-check command performs a read-only preflight for the next grant. Actual rotation and 24-hour steady operation still need validation. The public candidate passed 28 focused tests on each of Windows and Debian.

## 0.1.1-m0.17 (2026-11-16)

Timed producer and reviewer jobs can rotate to a new grant or cancel an unregistered candidate. The new grant starts paused.

- An isolated Debian VM resumed rotation after a process exit following registration. Prior records and budgets were preserved, a repeated command returned the same receipt, and the new grant stayed paused after reboot.
- Only an unregistered candidate can be canceled. A missing candidate can be canceled using its original path and reviewed digest. Cancellation is idempotent, registered candidates are refused, and the ledger bytes stay identical across cancellation.
- The initial verification script compared digests across the cold-backup phase. The corrected check verifies prior row digests and compares ledger bytes immediately before and after cancellation. Cold recovery preserved seven tasks, seven attempts, and three handoffs. Collector lag was zero across three streams. Additional model calls: zero.
- The public candidate passed 32 focused tests on each of Windows and Debian. A 24-hour soak, long-run budgeting, and native agent integration still need validation.

## 0.1.1-m0.18 (2026-11-16)

Shared ledger 0013 adds a resident Collector for Debian with bounded scheduled batches and recent health observations.

- A dedicated systemd timer maintains three streams. Writes are skipped when the maintenance or ledger lock is busy. Capacity thresholds gate the next batch, while logging limits control the write rate.
- An isolated Debian VM automatically collected five new events. Neither lock contention case wrote to the ledger. Timed collection resumed after a normal reboot, and all three streams had zero lag.
- Cold recovery matched seven tables and preserved seven original tasks, seven attempts, three handoffs, and the budgets. Additional model calls: zero.
- The public candidate passed 63 tests on Windows with one Linux-only lock test skipped, and 61 on Debian. Both source package types were rebuilt and verified offline. A 24-hour soak and full-system delivery still need validation.

## 0.1.1-m0.19 (2026-11-16)

Manager adds read-only ops.snapshot and handoff.get for operational observations and single-handoff evidence on a shared 0013 ledger.

- Both requests require an explicitly bound source. The existing protocol and operations remain compatible. Snapshot observations carry their own times. An idle on-demand local service is expected, and a missing permanent service stays unknown.
- The handoff query verifies ledger hash linkage, reports partial coverage, and marks CAS content integrity as unchecked.
- The stdin CLI checked service health and three existing handoff chains in an isolated Debian VM. Database and WAL bytes and modification times matched before and after. Additional model calls: zero.
- The public candidate passed 47 tests on each of Windows and Debian. Native product session integration and 24-hour steady operation still need validation.

## 0.1.1-m0.20 (2026-11-16)

The preview adds a 24-hour observation entry point for the shared coordinator and Collector, with startup validated on an isolated Debian VM.

- The observer samples through the read-only Manager path. A record is capped at 512 samples and 512 KiB. The journal rate limit controls write rate.
- The isolated VM recorded two good natural samples, and repeated enable kept the original start and deadline. The window was still running when checked. The 24-hour result is pending.
- Observation covers the shared coordinator and Collector. Model timers stayed disabled, prior tasks, attempts, and handoffs were preserved, and no new model calls occurred.
- Windows passed 39 tests with one Linux-only lock test skipped. Debian passed 40. Simulated clock and pause-race tests verified decision logic and lock ordering.
