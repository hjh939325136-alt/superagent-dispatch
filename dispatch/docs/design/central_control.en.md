# Central maintenance for mixed ledgers

[简体中文](central_control.md) · **English**

Use this entry point on an existing synthetic local ledger after mixed mode has been activated. It can inspect control records, set shared budgets, record handoffs, and expire an individual handoff. Task claims, approvals, final usage settlement, and execution remain with their route-specific interfaces. Central maintenance sends no messages.

## Run the CLI

Run from the `dispatch` source directory. Replace `ACTIVATION_HASH` with the verified digest and the path placeholders with trusted local absolute paths.

```sh
python -B -m sa_dispatch.central_control_cli --central-maintenance --expected-activation-ref "sha256:ACTIVATION_HASH" --ledger "LEDGER_PATH" --artifacts "CAS_PATH" --request "REQUEST_PATH"
```

Example status request:

```json
{"schema":"sa_dispatch.central_control_request.v1","synthetic":true,"operation":"status","params":{}}
```

Supply the explicit maintenance flag and expected activation reference. The ledger, control tables, and CAS must already exist. This operation performs no initialization, migration, or repair. The supported storage boundary is a local regular SQLite file in rollback-journal mode, on paths free of symlinks and junctions. The Windows and Debian checks used local storage.

`status` is read-only. `expiry_due` means an offer is due to expire, and `handoff_expire` changes only the handoff ID specified by the caller. `messages_sent: 0` reports the scope of this entry point. Confirming that a real recipient read a handoff requires separate evidence.

## Shared budgets

`budget_upsert` accepts `budget_id`, `period`, `unit`, `limit_units`, `starts_at`, `ends_at`, and `expected_config_hash`. Use a null hash to create a new budget. To update a limit, send the current `config_hash` from `status`. A competing configuration change requires a fresh review. The budget period, unit, and dates stay fixed. The limit must cover both spent and held usage.

Both execution routes continue to draw from the same ledger allowance. Budget maintenance preserves held usage and an uncertain task's status. A task's own route submits the evidence for final usage settlement.

## Handoffs and evidence

`handoff_offer` accepts `handoff_id`, `from_conv`, `to_conv`, `project_id`, `scope`, `scope_refs`, `expires_at`, `actor`, and `authorization_ref`. The actor must equal the sender. Supported scope labels are `read_status`, `instruct`, and `takeover`. These synthetic labels carry no authority over real conversations.

Use `handoff_offer_hash` to calculate the canonical digest of the offer, including its identities, parties, project, scope, ordered references, and UTC expiration. The authorization CAS document has this exact shape:

```json
{"schema":"sa_dispatch.central_handoff_authorization.v1","synthetic":true,"offer_hash":"sha256:OFFER_HASH","authorized_by":"synthetic-reviewer"}
```

`handoff_transition` accepts `handoff_id`, `state`, `actor`, and `receipt_ref`. The intended recipient can move an offer from `offered` to `accepted` or `rejected`, or from `accepted` to `completed` or `rejected`. A receipt binds the same offer, recipient, and decision:

```json
{"schema":"sa_dispatch.central_handoff_receipt.v1","synthetic":true,"offer_hash":"sha256:OFFER_HASH","recipient":"synthetic-recipient","decision":"accepted"}
```

A `completed` receipt also requires nonempty `evidence_refs`, each readable from CAS and matching its hash. A repeated offer or state receipt is idempotent. Substituting an authorization, extending the expiry, or replacing the completion receipt or outputs is rejected. State changes and their events commit together.

Expired legacy handoffs without the new authorization are visible and can be explicitly expired by ID. Their missing authorization cannot be supplied retroactively to accept or complete them.

## Trust boundary

This is a boundary for trusted local synthetic programs. OS-level isolation, protection against a hostile process on the same account, and human identity verification require separate measures. Synthetic receipts and authorizations do not confer real user permission. Cross-machine management, live data, and continuous deployment still need separate validation.

`tests/test_central_control_cli.py` runs the actual CLI in a subprocess and checks shared allowance across two routes, read-only status, immutable proofs, and completed handoff recovery. Its HTTP route uses a synthetic ledger fixture. It does not establish remote server coverage.
