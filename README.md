# SuperAgent M0 source candidate

Requires Python 3.11 or later with SQLite and the standard library. Use `python3` or `py -3.11` if your installation uses that name. Use fresh output directories on a local filesystem. The fixture launchers generate local bindings based on their output directory and the selected Python executable.

## Synthetic acceptance

From the directory containing the candidate, replace `SuperAgent M0` with the extracted directory name. The sibling output directories keep generated data out of the source tree.

```sh
python -B "SuperAgent M0/superagent.py" manager-fixture --output "M0 manager fixture"
python -B "SuperAgent M0/superagent.py" manager --binding "M0 manager fixture/manager-binding.json" < "SuperAgent M0/examples/manager-query.json"
python -B "SuperAgent M0/superagent.py" pipeline-fixture --output "M0 pipeline fixture"
```

For the Manager request in PowerShell, use `Get-Content -Raw "SuperAgent M0/examples/manager-query.json" | python -B "SuperAgent M0/superagent.py" manager --binding "M0 manager fixture/manager-binding.json"` with a UTF-8 pipeline. The pipeline fixture invokes the normal content Dispatcher CLI, reads back pinned research references, and verifies script lineage, approval state, and revocation in `report.json`. The toy script and approval have no human review or publication authority. Bindings contain machine-local absolute paths and must be generated again when relocating. When supplying absolute output or binding paths, omit parent-traversal segments such as `..`.

## Entrypoints

`dispatch selftest` exercises the separate synthetic Dispatcher bundle round trip. `research-search --help`, `research-binding --help`, and `content-contract --help` describe direct fixture operations. `research --config PATH` and `content --config PATH` register local components on the Dispatcher, and the pipeline fixture produces a working example TOML. `manager --binding PATH` consumes one JSON request on stdin. Its role fields are trusted-local selectors and provide no authentication. Use trusted local code, configuration and bindings. `manager sync` and `manager adopt` are explicit operator actions with separate bindings.

`retention init-fixture --new-root PATH` prepares a synthetic binding. `retention --binding PATH plan` produces a plan, and `apply-fixture` and `recover-fixture` require an isolated synthetic fixture and its fixture grant. `inventory --help` describes explicit JSON inventory projections. `tools.m0_storage.target` supplies the local storage API used by Manager.

## Bound synthetic backup

Create a protected, operator-maintained copy of `examples/backup-target.json` outside the source tree. Fill in the existing dedicated absolute target directory, a byte quota at or below 209,715,200, and the directory's `st_dev` and `st_ino` from `os.stat`. Use a trusted absolute `pwsh.exe` on Windows and JSON `null` on Linux. Keep the binding under the same local operator trust as the Python launcher and executable. The binding is local configuration, with no cross-user authentication or permission isolation. Re-enroll it when the target directory or host changes.

Run `python -B superagent.py bound-backup preflight --binding BINDING --plan PLAN` to validate a synthetic allowlist and physical separation before writes. Run the same command with `backup` in place of `preflight` to write the verified archive. Only the binding supplies target location and quota. Plans use `m0.synthetic_backup_plan.v1` with `synthetic_only`, an absolute `source_root`, a unique `op_id`, and a `files` array of relative `path` and `kind` pairs. Linux also requires the library's `linux_devices` declaration, whose identities are checked against the local devices. Restore to a new test directory with `python -B superagent.py backup restore --archive ARCHIVE --target-root NEW_DIRECTORY`; Linux additionally requires `--expected-target-st-dev`. The legacy Windows CLI's direct backup and preflight route remains closed in this candidate. Use `bound-backup` for Windows backup and preflight.

## Coordinator deployment files

`dispatch/deploy/` contains the Linux systemd installer, source check, synthetic state backup and verifier, smoke client, service unit and configuration templates. The source check discovers the included Dispatcher tests and runs its synthetic self-test. The identities example uses placeholders. See `dispatch/deploy/README.md` for target-host steps. This candidate check does not install or start the service.

This source snapshot covers the packaged M0 modules, synthetic fixtures, and launchers. It carries no deployed configuration, live bindings, service data, or operating history. This checkpoint does not establish 24-hour operation, real-data acceptance, cross-device execution, or production deployment. `SOURCE_PROVENANCE.json` records source and staged hashes, including the Windows backup path adjustments. `SHA256SUMS` covers every staged file other than the manifest itself. See `LICENSE` for the MIT terms.
