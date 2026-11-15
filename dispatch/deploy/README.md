# Linux deployment for SuperAgent Dispatch

This systemd package runs the Python dispatch coordinator and SQLite ledger for synthetic tasks. It uses a dedicated service account and IPv4 loopback port `8765`. The existing transport accepts explicit device credentials and supports a synthetic task/receipt handoff. Use Linux with systemd, `flock`, and system Python 3.11 or newer. The synthetic self-test also runs directly from source on other supported Python hosts.

1. From the `dispatch` directory, run `bash deploy/check-coordinator.sh` against the source release. Use a release ID such as the Git commit or release tag you verified.
2. On the target host, run `sudo bash deploy/install-coordinator.sh RELEASE_ID`. This installs root-owned application files and the service unit, keeps existing `/etc/sa-dispatch` settings and `/var/lib/sa-dispatch` state, and leaves service activation to the operator.
3. Provision device credentials. Generate long random tokens on a trusted machine and keep one protected file per device. The following example writes two new files and an identity file to a private temporary directory. Tokens stay out of stdout and the service identity file contains hashes only.

   ```bash
   umask 077
   auth_dir=$(mktemp -d)
   python3 - "$auth_dir" <<'PY'
   import hashlib, json, secrets, sys
   from pathlib import Path

   root = Path(sys.argv[1])
   devices = {}
   for name, role, caps in (('demo-submit', 'dispatcher', []),
                            ('demo-run', 'worker', ['component:mgmtcore'])):
       token = secrets.token_urlsafe(32)
       (root / f'{name}.token').write_text(token + '\n', encoding='utf-8')
       devices[name] = {'token_sha256': hashlib.sha256(token.encode()).hexdigest(),
                        'roles': [role], 'capabilities': caps}
   (root / 'identities.json').write_text(json.dumps({'devices': devices}), encoding='utf-8')
   PY
   sudo install -o root -g sa-dispatch -m 0640 "$auth_dir/identities.json" /etc/sa-dispatch/identities.json
   ```

   For an update, preserve the existing identity file unless intentionally rotating credentials. Keep token files in protected storage and give each client only its assigned token file through the approved channel. Check the example formats in this directory.
4. Start explicitly with `sudo systemctl start sa-dispatch.service`. Use `systemctl status sa-dispatch.service` and `sudo journalctl -u sa-dispatch.service -n 30` for diagnostics. Enable boot startup with `sudo systemctl enable sa-dispatch.service` after a successful smoke run.
5. On the coordinator or an authorized machine using the tunnel below, submit a fixture task, then complete it with the distinct worker identity. Use a fresh key for a new run and repeat a previous key to check deduplication.

   ```bash
   submission=$(python3 deploy/smoke-coordinator.py submit --dispatcher-token-file "$auth_dir/demo-submit.token" --key reboot-fixture-001)
   task_id=$(printf '%s' "$submission" | python3 -c 'import json, sys; print(json.load(sys.stdin)["task_id"])')
   # Optional reboot test: restart the service between the two commands.
   python3 deploy/smoke-coordinator.py complete --worker-token-file "$auth_dir/demo-run.token" --key reboot-fixture-001 --task-id "$task_id"
   ```

When testing over an existing trusted SSH connection, keep the same loopback port on both ends to match the transport's Host check. Run the clients on the forwarding machine. Network authorization and host identity verification are prerequisites for this step.

```bash
ssh -N -o ExitOnForwardFailure=yes -L 127.0.0.1:8765:127.0.0.1:8765 operator@coordinator
```

## Updates and backups

Stop the service with `sudo systemctl stop sa-dispatch.service` before running a replacement installation or state backup. Leave other programs that could access the service state stopped as well. The installer, backup script and service coordinate with a state directory lock. To select a previously installed version while stopped, use `sudo bash deploy/install-coordinator.sh --select RELEASE_ID`. Before restarting, check that its schema supports the current ledger. Keep a verified backup from before the update for schema changes.

```bash
sudo bash deploy/backup-coordinator.sh /path/on/independent-storage/NEW_BACKUP_DIRECTORY
sudo bash deploy/backup-coordinator.sh --verify /path/on/independent-storage/NEW_BACKUP_DIRECTORY
```

The backup uses SQLite's consistent backup API and includes the artifact and journal directories if present. The verifier checks SQLite integrity, foreign keys, and the content hashes of referenced artifacts. Device identity configuration and client credentials are provisioned separately. The scripts use the default service state paths. For a restore rehearsal, copy the verified backup into a fresh private directory and make the owner the user running the rehearsal. Create a test copy of `coordinator.example.toml` pointing all three paths into the restored directory. Provision test identities, start the same release in the foreground with `python3 -B -m sa_dispatch --config TEST_CONFIG serve --identities TEST_IDENTITIES --port 8766`, and use a smoke task or a saved synthetic task key to confirm state and deduplication. Pass `--port 8766` before the smoke command's `submit` or `complete` action. Use `check-coordinator.sh` to run the included regression checks independently of that state.
