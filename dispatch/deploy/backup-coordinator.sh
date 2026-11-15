#!/usr/bin/env bash
set -euo pipefail
umask 077

usage() {
    cat <<'EOF'
Usage: sudo bash backup-coordinator.sh NEW_BACKUP_DIRECTORY
       bash backup-coordinator.sh --verify BACKUP_DIRECTORY

Back up the default synthetic coordinator while its service is stopped. Keep the
backup directory on an independent device for recovery beyond this host. Service
configuration and tokens are not included. Verify before restoring to an isolated
directory. The script never writes into an existing backup or live state tree.
EOF
}

die() { printf '%s\n' "$*" >&2; exit 1; }

verify() {
    /usr/bin/python3 -I -B - "$1" <<'PY'
from hashlib import file_digest
from pathlib import Path
import re
import sqlite3
import sys

root = Path(sys.argv[1]).resolve(strict=True)
if any(item.is_symlink() or not (item.is_dir() or item.is_file()) for item in root.rglob('*')):
    sys.exit('Backup contains a link or non-regular file.')
ledger = root / 'dispatch.sqlite3'
with sqlite3.connect(ledger.as_uri() + '?mode=ro&immutable=1', uri=True) as conn:
    if conn.execute('PRAGMA quick_check').fetchall() != [('ok',)]:
        sys.exit('SQLite quick_check failed.')
    if conn.execute('PRAGMA foreign_key_check').fetchone() is not None:
        sys.exit('SQLite foreign key check failed.')
    if conn.execute("SELECT 1 FROM tasks WHERE access!='synthetic' LIMIT 1").fetchone() is not None:
        sys.exit('Only synthetic coordinator state is supported.')
    refs = conn.execute('SELECT content_hash FROM transport_artifact_grants UNION '
                        'SELECT artifact_hash FROM evidence WHERE artifact_hash IS NOT NULL')
    count = 0
    for (ref,) in refs:
        if re.fullmatch(r'sha256:[0-9a-f]{64}', ref) is None:
            sys.exit('Invalid artifact reference.')
        digest = ref[7:]
        artifact = root / 'artifacts' / 'sha256' / digest[:2] / digest
        with artifact.open('rb') as handle:
            if file_digest(handle, 'sha256').hexdigest() != digest:
                sys.exit('Artifact checksum mismatch.')
        count += 1
    print(f'Backup verified: SQLite, foreign keys and {count} referenced artifacts.')
PY
}

case ${1:-} in
    -h|--help) usage; exit 0 ;;
    --verify) [[ $# == 2 ]] || { usage >&2; exit 2; }; verify "$2"; exit ;;
esac
[[ $# == 1 ]] || { usage >&2; exit 2; }
[[ $EUID == 0 ]] || die 'Run backup as root to read the protected state directory.'
state=/var/lib/sa-dispatch
output=$(realpath -m -- "$1")
case $output in "$state"|"$state/"*) die 'Choose a backup destination outside the live state directory.' ;; esac

[[ -d /opt/sa-dispatch ]] || die 'Coordinator deployment is missing.'
exec 9>/opt/sa-dispatch/.maintenance.lock
flock -n 9 || die 'An install or backup is already in progress.'
[[ $(systemctl show --property=ActiveState --value sa-dispatch.service) == inactive ]] || die 'Stop sa-dispatch.service before backing up.'
[[ -f $state/dispatch.sqlite3 ]] || die 'Default coordinator ledger is missing.'
exec 8<"$state"
flock -n 8 || die 'Coordinator state is in use.'
/usr/bin/python3 -I -B - <<'PY'
import tomllib

with open('/etc/sa-dispatch/coordinator.toml', 'rb') as config:
    data = tomllib.load(config)
paths = {'db_path': '/var/lib/sa-dispatch/dispatch.sqlite3',
         'artifact_root': '/var/lib/sa-dispatch/artifacts',
         'journal_dir': '/var/lib/sa-dispatch/journal'}
if data.get('synthetic') is not True or any(data.get(key) != val for key, val in paths.items()):
    raise SystemExit('Backup requires the default synthetic coordinator state paths.')
PY
[[ ! -e $output && ! -L $output ]] || die 'Choose a new backup directory.'
stage=$(mktemp -d -- "$(dirname -- "$output")/.sa-dispatch-backup.XXXXXXXX")
trap 'rm -r -- "$stage"' EXIT

# Consolidate WAL into a self-contained backup database using SQLite's backup API.
/usr/bin/python3 -I -B - "$state/dispatch.sqlite3" "$stage/dispatch.sqlite3" <<'PY'
from pathlib import Path
import sqlite3
import sys

with sqlite3.connect(Path(sys.argv[1]).as_uri() + '?mode=ro', uri=True) as source:
    with sqlite3.connect(sys.argv[2]) as destination:
        source.backup(destination)
        destination.execute('PRAGMA journal_mode=DELETE')
PY
for child in artifacts journal; do
    if [[ -d $state/$child ]]; then cp -R --no-dereference -- "$state/$child" "$stage/$child"; fi
done
[[ $(systemctl show --property=ActiveState --value sa-dispatch.service) == inactive ]] || die 'Service started during backup. Stop it and retry.'
verify "$stage"
mv -T --no-clobber -- "$stage" "$output"
[[ ! -d $stage ]] || die 'Destination appeared during backup.'
trap - EXIT
printf 'Saved synthetic coordinator state to %s\n' "$output"
