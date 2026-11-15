#!/usr/bin/env bash
set -euo pipefail
umask 077

usage() {
    cat <<'EOF'
Usage: sudo bash install-coordinator.sh RELEASE_ID
       sudo bash install-coordinator.sh --select RELEASE_ID

Install a new version from this dispatch tree, or select a previously installed
version. Stop sa-dispatch.service first if it is running. Existing configuration
and state are preserved. Start and enable the service separately after checking
the configuration. Python 3.11+, systemd and a local service account are used.
EOF
}

die() { printf '%s\n' "$*" >&2; exit 1; }

case ${1:-} in
    -h|--help) usage; exit 0 ;;
    --select) select_existing=true; shift ;;
    *) select_existing=false ;;
esac
[[ $# == 1 ]] || { usage >&2; exit 2; }
release_id=$1
[[ $release_id =~ ^[a-zA-Z0-9][a-zA-Z0-9._-]{0,63}$ ]] || die 'Invalid release ID.'
[[ $EUID == 0 ]] || die 'Run this installer as root on the target Linux system.'
[[ -d /run/systemd/system ]] || die 'A running systemd system manager is required.'
/usr/bin/python3 -I -B -c 'import sys; sys.version_info >= (3, 11) or sys.exit("Python 3.11 or newer required.")'

root=/opt/sa-dispatch
config=/etc/sa-dispatch
unit=sa-dispatch.service
release=$root/releases/$release_id
source_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)

# Keep the root maintenance lock outside potentially group/world writable /run/lock.
install -d -o root -g root -m 0755 -- "$root" "$root/releases"
exec 9>"$root/.maintenance.lock"
flock -n 9 || die 'Another coordinator installation is in progress.'
active_state=$(systemctl show --property=ActiveState --value "$unit")
case $active_state in
    inactive|failed) ;;
    *) die "Stop $unit before selecting a release (state: $active_state)." ;;
esac
if [[ -d /var/lib/sa-dispatch ]]; then
    exec 8</var/lib/sa-dispatch
    flock -n 8 || die 'Coordinator state is in use.'
fi

# Use the dedicated account for state files. Never reuse another service group.
if id sa-dispatch >/dev/null 2>&1; then
    [[ $(id -gn sa-dispatch) == sa-dispatch ]] || die 'Unexpected sa-dispatch account group.'
else
    if ! getent group sa-dispatch >/dev/null; then
        groupadd --system sa-dispatch
    fi
    useradd --system --gid sa-dispatch --home-dir /var/lib/sa-dispatch --shell /usr/sbin/nologin sa-dispatch
fi

install -d -o root -g sa-dispatch -m 0750 -- "$config"

stage=''
new_link=''
new_unit=''
cleanup() {
    if [[ -n $stage ]]; then rm -r -- "$stage"; fi
    if [[ -n $new_link ]]; then rm -f -- "$new_link"; fi
    if [[ -n $new_unit ]]; then rm -f -- "$new_unit"; fi
}
trap cleanup EXIT

if $select_existing; then
    [[ -d $release ]] || die "Release $release_id is not installed."
else
    [[ ! -e $release && ! -L $release ]] || die "Release $release_id already exists. Use --select or choose a new ID."
    stage=$(mktemp -d -- "$root/releases/.install.XXXXXXXX")
    chmod 0755 -- "$stage"
    for package in sa_dispatch sa_dispatch_hotspot; do
        install -d -m 0755 -- "$stage/$package"
        install -m 0644 -- "$source_dir/$package/"*.py "$stage/$package/"
    done
    install -m 0644 -- "$source_dir/sa_dispatch/schema.sql" "$stage/sa_dispatch/"
    install -m 0644 -- "$source_dir/deploy/$unit" "$stage/$unit"
    install -m 0644 -- "$source_dir/deploy/coordinator.example.toml" "$stage/coordinator.example.toml"
    # Import staged code as the service user so the installer never runs it as root.
    (cd -- "$stage" && runuser -u sa-dispatch -- env -u PYTHONPATH -u PYTHONHOME /usr/bin/python3 -B -m sa_dispatch serve --help >/dev/null)
    mv -T -- "$stage" "$release"
    stage=''
fi

if [[ ! -e $config/coordinator.toml && ! -L $config/coordinator.toml ]]; then
    install -o root -g sa-dispatch -m 0640 -- "$release/coordinator.example.toml" "$config/coordinator.toml"
fi

# Each file replacement is atomic. The service stays stopped until explicitly started.
new_unit=$(mktemp -- "/etc/systemd/system/.$unit.XXXXXXXX")
install -m 0644 -- "$release/$unit" "$new_unit"
mv -Tf -- "$new_unit" "/etc/systemd/system/$unit"
new_unit=''
new_link="$root/.current.$$"
ln -s -- "releases/$release_id" "$new_link"
mv -Tf -- "$new_link" "$root/current"
new_link=''
systemctl daemon-reload
printf 'Selected %s. Configure %s/identities.json, then start %s when ready.\n' "$release_id" "$config" "$unit"
