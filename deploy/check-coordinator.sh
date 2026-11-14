#!/usr/bin/env bash
set -euo pipefail

# Run the existing acceptance suite against isolated, synthetic data.
dispatch_root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
dispatch_python=${DISPATCH_PYTHON:-python3}

if ! command -v -- "$dispatch_python" >/dev/null 2>&1; then
    printf 'Python executable not found: %s\n' "$dispatch_python" >&2
    exit 1
fi
# Resolve a relative interpreter path before switching to the source directory.
dispatch_python=$(command -v -- "$dispatch_python")
if [[ $dispatch_python == */* && $dispatch_python != /* ]]; then
    dispatch_python="$PWD/$dispatch_python"
fi

"$dispatch_python" -I -B -c 'import sys; sys.version_info >= (3, 11) or sys.exit("Python 3.11 or newer is required.")'

cd -- "$dispatch_root"
unset PYTHONPATH PYTHONHOME
"$dispatch_python" -B -m unittest discover -s tests -v
"$dispatch_python" -B -m sa_dispatch selftest
