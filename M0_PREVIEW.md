# M0 source preview

This separate preview branch adds the M0 launcher and local Dispatcher, Manager,
research/search, content, retention, inventory, and synthetic backup modules.
Use Python 3.11 or later with SQLite and the standard library.

For the published v0.1.0 layout, check out tag `v0.1.0` and run
`python -B -m sa_dispatch` from the repository root. In this preview, use
`python -I -B superagent.py dispatch` from the root. For direct module use
from the `dispatch` directory, the entry point is `python -B -m sa_dispatch`.
Review the preview fixture commands in [README.md](README.md).

The Manager accepts one JSON request on stdin. See
[`examples/manager-query.json`](examples/manager-query.json). The local
launcher prints CLI help with `manager --help`, `dispatch --help`,
`research-search --help`, `content-contract --help`, and `retention --help`.
Run the pipeline fixture to generate an example Dispatcher TOML and exercise
the research references and synthetic approval through the normal CLI.

## Reproduce the Dispatcher checks

From the preview root, run the following in a checkout with no external
`PYTHONPATH` or `PYTHONHOME`.

```sh
cd dispatch
python -B -m unittest discover -s tests -v
python -B -m sa_dispatch selftest
```

The public suite contains 226 tests and includes historical v1, v3, v4 and
v5 compatibility fixtures.
Historical fixture provenance retains the fields checked by the tests.
The original v2 history test and its fixture are omitted because a
hash-verified artifact records private host paths. The generated v2 tests
remain in the suite. Test subprocesses use Python and synthetic data.

Systemd templates and scripts are available under `dispatch/deploy/`.
This preview covers synthetic acceptance. Production data, 24-hour
operation, server installation, real-data migration, and cross-device
operation remain outside this preview. Use fresh local directories for
generated state and keep live credentials and bindings outside the source tree.

The release includes the MIT license, a byte manifest, and relative-source
provenance. The fixture archives contain the original Python and SQL source
used by the v4 and v5 compatibility tests. Git attributes preserve tracked
bytes for manifest verification.
