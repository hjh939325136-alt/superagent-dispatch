"""Freeze a minimal, secret-free R1 runtime from explicitly selected source trees."""
import argparse
import hashlib
import json
from pathlib import Path
import zipfile


def build(root, destination):
    files = {}
    for folder in ('dispatch/sa_dispatch', 'dispatch/sa_dispatch_hotspot'):
        for path in sorted((root / folder).rglob('*')):
            if path.is_file() and path.suffix in ('.py', '.sql') and '__pycache__' not in path.parts:
                files['payload/' + path.relative_to(root).as_posix()] = path.read_bytes()
    agent_modules = (
        '__init__.py',
        'deepseek_client.py',
        'provider_process.py',
        'readonly_status.py',
        'runtime.py',
        'test_deepseek_client.py',
        'test_provider_process.py',
        'test_readonly_status.py',
        'test_runtime.py',
        'test_worker.py',
        'worker.py',
    )
    for name in agent_modules:
        source = root / 'tools/m0_agent' / name
        files['payload/tools/m0_agent/' + name] = source.read_bytes()
    for name in ('__init__.py', 'credentials.py', 'trial.py', 'r1.py', 'test_r1.py'):
        raw = (root / 'tools/m0_installer' / name).read_bytes()
        files['payload/tools/m0_installer/' + name] = raw
        if name in ('credentials.py', 'trial.py', 'r1.py'):
            files['installer/' + name] = raw
    files['payload/LICENSE'] = (root / 'dispatch/LICENSE').read_bytes()
    manifest = {'schema':'superagent.r1.package.v1', 'scope':'r1_single_scheduled_agent',
        'files': {name:hashlib.sha256(raw).hexdigest() for name,raw in sorted(files.items())},
        'notes':['Synthetic fixed prompt only; no credentials included.',
                 'Simulation and live are separate immutable installations.',
                 'This package is not full M0 acceptance or physical-device acceptance.']}
    raw = json.dumps(manifest,sort_keys=True,separators=(',',':')).encode()
    digest = hashlib.sha256(raw).hexdigest()
    destination.mkdir(parents=True,exist_ok=True)
    path = destination / ('superagent-r1-' + digest[:12] + '.zip')
    with zipfile.ZipFile(path,'x',compression=zipfile.ZIP_DEFLATED) as archive:
        for name,content in sorted(files.items()):
            archive.writestr(name,content)
        archive.writestr('INSTALL_MANIFEST.json',raw)
    return {'path':str(path),'manifest_sha256':digest,'zip_sha256':hashlib.sha256(path.read_bytes()).hexdigest(),
        'files':len(files),'bytes':path.stat().st_size}


if __name__ == '__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--destination',type=Path,required=True)
    args=parser.parse_args()
    print(json.dumps(build(Path(__file__).resolve().parents[2],args.destination)))
