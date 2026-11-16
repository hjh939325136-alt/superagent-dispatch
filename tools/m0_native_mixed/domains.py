"""Create the owned synthetic domains for native mixed-route maintenance."""

from contextlib import closing
import hashlib
import json
from pathlib import Path
import re
import stat

from dispatch.sa_dispatch.config import load_config
from knowledge.m0_content.contract import Contract
from knowledge.m0_content.workflow_binding import (
    SCOPES, initialize_binding, load_workflow_binding, search_modules,
)
from knowledge.m0_content.workflow_content import initialize_workflow


ROOT = Path(__file__).resolve().parents[2]
SOURCE_TREES = (
    'dispatch/sa_dispatch', 'dispatch/sa_dispatch_hotspot', 'knowledge/m0_search',
    'knowledge/m0_content', 'tools/m0_manager', 'tools/m0_backup',
)
SOURCE_FIXTURES = (
    'knowledge/m0_search/fixtures/grants.json',
    'knowledge/m0_search/fixtures/research-scatter.json',
    'knowledge/m0_search/fixtures/research-content.json',
    'knowledge/m0_search/fixtures/signalhub.json',
    'knowledge/m0_content/fixture_authorization.json',
)
COMPONENTS = {
    'research': ('knowledge/m0_search/dispatch_component.py', '--source-binding'),
    'research_content': ('knowledge/m0_content/dispatch_component.py', '--workflow-binding'),
}
STAGE_MANIFEST = 'stage-manifest.json'


class DomainStageError(ValueError):
    pass


def _check_info(info):
    if stat.S_ISLNK(info.st_mode) or getattr(info, 'st_file_attributes', 0) & 0x400:
        raise DomainStageError('Links and reparse points are forbidden')


def _check_path(value, *, directory, missing=False):
    path = Path(value)
    if (not path.is_absolute() or path == Path(path.anchor) or '..' in path.parts
            or str(path).startswith(('\\\\', '//'))):
        raise DomainStageError('An absolute local path without traversal is required')
    if path.drive and any(':' in part or part.endswith(('.', ' ')) or Path(part).is_reserved()
                          for part in path.parts[1:]):
        raise DomainStageError('Windows aliases and alternate streams are forbidden')
    for parent in reversed(path.parents):
        info = parent.lstat()
        _check_info(info)
        if not stat.S_ISDIR(info.st_mode):
            raise DomainStageError('Expected a directory ancestor')
    try:
        info = path.lstat()
    except FileNotFoundError:
        if missing:
            return path
        raise
    _check_info(info)
    if directory and not stat.S_ISDIR(info.st_mode):
        raise DomainStageError('Expected a directory')
    if not directory and (not stat.S_ISREG(info.st_mode) or info.st_nlink != 1):
        raise DomainStageError('Expected a singly linked regular file')
    return path


def _sha256(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def pin_source_files(source_root):
    """Hash the bounded runtime trees and fixture inputs used for this stage."""
    root = _check_path(source_root, directory=True)
    paths = [_check_path(root / name, directory=False) for name in SOURCE_FIXTURES]
    pending = [_check_path(root / name, directory=True) for name in SOURCE_TREES]
    while pending:
        for path in pending.pop().iterdir():
            info = path.lstat()
            _check_info(info)
            if path.name == '__pycache__':
                continue
            if stat.S_ISDIR(info.st_mode):
                pending.append(path)
            elif path.suffix in {'.py', '.sql'}:
                paths.append(_check_path(path, directory=False))
    result = {path.relative_to(root).as_posix(): _sha256(path) for path in sorted(paths)}
    required = {script for script, _ in COMPONENTS.values()}
    required.update({'knowledge/m0_content/dispatch_entry.py', 'tools/m0_manager/__main__.py',
                     'dispatch/sa_dispatch/config.py'})
    if not required <= result.keys():
        raise DomainStageError('Frozen runtime source is incomplete')
    return result


def stage_domains(new_empty_owned_root, *, ledger, artifacts, source_root, executable, device_id):
    """Stage fixture databases, original bindings and an uninstalled local config.

    Ledger and CAS paths are configuration pointers. The caller owns the output
    directory and controls its access throughout staging and later activation.
    An incomplete stage remains available for inspection and cannot be reused.
    """
    root = _check_path(new_empty_owned_root, directory=True, missing=True)
    ledger = _check_path(ledger, directory=False, missing=True)
    artifacts = _check_path(artifacts, directory=True, missing=True)
    source_root = _check_path(source_root, directory=True)
    executable = _check_path(executable, directory=False)
    for other in (ledger, artifacts, executable, *(source_root / tree for tree in SOURCE_TREES)):
        if other.is_relative_to(root) or root.is_relative_to(other):
            raise DomainStageError('Staging must use a separate owned directory')
    if ledger.is_relative_to(artifacts) or artifacts.is_relative_to(ledger):
        raise DomainStageError('Ledger and artifact paths must be separate')
    if not isinstance(device_id, str) or re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.:-]{0,120}', device_id) is None:
        raise DomainStageError('An explicit local device identity is required')
    if root.exists() and any(root.iterdir()):
        raise DomainStageError('Staging root must be empty')

    source_files = pin_source_files(source_root)
    if source_root != ROOT and source_files != pin_source_files(ROOT):
        raise DomainStageError('Frozen source differs from the loaded staging implementation')

    root.mkdir(exist_ok=True)
    marker = root / STAGE_MANIFEST
    with marker.open('x', encoding='utf-8') as stream:
        json.dump({'state': 'incomplete'}, stream)
        stream.write('\n')
    if set(root.iterdir()) != {marker}:
        raise DomainStageError('Staging root changed before initialization')

    bound, catalog, _, workflow = search_modules()
    catalog_root = root / 'research'
    research_path = root / 'research-binding.json'
    content_path = root / 'content.sqlite3'
    workflow_path = root / 'workflow-binding.json'
    journal = root / 'journal'
    config_path = root / 'local.toml'
    catalog.initialize(catalog_root, operator_grant=catalog.OPERATOR)
    bound.initialize_binding(catalog_root, research_path)
    research = bound.load_binding(research_path)
    workflow.initialize_receipts(research, operator_grant=catalog.OPERATOR)
    with closing(Contract(content_path, create=True)):
        pass
    initialize_workflow(content_path)
    initialize_binding(research_path, content_path, workflow_path)
    binding = load_workflow_binding(workflow_path)
    journal.mkdir()

    values = {
        'db_path': str(ledger), 'artifact_root': str(artifacts), 'journal_dir': str(journal),
        'device_id': device_id, 'capabilities': list(SCOPES), 'lease_ttl_seconds': 30,
        'component_timeout_seconds': 20, 'plugins': list(COMPONENTS),
        'synthetic': True, 'execution_route': 'local_synthetic',
    }
    lines = [f'{key} = {json.dumps(value, ensure_ascii=False)}' for key, value in values.items()]
    for (name, (script, flag)), path in zip(COMPONENTS.items(), (research_path, workflow_path), strict=True):
        argv = [str(executable), '-B', str(source_root / script), flag, str(path)]
        lines += [f'[components.{name}]', 'platform = "any"',
                  'argv = ' + json.dumps(argv, ensure_ascii=False)]
    with config_path.open('x', encoding='utf-8', newline='\n') as stream:
        stream.write('\n'.join(lines) + '\n')
    load_config(config_path, allowed_plugins=set(COMPONENTS))
    if pin_source_files(source_root) != source_files:
        raise DomainStageError('Frozen source changed during staging')

    catalog_files = [catalog_root / 'catalog.json',
                     *(catalog_root / f'{name}.sqlite3' for name in ('catalog', *catalog.SOURCES))]
    result = {
        'state': 'staged', 'manifest': str(marker),
        'root': str(root), 'catalog_root': str(catalog_root), 'catalog_instance': binding.research.instance,
        'content_db': str(content_path), 'content_instance': binding.content_instance,
        'research_binding': str(research_path), 'research_binding_sha256': binding.research.digest,
        'workflow_binding': str(workflow_path), 'workflow_binding_sha256': binding.digest,
        'journal_dir': str(journal), 'local_config': str(config_path), 'source_root': str(source_root),
        'source_files': source_files,
        'files': {path.relative_to(root).as_posix(): _sha256(_check_path(path, directory=False))
                  for path in sorted((*catalog_files, research_path, content_path, workflow_path, config_path))},
    }
    pending_manifest = root / '.stage-manifest-pending'
    with pending_manifest.open('x', encoding='utf-8', newline='\n') as stream:
        json.dump(result, stream, ensure_ascii=False, sort_keys=True, indent=2)
        stream.write('\n')
    pending_manifest.replace(marker)
    return result
