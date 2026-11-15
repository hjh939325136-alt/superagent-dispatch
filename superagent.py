"""Run a SuperAgent M0 command from this source tree."""
import argparse
import runpy
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parent
MODULES = {
    'dispatch': 'sa_dispatch',
    'manager': 'tools.m0_manager',
    'manager-fixture': 'tools.m0_manager.lab',
    'pipeline-fixture': 'knowledge.m0_content.run_research_pipeline_lab',
    'retention': 'tools.m0_retention',
    'inventory': 'tools.m0_inventory.inventory',
    'backup': 'tools.m0_backup.backup',
    'bound-backup': 'tools.m0_portable.backup_target',
}
SCRIPTS = {
    'research': 'knowledge/m0_search/dispatch_entry.py',
    'research-binding': 'knowledge/m0_search/dispatch_binding.py',
    'research-search': 'knowledge/m0_search/search.py',
    'content': 'knowledge/m0_content/dispatch_entry.py',
    'content-contract': 'knowledge/m0_content/contract.py',
}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=(*MODULES, *SCRIPTS))
    parser.add_argument('arguments', nargs=argparse.REMAINDER)
    args = parser.parse_args()
    sys.path[:0] = [str(ROOT), str(ROOT / 'dispatch')]
    sys.argv[1:] = args.arguments
    if args.command in MODULES:
        runpy.run_module(MODULES[args.command], run_name='__main__', alter_sys=True)
    else:
        script = ROOT / SCRIPTS[args.command]
        sys.path.insert(0, str(script.parent))
        runpy.run_path(str(script), run_name='__main__')


if __name__ == '__main__':
    main()
