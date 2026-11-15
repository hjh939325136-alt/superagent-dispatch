"""stdin JSON -> stdout JSON, with no implicit sync or other side effect."""
import argparse
import json
import sys
from .manager import QUERY_SCHEMA_VERSION, run
from .readonly import SourceError
from .binding import parse_json


def main():
    if len(sys.argv)>1 and sys.argv[1] in ('sync','adopt'):
        # Explicit operator action only; never reachable via the JSON query router.
        from pathlib import Path
        sys.path.insert(0,str(Path(__file__).resolve().parents[2]/'dispatch'))
        from sa_dispatch.collector_cli import main as collector_main
        command=sys.argv[1]
        return collector_main([*sys.argv[2:],command])
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binding',required=True)
    args=parser.parse_args()
    try:
        raw=sys.stdin.buffer.read(65537)
        if len(raw)>65536:
            raise SourceError('E_REQUEST_TOO_LARGE')
        result=run(parse_json(raw),args.binding)
    except (OSError,ValueError,RecursionError,SourceError) as exc:
        result={'schema_version':QUERY_SCHEMA_VERSION,'ok':False,
                'error':exc.code if isinstance(exc,SourceError) else 'E_REQUEST_INVALID'}
    sys.stdout.buffer.write(json.dumps(result,ensure_ascii=False,sort_keys=True).encode('utf-8')+b'\n')
    return 0 if result['ok'] else 2


if __name__=='__main__':
    raise SystemExit(main())
