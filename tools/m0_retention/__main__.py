import argparse
import json
from pathlib import Path
from .binding import load_binding
from .plan import plan_retention
from .apply import apply_fixture,recover_fixture,read_retention_status
from .common import document
from .lab import create_fixture,run

def main():
    parser=argparse.ArgumentParser(description='Bounded synthetic retention; production apply unavailable')
    parser.add_argument('--binding',type=Path)
    sub=parser.add_subparsers(dest='op',required=True)
    for name in ('init-fixture','lab'):sub.add_parser(name).add_argument('--new-root',type=Path,required=True)
    plan=sub.add_parser('plan');plan.add_argument('--output',type=Path)
    apply=sub.add_parser('apply-fixture');apply.add_argument('--plan',type=Path,required=True);apply.add_argument('--fixture-grant',required=True)
    recover=sub.add_parser('recover-fixture');recover.add_argument('--operation-dir',type=Path,required=True);recover.add_argument('--fixture-grant',required=True)
    sub.add_parser('status')
    args=parser.parse_args()
    if args.op=='init-fixture':value={'binding':str(create_fixture(args.new_root))}
    elif args.op=='lab':value=run(args.new_root)
    else:
        if args.binding is None:parser.error('--binding required')
        binding=load_binding(args.binding)
        if args.op=='plan':
            if args.output and binding.mode!='fixture':parser.error('ordinary dry-run plans use stdout')
            value=plan_retention(binding)
            if args.output:
                if args.output.parent!=binding.root/'plans':parser.error('output must be in bound plans directory')
                document(binding.root,args.output,value)
        elif args.op=='apply-fixture':value=apply_fixture(binding,args.plan,args.fixture_grant)
        elif args.op=='recover-fixture':value=recover_fixture(binding,args.operation_dir,args.fixture_grant)
        else:value=read_retention_status(binding)
    print(json.dumps(value,ensure_ascii=False,sort_keys=True))

if __name__=='__main__':main()
