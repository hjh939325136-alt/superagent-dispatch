"""Opt-in synthetic host kind; execution uses the durable host-run command."""
import re

class HostKind:
    name='host.synthetic'
    version=1
    component='host_fixture'
    side_effect='none'
    enabled=True
    allow_no_new=False
    dedicated_runner=True
    def validate(self,params):
        if (set(params)!= {'prompt_ref','agent_id'} or
                not re.fullmatch('sha256:[0-9a-f]{64}',str(params['prompt_ref'])) or
                params['agent_id'] not in ('lead','research','writer','director','publisher')):
            raise ValueError('fixed synthetic prompt and known workbench role required')
        return params
    def scope(self,params):return {'agent_id':params['agent_id']}
    def request(self,task):raise ValueError('host.synthetic must use host-run with an isolated fixture workbench')

KINDS={HostKind.name:HostKind()}
