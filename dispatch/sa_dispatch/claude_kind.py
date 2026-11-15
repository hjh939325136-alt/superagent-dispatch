"""Opt-in text-only Claude task; executable selection belongs to the operator."""
import re

class ClaudeKind:
    name='claude.synthetic'
    version=1
    component='claude_runtime'
    side_effect='none'
    enabled=True
    allow_no_new=False
    dedicated_runner=True

    def validate(self, params):
        if (set(params)!={'prompt_ref','model'} or
                not re.fullmatch('sha256:[0-9a-f]{64}',str(params['prompt_ref'])) or
                params['model']!='claude-fable-5-2'):
            raise ValueError('fixed synthetic prompt and explicitly verified model required')
        return params

    def scope(self, params): return {'model':params['model']}

    def request(self, task):
        raise ValueError('claude.synthetic requires the dedicated claude-run lifecycle adapter')

KINDS={ClaudeKind.name:ClaudeKind()}
