from .models import InvalidTransition

TERMINAL = frozenset({'succeeded', 'no_new', 'failed', 'cancelled'})
TRANSITIONS = {
    'queued': {'leased', 'waiting_device', 'cancelled'},
    'waiting_device': {'queued', 'cancelled'},
    'leased': {'succeeded', 'no_new', 'failed', 'unknown', 'queued'},
    'unknown': {'succeeded', 'queued', 'failed'},
    **{state: set() for state in TERMINAL},
}

def validate_transition(old, new):
    if new not in TRANSITIONS.get(old, ()):
        raise InvalidTransition(f'{old} -> {new} is forbidden')
