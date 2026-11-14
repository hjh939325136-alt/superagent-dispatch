"""Build a task registry from adapters explicitly registered by an entry point."""


def task_registry(enabled, plugins):
    kinds = {}
    for name in enabled:
        for identity, kind in plugins[name].items():
            if identity != kind.name or identity in kinds:
                raise ValueError(f"conflicting task kind: {identity}")
            kinds[identity] = kind
    return kinds
