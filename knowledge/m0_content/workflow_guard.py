"""Keep research authorization/snapshot live until the dependent operation ends."""
from contextlib import contextmanager
from datetime import datetime,timezone
from .workflow_binding import WorkflowError, request_base, search_modules


class WorkflowGuard:
    def __init__(self,binding,snapshot):
        self.binding,self._snapshot=binding,snapshot
        self._active=True

    @property
    def active(self):
        return self._active and self._snapshot.active

    def inspect(self,context_id,manifest_sha256=None):
        if not self.active:
            raise WorkflowError('DEPENDENCY_GUARD_CLOSED')
        data=dict(self._snapshot.inspect(context_id,manifest_sha256))
        data.update({k:v for k,v in request_base(datetime.now(timezone.utc).isoformat()).items() if k!='as_of'})
        return data

    def check_external(self,mapping):
        if not isinstance(mapping,dict) or not self.active:
            return {'usable':False,'state':'unverified','reason_code':'DEPENDENCY_GUARD_REQUIRED'}
        try:
            expected={'workflow_binding':self.binding.digest,'source_binding':self.binding.research.digest,
                      'catalog_instance':self.binding.research.instance}
            if any(mapping.get(k)!=v for k,v in expected.items()):
                return {'usable':False,'state':'binding_changed','reason_code':'DEPENDENCY_BINDING_MISMATCH'}
            value=self.inspect(mapping['context_id'],mapping['manifest_sha256'])
            for key in ('refs','caller','domains','scopes','max_class'):
                actual=value.get(key);stored=mapping.get(key)
                if key in ('refs','domains','scopes'):
                    if not isinstance(stored,list) or sorted(stored)!=sorted(actual or []):
                        raise WorkflowError('DEPENDENCY_MANIFEST_MISMATCH')
                elif stored!=actual:
                    raise WorkflowError('DEPENDENCY_MANIFEST_MISMATCH')
            return {'usable':value.get('usable') is True,'state':value.get('status','unverified'),
                    'reason_code':'DEPENDENCY_CURRENT' if value.get('usable') is True else 'DEPENDENCY_UNUSABLE'}
        except (ValueError,KeyError,TypeError) as exc:
            return {'usable':False,'state':'withheld','reason_code':getattr(exc,'code','DEPENDENCY_UNVERIFIED')}


@contextmanager
def dependency_guard(binding):
    _,_,_,workflow=search_modules()
    with workflow.catalog_snapshot(binding.research,request_base(datetime.now(timezone.utc).isoformat())) as snapshot:
        guard=WorkflowGuard(binding,snapshot)
        try:yield guard
        finally:guard._active=False


def require_guard(guard,binding,expected_context_id,expected_manifest_sha256):
    if (not isinstance(guard,WorkflowGuard) or not guard.active or guard.binding.digest!=binding.digest or
        guard.binding.content_instance!=binding.content_instance):
        raise WorkflowError('DEPENDENCY_GUARD_REQUIRED')
    value=guard.inspect(expected_context_id,expected_manifest_sha256)
    if value.get('usable') is not True:
        raise WorkflowError('DEPENDENCY_UNUSABLE')
    return value
