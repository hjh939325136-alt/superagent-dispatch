"""Local content-addressed storage for task receipts and evidence."""
from pathlib import Path
import os
import re
import tempfile

from .canonical import sha256_ref


class ArtifactError(ValueError):
    def __init__(self, code, message):
        self.code = code
        super().__init__(message)


class ArtifactStore:
    def __init__(self, root):
        self.root = Path(root).resolve()

    def path(self, ref):
        if not isinstance(ref, str) or not re.fullmatch(r"sha256:[0-9a-f]{64}", ref):
            raise ArtifactError("E_INVALID_HASH", "expected a SHA-256 reference")
        digest = ref[7:]
        path = self.root / "sha256" / digest[:2] / digest
        if not path.resolve().is_relative_to(self.root):
            raise ArtifactError("E_INVALID_HASH", "artifact path escapes the configured root")
        return path

    def get(self, ref):
        try:
            content = self.path(ref).read_bytes()
        except FileNotFoundError as exc:
            raise ArtifactError("E_ARTIFACT_MISSING", "referenced artifact is missing") from exc
        if sha256_ref(content) != ref:
            raise ArtifactError("E_HASH_MISMATCH", "artifact contents did not match their reference")
        return content

    def put(self, content):
        ref = sha256_ref(content)
        path = self.path(ref)
        if path.exists():
            self.get(ref)
            return ref
        path.parent.mkdir(parents=True, exist_ok=True)
        fd, temp = tempfile.mkstemp(prefix=".part-", dir=path.parent)
        try:
            with os.fdopen(fd, "wb") as handle:
                handle.write(content)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temp, path)
        finally:
            Path(temp).unlink(missing_ok=True)
        self.get(ref)
        return ref
