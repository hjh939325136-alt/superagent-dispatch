import tempfile
import unittest

from sa_dispatch.artifacts import ArtifactError, ArtifactStore
from sa_dispatch.canonical import canonical_json


class ArtifactTests(unittest.TestCase):
    def test_canonical(self):
        self.assertEqual(canonical_json({"文": 2, "a": 1}), '{"a":1,"文":2}'.encode())
        self.assertEqual(canonical_json({"a": 1, "文": 2}), canonical_json({"文": 2, "a": 1}))
        with self.assertRaises(ValueError):
            canonical_json({"n": [1.0]})

    def test_reuse_and_detect_corruption(self):
        with tempfile.TemporaryDirectory() as folder:
            store = ArtifactStore(folder)
            ref = store.put(b"data")
            self.assertEqual(store.put(b"data"), ref)
            self.assertEqual(store.get(ref), b"data")
            store.path(ref).write_bytes(b"corrupted")
            with self.assertRaisesRegex(ArtifactError, "did not match"):
                store.get(ref)
            with self.assertRaisesRegex(ArtifactError, "did not match"):
                store.put(b"data")
