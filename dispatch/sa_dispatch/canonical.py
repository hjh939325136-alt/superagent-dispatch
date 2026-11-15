"""Canonical task identities shared by the dispatch store and portable bundles."""
from hashlib import sha256
import json


def canonical_json(value):
    def validate(item):
        if item is None or type(item) in (str, bool, int):
            return
        if isinstance(item, (list, tuple)):
            for child in item:
                validate(child)
            return
        if isinstance(item, dict) and all(isinstance(key, str) for key in item):
            for child in item.values():
                validate(child)
            return
        raise ValueError("canonical task data supports JSON values with integer numbers")

    validate(value)
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")


def sha256_ref(content):
    return "sha256:" + sha256(content).hexdigest()
