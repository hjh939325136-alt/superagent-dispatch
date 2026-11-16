"""Direct, bounded DeepSeek chat completion requests for approved text."""

from collections.abc import Callable
from dataclasses import dataclass, field
from http.client import HTTPException, HTTPSConnection
import json
import re
import ssl


HOST = "api.deepseek.com"
PATH = "/chat/completions"
MAX_REQUEST_BYTES = 64 * 1024
MAX_RESPONSE_BYTES = 256 * 1024
MAX_OUTPUT_TOKENS = 4096
SOCKET_TIMEOUT_SECONDS = 30


class DeepSeekError(Exception):
    """Safe to summarize by kind, status and outcome_unknown."""

    def __init__(
        self, kind: str, *, status: int | None = None, outcome_unknown: bool = False
    ) -> None:
        super().__init__(kind)
        self.kind = kind
        self.status = status
        self.outcome_unknown = outcome_unknown


@dataclass(frozen=True)
class TokenUsage:
    prompt_tokens: int
    completion_tokens: int
    total_tokens: int
    prompt_cache_hit_tokens: int | None
    prompt_cache_miss_tokens: int | None


@dataclass(frozen=True)
class Completion:
    response_id: str = field(repr=False)
    model: str = field(repr=False)
    finish_reason: str = field(repr=False)
    text: str = field(repr=False)
    usage: TokenUsage

    @property
    def truncated(self) -> bool:
        return self.finish_reason == "length"


def _direct_https(api_key: str, body: bytes) -> tuple[int, bytes]:
    # Construct the verifying context directly so it does not enable SSLKEYLOGFILE.
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    context.load_default_certs()
    connection = HTTPSConnection(HOST, timeout=SOCKET_TIMEOUT_SECONDS, context=context)
    try:
        connection.request(
            "POST", PATH, body=body,
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        )
        with connection.getresponse() as response:
            if response.status != 200:
                return response.status, b""
            length = response.length
            if length is not None and length > MAX_RESPONSE_BYTES:
                raise DeepSeekError("response_too_large", status=200, outcome_unknown=True)
            payload = response.read(MAX_RESPONSE_BYTES + 1)
            if length is not None and len(payload) != length:
                raise DeepSeekError("incomplete_response", status=200, outcome_unknown=True)
            return response.status, payload
    finally:
        connection.close()


def _token_count(usage: dict, field_name: str) -> int:
    count = usage[field_name]
    if type(count) is not int or count < 0:
        raise ValueError
    return count


def _parse_completion(payload: bytes) -> Completion:
    result = json.loads(payload.decode("utf-8"))
    choices = result["choices"]
    if not isinstance(choices, list) or len(choices) != 1:
        raise ValueError
    choice = choices[0]
    response_id = result["id"]
    model = result["model"]
    finish_reason = choice["finish_reason"]
    for metadata in (response_id, model):
        if not isinstance(metadata, str) or not metadata or not metadata.isprintable():
            raise ValueError
    if finish_reason not in ("stop", "length"):
        raise ValueError
    message = choice["message"]
    if message["role"] != "assistant" or message.get("tool_calls") not in (None, []):
        raise ValueError
    content = message["content"]
    if not isinstance(content, str):
        raise ValueError
    content.encode("utf-8")
    usage = result["usage"]
    if not isinstance(usage, dict):
        raise ValueError
    prompt = _token_count(usage, "prompt_tokens")
    completion = _token_count(usage, "completion_tokens")
    total = _token_count(usage, "total_tokens")
    hit = _token_count(usage, "prompt_cache_hit_tokens") if "prompt_cache_hit_tokens" in usage else None
    miss = _token_count(usage, "prompt_cache_miss_tokens") if "prompt_cache_miss_tokens" in usage else None
    if total != prompt + completion or (hit is not None and miss is not None and hit + miss != prompt):
        raise ValueError
    return Completion(response_id, model, finish_reason, content, TokenUsage(prompt, completion, total, hit, miss))


def complete_text(
    *, model: str, approved_text: str, api_key: str, max_tokens: int,
    _transport: Callable[[str, bytes], tuple[int, bytes]] = _direct_https,
) -> Completion:
    """Submit one approved text request. The private transport seam is for tests."""
    if (
        not isinstance(model, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}", model)
        or not isinstance(approved_text, str) or len(approved_text) > MAX_REQUEST_BYTES
        or not approved_text.strip()
        or not isinstance(api_key, str) or not re.fullmatch(r"[A-Za-z0-9._~+/=-]{1,1024}", api_key)
        or type(max_tokens) is not int or not 1 <= max_tokens <= MAX_OUTPUT_TOKENS
    ):
        raise DeepSeekError("invalid_request")
    try:
        body = json.dumps(
            {"model": model, "messages": [{"role": "user", "content": approved_text}],
             "max_tokens": max_tokens, "stream": False},
            ensure_ascii=False, separators=(",", ":"),
        ).encode("utf-8")
    except UnicodeError:
        body = None
    if body is None or len(body) > MAX_REQUEST_BYTES:
        raise DeepSeekError("invalid_request")

    failure = None
    try:
        status, payload = _transport(api_key, body)
    except TimeoutError:
        failure = "timeout"
    except (OSError, HTTPException):
        failure = "connection_error"
    if failure is not None:
        raise DeepSeekError(failure, outcome_unknown=True)

    if status in (401, 403):
        raise DeepSeekError("auth_rejected", status=status)
    if status == 429:
        raise DeepSeekError("rate_limited", status=status)
    if 300 <= status < 400:
        raise DeepSeekError("redirect_rejected", status=status, outcome_unknown=True)
    if status != 200:
        definite_rejection = status in (400, 402, 404, 413, 422)
        raise DeepSeekError("provider_error", status=status, outcome_unknown=not definite_rejection)
    if len(payload) > MAX_RESPONSE_BYTES:
        raise DeepSeekError("response_too_large", status=status, outcome_unknown=True)
    try:
        return _parse_completion(payload)
    except (KeyError, TypeError, ValueError, RecursionError):
        pass
    raise DeepSeekError("invalid_response", status=status, outcome_unknown=True)
