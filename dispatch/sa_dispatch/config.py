"""Load trusted, operator-owned component configuration."""

import re
import sys
import tomllib
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Mapping


class ConfigError(ValueError):
    def __init__(self, message: str, *, code: str = "E_CONFIG"):
        super().__init__(message)
        self.code = code


@dataclass(frozen=True)
class ComponentConfig:
    argv: tuple[str, ...]
    platform: str
    cwd: Path | None = None


@dataclass(frozen=True)
class Config:
    db_path: Path
    artifact_root: Path
    journal_dir: Path
    device_id: str
    capabilities: tuple[str, ...]
    lease_ttl_seconds: int
    component_timeout_seconds: int
    plugins: tuple[str, ...]
    components: Mapping[str, ComponentConfig]
    synthetic: bool = False
    execution_route: str | None = None


_ROOT_FIELDS = frozenset(Config.__dataclass_fields__)
_COMPONENT_FIELDS = frozenset(ComponentConfig.__dataclass_fields__)
_PLATFORMS = {"any", "win32", "linux", "darwin"}
_PLUGINS = {"hotspot"}
_SHELLS = {
    "cmd", "command", "powershell", "pwsh", "sh", "bash", "dash", "zsh", "ksh",
    "fish", "csh", "tcsh", "busybox", "wsl", "env", "mshta", "wscript", "cscript",
}


def _check_fields(data: dict, allowed: frozenset[str], required: frozenset[str], label: str) -> None:
    if set(data) - allowed or required - set(data):
        raise ConfigError(f"{label} has unknown or missing fields")


def _string(value: object, label: str) -> str:
    if not isinstance(value, str) or not value.strip() or "\x00" in value:
        raise ConfigError(f"{label} must be a nonempty string")
    return value


def _strings(value: object, label: str) -> tuple[str, ...]:
    if not isinstance(value, list):
        raise ConfigError(f"{label} must be a string array")
    return tuple(_string(item, label) for item in value)


def _absolute(value: object, label: str) -> Path:
    path = Path(_string(value, label))
    if not path.is_absolute():
        raise ConfigError(f"{label} must be an absolute path")
    return path


def _seconds(value: object, label: str) -> int:
    if type(value) is not int or value <= 0:
        raise ConfigError(f"{label} must be a positive integer")
    return value


def _component(entry: object) -> ComponentConfig:
    if not isinstance(entry, dict):
        raise ConfigError("component must be a table")
    _check_fields(entry, _COMPONENT_FIELDS, _COMPONENT_FIELDS - {"cwd"}, "component")
    platform = _string(entry["platform"], "component platform")
    if platform not in _PLATFORMS:
        raise ConfigError("unknown component platform")
    if platform not in {"any", sys.platform}:
        raise ConfigError("component platform does not match this host", code="E_PLATFORM")

    argv = _strings(entry["argv"], "component argv")
    if not argv:
        raise ConfigError("component argv must include an executable")
    executable = _absolute(argv[0], "component executable")
    executable_names = (executable, executable.resolve())
    if any(path.stem.casefold() in _SHELLS for path in executable_names):
        raise ConfigError("component executable cannot be a shell launcher")
    if any(path.suffix.casefold() in {".bat", ".cmd", ".ps1"} for path in executable_names):
        raise ConfigError("component executable cannot be a batch or PowerShell script")
    if sys.platform == "win32" and executable.suffix.casefold() != ".exe":
        raise ConfigError("component executable must have an .exe extension on Windows")
    cwd = _absolute(entry["cwd"], "component cwd") if "cwd" in entry else None
    return ComponentConfig(argv, platform, cwd)


def load_config(path: str | Path, *, allowed_plugins=_PLUGINS) -> Config:
    try:
        with Path(path).open("rb") as source:
            data = tomllib.load(source)
    except (OSError, UnicodeError, ValueError):
        raise ConfigError("unable to read component configuration TOML") from None

    _check_fields(data, _ROOT_FIELDS, _ROOT_FIELDS - {"synthetic", "execution_route"}, "configuration")
    synthetic = data.get("synthetic", False)
    if type(synthetic) is not bool:
        raise ConfigError("synthetic must be a boolean")
    route = data.get('execution_route')
    if route is not None and (route not in ('http_synthetic', 'local_synthetic') or not synthetic):
        raise ConfigError('execution_route requires a recognized synthetic route')
    plugins = _strings(data["plugins"], "plugins")
    if set(plugins) - set(allowed_plugins) or len(set(plugins)) != len(plugins):
        raise ConfigError("configuration contains an unregistered plugin")

    entries = data["components"]
    if not isinstance(entries, dict):
        raise ConfigError("components must be a table")
    components = {}
    for name, entry in entries.items():
        if re.fullmatch(r"[a-z][a-z0-9_-]*", name) is None:
            raise ConfigError("invalid component name")
        components[name] = _component(entry)
    lease_ttl = _seconds(data["lease_ttl_seconds"], "lease_ttl_seconds")
    component_timeout = _seconds(data["component_timeout_seconds"], "component_timeout_seconds")
    if lease_ttl <= component_timeout:
        raise ConfigError("lease_ttl_seconds must exceed component_timeout_seconds")
    return Config(
        db_path=_absolute(data["db_path"], "db_path"),
        artifact_root=_absolute(data["artifact_root"], "artifact_root"),
        journal_dir=_absolute(data["journal_dir"], "journal_dir"),
        device_id=_string(data["device_id"], "device_id"),
        capabilities=_strings(data["capabilities"], "capabilities"),
        lease_ttl_seconds=lease_ttl,
        component_timeout_seconds=component_timeout,
        plugins=plugins,
        components=MappingProxyType(components),
        synthetic=synthetic,
        execution_route=route,
    )
