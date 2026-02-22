from __future__ import annotations

import importlib
import json
import re
from dataclasses import dataclass
from getpass import getpass
from typing import Any
from signaldeck_sdk import Placeholder




# ---------- Placeholder handling ----------

_PLACEHOLDER_FULL_RE = re.compile(r"^\$([a-zA-Z0-9_]+)\$$")


def _find_placeholders(obj: Any) -> list[str]:
    """
    Return placeholders in first-seen order, scanning dict/list recursively.
    Only detects placeholders when the whole value is exactly "$NAME$".
    """
    seen: set[str] = set()
    ordered: list[str] = []

    def walk(x: Any) -> None:
        if isinstance(x, dict):
            for v in x.values():
                walk(v)
        elif isinstance(x, list):
            for v in x:
                walk(v)
        elif isinstance(x, str):
            m = _PLACEHOLDER_FULL_RE.match(x.strip())
            if m:
                name = m.group(1)
                if name not in seen:
                    seen.add(name)
                    ordered.append(name)

    walk(obj)
    return ordered


def _substitute_placeholders(obj: Any, values: dict[str, Any]) -> Any:
    """
    Substitute "$NAME$" leaf strings using provided values.
    Preserves types (int/bool/float) because placeholders are whole-value.
    """
    if isinstance(obj, dict):
        return {k: _substitute_placeholders(v, values) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_substitute_placeholders(v, values) for v in obj]
    if isinstance(obj, str):
        m = _PLACEHOLDER_FULL_RE.match(obj.strip())
        if m:
            key = m.group(1)
            if key not in values:
                raise KeyError(f"Missing placeholder value for ${key}$")
            return values[key]
        return obj
    return obj


# ---------- Prompting ----------

def _cast_value(raw: str, typ: str) -> Any:
    t = (typ or "str").lower()

    if t == "str" or t == "path":
        return raw
    if t == "int":
        return int(raw)
    if t == "float":
        return float(raw)
    if t == "bool":
        return raw.strip().lower() in ("1", "true", "yes", "y", "on")
    if t == "secret":
        # secret value comes from getpass, so keep as str
        return raw

    # unknown type -> treat as string
    return raw


def _prompt_for_value(meta: Placeholder, current_default: Any | None) -> Any:
    """
    Prompt user; Enter accepts default; type-casts based on meta.type.
    """
    prompt = meta.prompt or meta.name
    if meta.help:
        print(f"{prompt} ({meta.help})")

    default = current_default
    suffix = f" [{default}]" if default is not None and default != "" else ""
    while True:
        if meta.type.lower() == "secret":
            raw = getpass(f"{prompt}{suffix}: ").strip()
        else:
            raw = input(f"{prompt}{suffix}: ").strip()

        if raw == "":
            if default is not None:
                return default
            print("Value required.")
            continue

        try:
            return _cast_value(raw, meta.type)
        except Exception as e:
            print(f"Invalid value: {e}. Please try again.")


def _meta_map_from_processor(cls: type) -> dict[str, Placeholder]:
    """
    If the processor provides config_placeholders(), use it.
    Otherwise return empty.
    """
    metas: dict[str, Placeholder] = {}

    fn = getattr(cls, "config_placeholders", None)
    if fn and callable(fn):
        items = fn()
        if items:
            for it in items:
                # allow either Placeholder or dict-like
                if isinstance(it, Placeholder):
                    metas[it.name] = it
                elif isinstance(it, dict):
                    metas[it["name"]] = Placeholder(**it)
                else:
                    # duck-typing: has attributes
                    metas[getattr(it, "name")] = Placeholder(
                        name=getattr(it, "name"),
                        prompt=getattr(it, "prompt"),
                        type=getattr(it, "type", "str"),
                        default=getattr(it, "default", None),
                        help=getattr(it, "help", None),
                    )
    return metas


# ---------- Main entry: load + render ----------

def render_processor_config(processor_class_path: str) -> dict[str, Any]:
    """
    Load processor default config, interactively fill placeholders, return final dict.
    """
    mod_name, cls_name = processor_class_path.rsplit(".", 1)
    mod = importlib.import_module(mod_name)
    cls = getattr(mod, cls_name)

    # Load default config (required contract)
    if not hasattr(cls, "get_default_config") or not callable(getattr(cls, "get_default_config")):
        raise RuntimeError(
            f"{processor_class_path} does not implement get_default_config(). "
            "Ensure it inherits from signaldeck-sdk ProcessorBase."
        )

    default_cfg = cls.get_default_config()
    placeholders = _find_placeholders(default_cfg)

    if not placeholders:
        return default_cfg

    metas = _meta_map_from_processor(cls)

    # Ask user for values; reuse values for repeated placeholders.
    values: dict[str, Any] = {}
    for name in placeholders:
        meta = metas.get(name) or Placeholder(
            name=name,
            prompt=name.replace("_", " ").title(),
            type="str",
            default=None,
            help=None,
        )

        # default priority:
        # 1) already answered
        # 2) meta.default
        default = values.get(name, meta.default)
        values[name] = _prompt_for_value(meta, default)

    final_cfg = _substitute_placeholders(default_cfg, values)
    return final_cfg