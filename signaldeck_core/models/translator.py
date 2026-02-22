import json
from typing import Any, Protocol
from importlib import resources

from signaldeck_sdk.context import Translator

class TranslatorImpl(Translator):
    """
    A translator implementation.
    """

    def __init__(self, lang: str, fallback_lang: str):
        self.language = lang
        self.fallback_language = fallback_lang
        self._primary: dict[str, str] = {}
        self._fallback: dict[str, str] = {}

    def load_from_packages(self, packages: list[str]) -> None:
        """
        Load/merge translations from packages.

        Merge order matters: later packages overwrite earlier ones.
        This lets instance-specific packages (if you ever add them) override defaults.
        """
        self._primary = {}
        self._fallback = {}

        for pkg in packages:
            self._merge_into(self._fallback, pkg, self.fallback_language)
        for pkg in packages:
            self._merge_into(self._primary, pkg, self.language)

    def t(self, key: str, **kwargs: Any) -> str:
        """
        Translate key using active language + fallback language.
        If not found, returns the key itself.

        Supports Python format placeholders: "Hello {name}".
        """
        text = self._primary.get(key) or self._fallback.get(key) or key
        if kwargs:
            try:
                return text.format(**kwargs)
            except Exception:
                # Don't break rendering on formatting issues; return raw text.
                return text
        return text

    def _merge_into(self, target: dict[str, str], package: str, lang: str) -> None:
        """
        Merge locales/<lang>.json from 'package' into target if present.
        Missing files are ignored by design.
        """
        rel_path = f"locales/{lang}.json"
        try:
            # Works for wheels/zipimport as well
            file = resources.files(package).joinpath(rel_path)
            if not file.is_file():
                return
            data = file.read_text(encoding="utf-8")
            obj = json.loads(data)
            if isinstance(obj, dict):
                # only keep string values
                for k, v in obj.items():
                    if isinstance(k, str) and isinstance(v, str):
                        target[k] = v
        except ModuleNotFoundError:
            # package not installed/importable
            return
        except FileNotFoundError:
            return
        except Exception:
            # ignore broken locale files (or log if you prefer)
            return


