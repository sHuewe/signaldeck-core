import json
import logging.config
from pathlib import Path

def setup_logging(logging_config_path: str | None) -> None:
    """
    Configure logging via dictConfig if a JSON file is provided and exists.
    """
    if not logging_config_path:
        return

    p = Path(logging_config_path)
    if not p.exists():
        return

    with p.open("r", encoding="utf-8") as f:
        config_dict = json.load(f)

    logging.config.dictConfig(config_dict)
