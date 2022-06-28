import os
from pathlib import Path
from typing import TypedDict, Union

import toml

CONFIG_FILENAME = 'config.toml'


class ConfigType(TypedDict):
    """Структура конфига."""

    client_settings: dict
    entries: list[dict]


def get_config(path: Union[str, Path]) -> ConfigType:
    """Парсит и возвращает настройки конфига."""
    config_file_path = os.path.join(path, CONFIG_FILENAME)
    return ConfigType(**toml.load(config_file_path))
