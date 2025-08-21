"""
QUEENS settings

- Defaults bundled inside the package (queens/config/*).
- On first import, copy those defaults into the user data dir (via platformdirs).
- Always read runtime config from the user data dir, so users can edit JSON/Excel.
- DB/export paths can be overridden via config.ini (can be set up via CLI or library method).
- Logs are always in the user data dir.
"""

import json
import shutil
import configparser
from typing import Optional
from pathlib import Path

import logging
from logging.handlers import RotatingFileHandler

from platformdirs import user_data_dir
from importlib.resources import files, as_file

# ---------------------------------------------------------------------
# ppp identifiers
# ---------------------------------------------------------------------
APP_NAME = "queens"

# ---------------------------------------------------------------------
# user directories (AppData/… | ~/.local/share/… | ~/Library/Application Support/…)
# ---------------------------------------------------------------------
USER_DIR = Path(user_data_dir(APP_NAME))
USER_DIR.mkdir(parents=True, exist_ok=True)

LOG_DIR = USER_DIR / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

EXPORT_DEFAULT_DIR = USER_DIR / "exports"
EXPORT_DEFAULT_DIR.mkdir(parents=True, exist_ok=True)

CONFIG_INI = USER_DIR / "config.ini"

# ---------------------------------------------------------------------
# Packaged defaults (relative to the queens package root)
# these must be included in the wheel (see pyproject: tool.setuptools.package-data)
# ---------------------------------------------------------------------
CONFIG_FILES = {
    "etl_config.json": "config/etl_config.json",
    "schema.json": "config/schema.json",
    "templates.json": "config/templates.json",
    "urls.json": "config/urls.json",
}
# queens/config/templates/* are copied into USER_DIR/templates/
TEMPLATES_DIR_NAME = "templates"

# ---------------------------------------------------------------
# constants used across the package
# ---------------------------------------------------------------
DTYPES = {
    "TEXT": str,
    "INTEGER": int,
    "REAL": float,
    "DATETIME": None,
}

VALID_OPS = {
    "INTEGER": {"eq", "neq", "lt", "lte", "gt", "gte"},
    "REAL": {"eq", "neq", "lt", "lte", "gt", "gte"},
    "TEXT": {"eq", "neq", "like"},
}

OP_SQL = {
    "eq": "= ?",
    "neq": "<> ?",
    "lt": "< ?",
    "lte": "<= ?",
    "gt": "> ?",
    "gte": ">= ?",
    "like": "LIKE ?",
}

# ---------------------------------------------------------------------
# resource helpers
# ---------------------------------------------------------------------
# 'queens' when imported as queens.settings
PKG_NAME = __package__ or "queens"


def _pkg_to_fs_path(rel_path: str) -> Path:
    """
    Return a real filesystem path for a packaged resource (json/templates),
    whether running from an installed wheel, an editable install, or source tree.
    rel_path is relative to the queens package root (e.g. "config/etl_config.json").
    """
    ref = files(PKG_NAME).joinpath(rel_path)
    with as_file(ref) as src_path:
        return Path(src_path)


def _copy_if_missing(src_pkg_rel: str, dest_path: Path) -> None:
    """Copy a single resource from package to dest if dest does not exist."""
    if dest_path.exists():
        return
    src_path = _pkg_to_fs_path(src_pkg_rel)
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy(src_path, dest_path)


def _copytree_if_missing(src_pkg_rel_dir: str, dest_dir: Path) -> None:
    """Copy a directory tree from package to dest if dest does not exist."""
    if dest_dir.exists():
        return
    src_dir = _pkg_to_fs_path(src_pkg_rel_dir)
    shutil.copytree(src_dir, dest_dir)


def ensure_user_configs() -> None:
    """
    Ensure default JSONs and the templates directory exist in USER_DIR.
    IMPORTANT: paths are relative to package root dir — do not prefix with 'queens/'.
    """
    for file_name, pkg_rel in CONFIG_FILES.items():
        _copy_if_missing(pkg_rel, USER_DIR / file_name)
    _copytree_if_missing(f"config/{TEMPLATES_DIR_NAME}", USER_DIR / TEMPLATES_DIR_NAME)


# run on import to guarantee defaults are present for first use
ensure_user_configs()

# ---------------------------------------------------------------------
# overrides via config.ini
# ---------------------------------------------------------------------
_config = configparser.ConfigParser()
if CONFIG_INI.exists():
    _config.read(CONFIG_INI, encoding="utf-8")

_INI_SECTION = "paths"
_INI_DB_KEY = "db_path"
_INI_EXPORT_KEY = "export_path"


def _read_db_path_from_ini() -> Optional[Path]:
    if _config.has_option(_INI_SECTION, _INI_DB_KEY):
        return Path(_config.get(_INI_SECTION, _INI_DB_KEY)).expanduser()
    return None


def _read_export_path_from_ini() -> Optional[Path]:
    if _config.has_option(_INI_SECTION, _INI_EXPORT_KEY):
        return Path(_config.get(_INI_SECTION, _INI_EXPORT_KEY)).expanduser()
    return None


# ---------------------------------------------------------------------
# resolve actual paths
# ---------------------------------------------------------------------
_DEFAULT_DB_PATH = USER_DIR / "queens.db"

DB_PATH: Path = _read_db_path_from_ini() or _DEFAULT_DB_PATH
EXPORT_DIR: Path = _read_export_path_from_ini() or EXPORT_DEFAULT_DIR

# create parent directories if not exist
DB_PATH.parent.mkdir(parents=True, exist_ok=True)
EXPORT_DIR.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------------------
# load JSON configs from USER_DIR
# ---------------------------------------------------------------------
def _load_json(name: str):
    with open(USER_DIR / name, "r", encoding="utf-8") as f:
        return json.load(f)


ETL_CONFIG = _load_json("etl_config.json")
SCHEMA = _load_json("schema.json")
TEMPLATES = _load_json("templates.json")
URLS = _load_json("urls.json")

# path to user-editable templates dir (Excel files)
TEMPLATES_DIR = USER_DIR / TEMPLATES_DIR_NAME

# ---------------------------------------------------------------------
# public helpers
# ---------------------------------------------------------------------
def reload_settings() -> None:
    """
    Re-read config.ini and JSONs, recompute DB_PATH / EXPORT_DIR.
    Call this after your CLI or programmatic setter changes config.ini.
    """
    global _config, DB_PATH, EXPORT_DIR, ETL_CONFIG, SCHEMA, TEMPLATES, URLS

    _config = configparser.ConfigParser()
    if CONFIG_INI.exists():
        _config.read(CONFIG_INI, encoding="utf-8")

    DB_PATH = _read_db_path_from_ini() or _DEFAULT_DB_PATH
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    EXPORT_DIR = _read_export_path_from_ini() or EXPORT_DEFAULT_DIR
    EXPORT_DIR.mkdir(parents=True, exist_ok=True)

    ETL_CONFIG = _load_json("etl_config.json")
    SCHEMA = _load_json("schema.json")
    TEMPLATES = _load_json("templates.json")
    URLS = _load_json("urls.json")


def set_config(
        db_path: str = None,
        export_path: str = None) -> None:
    """
    Persist user defined configurations (same effect as CLI method config).
    - db_path: where the SQLite DB will live
    - export_path: default export folder
    Both paths are created if missing.
    Applies immediately
    """
    cfg = configparser.ConfigParser()
    if CONFIG_INI.exists():
        cfg.read(CONFIG_INI, encoding="utf-8")

    if _INI_SECTION not in cfg:
        cfg[_INI_SECTION] = {}

    if db_path:
        p = Path(db_path).expanduser()
        p.parent.mkdir(parents=True, exist_ok=True)
        cfg[_INI_SECTION][_INI_DB_KEY] = str(p)

    if export_path:
        p = Path(export_path).expanduser()
        p.mkdir(parents=True, exist_ok=True)
        cfg[_INI_SECTION][_INI_EXPORT_KEY] = str(p)

    with open(CONFIG_INI, "w", encoding="utf-8") as f:
        cfg.write(f)

    # refresh config settings
    reload_settings()



# ------------------------
# logging setup
# ------------------------

def setup_logging(
    level: str = None,
    to_console: bool = True,
    to_file: bool = True,
    file_name: str = "queens.log",
    max_bytes: int = 5_000_000,
    backups: int = 3,
    fmt: str = "%(asctime)s | %(levelname)s | %(name)s | %(message)s",
) -> None:
    """
    Configure root logging once. Safe to call multiple times (idempotent).
    Use only in entrypoints (CLI/API/notebooks) — never inside library modules.
    """
    # Resolve level
    if isinstance(level, str):
        level = level.lower()
        level = {"debug": logging.DEBUG, "info": logging.INFO, "warning": logging.WARNING,
                 "error": logging.ERROR, "critical": logging.CRITICAL}.get(level, logging.INFO)
    elif isinstance(level, int):
        pass
    else:
        level = logging.INFO

    root = logging.getLogger()
    if getattr(root, "_queens_logging_configured", False):
        return

    handlers = []

    if to_file:
        log_file = LOG_DIR / file_name
        log_file.parent.mkdir(parents=True, exist_ok=True)
        fh = RotatingFileHandler(str(log_file), maxBytes=max_bytes, backupCount=backups, encoding="utf-8")
        fh.setFormatter(logging.Formatter(fmt))
        handlers.append(fh)

    if to_console:
        ch = logging.StreamHandler()
        ch.setFormatter(logging.Formatter(fmt))
        handlers.append(ch)

    root.setLevel(level)
    for h in handlers:
        root.addHandler(h)

    root._queens_logging_configured = True  # prevent double-config
