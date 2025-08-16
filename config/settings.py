from core.utils import parse_json
from configparser import ConfigParser
# ---
# --------------------------------------------
# This script parses configuration files
# from raw .json and .ini into python objects
# These should be treated as environment vars.
# ---------------------------------------------

# static configuration strings
config_ini = ConfigParser()
config_ini.read("config/config.ini")

DB_PATH = config_ini["DATABASE"]["db_path"]
EXPORT_PATH = config_ini["EXPORTING"]["export_path"]
DEFAULT_LIMIT = config_ini["API"]["default_limit"]
MAX_LIMIT = config_ini["API"]["max_limit"]

# load .json configuration into dictionaries of variables
TEMPLATES = parse_json("config/templates.json")
URLS = parse_json("config/urls.json")
ETL_CONFIG = parse_json("config/etl_config.json")

# data type dict for schema enforcement
DTYPES = {
    "TEXT": str,
    "INTEGER": int,
    "REAL": float,
    "DATETIME": None
}


# valid query binary operators for each type of data
VALID_OPS = {
    "INTEGER": {"eq","neq","lt","lte","gt","gte"},
    "REAL": {"eq","neq","lt","lte","gt","gte"},
    "TEXT":  {"eq","neq","like"}
}

# operators mapped to SQL operators
OP_SQL = {
        "eq":  "= ?",   "neq": "<> ?",
        "lt":  "< ?",   "lte": "<= ?",
        "gt":  "> ?",   "gte": ">= ?",
        "like":"LIKE ?"
    }


# schema for DB tables
SCHEMA = parse_json("config/schema.json")