from src.utils import parse_json
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

# load .json configuration into dictionaries of variables
TEMPLATES = parse_json("config/templates.json")
URLS = parse_json("config/urls.json")
ETL_CONFIG = parse_json("config/etl_config.lson")

# data type dict for schema enforcement
DTYPES = {
    "TEXT": str,
    "INTEGER": int,
    "REAL": float,
    "DATETIME": None
}

# schema for DB tables
SCHEMA = parse_json("config/schema.json")