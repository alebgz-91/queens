from utils import parse_json

# -----------------------------------------------
# This script parses configuration files
# from raw .json and .ini into python objects
# These should be treated as environment vars.
# ---------------------------------------------

# load .json configuration into dictionaries of variables
TEMPLATES = parse_json("config/templates.json")
URLS = parse_json("config/urls.json")
ETL_CONFIG = parse_json("config/etl_config.lson")
