import json


def parse_json(path: str):
    """
    Opens a .json file and loads into a dictionary

    Args:
        path: file path

    Returns:
        a dictionary of the parsed content

    """
    try:
        with open(path, "r") as f:
            return json.load(f)

    except FileNotFoundError:
        raise FileNotFoundError(f"JSON file not found: {path}")

    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON format in {path}: {e}")


def table_to_chapter(table_number, data_collection):
    """
    Utility that returns a chapter key for a given table number. Can handle either raw table names
    (i.e. "1.2.3") or table keys (i.e. "dukes_1_2_3").
    Args:
        table_number: the full table number as a string
        data_collection: name of release (i.e. "dukes")

    Returns: chapter key as a string of the form 'chapter_{chapter_no}'

    """
    # remove data collection from table_number if present
    if data_collection in table_number:
        table_number = (table_number
                        .replace(data_collection, "")[1:]
                        .replace("_", ".")
                        .upper())

    first_char = table_number[0]

    if first_char.isnumeric():
        return f"chapter_{first_char}"
    else:
        if first_char in ["I", "J"]:
            return "chapter_1"
        else:
            # further logic to come
            raise NotImplemented("Work in process.")



def check_inputs (data_collection,table_key,etl_config):
    """
    Function that checks if a table is found in the ETL_CONFIG file
    Args:
        data_collection: Name of the data_collection
        table_key: Name of the table
        etl_config: dictionary of ETL configuration

    Returns: True if data_collection and table_key are found
    Raises:
        ValueError if either data_collection or table_key are not found

    """
    if data_collection not in etl_config:
        raise ValueError(f"{data_collection} data not found")
    else:
        for chapter_key in etl_config[data_collection]:
                if table_key not in etl_config[data_collection][chapter_key]:
                    raise ValueError(f"{table_key} value not found in {data_collection}")
    return True


def table_key_to_name(table_key: str, data_collection: str):
    """
    Transforms a table_key into a human-readable table name. For example, from dukes_1_2_3 to 1.2.3
    Args:
        table_key: original table key
        data_collection: data collection name

    Returns:
        the table name as a string

    """

    tab_name = (table_key
               .replace(data_collection, "")[1:]
               .replace("_", "."))

    return tab_name