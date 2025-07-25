import json
import inspect
import re

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



def check_inputs (data_collection: str,
                  table_name: str,
                  etl_config: dict):
    """
    Function that checks if a table is found in the ETL_CONFIG file
    Args:
        data_collection: Name of the data_collection
        table_name: Name of the table
        etl_config: dictionary of ETL configuration

    Returns:
        True if data_collection and table_name are found
    Raises:
        ValueError if either data_collection or table_key are not found

    """
    if data_collection not in etl_config:
        raise ValueError(f"{data_collection} data not found")
    else:
        for chapter_key in etl_config[data_collection]:
                if table_name not in etl_config[data_collection][chapter_key]:
                    raise ValueError(f"Table {table_name} value not found in {data_collection}")
    return True


def call_func(
        func: callable,
        args_dict: dict):
    """
    Call a function on a set of parameters, excluding unnecessary ones.

    Args:
        func: function callable object
        args_dict: dictionary of arguments

    Returns:
        the result of func on the subset of the arguments passed

    """

    # get the signature
    sig = inspect.signature(func)
    accepted_args = sig.parameters.keys()

    # filter the dict to only include valid arguments
    filtered_args = {k: v for k, v in args_dict.items() if k in accepted_args}

    return func(**filtered_args)


def remove_note_tags(text):
    """
    Remove notes indications of the type [note x] or [Note x]
    Args:
        text: text to parse

    Returns:

    """

    # Notes are always surrounded by square brackets
    if not isinstance(text, str):
        return text

    pattern = r"\[\s*note\s+\d+\s*\]"  # matches [note x] with optional whitespace
    return re.sub(pattern, "", text, flags=re.IGNORECASE).strip()
