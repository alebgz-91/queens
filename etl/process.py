from utils import table_to_chapter, check_inputs
from etl import transformations as pr
from etl.input_output import generate_config
from config.settings import TEMPLATES, ETL_CONFIG, URLS


# Initial main script to execute processing for specified tables

def update_tables(
        data_collection: str,
        table_list: list,
        raw_table_names=True):
    """
    Update a selection of tables, fetching new data from source URLs.

    Args:
        data_collection: name of the release the tables belong to. Myst be lowercase
        table_list: a list or iterable of tables to be parsed and updated
        raw_table_names: if False, table_list contains table_keys rather than table numbers, for example "dukes_1_2_3
 instead of "1.2.3"
    Returns:
        True if the processing is successful

    """

    try:
        for table in table_list:

            if raw_table_names:
                # generate keys to fetch config
                table_key = data_collection + "_" + table.replace(".", "_")
            else:
                table_key = table

            check_inputs(data_collection=data_collection,
                                        table_key=table_key,
                                        etl_config=ETL_CONFIG)


            chapter_key = table_to_chapter(table_number=table,
                                           data_collection=data_collection)

            # generate config dictionary
            config = generate_config(data_collection=data_collection,
                                     table_key = table_key,
                                     chapter_key=chapter_key,
                                     templates=TEMPLATES,
                                     urls=URLS,
                                     etl_config=ETL_CONFIG)

            # retrieve function callable and args
            f_name = config["f"]
            f_args = config["f_args"]
            f_call = getattr(pr, f_name)

            # execute
            res = f_call(**f_args)

            # TODO code that will write tables to DB
            # Need to wrap this into a separate module

            # placeholder for the time being: return results
            print(res.keys())

        return res
    except ValueError as E:
        print(f"Incorrect Imput: {E}")
        return None


def update_all_tables(data_collection: str):
    # to get the list of tables look at static config files
    config = ETL_CONFIG[data_collection]

    # go through each chapter and table
    for chapter_key in config.keys():
        chapter_print_name = chapter_key.replace("_", " ").title()
        print(f"Updating {chapter_print_name}...")

        # execute
        table_list = config[chapter_key].keys()

        res = update_tables(data_collection=data_collection,
                            table_list=table_list,
                            raw_table_names=False)
    return True
