import utils as u
import etl.transformations as tr
import etl.input_output as io
import config.settings as stgs
import sql.sql_utils as sql


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
        # create the raw table if does not exist
        sql_create_main_tab = sql.generate_create_table_sql(
            table_name=data_collection,
            table_env="raw",
            schema_dict=stgs.SCHEMA
        )

        sql_creat_log = sql.generate_create_log_sql()

        sql.execute_sql(
            conn_path=stgs.DB_PATH,
            sql=sql_creat_log + "\n" + sql_create_main_tab
        )

        for table in table_list:

            if raw_table_names:
                # generate keys to fetch config
                table_key = data_collection + "_" + table.replace(".", "_")
            else:
                table_key = table

            u.check_inputs(data_collection=data_collection,
                         table_key=table_key,
                         etl_config=stgs.ETL_CONFIG)

            chapter_key = u.table_to_chapter(table_number=table,
                                           data_collection=data_collection)

            # generate config dictionary
            config = io.generate_config(data_collection=data_collection,
                                     table_key=table_key,
                                     chapter_key=chapter_key,
                                     templates=stgs.TEMPLATES,
                                     urls=stgs.URLS,
                                     etl_config=stgs.ETL_CONFIG)

            # retrieve function callable and args
            f_name = config["f"]
            f_args = config["f_args"]
            f_call = getattr(tr, f_name)

            # execute
            res = u.call_func(func=f_call, args_dict=f_args)

            # placeholder for the time being: return results
            print("Enforcing schema...")
            for table_key in res:
                df = tr.validate_schema(
                    data_collection=data_collection,
                    table_key=table_key,
                    df=res[table_key],
                    schema_dict=stgs.SCHEMA)

                # write into raw table
                to_table = data_collection + "_raw"
                ingest_id = sql.ingest_frame(
                    df=df,
                    table_name=table_key,
                    to_table=to_table,
                    url=f_args["url"],
                    conn_path=stgs.DB_PATH
                )

        return True

    except ValueError as E:
        print(f"Error: {E}")


def update_all_tables(data_collection: str):
    # to get the list of tables look at static config files
    config = stgs.ETL_CONFIG[data_collection]

    # go through each chapter and table
    for chapter_key in config.keys():
        chapter_print_name = chapter_key.replace("_", " ").title()
        print(f"Updating {chapter_print_name}...")

        # execute
        table_list = config[chapter_key].keys()

        update_tables(data_collection=data_collection,
                      table_list=table_list,
                      raw_table_names=False)
    return True
