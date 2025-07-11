from IGNORE_dev_notebook import data_collection
from utils import set_dukes_config, table_to_chapter
import data_funcs as pr
from mapping import DUKES_TEMPLATES, DUKES_CHAPTERS_URLS

# Initial main script to execute processing for specified tables

def update_tables(
        data_collection: str,
        table_list: list):
    # generate config dictionary
    dukes_config = set_dukes_config(dukes_templates=DUKES_TEMPLATES,
                                    dukes_chapter_urls=DUKES_CHAPTERS_URLS)

    for table in table_list:

        # generate keys to fetch config
        table_key = data_collection + "_" + table.replace(".", "_")

        #TODO - validate table name
        # Need to create an auxiliary function to check if the table exists or not

        chapter_key = table_to_chapter(table_number=table,
                                       data_collection=data_collection)



        # chapter templates
        template_file_path = dukes_config[chapter_key]["template_file_path"]

        # function name and args
        f_name = dukes_config[chapter_key][table_key]["f"]
        f_args = dukes_config[chapter_key][table_key]["f_args"]

        # additional_args
        f_args.update({
            "data_collection": data_collection,
            "template_file_path": template_file_path
        })

        # get callable from module
        f_call = getattr(pr, f_name)
        res = f_call(**f_args)

        #TODO code that will write tables to DB
        # Need to wrap this into a separate module

        # placeholder for the time being
        print(res.keys())


def update_all_tables(data_collection: str):

    config = set_dukes_config(dukes_templates=DUKES_TEMPLATES, dukes_chapter_urls=DUKES_CHAPTERS_URLS)

    # pass the list of keys to the update_tables function
    update_tables(data_collection = data_collection,
                  table_list=config.keys())



