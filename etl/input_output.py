import sqlite3
import pandas as pd
import requests
from bs4 import BeautifulSoup
import re
import utils as u
import sql.sql_utils as sql
import config.settings as stgs
import os
import logging
import datetime



def read_and_wrangle_wb(
        file_path: str,
        sheet_name: str = None):

    """
    Read Excel workbooks removing unnecessary header rows.
    By default, the function parses the whole workbook, excluding sheets with a single non-empty column.
    Thr behaviour can be modified to read a specific sheet, in which case the function returns a dataframe
    instead of a dictionary.

    Args:
        file_path: `io` argument in read_excel
        sheet_name: name of sheet to read

    Returns:
        a dictionary of `pd.Dataframe is sheet_name = None or a pd.DataFrame otherwise`
    """

    # read the workbook
    wb = pd.ExcelFile(file_path)

    # get the list of sheets
    sheets = wb.sheet_names

    if sheet_name is not None:
        sheets = [sheet_name]

    # parse each worksheet removing headers
    wb_as_dict = {}

    for sheet in sheets:

        # first row will always include title
        h = 0
        df = wb.parse(sheet, header=h)

        # skip sheet if believed to be non-data
        # i.e. if 1 column only
        if len(df.columns) == 1:
            continue

        # increase header until the actual table heading is reached
        while "Unnamed" in df.columns[1]:
            h += 1
            df = wb.parse(sheet, header=h)

        # add to dictionary
        wb_as_dict.update({sheet: df})

    # close the Excel workbook
    wb.close()

    # return df if specific sheet is required
    if sheet_name is not None:
        return wb_as_dict[sheet_name]
    else:
        return wb_as_dict


#TODO: abstract the function below so that it works with other data collections
# This also requires changing the signature to include a data_collection argument

def get_dukes_urls(url):
    """
    Use requests and BeautifulSoup to extract links to Excel
    files from the GOV.UK website and organise them into
    a dictionary with the following structure:
    ```
    {"dukes_table_no":
        {"name": "table_name",
        "url": "table url.xlsx"}
    ...}

    All the parameters are inferred from the webpage content. Table numbers
    are extracted from the URL title as well as the table name, the URL is automatically parsed.

    Args:
        url: the HTTP address of the DUKES chapter

    Returns:
        a dictionary of DUKES tables with their respective urls.
    """

    # Fetch the page content
    response = requests.get(url)
    soup = BeautifulSoup(response.content, "html.parser")

    # Initialize the result dictionary
    dukes_tables = {}

    # Find all links to Excel files
    for link in soup.find_all("a", href=True):
        href = link["href"]
        if href.endswith(".xlsx") or href.endswith(".xls"):
            # Extract the table number using regex
            match = re.search(r"DUKES\s*(([A-Z]|\d+)(\.\d+)*)([a-z]*)",
                              link.text,
                              re.IGNORECASE)
            if match:
                table_number = match.group(1).replace(".", "_")
                suffix = match.group(4).lower()
                key = f"dukes_{table_number}{suffix}"
                name = link.text.strip()
                full_url = href if href.startswith("http") else f"https://www.gov.uk{href}"
                dukes_tables[key] = {"name": name, "url": full_url}

    return dukes_tables


def generate_config(data_collection: str,
                    table_key: str,
                    chapter_key: str,
                    templates: dict,
                    urls: dict,
                    etl_config: dict):
    """
    Generates a dictionary with all necessary information for the ETL to run properly on a table.
    This requires environment variables to be set correctly in the config/ directory.
    Args:
        data_collection: the collection the table belongs to
        table_key: table key in the form data_collection + "_" + table number
        chapter_key: chapter of the table in the form "chapter_x"
        templates: dictionary of templates by data_collection. Should be set in config/.
        urls: dictionary of URLs for individual chapter by data_collections. Should be set in config/.
        etl_config: detailed runtime parameters for the ETL. Should be set in config/.

    Returns:

    """
    # get static config dict
    config = etl_config[data_collection][chapter_key][table_key]

    # determine table url
    chapter_page_url = urls[data_collection][chapter_key]
    url = get_dukes_urls(url=chapter_page_url)[table_key]["url"]

    # determine the template file path
    template_file_path = templates[data_collection][chapter_key]

    # add url, template_path and data_collection to f_args
    config["f_args"].update({
        "url": url,
        "template_file_path": template_file_path,
        "data_collection": data_collection
    })

    return config


def export_table(data_collection: str,
                 output_path: str,
                 output_ts: str,
                 file_type: str,
                 table_name: str = None,
                 table_key: str = None):
    try:
        if not (table_key or table_name):
            raise TypeError("Must pass table identified.")

        if table_key is None:
            table_key = data_collection + "_" + table_name.replace(".", "_")

        if table_name is None:
            table_name = u.table_key_to_name(table_key=table_key,
                                             data_collection=data_collection)

        # read data from sql
        query = f"""
            SELECT *
            FROM 
                {data_collection}_prod
            WHERE 
                data_collection = ?
                AND table_name = ?
        """
        df = sql.read_sql_as_frame(conn_path=stgs.DB_PATH,
                                   query=query,
                                   query_params=(data_collection, table_name))

        file_name = table_key + "_" + output_ts + f".{file_type}"
        output_path = os.path.join(output_path, file_name)

        logging.info(f"Saving {table_key} to {file_type}")
        if file_type == "csv":
            df.to_csv(output_path)
        elif file_type == "parquet":
            df.to_parquet(output_path)
        elif file_type == "xlsx":
            df.to_excel(output_path, sheet_name=table_name)
        else:
            raise TypeError(f"Exporting unsupported to file type {file_type}.")

    except Exception as e:
        logging.error(f"Export failed for {table_key}: \n{e}")

    logging.info(f"Successfully created {output_path + file_name}")


def export_all(data_collection: str,
               output_path: str,
               file_type: str,
               bulk_export: bool = False):
    output_ts = datetime.datetime.now().isoformat()

    try:
        if not bulk_export:
            chapter_list = stgs.ETL_CONFIG[data_collection].keys()

            for chapter in chapter_list:
                logging.info(f"Saving tables from {data_collection}, {chapter}")
                table_list = stgs.ETL_CONFIG[data_collection][chapter].keys()

                for table_key in table_list:
                    export_table(data_collection=data_collection,
                                 output_path=output_path,
                                 output_ts=output_ts,
                                 file_type=file_type,
                                 table_key=table_key)

                logging.info(f"Finished exporting [chapter]")

            logging.info(f"All tables from {data_collection} exported succesfully to {file_type}")

        else:
            # export all tables in the data collection to a single file
            logging.info(f"Reading the {data_collection} production table.")
            df = sql.read_sql_as_frame(conn_path=stgs.DB_PATH,
                                       query=f"SELECT * FROM {data_collection}_prod")
            file_name = f"{data_collection}_{output_ts}.{file_type}"
            output_path = os.path.join(output_path, file_name)

            logging.info(f"Saving {data_collection} to {file_type}.")

            if file_type == "csv":
                df.to_csv(output_path)
            elif file_type == "parquet":
                df.to_parquet(output_path)
            elif file_type == "xlsx":
                # save one table per sheet in a single workbook
                logging.info(f"Creating workbook {output_path + file_name}.")
                with pd.ExcelWriter(output_path) as wr:

                    for table_name in df["table_name"].unique():
                        df[df.table_name == table_name].to_excel(wr, sheet_name=table_name)


            else:
                raise TypeError(f"Exporting unsupported to file type {file_type}.")

            logging.info(f"Successfully saved {output_path + file_name}")

    except Exception as e:
        logging.error(f"Export failed for {table_key}: \n{e}")


