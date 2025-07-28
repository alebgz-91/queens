import sqlite3
import pandas as pd
import requests
from bs4 import BeautifulSoup
import re
import sql.sql_utils as sql
import config.settings as stgs
import os
import logging
import datetime



def read_and_wrangle_wb(
        file_path: str,
        has_multi_headers: bool = False,
        sheet_name: str = None,
        skip_sheets: list = None
):

    """
    Read Excel workbooks removing unnecessary header rows.
    By default, the function parses the whole workbook, excluding sheets with a single non-empty column.
    Thr behaviour can be modified to read a specific sheet, in which case the function returns a dataframe
    instead of a dictionary.

    Args:
        file_path: `io` argument in read_excel
        has_multi_headers: whether the table has a two-level column headings that starts on column B. If columb B has a single header, it will be ignored automatically.
        sheet_name: name of sheet to read
        skip_sheets: list of sheets to ignore when parsing the whole workbook.

    Returns:
        a dictionary of `pd.Dataframe is sheet_name = None or a pd.DataFrame otherwise`
    """

    # read the workbook
    wb = pd.ExcelFile(file_path)

    # get the list of sheets
    sheets = wb.sheet_names

    if sheet_name is not None:
        sheets = [sheet_name]
    elif skip_sheets:
        sheets = set(sheets) - set(skip_sheets)

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

        # remove another row if table has multiindex columns
        if has_multi_headers:
            df = wb.parse(sheet, header=h+1)

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
    Scrapes GOV.UK for links to DUKES Excel tables, extracting their numbers and URLs.

    Args:
        url (str): The URL of the DUKES chapter page.

    Returns:
        dict: Dictionary in the form:
              {
                  "1.1": {
                      "name": "DUKES 1.1 Table name",
                      "url": "https://..."
                  },
                  ...
              }
    """
    response = requests.get(url)
    soup = BeautifulSoup(response.content, "html.parser")
    dukes_tables = {}

    for link in soup.find_all("a", href=True):
        href = link["href"]
        if href.lower().endswith((".xlsx", ".xls")):
            link_text = link.text.strip()

            # Allow optional comma and whitespace between DUKES and number
            match = re.search(r"DUKES[\s,]*((\d+)(\.\d+)*)([a-z]*)",
                              link_text,
                              re.IGNORECASE)

            if match:
                table_number = match.group(1)
                suffix = match.group(4).lower()
                key = f"{table_number}{suffix}"
                full_url = href if href.startswith("http") else f"https://www.gov.uk{href}"

                dukes_tables[key] = {
                    "name": link_text,
                    "url": full_url
                }

    return dukes_tables


def generate_config(data_collection: str,
                    table_name: str,
                    chapter_key: str,
                    templates: dict,
                    urls: dict,
                    etl_config: dict):
    """
    Generates a dictionary with all necessary information for the ETL to run properly on a table.
    This requires environment variables to be set correctly in the config/ directory.
    Args:
        data_collection: the collection the table belongs to
        table_name: table number
        chapter_key: chapter of the table in the form "chapter_x"
        templates: dictionary of templates by data_collection. Should be set in config/.
        urls: dictionary of URLs for individual chapter by data_collections. Should be set in config/.
        etl_config: detailed runtime parameters for the ETL. Should be set in config/.

    Returns:

    """
    # get static config dict
    config = etl_config[data_collection][chapter_key][table_name]

    # determine table url
    chapter_page_url = urls[data_collection][chapter_key]
    url = get_dukes_urls(url=chapter_page_url)[table_name]["url"]

    # determine the template file path
    template_file_path = templates[data_collection][chapter_key]

    # add url, template_path and data_collection to f_args
    config["f_args"].update({
        "url": url,
        "template_file_path": template_file_path,
        "data_collection": data_collection
    })

    return config


def export_table(
        data_collection: str,
        file_type: str,
        table_name: str,
        output_path: str,
        output_ts: str = None
):
    """
    Utility that can export a specific table_name within a data_collection to
    flat files. Suports csv, parquet and Excel (xlsx)
    Args:
        data_collection: the data collection name
        output_path:
        output_ts: destination folder of the files. Default is data/outputs/exported/
        file_type: either 'csv', 'parquet' or 'xlsx'
        table_name: the name of the table to export (i.e. 1.2)

    Returns:
        None

    """
    try:

        if output_ts is None:
            output_ts = str(datetime.date.today())

        # get absolute path and normalise path format
        output_path = os.path.abspath(output_path)

        # read data from sql
        query = f"""
            SELECT *
            FROM 
                {data_collection}_prod
            WHERE 
                table_name = ?
        """
        df = sql.read_sql_as_frame(conn_path=stgs.DB_PATH,
                                   query=query,
                                   query_params=(table_name,))

        file_name = (data_collection
                     + "_"
                     + table_name.replace(".","_")
                     + "_"
                     + output_ts
                     + f".{file_type}")
        output_path = os.path.join(output_path, file_name)

        logging.info(f"Saving {data_collection} {table_name} to {file_type}")
        if file_type == "csv":
            df.to_csv(output_path, index=False)
        elif file_type == "parquet":
            df.to_parquet(output_path, index=False)
        elif file_type == "xlsx":
            df.to_excel(output_path, sheet_name=table_name, index=False)
        else:
            raise TypeError(f"Exporting unsupported to file type {file_type}.")

    except Exception as e:
        logging.error(f"Export failed for {data_collection} {table_name}: \n{e}")

    logging.info(f"Successfully created {output_path + file_name}")


def export_all(
        data_collection: str,
        file_type: str,
        output_path: str,
        bulk_export: bool
):
    """
    Export all table sin a given data_collection to flat files. Supports csv, parquet and Excel file types.
    Tables can either be saved as individual files (bulk = False, the detault) or
    as a sigle file (bulk = True). For bulk export to Excel, the individual tables are
    written to separate sheets of the same workbook.
    Args:
        data_collection: Name of the data collection
        file_type: Either 'csv', 'parquet' of 'xlsx'
        bulk_export: if True, exports all tables into a single file. Default is False

    Returns:

    """

    # get absolute path and current timestamp
    output_path = os.path.abspath(output_path)
    output_ts = str(datetime.date.today())

    try:
        if not bulk_export:
            chapter_list = stgs.ETL_CONFIG[data_collection].keys()

            for chapter in chapter_list:
                logging.info(f"Saving tables from {data_collection}, {chapter}")
                table_list = stgs.ETL_CONFIG[data_collection][chapter].keys()

                for table_key in table_list:
                    export_table(
                        data_collection=data_collection,
                        output_path=output_path,
                        output_ts=output_ts,
                        file_type=file_type,
                             table_name=table_key)

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
                df.to_csv(output_path, index=False)
            elif file_type == "parquet":
                df.to_parquet(output_path, index=False)
            elif file_type == "xlsx":
                # save one table per sheet in a single workbook
                logging.info(f"Creating workbook {output_path + file_name}.")
                with pd.ExcelWriter(output_path) as wr:

                    for table_name in df["table_name"].unique():
                        df[df.table_name == table_name].to_excel(wr,
                                                                 sheet_name=table_name,
                                                                 index=False)


            else:
                raise TypeError(f"Exporting unsupported to file type {file_type}.")

            logging.info(f"Successfully saved {output_path + file_name}")

    except Exception as e:
        logging.error(f"Export failed for {table_name}: \n{e}")


