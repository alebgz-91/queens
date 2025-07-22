import sqlite3

import pandas as pd
import requests
from bs4 import BeautifulSoup
import re
import sqlite3


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
