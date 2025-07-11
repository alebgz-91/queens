import pandas as pd
import requests
from bs4 import BeautifulSoup
import re


def read_sheet_with_titles(file_path, sheet_name):
    """
    Wrapper of pd.read_excel that reads a worksheet from an Excel file removing any rows above
    the actual column headings.

    Args:
        file_path: `io` argument in read_excel
        sheet_name: name of sheet to read

    Returns:
        a `pd.Dataframe`
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



def set_dukes_config(dukes_chapter_urls: dict, dukes_templates: dict):
    """
    Utility function that compiles a dictionary with processing parameters
    for each DUKES table.

    Args:
        dukes_templates: dictionary of local paths for mapping templates (by chapter)
        dukes_chapter_urls: dictionary of URLs, with keys such as 'chapter_x' and values as the url of the chapter page

    Returns:
        a nested dict (JSON style) with initialisation parameters for preprocessing tables

    """
    # scrape the links for all tables
    dukes_tables_urls = {}

    for chapter, url in dukes_chapter_urls.items():
        tb_urls = get_dukes_urls(url = url)
        dukes_tables_urls.update(tb_urls)


    # mapping table to processing method - JSON style
    dukes_config = {
        "chapter_1": {
            "template_file_path": dukes_templates["chapter_1"],
            "dukes_1_1": {
                "f": "process_multi_sheets_to_frame",
                "f_args": {
                    "url": dukes_tables_urls["dukes_1_1"]["url"],
                    "table_name": "1.1"
                }
            },

            "dukes_1_2": {
                "f": "process_multi_sheets_to_frame",
                "f_args": {
                    "url": dukes_tables_urls["dukes_1_2"]["url"],
                    "table_name": "1.2"
                }
            },

            "dukes_1_3": {
                "f": "process_sheet_to_frame",
                "f_args": {
                    "url": dukes_tables_urls["dukes_1_3"]["url"],
                    "sheet_names": ["1.3.A", "1.3.B"]
                }
            },

            "dukes_1_1_1": {
                "f": "process_sheet_to_frame",
                "f_args": {
                    "url": dukes_tables_urls["dukes_1_1_1"]["url"],
                    "sheet_names": ["1.1.1.A", "1.1.1.B", "1.1.1.C"]
                }
            },

            "dukes_1_1_2": {
                "f": "process_sheet_to_frame",
                "f_args": {
                    "url": dukes_tables_urls["dukes_1_1_2"]["url"],
                    "sheet_names": ["1.1.2"],
                    "map_on_cols": True
                }
            },

            "dukes_1_1_3": {
                "f": "process_sheet_to_frame",
                "f_args": {
                    "url": dukes_tables_urls["dukes_1_1_3"]["url"],
                    "sheet_names": ["1.1.3"],
                    "map_on_cols": True
                }
            },

            "dukes_1_1_4": {
                "f": "process_sheet_to_frame",
                "f_args": {
                    "url": dukes_tables_urls["dukes_1_1_4"]["url"],
                    "sheet_names": ["1.1.4"],
                    "map_on_cols": True
                }
            },

            "dukes_1_1_5": {
                "f": "process_dukes_1_1_5",
                "f_args": {
                    "url": dukes_tables_urls["dukes_1_1_5"]["url"],
                }
            },

            "dukes_1_1_6": {
                "f": "process_sheet_to_frame",
                "f_args": {
                    "url": dukes_tables_urls["dukes_1_1_6"]["url"],
                    "sheet_names": ["1.1.6"],
                    "map_on_cols": True
                }
            },

            "dukes_I_1": {
                "f": "process_multi_sheets_to_frame",
                "f_args": {
                    "url": dukes_tables_urls["dukes_I_1"]["url"],
                    "table_name": "I.1"
                }
            },

            "dukes_J_1": {
                "f": "process_multi_sheets_to_frame",
                "f_args": {
                    "url": dukes_tables_urls["dukes_J_1"]["url"],
                    "table_name": "J.1"
                }
            }
            }

    }

    return dukes_config



