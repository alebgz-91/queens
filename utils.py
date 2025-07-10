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
        # i.e. if 1 column only)
        if (len(df.columns) == 1):
            continue

        # increase header until the actual table heading is reached
        while "Unnamed" in df.columns[1]:
            h += 1
            df = wb.parse(sheet, header=h)

        # add to dictionary
        wb_as_dict.update({sheet: df})

    # return df if specific sheet is required
    if sheet_name is not None:
        return wb_as_dict[sheet_name]
    else:
        return wb_as_dict

    return df


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


