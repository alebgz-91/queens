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
    # first row will always include title
    h = 1
    df = pd.read_excel(file_path,
                       sheet_name=sheet_name,
                       header=h)

    while "Unnamed" in df.columns[1]:
        h += 1
        df = pd.read_excel(file_path,
                           sheet_name=sheet_name,
                           header=h)
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
            match = re.search(r"DUKES\s*(\d+(\.\d+)*)([a-z]*)",
                              link.text,
                              re.IGNORECASE)
            if match:
                table_number = match.group(1).replace(".", "_")
                suffix = match.group(3)
                key = f"dukes_{table_number}{suffix}"
                name = link.text.strip()
                full_url = href if href.startswith("http") else f"https://www.gov.uk{href}"
                dukes_tables[key] = {"name": name, "url": full_url}

    return dukes_tables


