import pandas as pd
import requests
from bs4 import BeautifulSoup
import re



def read_sheet_with_titles(file_path, sheet_name):
    """
    Wrapper of pd.read_excel that reads a worksheet from an Excel file removing any rows above
    the actual column headings.

    Parameters:
    * file_path: `io` argument in read_excel
    * sheet_name: name of sheet to read

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


def clear_notes(label: str):
    return label.split("[")[0].strip()



# URL of the GOV.UK page
# testing for Chapter 1
url_ch_1 = "https://www.gov.uk/government/statistics/energy-chapter-1-digest-of-united-kingdom-energy-statistics-dukes"


def get_dukes_urls(url):
    """
    Use requests and BeautifulSoup to extract links to Excel
    files from the GOV.UK website and organise them into
    a dictionary with the following structure:
    ```
    {
        "dukes_x_y": {
                        "name": "table name",
                        "url": "table url.xlsx"
                    },
        ...}
    ```
    The parameters x,y indicate the DUKES table number and could be
    2 or three tier (i.e. 1_1_1 or 3_1). The keys in the inner dictionary
    are always "name" and "url".

    Parameters:
    * url: the http address of the DUKES chapter
    Returns:
    a `dict` of DUKES tables and their respective urls.
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



#### TESTING

dukes_tables_ch_1 = get_dukes_urls(url = url_ch_1)
table = read_sheet_with_titles(dukes_tables_ch_1["dukes_1_3"]["url"],
                               sheet_name="1.3.A")

table["Column1"] = table["Column1"].apply(clear_notes)

table["Sector"] = table["Column1"].apply(
    lambda x: x.split("-")[0].strip() if ("of which" not in x) else None
)

table["Subsector"] = table["Column1"].apply(
    lambda x: x.split("of which")[-1].strip() if ("of which" in x) else None
).ffill()


table["Fuel"] = table["Column1"].apply(
    lambda x: x.split("-")[-1].strip() if ("of which" not in x) else None
)



