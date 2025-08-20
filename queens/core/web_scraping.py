import re
import requests
from bs4 import BeautifulSoup


def _get_dukes_urls(url: str)-> dict
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
            pattern = r"DUKES[\s,]*((?:\d+(?:\.\d+)*|[A-Z](?:\.\d+)+))([a-z]*)"
            match = re.search(pattern,
                              link_text,
                              re.IGNORECASE)

            if match:
                table_number = match.group(1)
                suffix = match.group(2).lower()
                key = f"{table_number}{suffix}"
                full_url = href if href.startswith("http") else f"https://www.gov.uk{href}"

                dukes_tables[key] = {
                    "description": link_text,
                    "url": full_url
                }

    return dukes_tables



SCRAPERS_MAP = {
    "dukes": _get_dukes_urls
}


def scrape_urls(data_collection: str, url: str)-> dict:
    """
    Scrape the table urls from the given chapter page
    Args:
        data_collection: name of the data collection
        url: chapter page containing the urls to scrape

    Returns:
        a dictionary of table information
    """

    func = SCRAPERS_MAP.get(data_collection)

    if func is None:
        raise NotImplementedError(f"No scraping for this data collection has been implemented: {data_collection}")

    return  func(url)