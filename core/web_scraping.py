import re
import requests
from bs4 import BeautifulSoup


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
