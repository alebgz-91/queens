import pandas as pd

from utils import *


def transform_dukes_1_3(url: str, sheet_suffixes: list):
    """
    Clean DUKES 1.3 and transform to machine readable format.
    Args:
        url: direct URL to Excel workbook
        sheet_suffixes: a list of suffixes corresponding to sheet names, i.e. 1.3.A and 1.3.B

    Returns:
        a dictionary of transformed dataframes, one for each sheet passed in sheet_suffixes

    """

    out = {}

    for sheet in sheet_suffixes:
        # get table from GOV.UK
        table = read_sheet_with_titles(file_path = url,
                                       sheet_name = f"1.3.{sheet}")

        # drop first column as it contains raw labels
        table.drop(columns=table.columns[0], inplace=True)

        # get corresponding template
        template = pd.read_excel(io = "data/templates/dukes_ch_1.xlsx",
                                 sheet_name = f"1.3.{sheet}")

        # join with template
        table = pd.merge(table,
                         template,
                         right_on="row",
                         left_index=True)

        table = pd.melt(table,
                        id_vars = template.columns,
                        var_name = "Year",
                        value_name = "Value")

        out.update({f"dukes_1_1_{sheet}": table})

    return out



dukes_tables_ch_1 = get_dukes_urls(
    "https://www.gov.uk/government/statistics/energy-chapter-1-digest-of-united-kingdom-energy-statistics-dukes")

url = dukes_tables_ch_1["dukes_1_1_1"]["url"]

def transform_dukes_1_1_1(url: str):
    """
    Clean DUKES 1.3 and transform to flat format.

    Args:
        url: full HTTP address of Excel table

    Returns:
        a dictionary containing the transformed sheets as a pd.DataFrame

    """
    # read and transpose
    table = read_sheet_with_titles(url, sheet_name="1.1.1").T

    # remove raw columns labels
    table.drop(columns=[table.columns[0]], inplace=True)

    template = pd.read_excel("data/templates/dukes_ch_1_xlsx",
                             sheet_name="1.1.1")

    table = pd.merge(table,
                     template,
                     left_on = "row",
                     right_index=True)

    # to flat format
    table = pd.melt(table,
                    id_vars = template.columns,
                    var_name = "Year",
                    value_name = "Value")

    return {"dukes_1_1_1": table}




