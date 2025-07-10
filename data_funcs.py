import pandas as pd
from utils import *

TEMPLATE_PATH_CH_1 = "data/templates/dukes_ch_1.xlsx"


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
        table = read_sheet_with_titles(file_path=url,
                                       sheet_name=f"1.3.{sheet}")

        # drop first column as it contains raw labels
        table.drop(columns=table.columns[0], inplace=True)

        # get corresponding template
        template = pd.read_excel(io=TEMPLATE_PATH_CH_1,
                                 sheet_name=f"1.3.{sheet}")

        # join with template
        table = pd.merge(table,
                         template,
                         right_on="row",
                         left_index=True)

        table = pd.melt(table,
                        id_vars=template.columns,
                        var_name="Year",
                        value_name="Value")

        out.update({f"dukes_1_1_{sheet}": table})

    return out


def transform_dukes_1_1_1(url: str, sheet_list: list):
    """
    Cleans the sheets in table 1.1.1 and transform to flat format
    Args:
        url: full HTML address of table 1.1.1
        sheet_list: list of sheets to parse

    Returns:
        a dictionary containing the transformed tables and

    """

    out = {}

    for table in table_list:
        table = read_sheet_with_titles(utl, sheet_name="1.1.1." + sheet)

        # remove col1 and rename index
        table.drop(columns=["Column1"], inplace=True)
        table.index.name = "row"

        # load template
        template = pd.read_excel(TEMPLATE_PATH_CH_1,
                                 sheet_name="1.1.1." + sheet)

        res = pd.merge(table,
                       template,
                       on="row")

        # flatten columns
        res = pd.melt(res,
                      id_vars=template.columns,
                      var_name="Year",
                      value_name="Value")

        out.update({"dukes_1_1_1": res})

    return out


def transform_dukes_1_1_x(url: str, table_id: int):
    """
    Clean DUKES 1.1.x for x = 2 to 6 and transform to flat format.

    Args:
        url: full HTTP address of Excel table
        table_id: the integer x identifying tables 1.1.x

    Returns:
        a dictionary containing the transformed sheets as a pd.DataFrame

    """
    # read and transpose
    # Need to make sure the Year column ends in the headers
    table_no = "1.1." + str(table_id)
    table = (read_sheet_with_titles(url,
                                    sheet_name=table_no)
             .set_index("Year")
             .T
             .reset_index(drop=False))

    # remove raw columns labels
    table.drop(columns=[table.columns[0]], inplace=True)

    template = pd.read_excel(TEMPLATE_PATH_CH_1,
                             sheet_name=table_no)

    table = pd.merge(table,
                     template,
                     right_on="row",
                     left_index=True)

    # to flat format
    table = pd.melt(table,
                    id_vars=template.columns,
                    var_name="Year",
                    value_name="Value")

    return {f"dukes_1_1_{table_id}": table}


def transform_dukes_1_1_5(url: str):
    """
    Function that transforms table 1.1.5 (a multi-sheet time series of energy
    consumption) into a single machine friendly table
    Args:
        url: the full HTTP address of the table

    Returns:
        a dictionary containing the transformed sheet as a single dataframe
    """
    # read the whole workbook to get a list of sheets
    wb = pd.read_excel(url, sheet_name=None)
    sheets = list(wb.keys())

    # remote the non-data sheets
    sheets = [s for s in sheets if "1.1.5" in s]

    # process each sheet separately, then collate into single dataframe
    res = pd.DataFrame()
    for s in sheets:
        tab = read_sheet_with_titles(url, sheet_name=s)

        # encode sector from sheet name
        sector = s.split("1.1.5")[1].strip()

        # flatten columns
        tab = pd.melt(tab,
                      id_vars="Year",
                      var_name="fuel",
                      value_name="energy")
        tab["sector"] = sector

        # clean up fuel names by removing notes
        tab["fuel"] = tab["fuel"].apply(lambda x: x.split("[note")[0].strip())

        # append to master df
        res = pd.concat([res, tab], axis=0)

    res["unit"] = "ktoe"

    return {"dukes_1_1_5": res}


table_no = "1.2"


def transform_balance_multisheet(url: str, table_no: str):
    """
    A chapter-agnostic function for reading balance workbooks
    where years are reported on separate sheets and fuels are reported on columns

    There are special behaviours for tables 1.1 and 6.1, which
    have slight format changes across the sheets.

    Args:
        url: the full HTTP address of the table
        table_no: the DUKES table number (x.y.z)

    Returns:
        a dictionary containing the transformed sheet as a single dataframe
    """
    # read the whole workbook
    wb = read_sheet_with_titles(url, sheet_name=None)

    # read the template
    template = read_sheet_with_titles(TEMPLATE_PATH_CH_1,
                                      sheet_name=table_no)

    res = pd.DataFrame()

    # process each sheet
    # note that there will be unwanted wheets, hence we need to exclude them
    for sheet in wb.keys():

        # skip all sheets named not like a year
        if not sheet.isnumeric():
            continue

        tab = wb[sheet]
        tab.drop(columns=tab.columns[0],
                 inplace=True)

        # get index data from template
        tab = pd.merge(tab,
                       template,
                       left_index=True,
                       right_on="row")

        # flatten
        tab = pd.melt(tab,
                      id_vars=template.columns,
                      var_name="fuel",
                      value_name="value")

        # append to master
        res = pd.concat([res, tab], axis=0)

    # clean former column heading
    if table_no == "J.1":
        # for heat reallocation, units need to be inferred
        res["unit"] = (res["fuel"]
                       .apply(lambda x: x.split("[")[-1])
                       .str.replace("]", "")
                       .str.strip())

    res["fuel"] = res["fuel"].apply(lambda x: x.split("[")[0].strip())

    table_name = table_no.replace(".", "_")
    return {"dukes_" + table_name: res}