from src.read_write import read_and_wrangle_wb
from src.utils import remove_note_tags
import pandas as pd


def process_sheet_to_frame(
        url: str,
        template_file_path: str,
        data_collection: str,
        sheet_names: list,
        var_to_melt: str = "Year",
        has_multi_headers: bool = False,
        transpose_first: bool = False,
        drop_cols: list = None,
        ignore_mapping: bool = False,
        id_var_position: int = None,
        id_var_name: str = None,
        unit: str = None
):
    """
    A chapter-agnostic function that processes individual sheets into separate frames.
    The list of sheet provided will be parsed and each worksheet will be returned as an individual
    processed dataframe.
    The function handles tables with time index on either axes, allowing the re-mapping of column
    headings through a template if needed.

    Args:
        sheet_names: list of sheets to be processed
        template_file_path: local path of mapping template
        url: the full HTML path of the workbook
        data_collection: name of the series the workbook belongs to (i.e. "dukes")        sheet_names: list of sheets to be processed
        var_to_melt: if map_on_cols is False, this is the name of the variable on the columns, otherwise is the name of the index column. Default is "Year"
        has_multi_headers: whether the table as a 2-levels header that starts on column B.
        drop_cols: list of column names to drop before transposing (if required) and processing. Columns can vary across sheets.
        transpose_first: whether to transpose the table before doing any reshaping. This will use var_to_mel as name for the transposed column headings
        ignore_mapping: if True, ignores the template and reconstructs the index columns using input data
        id_var_position: the 0-indexed position of the column to use as "label" and primary index
        id_var_name: the logical name that the column in id_var_position should assume on the final dataset
        unit: string for unit in table

    Returns:

    """

    if ignore_mapping and not (id_var_name or id_var_position or unit):
        raise ValueError("Must provide details of id columns.")

    out = {}

    for sheet in sheet_names:

        # read raw sheet
        table = read_and_wrangle_wb(file_path=url,
                                    sheet_name=sheet,
                                    has_multi_headers=has_multi_headers)

        # remove unwanted columns
        if drop_cols:
            table.drop(columns=drop_cols,
                       errors="ignore",
                       inplace=True)

        # if transposing, make sure the right column is pivoted into the headers
        if transpose_first:
            table = (table
                     .set_index(var_to_melt)
                     .T
                     .reset_index(drop=False))

        if ignore_mapping:

            # in this case, all index vars need to be reconstructed from
            # available data and from user input
            table["row"] = range(len(table))
            id_var_original_name = table.columns[id_var_position]

            table["label"] = table[id_var_original_name]
            table = table.rename(
                columns={id_var_original_name: id_var_name}
            )
            table["unit"] = unit

            id_vars = ["row",
                       "label",
                       "unit",
                       id_var_name]

        else:
            # all id columns come from template
            table.drop(columns = table.columns[0],
                       inplace=True)

            # get corresponding template
            template = read_and_wrangle_wb(file_path = template_file_path,
                                           sheet_name = sheet)

            # join with template
            table = pd.merge(table,
                             template,
                             right_on = "row",
                             left_index = True)

            id_vars = list(template.columns)

        # variable on columns to lowercase
        var_to_melt = var_to_melt.lower()

        table = pd.melt(table,
                        id_vars = id_vars,
                        var_name = var_to_melt,
                        value_name = "value")

        # remove notes
        for c in table.columns:
            if (c != "label") and (table[c].dtype == "O"):
                table[c] = table[c].apply(remove_note_tags)

        # set index
        table.set_index(id_vars + [var_to_melt],
                        inplace=True)

        out.update({sheet: table})

    return out




def process_dukes_1_1_5(url: str):
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
        tab = read_and_wrangle_wb(url, sheet_name=s)

        # get row number as a column
        tab["row"] = range(len(tab))

        # keep original column
        tab["label"] = tab["Year"].astype(str)

        # encode sector from sheet name
        sector = s.split("1.1.5")[1].strip()

        # flatten columns
        tab = pd.melt(tab,
                      id_vars=["Year", "row", "label"],
                      var_name="fuel",
                      value_name="value")
        tab["sector"] = sector

        # append to master df
        res = pd.concat([res, tab], axis=0)

    res["unit"] = "ktoe"
    res.rename(columns={"Year": "year"}, inplace=True)

    for c in res.columns:
        if (c != "label") and (res[c].dtype == "O"):
            res[c] = res[c].apply(remove_note_tags)

    index_cols = ["year", "sector", "fuel"]
    res.set_index(index_cols, inplace=True)

    return {"1.1.5": res}




def process_multi_sheets_to_frame(
        url: str,
        template_file_path: str,
        data_collection: str,
        table_name: str,
        var_on_sheets: str = "year",
        var_on_cols: str = "fuel",
        has_multi_headers: bool = False,
        skip_sheets: list = None,
        drop_cols: list = None,
        transpose_first: bool = False,
        ignore_mapping: bool = False,
        id_var_position: int = None,
        id_var_name: str = None,
        unit: str = None
):
    """
    A chapter-agnostic function for processing multisheet workbooks
    where each year is reported on a separate sheet, while columns on each sheet need to be melted.
    The function has special conditional behaviour for some tables that require extra manipulation.

    Args:
        data_collection: name of collection the table belongs to
        template_file_path: local path of mapping template
        url: the full HTTP address of the table
        table_name: the DUKES table number (x.y.z)
        has_multi_headers: whether the table as a 2-level header that starts on column B
        var_on_cols: name of the column headings variable (default is fuel)
        var_on_sheets: name of the variable on sheet names (default is year)
        skip_sheets: list of sheets to discard
        drop_cols: list of column names to drop before transposing (if required) and processing. Columns can vary across sheets.
        transpose_first: if True, every sheet is transposed before applying the mapping template
        ignore_mapping: if True, ignores the template and reconstructs index variables with input data
        id_var_position: 0-indexed column position for the "label" variable
        id_var_name: the name that the row index label should assume in the final dtaset
        unit: string for unit

    Returns:
        a dictionary containing the transformed sheets as a single dataframe
    """
    if ignore_mapping and not (id_var_position or id_var_name or unit):
        raise ValueError("must provide id columns details.")

    # read the whole workbook
    wb = read_and_wrangle_wb(url,
                                 skip_sheets=skip_sheets)

    if not ignore_mapping:
        # read the template
        template = read_and_wrangle_wb(template_file_path,
                                       sheet_name=table_name,
                                       skip_sheets=skip_sheets,
                                       has_multi_headers=has_multi_headers)

    res = pd.DataFrame()

    # process each sheet
    # note that there will be unwanted sheets, hence we need to exclude them
    for sheet in wb:

        # skip all sheets named not like a year
        if not sheet.isnumeric():
            continue

        tab = wb[sheet]

        if drop_cols:
            tab.drop(columns=drop_cols,
                     errors="ignore",
                     inplace=True)

        if transpose_first:
            tab = (tab.set_index(tab.columns[0])
                   .T
                   .reset_index(drop=False))

        if ignore_mapping:

            tab["row"] = range(len(tab))
            id_var_original_name = tab.columns[id_var_position]
            tab["label"] = tab[id_var_original_name]
            tab = tab.rename(
                columns={id_var_original_name: id_var_name}
            )
            tab["unit"] = unit
            id_vars = ["row",
                       "label",
                       id_var_name,
                       "unit"]

        else:
            # all id vars come from template
            tab.drop(columns=tab.columns[0],
                     inplace=True)

            # get index data from template
            # template is defined if ignore_mapping = False
            tab = pd.merge(tab,
                           template,
                           left_index=True,
                           right_on="row")
            id_vars = list(template.columns)

        # flatten
        tab = pd.melt(tab,
                      id_vars=id_vars,
                      var_name=var_on_cols,
                      value_name="value")

        # add sheet name as a variable
        tab[var_on_sheets] = int(sheet)

        # append to master
        res = pd.concat([res, tab], axis=0)

    # clean former column heading
    if table_name == "J.1":
        # for heat reallocation, units need to be inferred
        res["unit"] = (res["fuel"]
                       .apply(lambda x: x.split("(")[-1])
                       .str.replace(")", "")
                       .str.strip())

        res["fuel"] = res["fuel"].apply(lambda x: x.split("(")[0].strip())

    for c in res.columns:
        if (c != "label") and (res[c].dtype == "O"):
            res[c] = res[c].apply(remove_note_tags)

    # set index
    res.set_index(id_vars + [var_on_sheets, var_on_cols],
                 inplace=True)

    return {table_name: res}



