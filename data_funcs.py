from utils import *

template_file_path = "data/templates/dukes_ch_1.xlsx"


def process_sheet_to_frame(
        url: str,
        template_file_path:str,
        data_collection: str,
        sheet_names: list,
        var_to_melt: str = "Year",
        extra_id_vars: list = [],
        map_on_cols: bool = False):
    """
    A chapter-agnostic function that processes individual sheets into separate frames.
    The list of sheet provided will be parsed and each worksheet will be returned as an individual
    processed dataframe.
    The function handles tables with time index on either axes, allowing the re-mapping of column
    headings through a template if needed.

    Args:
        url: the full HTML path of the workbook
        data_collection: name of the series the workbook belongs to (i.e. "dukes")        sheet_names: list of sheets to be processed
        var_to_melt: if map_on_cols is False, this is the name of the variable on the columns, otherwise is the name of the index column. Default is "Year"
        extra_id_vars: additional columns to be used as id_vars. Not usable if map_on_cols is Trye,
        map_on_cols: whether to transpopse the table before mapping to the template. Default is False.

    Returns:

    """

    # if wishing to use mapping on cols then cannot allow exta id_vars
    if map_on_cols and (len(extra_id_vars) != 0):
        raise ValueError("Cannot include extra id vars while transposing: use the mapping template instead.")

    out = {}

    for sheet in sheet_names:

        # get table from GOV.UK
        if map_on_cols:
            table = (read_sheet_with_titles(file_path = url,
                                            sheet_name = sheet)
                     .set_index(var_to_melt)
                     .T
                     .reset_index(drop=False))
        else:
            table = read_sheet_with_titles(file_path = url,
                                           sheet_name = sheet)

        # first columns is dropped unless otherwise specified
        if table.columns[0] not in extra_id_vars:
            table.drop(columns = table.columns[0],
                       inplace=True)

        # get corresponding template
        template = read_sheet_with_titles(file_path = template_file_path,
                                          sheet_name = sheet)

        # join with template
        table = pd.merge(table,
                         template,
                         right_on = "row",
                         left_index = True)

        table = pd.melt(table,
                        id_vars = list(template.columns) + extra_id_vars,
                        var_name = var_to_melt,
                        value_name = "Value")

        output_name = data_collection + "_" + sheet.replace(".", "_")
        out.update({output_name: table})

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




def process_multi_sheets_to_frame(
        url: str,
        template_file_path: str,
        data_collection: str,
        table_name: str):
    """
    A chapter-agnostic function for processing multisheet workbooks
    where each year is reported on a separate sheet, while columns on each sheet need to be melted.
    The function has special conditional behaviour for some tables that require extra manipulation.

    Args:
        url: the full HTTP address of the table
        table_name: the DUKES table number (x.y.z)

    Returns:
        a dictionary containing the transformed sheets as a single dataframe
    """
    # read the whole workbook
    wb = read_sheet_with_titles(url, sheet_name=None)

    # read the template
    template = read_sheet_with_titles(template_file_path,
                                      sheet_name=table_name)

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
    if table_name == "J.1":
        # for heat reallocation, units need to be inferred
        res["unit"] = (res["fuel"]
                       .apply(lambda x: x.split("(")[-1])
                       .str.replace(")", "")
                       .str.strip())

    res["fuel"] = res["fuel"].apply(lambda x: x.split("(")[0].strip())

    output_name = data_collection  + "_" + table_name.replace(".", "_")
    return {output_name: res}