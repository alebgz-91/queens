import datetime
from etl.input_output import read_and_wrangle_wb
from utils import remove_note_tags
import pandas as pd
from config.settings import DTYPES

def process_sheet_to_frame(
        url: str,
        template_file_path: str,
        data_collection: str,
        sheet_names: list,
        var_to_melt: str = "Year",
        map_on_cols: bool = False):
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
        map_on_cols: whether to transpose the table before mapping to the template. Default is False.

    Returns:

    """

    out = {}

    for sheet in sheet_names:

        # get table from GOV.UK
        if map_on_cols:
            table = (read_and_wrangle_wb(file_path = url,
                                         sheet_name = sheet)
                     .set_index(var_to_melt)
                     .T
                     .reset_index(drop=False))
        else:
            table = read_and_wrangle_wb(file_path = url,
                                        sheet_name = sheet)

        # first columns is dropped unless otherwise specified
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

        # variable on columns to lowercase
        var_to_melt = var_to_melt.lower()

        table = pd.melt(table,
                        id_vars = list(template.columns),
                        var_name = var_to_melt,
                        value_name = "value")

        # remove notes
        for c in table.columns:
            if (c != "label") and (table[c].dtype == "O"):
                table[c] = table[c].apply(remove_note_tags)

        # set index
        table.set_index(list(template.columns) + [var_to_melt],
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
        tab.index.name = "row"
        tab.reset_index(drop=False, inplace=True)

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
        table_name: str):
    """
    A chapter-agnostic function for processing multisheet workbooks
    where each year is reported on a separate sheet, while columns on each sheet need to be melted.
    The function has special conditional behaviour for some tables that require extra manipulation.

    Args:
        data_collection: name of collection the table belongs to
        template_file_path: local path of mapping template
        url: the full HTTP address of the table
        table_name: the DUKES table number (x.y.z)

    Returns:
        a dictionary containing the transformed sheets as a single dataframe
    """
    # read the whole workbook
    print(url)
    wb = read_and_wrangle_wb(url)

    # read the template
    template = read_and_wrangle_wb(template_file_path,
                                   sheet_name=table_name)

    res = pd.DataFrame()

    # process each sheet
    # note that there will be unwanted sheets, hence we need to exclude them
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

        # add year from sheet name
        tab["year"] = int(sheet)

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
    res.set_index(list(template.columns) + ["fuel", "year"],
                 inplace=True)

    return {table_name: res}



def validate_schema(
        data_collection: str,
        table_name: str,
        df: pd.DataFrame,
        schema_dict: dict
):

    # check for duplicates
    if df.index.duplicated().sum() > 0:
        raise ValueError(f"There are duplicates in table {table_name} of data collection {data_collection}. Check mapping table.")

    schema = schema_dict[data_collection]

    # Add constant index columns
    df.reset_index(drop=False, inplace=True)


    # add id cols as a column                               data_collection=data_collection)
    df["table_name"] = table_name

    # check data types and cast
    for col_name in df:

        if col_name not in schema:
            raise ValueError(f"Unexpected column not in schema for table {data_collection} {table_name}: {col_name}")

        exp_dtype = schema[col_name]["type"]
        exp_null = schema[col_name]["nullable"]

        if DTYPES[exp_dtype] is float:
            df[col_name] = pd.to_numeric(df[col_name],
                                         errors="coerce")

            # check that the conversion has gone well. Some nulls are expected
            # due to suppression symbols being present in the data
            # but there should be non-null values
            non_null_count = df[col_name].notnull().sum()
            if non_null_count == 0:
                raise ValueError(f"Values cannot be parse to numeric data. Check transformator for table {data_collection} {table_name}.")

        elif DTYPES[exp_dtype] is int:
            df[col_name] = pd.to_numeric(df[col_name],
                                             errors="coerce",
                                             downcast="integer")
        elif DTYPES[exp_dtype] is str:
            df[col_name] = df[col_name].astype(str)

        else:
            # no action for now, will likely need to handle further types
            pass

        # check nulls
        n_rows = len(df)
        n_non_nulls = df[col_name].notnull().sum()
        if (n_rows > n_non_nulls) and (not exp_null):
            raise ValueError(f"Column {col_name} is not nullable but NULLs were found.")

    return df
