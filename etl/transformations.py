import logging
import re
import numpy as np

from core.read_write import read_and_wrangle_wb
from core.utils import remove_note_tags
import pandas as pd

def _postprocess_1_1_5(out_dict: dict):
    """
    Recode sector removing the sheet name tags.
    """

    out = {}
    for key, df in out_dict.items():
        index_cols = list(df.index.names)
        df.reset_index(drop=False, inplace=True)
        df["sector"] = df["sector"].apply(lambda s: s.split("1.1.5")[1].strip())

        df.set_index(index_cols, inplace=True)
        out.update({key: df})

    return out


def _postprocess_J_1(out_dict: dict):
    """
    Reconstructs fuel and units for table J.1 due to unusual layout
    """

    # for heat reallocation, units need to be inferred
    out = {}
    for key, df in out_dict.items():
        index_cols = list(df.index.names)
        df.reset_index(drop=False, inplace=True)

        df["unit"] = (df["fuel"]
                       .apply(lambda x: x.split("(")[-1])
                       .str.replace(")", "")
                       .str.strip())

        df["fuel"] = df["fuel"].apply(lambda x: x.split("(")[0].strip())

        df.set_index(index_cols, inplace=True)
        out[key] = df

    return out


def _postprocess_5_2(out_dict):
    """
    Split compound variable into separate columns
    """
    out = {}

    for key,df in out_dict.items():
        index_cols = list(df.index.names)
        df.reset_index(drop=False, inplace=True)

        df["sector"] = (df["raw_idx"]
                        .apply(lambda s: s.split("(")[0].strip())
                        .str.replace("Total", "All"))
        df["year"] = (df["raw_idx"]
                      .apply(lambda s: s.split("(")[1].strip())
                      .str.replace(")", "")
                      .str.strip()
                      .astype(int))
        # remove compound index and add resulting variables
        index_cols.remove("raw_idx")
        index_cols.extend(["sector", "year"])

        df.set_index(index_cols, inplace=True)
        out[key] = df


def _postprocess_normalize_names(out_dict: dict):
    """
    Recodes sheet names in non-standard form to ordinary ids (i.e. 4.4a to 4.4.A)

    """

    out = {}
    for key, df in out_dict.items():
        key_number = key[:-1]
        key_suffix = key[-1]

        new_key = str(key_number) + "." + key_suffix.upper()
        out[new_key] = df

    return out


def _clean_up_str_cols(df: pd.DataFrame):
    """
    Cleans string columns, stripping trailing whitespace and notes tags

    """
    # remove notes
    index_cols = list(df.index.names)
    df.reset_index(drop=False, inplace=True)

    for c in df.columns:
        if (c != "label") and (df[c].dtype == "O"):
            df[c] = df[c].apply(remove_note_tags)

    return df.set_index(index_cols)


# maps tables to custom postprocessing if needed
POSTPROCESSING_MAP = {
    "1.1.5": _postprocess_1_1_5,
    "J.1": _postprocess_J_1,
    "4.4": _postprocess_normalize_names,
    "4.5": _postprocess_normalize_names,
    "5.2": _postprocess_5_2
}


def _postprocess(out_dict: dict, table_name: str):
    """
    Applies custom postprocessing (if exists) and cleans up object columns
    """"""
    Args:
        out_dict: 
        table_name: 

    Returns:

    """
    # shallow copy to avoid mutating the dict
    out = dict(out_dict)

    if table_name:
        func = POSTPROCESSING_MAP.get(table_name)
        if func is not None:
            logging.debug(f"Applying custom postprocessing function {func}")
            out = func(out_dict)

    # clean columns from note tags
    logging.debug("Cleaning note tags.")
    for key, df in out.items():
        out.update({key: _clean_up_str_cols(df)})

    return out


def _is_data_sheet(
        sheet_name: str,
        pattern: str = None) -> bool:
    """
    If `pattern` is provided return True when it matches `sheet_name`
    (using regex.search for flexibility). If no pattern is provided, fall back to numeric sheets.
    """
    if pattern is None:
        return sheet_name.isnumeric()

    try:
        regex = re.compile(pattern)
    except re.error as e:
        raise ValueError(f"Invalid sheet selection regex: {e}")

    return bool(regex.search(sheet_name))



def process_sheet_to_frame(
        url: str,
        template_file_path: str,
        data_collection: str,
        sheet_names: list,
        table_name: str = None,
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

    logging.debug("Processing sheets.")
    for sheet in sheet_names:
        logging.debug(f"Processing sheet {sheet}")

        logging.debug("Reading raw data")
        table = read_and_wrangle_wb(file_path=url,
                                    sheet_name=sheet,
                                    has_multi_headers=has_multi_headers)

        logging.debug("Dropping columns if required")
        if drop_cols:
            table.drop(columns=drop_cols,
                       errors="ignore",
                       inplace=True)

        logging.debug("Transposing if required.")
        # if transposing, make sure the right column is pivoted into the headers
        if transpose_first:
            table = (table
                     .set_index(var_to_melt)
                     .T
                     .reset_index(drop=False))

        if ignore_mapping:

            logging.debug("Applying manual mapping.")
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
            logging.debug("Applying template mapping.")
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

        logging.debug("Flattening columns.")
        table = pd.melt(table,
                        id_vars = id_vars,
                        var_name = var_to_melt,
                        value_name = "value")


        # set index

        logging.debug("Setting index.")
        table.set_index(id_vars + [var_to_melt],
                        inplace=True)

        out.update({sheet: table})

    logging.debug("Postprocessing.")
    out = _postprocess(out_dict=out,
                       table_name=table_name)

    return out


def process_multi_sheets_to_frame(
        url: str,
        template_file_path: str,
        data_collection: str,
        table_name: str,
        var_on_sheets: str = "year",
        sheet_name_pattern: str = None,
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
        sheet_name_pattern: a regex patter to match against sheet names. Only sheets that match are processed and consolidated
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

    logging.debug("Read the whole workbook.")
    wb = read_and_wrangle_wb(url,
                                 skip_sheets=skip_sheets)

    if not ignore_mapping:
        logging.debug("Reading template.")
        template = read_and_wrangle_wb(template_file_path,
                                       sheet_name=table_name,
                                       skip_sheets=skip_sheets,
                                       has_multi_headers=has_multi_headers)

    res = pd.DataFrame()

    logging.debug("Processing each sheet")
    # note that there will be unwanted sheets, hence we need to exclude them
    for sheet in wb:

        # skip all sheets named not like a year
        if not _is_data_sheet(sheet, sheet_name_pattern):
            logging.debug(f"Sheet {sheet} was excluded." )
            continue

        tab = wb[sheet]

        if drop_cols:
            logging.debug(f"Dropping columns in sheet {sheet}.")
            tab.drop(columns=drop_cols,
                     errors="ignore",
                     inplace=True)

        if transpose_first:
            logging.debug(f"Transposing sheet {sheet}")
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
        tab[var_on_sheets] = sheet

        # append to master
        res = pd.concat([res, tab], axis=0)
        logging.debug(f"Finished with sheet {sheet}")



    logging.debug("Setting index.")
    res.set_index(id_vars + [var_on_sheets, var_on_cols],
                 inplace=True)

    out = {table_name: res}

    logging.debug("Postprocessing.")
    out = _postprocess(out_dict=out, table_name=table_name)

    return out


def process_dukes_5_1(
        url: str,
        template_file_path: str
):
    """
    Calls transformer on separate sheets with different parameters

    """
    logging.debug("Processing 5.1 main sheet.")
    t_5_1 = process_sheet_to_frame(
        url=url,
        data_collection="dukes",
        template_file_path=template_file_path,
        sheet_names=["5.1"]
    )

    logging.debug("Processing 5.1.A")
    t_5_1_A = process_sheet_to_frame(
        url=url,
        data_collection="dukes",
        template_file_path=template_file_path,
        sheet_names=["5.1.A"],
        drop_cols=["Generator type", "Technology"]
    )

    return {**t_5_1, **t_5_1_A}




def _process_dukes_5_6_summaries(
    url: str,
    template_file_path: str,
    sheet_name: str,
    fixed_header: int
):

    logging.debug("Read shifted dataframe")
    df = read_and_wrangle_wb(url,
                             sheet_name=sheet_name,
                             fixed_header=fixed_header)

    logging.debug("Infer first year in table")
    first_year = df.columns[0].split("5.6.J")[1].split("summary")[0].strip()

    logging.debug("Read dataframe with proper headers")
    df = read_and_wrangle_wb(url,
                             sheet_name=sheet_name,
                             fixed_header=fixed_header + 1)

    logging.debug("Fill in year using interim titles")
    df["year"] = (df["Generator type"]
                  .apply(
        lambda s: s.split("5.6.J")[1].split("summary")[0].strip() if "Table" in s else np.nan)
                  .fillna(method="ffill")

                  .fillna(first_year)

                  )

    # flag rows to keep
    df["keep_row"] = df["Generator type"].apply(
        lambda s: 1 if ("Generator type" not in s) and ("Table" not in s) else np.nan)
    df = (df
          .dropna(axis=0, subset=["keep_row"])
          .reset_index(drop=True))

    df.index.name = "row_raw"
    df.reset_index(drop=False, inplace=True)

    logging.debug("Read template")
    template = read_and_wrangle_wb(template_file_path,
                                   sheet_name=sheet_name)

    # construct joining key
    n_rows = len(template)
    df["row_mod"] = df["row_raw"] % n_rows

    logging.debug("Apply template")
    df = pd.merge(df,
                  template,
                  left_on="row_mod",
                  right_on="row",
                  how="inner")

    logging.debug("Remove working columns")
    df.drop(
        columns=["row",
                 "Generator type",
                 "Indicator",
                 "row_mod",
                 "keep_row"],
        inplace=True
    )

    df.rename(columns={"row_raw": "row"}, inplace=True)

    logging.debug("Melt fuel columns")
    df = pd.melt(df,
                 id_vars=["row", "year", "group", "item", "unit", "label"],
                 var_name="fuel",
                 value_name="value")

    df["fuel"] = df["fuel"].apply(remove_note_tags)

    df.set_index(list(template.columns) + ["fuel"])

    return df


def process_dukes_5_6(
        url: str,
        template_file_path: str
):
    # three sheets to process
    logging.debug("Processing main 5.6 sheet")
    k_1, t_1 = process_sheet_to_frame(
        url=url,
        template_file_path=template_file_path,
        sheet_names=["5.6"],
        data_collection="dukes",
        drop_cols=["Fuel"]
    ).items()

    logging.debug("Processing 5.6 conventional thermal and CCGT")
    k2, t_2 = process_sheet_to_frame(
        url=url,
        template_file_path=template_file_path,
        data_collection="dukes",
        sheet_names=["5.6 Conventional thermal & CCGT"],
        drop_cols=["Generator category"]
    ).items()

    logging.debug("Processing 5.6 Annual summaries")
    t_3 = _process_dukes_5_6_summaries(
        url=url,
        template_file_path=template_file_path,
        sheet_name="5.6 Annual summaries",
        fixed_header=5
    )

    return {
        "5.6.A_G": t_1,
        "5.6.H_I": t_2,
        "5.6.J": t_3
    }