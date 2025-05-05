from utils import *

dukes_tables_ch_1 = get_dukes_urls(
    "https://www.gov.uk/government/statistics/energy-chapter-1-digest-of-united-kingdom-energy-statistics-dukes")

url = dukes_tables_ch_1["dukes_1_3"]["url"]
sheet = "1.3,A"

def transform_dukes_3_1(url: str, sheet_suffixes: list):
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

