import pandas as pd
import pytest

import queens.etl.transformations as tr


# shared fixtures ---------------------------------------------------------

@pytest.fixture(autouse=True)
def patch_remove_note_tags(monkeypatch):
    """
    Make note-tag cleaning deterministic for tests.
    Only strips the literal ' [note]' suffix from strings.
    """
    def fake_remove_note_tags(x):
        if isinstance(x, str):
            return x.replace(" [note]", "")
        return x

    # transformations.py imports remove_note_tags into its module namespace
    monkeypatch.setattr(tr, "remove_note_tags", fake_remove_note_tags)
    yield


# ------------------------------
# unit tests for small helpers
# ------------------------------

def test_is_data_sheet_numeric_and_regex():
    assert tr._is_data_sheet("2019") is True
    assert tr._is_data_sheet("meta") is False
    # regex pattern
    assert tr._is_data_sheet("5.6 Annual summaries", r"5\.6") is True
    assert tr._is_data_sheet("Notes", r"^\d{4}$") is False

def test_is_data_sheet_invalid_regex_raises():
    with pytest.raises(ValueError):
        tr._is_data_sheet("2019", "(")  # invalid regex


# --- process_sheet_to_frame --------------------------------------------------

def test_process_sheet_to_frame_template_mapping(monkeypatch):
    """
    ignore_mapping=False path:
    - Each sheet is read from 'url'
    - A per-sheet template is read from 'template_file_path'
    - First data column is dropped before merge
    - Melt uses var_to_melt (lowercased) as the variable name
    """
    # two simple sheets with years on columns
    sheetA = pd.DataFrame({"X": ["A", "B", "C"], 2019: [1, 2, 3], 2020: [4, 5, 6]})
    sheetB = pd.DataFrame({"X": ["D", "E", "F"], 2019: [7, 8, 9], 2020: [10, 11, 12]})

    # templates keyed by sheet name
    templateA = pd.DataFrame({
        "row": [0, 1, 2],
        "label": ["la", "lb", "lc"],
        "unit": ["ktoe", "ktoe", "ktoe"],
        "fuel": ["Gas", "Coal", "Oil"],
    })
    templateB = pd.DataFrame({
        "row": [0, 1, 2],
        "label": ["ld", "le", "lf"],
        "unit": ["ktoe", "ktoe", "ktoe"],
        "fuel": ["Bio", "Wind", "Solar"],
    })

    def fake_read_and_wrangle(file_path, has_multi_headers=False, sheet_name=None,
                              skip_sheets=None, fixed_header=None):
        # table reads
        if file_path == "URL" and sheet_name == "SheetA":
            return sheetA.copy()
        if file_path == "URL" and sheet_name == "SheetB":
            return sheetB.copy()

        # template reads
        if file_path == "TPL" and sheet_name == "SheetA":
            return templateA.copy()
        if file_path == "TPL" and sheet_name == "SheetB":
            return templateB.copy()
        raise AssertionError("Unexpected read_and_wrangle_wb call")

    monkeypatch.setattr(tr, "read_and_wrangle_wb", fake_read_and_wrangle)

    out = tr.process_sheet_to_frame(
        url="URL",
        template_file_path="TPL",
        data_collection="dukes",
        sheet_names=["SheetA", "SheetB"],
        var_to_melt="Year",  # should be lowercased internally
        has_multi_headers=False,
        transpose_first=False,
        drop_cols=None,
        ignore_mapping=False,
    )

    assert set(out.keys()) == {"SheetA", "SheetB"}

    for k, df in out.items():
        # Index should include template id_vars + 'year'
        idx_names = list(df.index.names)
        assert idx_names[:4] == ["row", "label", "unit", "fuel"]
        assert idx_names[-1] == "year"

        # Basic shape: 3 rows × 2 years = 6
        assert len(df.reset_index()) == 6

    # -check a value
    dfA = out["SheetA"].reset_index()
    v2019_row0 = dfA[(dfA["row"] == 0) & (dfA["year"] == 2019)]["value"].iloc[0]
    v2020_row0 = dfA[(dfA["row"] == 0) & (dfA["year"] == 2020)]["value"].iloc[0]
    assert (v2019_row0, v2020_row0) == (1, 4)


def test_process_sheet_to_frame_manual_ignore_mapping(monkeypatch):
    """
    ignore_mapping=True path:
    - Rebuild id vars from the sheet
    - Drop spare column
    - Note-tag cleaner applies to object cols except 'label'
    """
    sheet = pd.DataFrame({
        "LabelCol": ["AA [note]", "BB", "CC"],  # id_var_position = 0
        2019: [100, 200, 300],
        "SPARE": ["x", "y", "z"],               # will be dropped
        2020: [400, 500, 600],
    })

    def fake_read_and_wrangle(file_path, has_multi_headers=False, sheet_name=None,
                              skip_sheets=None, fixed_header=None):
        assert file_path == "URL"
        assert sheet_name == "OnlySheet"
        return sheet.copy()

    monkeypatch.setattr(tr, "read_and_wrangle_wb", fake_read_and_wrangle)

    out = tr.process_sheet_to_frame(
        url="URL",
        template_file_path="IGNORED",
        data_collection="dukes",
        sheet_names=["OnlySheet"],
        drop_cols=["SPARE"],
        ignore_mapping=True,
        id_var_position=0,
        id_var_name="sector",
        unit="ktoe",
    )

    df = out["OnlySheet"].reset_index()

    # ID vars built
    for col in ["row", "label", "unit", "sector"]:
        assert col in df.columns

    # Cleaner applied: 'sector' cleaned, 'label' left untouched
    assert df["sector"].tolist()[0] == "AA"
    assert df["label"].tolist()[0] == "AA [note]"

    # Melt produced 'value' and lowercased melted var ('year')
    assert "value" in df.columns
    assert "year" in df.columns
    assert len(df) == 3 * 2  # 3 ids × 2 years


def test_postprocess_normalize_names(monkeypatch):
    """
    Table-name-driven postprocessing for 4.4: sheet keys normalized (e.g., 4.4a -> 4.4.A)
    """
    # Minimal sheet tables to pass through mapping
    base = pd.DataFrame({"X": ["r1", "r2"], 2019: [1, 2]})
    tpl = pd.DataFrame({"row": [0, 1], "label": ["L1", "L2"], "unit": ["ktoe", "ktoe"], "fuel": ["F1", "F2"]})

    def fake_read_and_wrangle(file_path, has_multi_headers=False, sheet_name=None,
                              skip_sheets=None, fixed_header=None):
        # Table sheets
        if file_path == "URL" and sheet_name in {"4.4a", "4.4b"}:
            return base.copy()
        # Templates keyed by sheet
        if file_path == "TPL" and sheet_name in {"4.4a", "4.4b"}:
            return tpl.copy()
        raise AssertionError("Unexpected read call")

    monkeypatch.setattr(tr, "read_and_wrangle_wb", fake_read_and_wrangle)

    out = tr.process_sheet_to_frame(
        url="URL",
        template_file_path="TPL",
        data_collection="dukes",
        sheet_names=["4.4a", "4.4b"],
        table_name="4.4",  # triggers _postprocess_normalize_names
        ignore_mapping=False,
    )

    assert set(out.keys()) == {"4.4.A", "4.4.B"}  # normalized keys
    for df in out.values():
        # Ensure cleaning step ran (index preserved, label not cleaned)
        d = df.reset_index()
        assert "label" in d.columns
        assert "unit" in d.columns


# process_multi_sheets_to_frame ------------------------------------------

def test_process_multi_sheets_to_frame_with_template(monkeypatch):
    """
    ignore_mapping=False path:
    - Workbook dict with sheets '2019','meta','2020'
    - Template read once for table_name
    - Melt over var_on_cols='fuel'; add var_on_sheets='year'
    """
    wb = {
        "2019": pd.DataFrame({"ROWHDR": ["r1", "r2"], "Gas": [1, 2], "Coal": [3, 4]}),
        "meta": pd.DataFrame({"ignore": [0]}),  # should be skipped
        "2020": pd.DataFrame({"ROWHDR": ["r1", "r2"], "Gas": [5, 6], "Coal": [7, 8]}),
    }
    template = pd.DataFrame({"row": [0, 1], "label": ["L1", "L2"], "unit": ["ktoe", "ktoe"], "sector": ["S1", "S2"]})

    def fake_read_and_wrangle(file_path, has_multi_headers=False, sheet_name=None,
                              skip_sheets=None, fixed_header=None):
        # Reading whole workbook
        if file_path == "URL" and sheet_name is None:
            return {k: v.copy() for k, v in wb.items()}
        # Reading template (by table_name)
        if file_path == "TPL" and sheet_name == "1.2":
            return template.copy()
        raise AssertionError("Unexpected read call")

    monkeypatch.setattr(tr, "read_and_wrangle_wb", fake_read_and_wrangle)

    out = tr.process_multi_sheets_to_frame(
        url="URL",
        template_file_path="TPL",
        data_collection="dukes",
        table_name="1.2",
        var_on_sheets="year",
        var_on_cols="fuel",
        has_multi_headers=False,
        skip_sheets=["meta"],
        transpose_first=False,
        ignore_mapping=False,
    )

    assert set(out.keys()) == {"1.2"}
    df = out["1.2"].reset_index()

    # id vars from template + variables from sheets/columns
    for col in ["row", "label", "unit", "sector", "year", "fuel", "value"]:
        assert col in df.columns

    # 2 rows × 2 fuels × 2 years = 8
    assert len(df) == 8

    # Spot-check values
    v_2019_gas_r0 = df[(df["row"] == 0) & (df["year"] == "2019") & (df["fuel"] == "Gas")]["value"].iloc[0]
    v_2020_coal_r1 = df[(df["row"] == 1) & (df["year"] == "2020") & (df["fuel"] == "Coal")]["value"].iloc[0]
    assert (v_2019_gas_r0, v_2020_coal_r1) == (1, 8)


def test_process_multi_sheets_to_frame_manual_ignore_mapping(monkeypatch):
    """
    ignore_mapping=True path:
    - Build id vars from sheets
    - transpose_first=True path covered
    - Note tags stripped on object cols (except 'label')
    """
    # Before transpose, first column becomes header after set_index(...).T
    wb = {
        "2019": pd.DataFrame({
            "IDcol": ["A [note]", "B"],
            "DROP": ["x", "y"],
            "c1": [10, 20],
        }),
        "2020": pd.DataFrame({
            "IDcol": ["A", "B"],
            "DROP": ["m", "n"],
            "c1": [30, 40],
        }),
    }

    def fake_read_and_wrangle(file_path, has_multi_headers=False, sheet_name=None,
                              skip_sheets=None, fixed_header=None):
        if file_path == "URL" and sheet_name is None:
            return {k: v.copy() for k, v in wb.items()}
        raise AssertionError("Unexpected read call")

    monkeypatch.setattr(tr, "read_and_wrangle_wb", fake_read_and_wrangle)

    out = tr.process_multi_sheets_to_frame(
        url="URL",
        template_file_path="IGNORED",
        data_collection="dukes",
        table_name="X.9",
        var_on_sheets="year",
        var_on_cols="fuel",
        has_multi_headers=False,
        skip_sheets=None,
        drop_cols=["DROP"],
        transpose_first=True,
        ignore_mapping=True,
        id_var_position=0,
        id_var_name="id_clean",
        unit="ktoe",
    )

    df = out["X.9"].reset_index()

    # Manual id_vars exist
    for c in ["row", "label", "id_clean", "unit", "year", "fuel", "value"]:
        assert c in df.columns

    # 'label' comes from the transposed index; it's "c1" here (no note to strip)
    assert set(df["label"]) == {"c1"}

    # yhe note tag originally lived in a column header ("A [note]"),
    # whhich becomes the melted variable `fuel`. Cleaner should remove it.
    assert " [note]" not in df["fuel"].astype(str).str.cat(sep="")

    # Some rows present
    assert len(df) > 0
