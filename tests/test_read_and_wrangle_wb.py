import io
import pandas as pd
import pytest

from queens.core.read_write import read_and_wrangle_wb


def _xlsx_bytes_from_sheets(sheets: dict[str, pd.DataFrame], header=False, index=False) -> io.BytesIO:
    """
    Create an in-memory Excel workbook from {sheet_name: dataframe_like_rows}.
    If you pass rows (list-of-lists), wrap with pd.DataFrame first in the test.
    """
    bio = io.BytesIO()
    with pd.ExcelWriter(bio, engine="openpyxl") as w:
        for name, df in sheets.items():
            # Allow passing rows (list-of-lists) for convenience
            if not isinstance(df, pd.DataFrame):
                df = pd.DataFrame(df)
            df.to_excel(w, sheet_name=name, header=header, index=index)
    bio.seek(0)
    return bio


def test_read_whole_workbook_auto_header_and_exclude_single_column():
    """
    - Auto header detection should skip initial 'title' rows until second column is not 'Unnamed'
    - Sheets with a single column are excluded
    - Returns a dict when sheet_name=None
    """
    # '2019' sheet has title rows, then the real header at row 2: ["ROWHDR", "A", "B"]
    rows_2019 = [
        ["Main Title", "", ""],       # header=0 -> Unnamed in col 2
        ["Subtitle", "", ""],         # header=1 -> Unnamed in col 2
        ["ROWHDR", "A", "B"],         # header=2 (expected)
        ["r1", 1, 2],
        ["r2", 3, 4],
    ]
    # single-column sheet should be excluded
    rows_meta = [
        ["Just notes"],
        ["Ignore me"],
    ]

    bio = _xlsx_bytes_from_sheets(
        {"2019": rows_2019, "meta": rows_meta},
        header=False,
        index=False,
    )

    out = read_and_wrangle_wb(bio)
    assert isinstance(out, dict)
    assert set(out.keys()) == {"2019"}  # 'meta' excluded (single column)

    df = out["2019"]
    assert list(df.columns[:3]) == ["ROWHDR", "A", "B"]
    # Spot check a value
    assert df.iloc[0, 1] == 1  # A @ r1


def test_read_specific_sheet_and_missing_sheet_raises():
    rows_2020 = [
        ["Hdr", "A", "B"],
        ["r1", 10, 20],
    ]
    bio = _xlsx_bytes_from_sheets({"2020": rows_2020}, header=False, index=False)

    # Specific sheet returns a DataFrame
    df = read_and_wrangle_wb(bio, sheet_name="2020")
    assert isinstance(df, pd.DataFrame)
    assert list(df.columns)[:3] == ["Hdr", "A", "B"]

    # Missing sheet should raise KeyError
    with pytest.raises(KeyError):
        read_and_wrangle_wb(bio, sheet_name="2019")  # not present


def test_skip_sheets_excludes():
    rows_2019 = [["Hdr", "A"], ["r1", 1]]
    rows_2020 = [["Hdr", "A"], ["r1", 2]]
    rows_meta = [["Only one col"], ["x"]]

    bio = _xlsx_bytes_from_sheets(
        {"2019": rows_2019, "2020": rows_2020, "meta": rows_meta},
        header=False,
        index=False,
    )

    out = read_and_wrangle_wb(bio, skip_sheets=["2020"])
    assert set(out.keys()) == {"2019"}  # 'meta' excluded automatically; '2020' skipped


def test_has_multi_headers_increments_header():
    """
    When has_multi_headers=True, the function parses with header=(inferred_h + 1).
    We build a sheet where:
      - header=0 -> Unnamed in col 2
      - header=1 -> ["ROWHDR","A","B"]
      - header=2 -> ["lvl2","A2","B2"]  <-- expected when has_multi_headers=True
    """
    rows = [
        ["Title", "", ""],         # h=0 -> Unnamed
        ["ROWHDR", "A", "B"],      # h=1 -> ordinary header
        ["lvl2", "A2", "B2"],      # h=2 -> used when has_multi_headers=True
        ["r1", 1, 2],
    ]
    bio = _xlsx_bytes_from_sheets({"mh": rows}, header=False, index=False)

    df = read_and_wrangle_wb(bio, sheet_name="mh", has_multi_headers=True)
    assert list(df.columns[:3]) == ["lvl2", "A2", "B2"]


def test_fixed_header_overrides_auto_detection():
    """
    fixed_header should override auto header detection entirely.
    We place the real header at row index 3.
    """
    rows = [
        ["Title", "", ""],     # 0
        ["More", "", ""],      # 1
        ["Almost", "", ""],    # 2
        ["Hdr", "Y1", "Y2"],   # 3 <- fixed_header
        ["r1", 7, 8],
    ]
    bio = _xlsx_bytes_from_sheets({"fx": rows}, header=False, index=False)

    df = read_and_wrangle_wb(bio, sheet_name="fx", fixed_header=3)
    assert list(df.columns[:3]) == ["Hdr", "Y1", "Y2"]
    # Spot check a value
    assert df.iloc[0, 2] == 8  # Y2 @ r1
