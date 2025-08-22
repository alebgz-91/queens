import pandas as pd
import pytest
from pathlib import Path

import queens.etl.validation as v


# ----------------------------
# generate_config
# ----------------------------

def test_generate_config_success(monkeypatch, tmp_path):
    # fake config dicts
    etl_config = {
        "dukes": {
            "chapter_5": {
                "5.6": {"f_args": {"ignore_mapping": False}}
            }
        }
    }
    urls = {"dukes": {"chapter_5": "https://example.com/ch5"}}
    templates = {"dukes": {"chapter_5": "dukes_ch_5.xlsx"}}

    # patch settings.TEMPLATES_DIR -> tmp path
    monkeypatch.setattr(v.s, "TEMPLATES_DIR", tmp_path)

    # patch web scraping to return a mapping that contains the target table
    def fake_scrape_urls(data_collection, url):
        assert data_collection == "dukes" and url == "https://example.com/ch5"
        return {"5.6": {"url": "https://example.com/wb.xlsx", "description": "Table 5.6 desc"}}

    monkeypatch.setattr(v.ws, "scrape_urls", fake_scrape_urls)

    cfg = v.generate_config(
        data_collection="dukes",
        table_name="5.6",
        chapter_key="chapter_5",
        templates=templates,
        urls=urls,
        etl_config=etl_config,
    )

    # f_args enriched
    assert cfg["f_args"]["url"] == "https://example.com/wb.xlsx"
    assert cfg["f_args"]["data_collection"] == "dukes"
    assert Path(cfg["f_args"]["template_file_path"]).name == "dukes_ch_5.xlsx"
    assert cfg["table_description"] == "Table 5.6 desc"


def test_generate_config_missing_table_raises(monkeypatch, tmp_path):
    etl_config = {"dukes": {"chapter_5": {"5.7": {"f_args": {}}}}}
    urls = {"dukes": {"chapter_5": "https://example.com/ch5"}}
    templates = {"dukes": {"chapter_5": "dukes_ch_5.xlsx"}}
    monkeypatch.setattr(v.s, "TEMPLATES_DIR", tmp_path)

    def fake_scrape_urls(data_collection, url):
        # table "5.6" is not present -> KeyError expected
        return {"5.5": {"url": "x", "description": "y"}}

    monkeypatch.setattr(v.ws, "scrape_urls", fake_scrape_urls)

    with pytest.raises(KeyError):
        v.generate_config("dukes", "5.6", "chapter_5", templates, urls, etl_config)


# ----------------------------
# validate_schema
# ----------------------------

@pytest.fixture
def patched_dtypes(monkeypatch):
    # minimal dtype policy used by validate_schema
    monkeypatch.setattr(v.s, "DTYPES", {"int": int, "float": float, "str": str})
    yield

def _schema_for_dukes():
    # columns expected in df after reset_index + constant 'table_name'
    return {
        "row": {"type": "int", "nullable": False},
        "label": {"type": "str", "nullable": False},
        "fuel": {"type": "str", "nullable": False},
        "year": {"type": "int", "nullable": False},
        "value": {"type": "float", "nullable": True},
        "table_name": {"type": "str", "nullable": False},
    }

def test_validate_schema_success(patched_dtypes):
    # Make a frame with a MultiIndex ['row','label','fuel','year']
    idx = pd.MultiIndex.from_frame(pd.DataFrame({
        "row": [0, 1, 2],
        "label": ["L0", "L1", "L2"],
        "fuel": ["Coal", "Gas", "Oil"],
        "year": [2020, 2020, 2020],
    }))
    df = pd.DataFrame({"value": ["1.0", "2.5", "3.0"]}, index=idx)

    schema_dict = {"dukes": _schema_for_dukes()}

    out = v.validate_schema(
        data_collection="dukes",
        table_name="1.1",
        df=df.copy(),
        schema_dict=schema_dict,
    )

    # table_name column added; value coerced to numeric (non-null present)
    assert "table_name" in out.columns
    assert pd.api.types.is_float_dtype(out["value"])

def test_validate_schema_duplicates_raise(patched_dtypes):
    # duplicate on (fuel, year) after removing row/label from index
    idx = pd.MultiIndex.from_frame(pd.DataFrame({
        "row": [0, 1],
        "label": ["L0", "L1"],
        "fuel": ["Coal", "Coal"],
        "year": [2020, 2020],
    }))
    df = pd.DataFrame({"value": [1.0, 2.0]}, index=idx)
    schema_dict = {"dukes": _schema_for_dukes()}

    with pytest.raises(ValueError):
        v.validate_schema("dukes", "1.1", df.copy(), schema_dict)

def test_validate_schema_unexpected_col_raises(patched_dtypes):
    idx = pd.MultiIndex.from_frame(pd.DataFrame({
        "row": [0],
        "label": ["L0"],
        "fuel": ["Coal"],
        "year": [2020],
    }))
    df = pd.DataFrame({"value": [1.0], "unexpected": [123]}, index=idx)
    schema_dict = {"dukes": _schema_for_dukes()}  # no 'unexpected'

    with pytest.raises(ValueError):
        v.validate_schema("dukes", "1.1", df.copy(), schema_dict)

def test_validate_schema_nullability_violation(patched_dtypes):
    # fuel is non-nullable in schema; inject a null
    idx = pd.MultiIndex.from_frame(pd.DataFrame({
        "row": [0],
        "label": ["L0"],
        "fuel": [None],
        "year": [2020],
    }))
    df = pd.DataFrame({"value": [1.0]}, index=idx)
    schema_dict = {"dukes": _schema_for_dukes()}

    with pytest.raises(ValueError):
        v.validate_schema("dukes", "1.1", df.copy(), schema_dict)


# ----------------------------
# normalize_filters
# ----------------------------

def test_normalize_filters_splits_or_list(monkeypatch):
    # make to_nested a no-op so we test only splitting behavior
    monkeypatch.setattr(v.u, "to_nested", lambda d: d)

    base, or_groups = v.normalize_filters({
        "year": {"gte": 2020},
        "$or": [{"fuel": {"like": "Coal%"}}, {"fuel": {"like": "Petroleum%"}}],
    })

    assert base == {"year": {"gte": 2020}}
    assert or_groups == [{"fuel": {"like": "Coal%"}}, {"fuel": {"like": "Petroleum%"}}]

def test_normalize_filters_splits_or_dict(monkeypatch):
    monkeypatch.setattr(v.u, "to_nested", lambda d: d)

    base, or_groups = v.normalize_filters({
        "year": {"gte": 2020},
        "$or": {"fuel": {"like": "Coal%"}},
    })

    assert base == {"year": {"gte": 2020}}
    assert or_groups == [{"fuel": {"like": "Coal%"}}]


# ----------------------------
# validate_query_filters
# ----------------------------

@pytest.fixture
def patched_ops_and_load(monkeypatch):
    # VALID_OPS keyed by SQL type (as returned by load_column_info)
    monkeypatch.setattr(v.s, "VALID_OPS", {
        "INTEGER": {"eq", "neq", "lt", "lte", "gt", "gte"},
        "REAL": {"eq", "neq", "lt", "lte", "gt", "gte"},
        "TEXT": {"eq", "neq", "like"},
    })

    # load_column_info -> (sql_types, cast_map)
    def fake_load_column_info(conn_path, data_collection, table_name):
        sql_types = {"year": "INTEGER", "value": "REAL", "fuel": "TEXT"}
        cast_map = {"year": int, "value": float, "fuel": str}
        return sql_types, cast_map

    monkeypatch.setattr(v.rw, "load_column_info", fake_load_column_info)
    yield

def test_validate_query_filters_success(patched_ops_and_load):
    schema_dict = {"dukes": {"year": {}, "value": {}, "fuel": {}}}  # present in schema
    group = {
        "year": {"gte": "2020"},     # cast to int
        "value": {"lt": "12.3"},     # cast to float
        "fuel": {"like": "Coal%"}    # keep string
    }

    out = v.validate_query_filters(
        data_collection="dukes",
        table_name="1.1",
        group=group,
        conn_path="ignored.db",
        schema_dict=schema_dict,
    )

    assert out["year"]["gte"] == 2020 and isinstance(out["year"]["gte"], int)
    assert out["value"]["lt"] == 12.3 and isinstance(out["value"]["lt"], float)
    assert out["fuel"]["like"] == "Coal%"

def test_validate_query_filters_invalid_operator(patched_ops_and_load):
    schema_dict = {"dukes": {"year": {}, "value": {}, "fuel": {}}}
    group = {"year": {"betweenish": [2019, 2020]}}
    with pytest.raises(ValueError):
        v.validate_query_filters("dukes", "1.1", group, "ignored.db", schema_dict)

def test_validate_query_filters_like_non_string(patched_ops_and_load):
    schema_dict = {"dukes": {"fuel": {}}}
    group = {"fuel": {"like": 123}}
    with pytest.raises(TypeError):
        v.validate_query_filters("dukes", "1.1", group, "ignored.db", schema_dict)

def test_validate_query_filters_unknown_col_in_schema(patched_ops_and_load):
    schema_dict = {"dukes": {"year": {}}}  # 'bad' not allowed at schema level
    group = {"bad": {"eq": 1}}
    with pytest.raises(KeyError):
        v.validate_query_filters("dukes", "1.1", group, "ignored.db", schema_dict)

def test_validate_query_filters_not_queryable_column(patched_ops_and_load):
    # present in schema_dict, but not returned by load_column_info -> not queryable
    schema_dict = {"dukes": {"label": {}, "year": {}}}
    group = {"label": {"eq": "A"}}
    with pytest.raises(NameError):
        v.validate_query_filters("dukes", "1.1", group, "ignored.db", schema_dict)
