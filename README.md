# QUEENS: QUEryable Energy National Statistics

**QUEENS** is a Python package that:
- **ingests** UK energy National Statistics Excel tables into SQLite,
- **stages** a consistent snapshot (one version per table at a time),
- **serves** the staged data via **FastAPI**,
- exposes a **CLI** and **Python facade** for querying and export.

Think of it as the **royal version of DUKES** 👑 — a reliable, machine-readable layer over DESNZ publications (DUKES, Energy Trends).

---

## Why this exists

I used to work in the DESNZ team that publishes DUKES and related collections. We constantly received requests from policy colleagues and modellers for data and insights that required a lot of manual manipulation of the published tables. I always wished there was a queryable counterpart to the public-facing Excel files. **QUEENS** is a product of that mindset: reproduciblet ingestion + strict schema validation + data versioning + a simple API so analysts can get on with their work without risking being inconsistent with the published stats.

---

## Install

```
pip install queens
```

---

## 10-second quickstart

### CLI
```bash
# ingest a table (or omit --table to ingest all)
queens ingest dukes --table 5.6

# stage the latest snapshot
queens stage dukes

# run the API (defaults to http://127.0.0.1:8000)
queens serve
```

### Python
```python
import queens as q

q.setup_logging(level="info") # optional
q.ingest("dukes", tables="6.1")
q.stage("dukes")
df = q.query("dukes", "6.1", filters={"year": {"gte": 2020}})
print(df.head())
```

> Full walkthroughs (config, filters, pagination, exports, etc.): see demo notebooks in `examples/`.

---

## Documentation

- [Architecture](docs/architecture.md)
- [Configuration & Paths](docs/configuration.md)
- [ETL & Versioning](docs/versioning.md)
- [CLI](docs/cli.md)
- [API](docs/api.md)
- [Library (facade)](docs/library.md)
- [Filtering rules](docs/filters.md)
- [Troubleshooting](docs/troubleshooting.md)

---

## Key ideas (at a glance)

- **Read from GOB.UK**: data are sourced directly from the official source, ensuring consistency with the publicly available version.
- **RAW → PROD**: raw ingests are versioned; staging creates a consistent **snapshot** per table in `*_prod`.
- **Strict validation**: schema and dtypes enforced; duplicates rejected; metadata (`_metadata`) is rebuilt on stage.
- **Queryable API**: `/data/{collection}` with **JSON filters** (flat or nested, `$or` supported), cursor pagination by `rowid`.
- **Portable**: SQLite under the hood; exports to CSV/Parquet/Excel.

---

## Notes

- Data sources are public National Statistics from DESNZ pages. QUEENS automates access and reshaping; it does **not** alter official figures beyond deterministic formatting (long/flat) and indexing (mapping out to nested indexes).
- For Parquet, install **pyarrow** or **fastparquet**.
- The CLI `serve` command uses sensible defaults; if you expose host/port, ensure flags match your installed version.
