# QUEENS  

[![PyPI version](https://img.shields.io/pypi/v/queens.svg)](https://pypi.org/project/queens/)
![PyPI - Python Version](https://img.shields.io/pypi/pyversions/queens.svg)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

QUEENS (QUEryable Energy National Statistics) is a Python package that:  

- ingests UK energy National Statistics Excel tables into SQLite,  
- stages a consistent snapshot (one version per table at a time),  
- serves the staged data via FastAPI,  
- exposes a CLI and Python facade for querying and export.  

Think of it as the royal counterpart to DUKES — a principled, machine-readable layer over DESNZ publications (DUKES, Energy Trends etc.).  

There is a live demo on Render, with a sample database holding all transformed DUKES 2025 tables.
- Live API base url: [https://queens-5stl.onrender.com]()
- Docs/Swagger: [https://queens-5stl.onrender.com/docs]()

See examples in [this notebook](examples/live_api_demo.ipynb)!

---

## Why this exists  

I used to work in the Energy Statistics team, collaborating at the publication of DUKES and related collections. We constantly received requests from policy colleagues and modellers for data and insights that required considerable manual manipulation of the published tables.  

I always wished there was a queryable counterpart to the public-facing Excel files — something that preserved the authority of the published stats, but removed the drudgery.  

QUEENS is the result of that mindset: reproducible ingestion, strict schema validation, data versioning, and a simple API so analysts can focus on insight rather than wrangling — all while ensuring consistency with the official published numbers.  

Another driver was that whenever we provided figures externally, we were obliged to use the published versions (since they were the “true” source), even though internal files were easier to handle but often out of sync. With QUEENS, the published spreadsheets become directly usable, versioned, and queryable.  

---

## Customisable and extendable  

Although QUEENS ships ready-made for DUKES and related DESNZ tables, it isn’t limited to them.  
Users can extend it to other collections by providing their own table templates and schema definitions.  

Because ingestion is versioned, you can safely ingest multiple vintages of the same tables and then stage whichever version you wish.  
This makes it straightforward to track revisions, compare snapshots across releases, or reproduce results tied to a specific publication date.  

In this way, QUEENS can serve as a general-purpose bridge between human-readable official spreadsheets and clean, queryable datasets — one that not only structures the data, but also preserves its history.  

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

- **Read from GOV.UK**: data are sourced directly from the official source, ensuring consistency with the publicly available version.
- **RAW → PROD**: raw ingests are versioned; staging creates a consistent **snapshot** per table in `*_prod`.
- **Strict validation**: schema and dtypes enforced; duplicates rejected; metadata (`_metadata`) is rebuilt on stage.
- **Queryable API**: `/data/{collection}` with **JSON filters** (flat or nested, `$or` supported), cursor pagination by `rowid`.
- **Portable**: SQLite under the hood; exports to CSV/Parquet/Excel.

---

## Notes

- Data sources are public National Statistics from DESNZ pages. QUEENS automates access and reshaping; it does **not** alter official figures beyond deterministic formatting (long/flat) and indexing (mapping out to nested indexes).
- For Parquet, install **pyarrow** or **fastparquet**.
- The CLI `serve` command uses sensible defaults; if you expose host/port, ensure flags match your installed version.

## Future development.
- Extension to other data collections (Energy Trends, Energy Emissions statistics...).
- Handling schema evolution of templates - e.g. if a table changes format at some point, being able to ingest both versions.

## Version history
```
0.1.1 - 24 August 2025:
    First release
    
0.1.2 - 29 August 2025:
    Deployed a live demo on Render
    Fix: bug in CLI method serve that prevented passing custom host
```

## Aythor and contacts
Alessandro Bigazzi (maintainer).

If you find a bug, please open an issue. 
For other enquiries, please [e.mail me](alessandro.bigazzi@aol.com).