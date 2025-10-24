# Proyecto_BDA — ETL / Data Warehouse Quickstart

This repository contains a small ETL pipeline that extracts flight and logbook
data from operational sources, applies transformations and business-rule based
cleaning, and loads results into a DuckDB-backed data warehouse for analysis.

Core components
- `extract.py` — source data access (PostgreSQL queries and CSV readers).
- `transform.py` — pandas-based transformations, aggregations and cleaning.
- `load.py` — dimension upserts (pygrametl) and bulk fact loads (DuckDB).
- `dw.py` — DW schema DDL and pygrametl object definitions (CachedDimension, FactTable).
- `etl_control_flow.py` — orchestrates extract → transform → load.
- `query_test.py` — run example DW and baseline queries and pretty-print results.
- `requirements.txt` — inferred third-party dependencies.

Quick setup (Windows PowerShell)
It is recommended to use a virtual environment:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install --upgrade pip
pip install -r .\requirements.txt
```

For reproducible, pinned dependencies after you validate versions:

```powershell
pip freeze > requirements-frozen.txt
```

Database configuration
The ETL scripts read a simple `db_conf.txt` in the repository root with
one `key=value` per line. Required keys:

- `dbname`
- `user`
- `password`
- `ip` (or `host`)
- `port`

Example `db_conf.txt`:

```
dbname=your_db
user=your_user
password=your_pass
ip=127.0.0.1
port=5432
```

Run the ETL
- Orchestrator (extract → transform → load):

```powershell
python etl_control_flow.py
```

- Run the example queries and pretty-print results:

```powershell
python query_test.py
```

Notes on types and editors
- Some imports use `# type: ignore` (e.g. `tabulate`) when no type stubs are
  available. If you use mypy or pyright and want stricter checks, add stubs or
  configure your type checker.

Suggested next steps
- Pin exact, tested package versions for CI (`requirements-frozen.txt`).
- Add unit tests (pytest) for key transform functions.
- Add a `requirements-dev.txt` with developer tooling (black, isort, mypy).
