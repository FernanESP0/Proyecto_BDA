"""
ETL Loading Module

This module provides focused, side-effect-free loaders that persist already-transformed
records into the target Data Warehouse (DuckDB) schema. It offers two complementary
loading strategies:

- Dimension upsert using pygrametl's CachedDimension.ensure(...), which guarantees
    idempotent inserts and fast lookups when keys already exist.
- Fact table bulk inserts via DuckDB's DataFrame registration mechanism for high
    throughput when loading large batches.

Design goals
- Keep business logic out of the loader: inputs are expected to be clean, shaped
    records; this module only persists.
- Be explicit about each target table to ensure schema evolution remains clear.
- Fail fast with informative messages while making best-effort to release resources.
"""

from tqdm import tqdm  # type: ignore
from typing import Iterator, Any
import pandas as pd 


# =============================================================================
# Dimension loading helpers (Method: pygrametl CachedDimension)
# These functions assume the DW object exposes CachedDimension instances with
# the "ensure" API, so they can be called repeatedly without creating duplicates.
# =============================================================================

def load_aircrafts(dw: Any, aircraft_iterator: Iterator) -> None:
    """
    Load aircraft rows into the Aircrafts dimension using pygrametl's cache.

    Parameters
    - dw: Data warehouse handle exposing "aircrafts_dim" (CachedDimension).
    - aircraft_iterator: Iterator yielding dicts with aircraft attributes
      matching the dimension schema.

    Notes
    - Uses ensure(...) to upsert by natural key; safe to call multiple times.
    """
    print("Loading dimension: Aircraft (pygrametl cache)...")
    for row in tqdm(aircraft_iterator, desc="Dim: Aircraft"):
        dw.aircrafts_dim.ensure(row)


def load_reporters(dw: Any, reporter_iterator: Iterator) -> None:
    """
    Load reporter rows into the Reporters dimension using pygrametl's cache.

    Parameters
    - dw: Data warehouse handle exposing "reporters_dim" (CachedDimension).
    - reporter_iterator: Iterator yielding dicts with reporter attributes.
    """
    print("Loading dimension: Reporter (pygrametl cache)...")
    for row in tqdm(reporter_iterator, desc="Dim: Reporter"):
        dw.reporters_dim.ensure(row)


def load_dates(dw: Any, date_iterator: Iterator) -> None:
    """
    Load date rows into the Dates dimension using pygrametl's cache.

    Parameters
    - dw: Data warehouse handle exposing "dates_dim" (CachedDimension).
    - date_iterator: Iterator yielding dicts with date attributes.
    """
    print("Loading dimension: Date (pygrametl cache)...")
    for row in tqdm(date_iterator, desc="Dim: Date"):
        dw.dates_dim.ensure(row)


def load_months(dw: Any, month_iterator: Iterator) -> None:
    """
    Load month rows into the Months dimension using pygrametl's cache.

    Parameters
    - dw: Data warehouse handle exposing "months_dim" (CachedDimension).
    - month_iterator: Iterator yielding dicts with month attributes.
    """
    print("Loading dimension: Month (pygrametl cache)...")
    for row in tqdm(month_iterator, desc="Dim: Month"):
        dw.months_dim.ensure(row)

# =============================================================================
# Functions for Loading Fact Tables (Method: DuckDB Bulk Insert)
# =============================================================================


def _load_fact_table_bulk(
    dw: Any, 
    table_name: str,
    iterator: Iterator, 
    table_desc: str
) -> None:
    """
    Bulk-load a fact table by materializing an iterator into a DataFrame and
    inserting it via DuckDB's DataFrame registration.

    Parameters
    - dw: Data warehouse handle that exposes a native DuckDB connection as
      "conn_duckdb" with register/unregister/execute/commit.
    - table_name: Target fact table name in DuckDB.
    - iterator: Iterator yielding dictionaries that align with table columns.
    - table_desc: Human-friendly table description for logs.

    Behavior
    - No-op if the iterator yields no records.
    - Registers a temporary in-memory view, bulk inserts into the target, and
      unregisters the view in a finally block.
    - Raises the original exception after logging if an error occurs.
    """
    print(f"Loading fact table: {table_desc} (DuckDB Bulk Method)...")

    # 1. Materialize the iterator into a DataFrame
    print("Materializing iterator into DataFrame...")
    df_to_insert = pd.DataFrame(iterator)

    if df_to_insert.empty:
        print(f"No data to load for {table_desc}.")
        return

    # 2. Get the NATIVE DuckDB connection (the one with .register())
    conn = dw.conn_duckdb
    virtual_table_name = "df_fact_virtual"

    try:
        # 3. Register the DataFrame as a temporary virtual table
        conn.register(virtual_table_name, df_to_insert) 

        # 4. Execute the bulk insert
        print(f"Bulk inserting {len(df_to_insert)} rows into {table_name}...")
        conn.execute(f"INSERT INTO {table_name} SELECT * FROM {virtual_table_name}")

        # 5. Commit the transaction
        conn.commit() 
        print("Load complete.")

    except Exception as e:
        print(f"Error during DuckDB bulk load: {e}")
        raise
    finally:
        # 6. Clean up (unregister) the virtual table
        try:
            conn.unregister(virtual_table_name)
        except Exception:
            # Best-effort cleanup; safe to ignore failures here
            pass


def load_flights_operations_daily(dw: Any, flights_daily_iterator: Iterator) -> None:
    """
    Load the Flight_operations_Daily fact table in bulk.

    Parameters
    - dw: Data warehouse handle exposing "flight_fact" (FactTable) and a
      native DuckDB connection.
    - flights_daily_iterator: Iterator of records produced by the transform stage.
    """
    _load_fact_table_bulk(
        dw=dw,  
        table_name=dw.flight_fact.name, 
        iterator=flights_daily_iterator,
        table_desc="Flight Operations Daily"
    )


def load_aircrafts_monthly_snapshot(dw: Any, aircraft_monthly_iterator: Iterator) -> None:
    """
    Load the Aircraft_Monthly_Summary fact table in bulk.

    Parameters
    - dw: Data warehouse handle exposing "aircraft_monthly_fact" (FactTable).
    - aircraft_monthly_iterator: Iterator of monthly snapshot records.
    """
    _load_fact_table_bulk(
        dw=dw, 
        table_name=dw.aircraft_monthly_fact.name, 
        iterator=aircraft_monthly_iterator,
        table_desc="Aircraft Monthly Snapshot"
    )


def load_logbooks(dw: Any, logbooks_iterator: Iterator) -> None:
    """
    Load the Logbooks fact table in bulk.

    Parameters
    - dw: Data warehouse handle exposing "logbook_fact" (FactTable).
    - logbooks_iterator: Iterator of logbook count records.
    """
    _load_fact_table_bulk(
        dw=dw,
        table_name=dw.logbook_fact.name,
        iterator=logbooks_iterator,
        table_desc="Logbooks"
    )