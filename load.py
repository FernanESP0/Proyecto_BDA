"""
ETL Loading Script 

This module contains all the functions responsible for loading the transformed data
into the target data warehouse tables.
"""

from tqdm import tqdm # type: ignore
from typing import Iterator, Dict, Any
import pandas as pd 


# =============================================================================
# Funciones de Carga de Tablas de Dimensión (Método: pygrametl Cache)
# (Estas funciones asumen que tu dw.py usa 'Dimension' como te indiqué)
# =============================================================================

def load_aircrafts(dw: Any, aircraft_iterator: Iterator[Dict[str, str]]) -> None:
    """
    Load data into Aircraft dimension using pygrametl.ensure().
    """
    print("Loading dimension: Aircraft (pygrametl cache)...")
    for row in tqdm(aircraft_iterator, desc="Dim: Aircraft"):
        dw.aircrafts_dim.ensure(row)


def load_reporters(dw: Any, reporter_iterator: Iterator[Dict[str, str]]) -> None:
    """
    Load data into Reporters dimension using pygrametl.ensure().
    """
    print("Loading dimension: Reporter (pygrametl cache)...")
    for row in tqdm(reporter_iterator, desc="Dim: Reporter"):
        dw.reporters_dim.ensure(row)


def load_dates(dw: Any, date_iterator: Iterator[Dict[str, Any]]) -> None:
    """
    Load data into Date dimension using pygrametl.ensure().
    """
    print("Loading dimension: Date (pygrametl cache)...")
    for row in tqdm(date_iterator, desc="Dim: Date"):
        dw.dates_dim.ensure(row)


def load_months(dw: Any, month_iterator: Iterator[Dict[str, Any]]) -> None:
    """
    Load data into Month dimension using pygrametl.ensure().
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
    iterator: Iterator[Dict[str, Any]], 
    table_desc: str
) -> None:
    """
    Generic bulk load function for fact tables using DuckDB's bulk insert method.
    """
    print(f"Loading fact table: {table_desc} (DuckDB Bulk Method)...")

    # 1. Materialize the iterator into a DataFrame
    print("   Materializing iterator into DataFrame...")
    df_to_insert = pd.DataFrame(iterator)

    if df_to_insert.empty:
        print(f"   No data to load for {table_desc}.")
        return

    # 2. Get the NATIVE DuckDB connection (the one with .register())
    conn = dw.conn_duckdb
    virtual_table_name = "df_fact_virtual"

    try:
        # 3. Register the DataFrame
        conn.register(virtual_table_name, df_to_insert) # Esto funcionará

        # 4. Execute the bulk insert
        print(f"Bulk inserting {len(df_to_insert)} rows into {table_name}...")
        conn.execute(f"INSERT INTO {table_name} SELECT * FROM {virtual_table_name}")

        # 5. Commit the transaction
        conn.commit() 
        print("   Load complete.")

    except Exception as e:
        print(f"   Error during DuckDB bulk load: {e}")
        conn.rollback() 
    finally:
        # 6. Clean up (unregister) the virtual table
        try:
            conn.unregister(virtual_table_name)
        except Exception:
            pass # Ignore error


def load_flights_operations_daily(dw: Any, flights_daily_iterator: Iterator[Dict[str, Any]]) -> None:
    """
    Massive load of daily flight operations data.
    """
    _load_fact_table_bulk(
        dw=dw,  
        table_name=dw.flight_fact.name, 
        iterator=flights_daily_iterator,
        table_desc="Flight Operations Daily"
    )


def load_aircrafts_monthly_snapshot(dw: Any, aircraft_monthly_iterator: Iterator[Dict[str, Any]]) -> None:
    """
    Massive load of aircraft monthly snapshot data.
    """
    _load_fact_table_bulk(
        dw=dw, 
        table_name=dw.aircraft_monthly_fact.name, 
        iterator=aircraft_monthly_iterator,
        table_desc="Aircraft Monthly Snapshot"
    )


def load_logbooks(dw: Any, logbooks_iterator: Iterator[Dict[str, Any]]) -> None:
    """
    Carga masiva de datos agregados de logbooks.
    """
    _load_fact_table_bulk(
        dw=dw,
        table_name=dw.logbook_fact.name,
        iterator=logbooks_iterator,
        table_desc="Logbooks"
    )