"""
ETL Loading Script

This module contains all functions responsible for loading the transformed data
into the target data warehouse tables. Each function is designed to take a 
pygrametl data warehouse object and a data iterator as input.
"""

from tqdm import tqdm # type: ignore
from typing import Iterator, Dict, Any

# For type hinting the 'dw' object. You would replace 'Any' with your
# actual DataWarehouse class if you have one defined.
# from dw_schema import DataWarehouse 

# =============================================================================
# Dimension Table Loading Functions
# =============================================================================


def load_aircrafts(dw: Any, aircraft_iterator: Iterator[Dict[str, str]]) -> None:
    """
    Loads transformed aircraft data into the Aircraft dimension table.

    Args:
        dw: The pygrametl data warehouse connection object.
        aircraft_iterator: An iterator yielding dictionaries of aircraft data.
    """
    print("Loading dimension: Aircraft...")
    for row in tqdm(aircraft_iterator, desc="Dim: Aircraft"):
        dw.aircrafts_dim.insert(row)


def load_reporters(dw: Any, reporter_iterator: Iterator[Dict[str, str]]) -> None:
    """
    Loads transformed and merged reporter data into the Reporter dimension table.

    Args:
        dw: The pygrametl data warehouse connection object.
        reporter_iterator: An iterator yielding dictionaries of reporter data.
    """
    print("Loading dimension: Reporter...")
    for row in tqdm(reporter_iterator, desc="Dim: Reporter"):
        dw.reporters_dim.insert(row)


def load_dates(dw: Any, date_iterator: Iterator[Dict[str, Any]]) -> None:
    """
    Loads date data into the Date dimension table.
    
    Args:
        dw: The pygrametl data warehouse connection object.
        date_iterator: An iterator yielding dictionaries with date data.
    """
    print("Loading dimension: Date...")
    for row in tqdm(date_iterator, desc="Dim: Date"):
        dw.dates_dim.insert(row)


def load_months(dw: Any, month_iterator: Iterator[Dict[str, Any]]) -> None:
    """
    Loads month data into the Month dimension table.

    Args:
        dw: The pygrametl data warehouse connection object.
        month_iterator: An iterator yielding dictionaries with month and year data.
    """
    print("Loading dimension: Month...")
    for row in tqdm(month_iterator, desc="Dim: Month"):
        dw.months_dim.insert(row)

# =============================================================================
# Fact Table Loading Functions
# =============================================================================


def load_flights_operations_daily(dw: Any, flights_daily_iterator: Iterator[Dict[str, Any]]) -> None:
    """
    Loads aggregated daily flight operations data into the corresponding fact table.

    Args:
        dw: The pygrametl data warehouse connection object.
        flights_daily_iterator: An iterator yielding aggregated daily flight facts.
    """
    print("Loading fact table: Flight Operations Daily...")
    for row in tqdm(flights_daily_iterator, desc="Fact: Daily Flights"):
        dw.flight_fact.insert(row)


def load_aircrafts_monthly_snapshot(dw: Any, aircraft_monthly_iterator: Iterator[Dict[str, Any]]) -> None:
    """
    Loads aggregated monthly aircraft snapshot data into the corresponding fact table.

    Args:
        dw: The pygrametl data warehouse connection object.
        aircraft_monthly_iterator: An iterator yielding aggregated monthly aircraft facts.
    """
    print("Loading fact table: Aircraft Monthly Snapshot...")
    for row in tqdm(aircraft_monthly_iterator, desc="Fact: Monthly Snapshot"):
        dw.aircraft_monthly_fact.insert(row)


def load_logbooks(dw: Any, logbooks_iterator: Iterator[Dict[str, Any]]) -> None:
    """
    Loads aggregated logbook count data into the Logbooks fact table.

    Args:
        dw: The pygrametl data warehouse connection object.
        logbooks_iterator: An iterator yielding aggregated logbook facts.
    """
    print("Loading fact table: Logbooks...")
    for row in tqdm(logbooks_iterator, desc="Fact: Logbooks"):
        dw.logbook_fact.insert(row)