"""
ETL Transformation and Data Quality Module

This module encapsulates vectorized transformations that reshape raw inputs
from the extraction layer into structures expected by the DW dimensions and
fact tables. It also includes data quality routines that implement business
rules (BR) for detection and remediation of common issues.

Principles
- Favor pure, deterministic transformations over side effects; functions yield
    dictionaries ready for loading or return new DataFrames.
- Use pandas/numpy for performance and clarity; avoid per-row Python loops in
    favor of vectorized operations and groupby aggregations.
- Log data-quality violations to a file with enough context for triage.
"""

from tqdm import tqdm # type: ignore
import logging
from datetime import datetime
from pygrametl.datasources import SQLSource, CSVSource # type: ignore
import calendar
from pygrametl.tables import CachedDimension # type: ignore
from typing import Iterator, Tuple, Set
import pandas as pd
import numpy as np

# Configure logging
logging.basicConfig(
    filename='cleaning.log',
    filemode='w',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# =============================================================================
# Helper functions
# =============================================================================

def build_dateCode(date: datetime) -> str:
    """
    Format a datetime into an ISO-like date string (YYYY-MM-DD).

    Parameters
    - date: datetime to format (timezone-naive or aware; tz info is ignored).

    Returns
    - String with the date components zero-padded to 2 digits for month/day.
    """
    return f"{date.year}-{date.month:02d}-{date.day:02d}"


def get_date(date_str: str) -> Tuple[int, int, int]:
    """
    Parse a YYYY-MM-DD string into a (day, month, year) tuple of ints.

    Parameters
    - date_str: String in the format produced by build_dateCode.

    Returns
    - Tuple (day_num, month_num, year).
    """
    year = int(date_str[0:4])
    month_num = int(date_str[5:7])
    day_num = int(date_str[8:10])
    return (day_num, month_num, year)

# =============================================================================
# Dimension Table Transformations
# =============================================================================

def get_aircrafts(aircraft_src: CSVSource) -> Iterator:
    """
    Adapt raw aircraft CSV rows to the Aircrafts dimension schema.

    Parameters
    - aircraft_src: pygrametl CSVSource over the aircraft lookup.

    Yields
    - Dicts containing the natural key and attributes expected by the dimension.
    """
    for row in aircraft_src:
        yield {
            'Aircraft_Registration_Code': row['aircraft_reg_code'],
            'Manufacturer_Serial_Number': row['manufacturer_serial_number'],
            'Aircraft_Model': row['aircraft_model'],
            'Aircraft_Manufacturer_Class': row['aircraft_manufacturer']
        }


def get_reporters(reporter_src: SQLSource, maintenance_personnel_src: CSVSource) -> Iterator:
    """
    Merge and normalize reporter sources into the Reporters dimension schema.

    Parameters
    - reporter_src: SQLSource yielding (executionplace, reporteurclass).
    - maintenance_personnel_src: CSVSource providing airports for MAREP records.

    Yields
    - Unique dicts keyed by (Reporter_Class, Report_Airport_Code).
    """
    reporters_seen: Set[tuple[str, str]] = set()

    # Process maintenance personnel (MAREP)
    for row in maintenance_personnel_src:
        if ('MAREP', row['airport']) not in reporters_seen:
            reporters_seen.add(('MAREP', row['airport']))
            yield {
                'Reporter_Class': 'MAREP',
                'Report_Airport_Code': row['airport'],
            }

    # Process the remaining reporters (PIREP or MAREP) for a given airport
    for row in reporter_src:
        if (row['reporteurclass'], row['executionplace']) not in reporters_seen:
            reporters_seen.add((row['reporteurclass'], row['executionplace']))
            yield {
                'Reporter_Class': row['reporteurclass'],
                'Report_Airport_Code': row['executionplace'],
            }


def generate_date_dimension_rows(
    flights_df: pd.DataFrame, 
    tech_logs_df: pd.DataFrame, 
    maint_df: pd.DataFrame
) -> Iterator:
    """
    Generate unique rows for the Dates dimension from multiple sources.

    Parameters
    - flights_df: DataFrame with scheduleddeparture.
    - tech_logs_df: DataFrame with reportingdate.
    - maint_df: DataFrame with scheduleddeparture.

    Yields
    - Dicts with Full_Date, Day_Num, Month_Num, Year.
    """
    # 1. Extract all dates
    flight_dates = flights_df['scheduleddeparture']
    tech_log_dates = tech_logs_df['reportingdate']
    maint_dates = maint_df['scheduleddeparture']

    # 2. Concatenate, drop nulls, and duplicates
    all_dates = pd.concat([flight_dates, tech_log_dates, maint_dates], ignore_index=True)
    unique_dates = all_dates.dt.normalize().unique()
    
    dates_df = pd.DataFrame(unique_dates, columns=['date_obj'])

    print("Generating unique Date dimension rows...")
    for date_obj in tqdm(dates_df['date_obj']):
        date_str = build_dateCode(date_obj) 
        day_num, month_num, year = get_date(date_str)
        yield {
            'Full_Date': date_str,
            'Day_Num': day_num,
            'Month_Num': month_num,
            'Year': year,
        }


def generate_month_dimension_rows(
    flights_df: pd.DataFrame, 
    tech_logs_df: pd.DataFrame, 
    maint_df: pd.DataFrame
) -> Iterator:
    """
    Produce unique month-year combinations for the Months dimension.

    Parameters
    - flights_df: DataFrame with scheduleddeparture.
    - tech_logs_df: DataFrame with reportingdate.
    - maint_df: DataFrame with scheduleddeparture.

    Yields
    - Dicts with Month_Num and Year.
    """
    # 1. Extract all dates
    flight_dates = flights_df['scheduleddeparture']
    tech_log_dates = tech_logs_df['reportingdate']
    maint_dates = maint_df['scheduleddeparture']

    # 2. Concatenate and get unique months/years
    all_dates = pd.concat([flight_dates, tech_log_dates, maint_dates], ignore_index=True)

    # Create a temporary DataFrame for 'drop_duplicates'
    months_df = pd.DataFrame({
        'Month_Num': all_dates.dt.month,
        'Year': all_dates.dt.year
    })
    
    unique_months_df = months_df.drop_duplicates()

    print("Generating unique Month dimension rows...")
    for row in tqdm(unique_months_df.to_dict('records'), desc="Generating Months"):
        yield row

# =============================================================================
# Fact table transformations
# =============================================================================

def get_flights_operations_daily(   
    flights_df: pd.DataFrame,
    dates_dim: CachedDimension,
    aircrafts_dim: CachedDimension
) -> Iterator:
    """
    Aggregate flight data by day and aircraft using vectorized pandas.

    Parameters
    - flights_df: DataFrame of flights with actual/scheduled timestamps.
    - dates_dim: CachedDimension for resolving Date_ID by Full_Date.
    - aircrafts_dim: CachedDimension for resolving Aircraft_ID by registration.

    Yields
    - Fact rows with Date_ID, Aircraft_ID, FH, Takeoffs, DFC, CFC, TDM.
    """
    print("Vectorizing flight operations measures...")
    df = flights_df.copy()

    # 1. Pre-processing and vectorized calculations
    df['dep_date_str'] = df['scheduleddeparture'].dt.strftime('%Y-%m-%d')
    is_cancelled = df['cancelled'] | df['actualdeparture'].isnull()

    # FH (Flight Hours)
    duration = (df['actualarrival'] - df['actualdeparture']).dt.total_seconds() / 3600.0 # type: ignore[attr-defined]
    df['FH'] = np.where(is_cancelled, 0.0, duration.fillna(0.0))
    
    # Takeoffs
    df['Takeoffs'] = np.where(is_cancelled, 0, 1)
    
    # CFC (Cancelled Flight Count)
    df['CFC'] = np.where(is_cancelled, 1, 0)

    # --- Delay Logic ---
    arrival_delay_minutes = (df['actualarrival'] - df['scheduledarrival']).dt.total_seconds() / 60.0 # type: ignore[attr-defined]

    # Condition: (Has 'delaycode' AND delay is > 15 min)
    is_delayed = (df['delaycode'].notnull()) & (arrival_delay_minutes > 15)
    
    # Store only if delayed and arrival_delay_minutes > 15 because is the 
    # info that will be used to compute TDM and DFC
    df['TDM'] = np.where(is_delayed, arrival_delay_minutes.fillna(0.0), 0.0)
    df['DFC'] = np.where(is_delayed, 1, 0)

    # 2. Aggregation (Group By)
    print("Aggregating daily flight operations (pandas)...")
    agg_df = df.groupby(
        ['dep_date_str', 'aircraftregistration']
    ).agg(
        FH=('FH', 'sum'),
        Takeoffs=('Takeoffs', 'sum'),
        CFC=('CFC', 'sum'),
        DFC=('DFC', 'sum'),
        TDM=('TDM', 'sum')
    ).reset_index()

    # 3. Lookup keys and Yield
    print("Yielding final daily flight facts...")
    for row in tqdm(agg_df.to_dict('records'), desc="Finalizing Daily Facts"):
        try:
            date_id = dates_dim.lookup({'Full_Date': row['dep_date_str']})
            aircraft_id = aircrafts_dim.lookup({'Aircraft_Registration_Code': row['aircraftregistration']})
            
            yield {
                'Date_ID': date_id,
                'Aircraft_ID': aircraft_id,
                'FH': row['FH'],
                'Takeoffs': int(row['Takeoffs']),
                'DFC': int(row['DFC']),
                'CFC': int(row['CFC']),
                'TDM': int(round(row['TDM']))
            }
        except (KeyError, TypeError) as e:
            logging.warning(f"Skipping aggregated flight record due to missing dimension key: {e}. Row: {row}")


def get_aircrafts_monthly_snapshot(
    maintenance_df: pd.DataFrame, 
    months_dim: CachedDimension, 
    aircrafts_dim: CachedDimension
) -> Iterator:
    """
    Aggregate maintenance windows by month and aircraft.

    Parameters
    - maintenance_df: DataFrame of maintenance intervals and flags.
    - months_dim: CachedDimension for resolving Month_ID.
    - aircrafts_dim: CachedDimension for resolving Aircraft_ID.

    Yields
    - Fact rows with Month_ID, Aircraft_ID, ADIS, ADOSS, ADOSU.
    """
    
    print("Vectorizing monthly maintenance measures...")
    df = maintenance_df.dropna(subset=['scheduleddeparture', 'scheduledarrival', 'aircraftregistration']).copy()

    # 1. Pre-processing and vectorized calculations
    df['year'] = df['scheduleddeparture'].dt.year
    df['month_num'] = df['scheduleddeparture'].dt.month
    
    duration_hours = (df['scheduledarrival'] - df['scheduleddeparture']).dt.total_seconds() / 3600.0 # type: ignore[attr-defined]
    pct_of_day = (duration_hours / 24.0).clip(lower=0.0, upper=1.0) # Ensure within [0,1]

    df['scheduled_pct'] = np.where(df['programmed'], pct_of_day, 0.0)
    df['unscheduled_pct'] = np.where(~df['programmed'], pct_of_day, 0.0)

    # 2. Aggregation (Group By)
    print("Aggregating monthly aircraft snapshots (pandas)...")
    agg_df = df.groupby(
        ['year', 'month_num', 'aircraftregistration']
    ).agg(
        ADOSS=('scheduled_pct', 'sum'),
        ADOSU=('unscheduled_pct', 'sum')
    ).reset_index()

    # 3. Lookup keys and Yield
    print("Yielding final monthly aircraft facts...")
    for row in tqdm(agg_df.to_dict('records'), desc="Finalizing Monthly Facts"):
        try:
            month_id = months_dim.lookup({'Month_Num': row['month_num'], 'Year': row['year']})
            aircraft_id = aircrafts_dim.lookup({'Aircraft_Registration_Code': row['aircraftregistration']})
            
            _, num_days_in_month = calendar.monthrange(row['year'], row['month_num'])
            ados = row['ADOSS'] + row['ADOSU']
            adis = num_days_in_month - ados
            
            yield {
                'Month_ID': month_id,
                'Aircraft_ID': aircraft_id,
                'ADIS': adis,
                'ADOSS': row['ADOSS'],
                'ADOSU': row['ADOSU']
            }
        except (KeyError, TypeError) as e:
            logging.warning(f"Skipping aggregated maintenance record due to missing dimension key: {e}. Row: {row}")


def get_logbooks(
    technical_logbooks_df: pd.DataFrame,
    months_dim: CachedDimension, 
    aircrafts_dim: CachedDimension, 
    reporters_dim: CachedDimension
) -> Iterator:
    """
    Aggregate technical logbook entries by (year, month, aircraft, role, airport).

    Parameters
    - technical_logbooks_df: DataFrame with reportingdate, registration, role.
    - months_dim: CachedDimension for Month_ID.
    - aircrafts_dim: CachedDimension for Aircraft_ID.
    - reporters_dim: CachedDimension for Reporter_ID.

    Yields
    - Fact rows with Month_ID, Aircraft_ID, Reporter_ID, Log_Count.
    """

    print("Vectorizing logbook measures...")
    df = technical_logbooks_df.dropna(
        subset=['reportingdate', 'aircraftregistration', 'reporteurclass', 'executionplace']
    ).copy()
    
    # 1. Pre-processing
    df['year'] = df['reportingdate'].dt.year
    df['month_num'] = df['reportingdate'].dt.month

    # 2. Aggregation (Group By)
    print("Aggregating logbook entries (pandas)...")
    agg_df = (
        df.groupby(['year', 'month_num', 'aircraftregistration', 'reporteurclass', 'executionplace'])
        .size()
        .reset_index()
        .rename(columns={0: 'Log_Count'})
    )

    # 3. Lookup keys and Yield
    print("Yielding final logbook facts...")
    for row in tqdm(agg_df.to_dict('records'), desc="Finalizing Logbooks"):
        try:
            month_id = months_dim.lookup({'Month_Num': row['month_num'], 'Year': row['year']})
            aircraft_id = aircrafts_dim.lookup({'Aircraft_Registration_Code': row['aircraftregistration']})
            reporter_id = reporters_dim.lookup({'Reporter_Class': row['reporteurclass'], 'Report_Airport_Code': row['executionplace']})

            yield {
                'Month_ID': month_id,
                'Aircraft_ID': aircraft_id,
                'Reporter_ID': reporter_id,
                'Log_Count': row['Log_Count']
            }
        except KeyError as e:
            logging.warning(f"Skipping aggregated logbook record due to missing dimension key: {e}. Row: {row}")


# =============================================================================
# Business Rules (BR) Cleaning Functions 
# =============================================================================

def check_and_fix_1st_BR(flights_df: pd.DataFrame) -> pd.DataFrame:
    """
    BR1: Swap arrival/departure when arrival precedes departure.

    Parameters
    - flights_df: Original flights DataFrame (will not be modified in-place).

    Returns
    - A copy with offending rows corrected by swapping the two timestamps.
    """
    print("Applying BR1 (Arrival/Departure Swap)...")
    df = flights_df.copy()

    # Condition: (arr and dep are not null) AND (arr < dep)
    cond = (df['actualarrival'].notnull()) & (df['actualdeparture'].notnull()) & (df['actualarrival'] < df['actualdeparture'])

    # Log violations
    violations = df[cond]
    if not violations.empty:
        print(f"BR1 Violation: Found {len(violations)} flights with arrival before departure. Swapping...")
        for flight_id in violations['id']:
            logging.info(f"BR1 Violation: Swapping arrival and departure for flight id: {flight_id}.")

    # Execute the swap
    df.loc[cond, ['actualarrival', 'actualdeparture']] = df.loc[cond, ['actualdeparture', 'actualarrival']].values
    
    return df


def check_and_fix_2nd_BR(flights_df: pd.DataFrame) -> pd.DataFrame:
    """
    BR2: Remove overlapping flights for the same aircraft.

    Parameters
    - flights_df: Original flights DataFrame.

    Returns
    - A filtered DataFrame without flights that overlap the next departure.
    """
    print("Applying BR2 (Flight Overlap)...")
    
    # 1. Filter non-cancelled flights and sort by aircraft and departure time
    df = flights_df[~flights_df['cancelled']].copy()
    df.sort_values(by=['aircraftregistration', 'actualdeparture'], inplace=True)

    # 2. Find overlaps
    # Compare the arrival of a flight with the *next* departure of the *same* aircraft
    df['next_departure'] = df.groupby('aircraftregistration')['actualdeparture'].shift(-1)

    # An overlap occurs if the arrival is *after* the *next* departure
    overlap_cond = df['actualarrival'] > df['next_departure']

    # 3. Identify and log the IDs to ignore (f1 in your original logic)
    ignored_flights_ids = set(df[overlap_cond]['id'])
    
    if ignored_flights_ids:
        print(f"BR2 Violation: Found {len(ignored_flights_ids)} overlapping flights to ignore.")
        for flight_id in ignored_flights_ids:
             logging.info(f"BR2 Violation: Overlap detected. Ignoring flight {flight_id}.")

    # 4. Return the original DataFrame without the ignored flights
    return flights_df[~flights_df['id'].isin(ignored_flights_ids)]


def check_and_fix_3rd_BR(
    post_flights_reports_df: pd.DataFrame, 
    aircrafts: CSVSource
):
    """
    BR3: Identify post-flight reports referring to non-existent aircraft.

    Parameters
    - post_flights_reports_df: DataFrame of post-flight reports (AMOS).
    - aircrafts: CSVSource for valid aircraft registration codes.

    Returns
    - DataFrame containing only the invalid reports (to be ignored upstream).
    """
    print("Applying BR3 (Aircraft Existence)...")

    # 1. Get valid codes
    aircraft_reg_codes = set([row['aircraft_reg_code'] for row in aircrafts])
    
    # 2. Find invalid rows
    invalid_mask = ~post_flights_reports_df['aircraftregistration'].isin(aircraft_reg_codes)

    # 3. Log violations
    invalid_reports = post_flights_reports_df[invalid_mask]
    if not invalid_reports.empty:
        print(f"BR3 Violation: Found {len(invalid_reports)} reports with non-existent aircraft. Ignoring...")
        for _, row in invalid_reports.iterrows():
            logging.warning(f"BR3 Violation: Aircraft {row['aircraftregistration']} not found. Ignoring report {row['pfrid']}.")

    # 4. Return only valid rows
    return post_flights_reports_df[~invalid_mask]