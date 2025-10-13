"""
ETL Transformation and Cleaning Script

This module contains all the functions required to transform raw source data 
into the structures expected by the data warehouse dimensions and fact tables.
It also includes functions for cleaning data according to predefined business rules.
"""
from tqdm import tqdm # type: ignore
import logging
from datetime import timezone, datetime
from itertools import chain
from pygrametl.datasources import SQLSource, CSVSource # type: ignore
import calendar
from pygrametl.tables import CachedDimension, SnowflakedDimension # type: ignore
from typing import Iterator, Dict, Any, Tuple, List, Set, Optional


# Configure logging
logging.basicConfig(
    filename='cleaning.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)


# =============================================================================
# Helper Functions
# =============================================================================


def build_dateCode(date: datetime) -> str:
    """Builds a 'YYYY-MM-DD' string from a datetime object."""
    return f"{date.year}-{date.month:02d}-{date.day:02d}"


def get_date(date_str: str) -> Tuple[int, int, int]:
    """Converts a 'YYYY-MM-DD' string into a (Day, Month, Year) tuple."""
    year = int(date_str[0:4])
    month_num = int(date_str[5:7])
    day_num = int(date_str[8:10])
    return (day_num, month_num, year)


def calculate_minutes(time_str: str) -> int:
    """Converts a 'HH:MM:SS' string into total minutes."""
    if not time_str:
        return 0
    h, m, s = map(int, time_str.split(':'))
    return int(round(h * 60 + m + (s / 60)))


def calculate_hours(start_time: str, end_time: str) -> float:
    """Calculates the duration in hours between two timestamp strings."""
    if not start_time or not end_time:
        return 0.0
    # Note: Expects a very specific format.
    fmt = "%Y-%m-%d %H:%M:%S.%f"
    try:
        start_dt = datetime.strptime(start_time, fmt)
        end_dt = datetime.strptime(end_time, fmt)
        duration = end_dt - start_dt
        return duration.total_seconds() / 3600.0
    except ValueError:
        logging.warning(f"Could not parse timestamps: {start_time}, {end_time}")
        return 0.0


def calculate_perct_of_day(hours: float) -> float:
    """Calculates what percentage of a 24-hour day the given hours represent."""
    if hours < 0:
        return 0.0
    # Cap at 100% in case of data errors leading to >24h duration
    return min((hours / 24.0), 1.0)


# =============================================================================
# Dimension Table Transformations
# =============================================================================


def get_aircrafts(aircraft_src: CSVSource) -> Iterator[Dict[str, str]]:
    """Transforms aircraft source data for the Aircraft dimension."""
    for row in aircraft_src:
        yield {
            'Aircraft_Registration_Code': row['aircraft_reg_code'],
            'Manufacturer_Serial_Number': row['manufacturer_serial_number'],
            'Aircraft_Model': row['aircraft_model'],
            'Aircraft_Manufacturer_Class': row['aircraft_manufacturer']
        }


def get_reporters(reporter_src: SQLSource, maintenance_personnel_src: CSVSource) -> Iterator[Dict[str, str]]:
    """Merges and transforms pilots and maintenance personnel for the Reporter dimension."""
    reporters_seen: Set[str] = set()
    
    # Process maintenance personnel (MAREP)
    for row in maintenance_personnel_src:
        if row['reporteurid'] not in reporters_seen:
            reporters_seen.add(row['reporteurid'])
            yield {
                'Reporter_Code': row['reporteurid'],
                'Report_Airport_Code': row['airport'],
                'Reporter_Class': 'MAREP'
            }
            
    # Process pilots (PIREP), avoiding duplicates
    for row in reporter_src:
        if row['reporteurid'] not in reporters_seen:
            reporters_seen.add(row['reporteurid'])
            yield {
                'Reporter_Code': row['reporteurid'],
                'Report_Airport_Code': row['executionplace'],
                'Reporter_Class': 'PIREP'
            }


def _extract_dates(flights_src: SQLSource, technical_logbooks_src: SQLSource, maintenance_src: SQLSource) -> Iterator[datetime]:
    """Helper generator to yield all unique date objects from different sources."""
    # chain() efficiently combines multiple iterators
    for row in chain(flights_src, technical_logbooks_src, maintenance_src):
        # This condition will be true for rows from flights_src OR maintenance_src
        if 'scheduleddeparture' in row and row['scheduleddeparture']:
            yield row['scheduleddeparture']
        # This condition will only be evaluated for rows from other sources,
        # like technical_logbooks_src.
        elif 'executiondate' in row and row['executiondate']:
            yield row['executiondate']


def get_dates(flights_src: SQLSource, technical_logbooks_src: SQLSource, maintenance_src: SQLSource) -> Iterator[Dict[str, Any]]:
    """Generates unique records for the Day and Month dimensions, enriching the month data."""
    dates_seen: Set[str] = set()
    
    # Process all dates from a single, combined source stream
    for date_obj in _extract_dates(flights_src, technical_logbooks_src, maintenance_src):
        date_str = build_dateCode(date_obj)
        if date_str not in dates_seen:
            dates_seen.add(date_str)
            
            day_num, month_num, year = get_date(date_str)
            
            yield {
                'Day_Num': day_num,
                'Month_Num': month_num,
                'Year': year,
            }


# =============================================================================
# Fact Table Transformations
# =============================================================================


def get_flights_operations_daily(
    flights_src: SQLSource,
    operation_interruption_src: SQLSource,
    date_dim: SnowflakedDimension,
    aircraft_dim: CachedDimension
) -> Iterator[Dict[str, Any]]:
    """
    Aggregates flight data by day and aircraft to create daily operational facts.
    This version is optimized to correctly and efficiently calculate delay metrics.
    """
    
    # Pre-aggregate total delay minutes by delay code.
    # This avoids a slow, nested loop. We create a lookup dictionary first.
    print("Pre-aggregating delay information for performance...")
    tdm_lookup: Dict[Tuple[int, str, str], int] = {}
    for row in tqdm(operation_interruption_src, desc="Processing Delays"):
        delay_code = row['delaycode']
        # Sum the duration for each delay code
        tdm_lookup[(delay_code, row['aircraftregistration'], build_dateCode(row['scheduleddeparture']))] = tdm_lookup.get((delay_code, row['aircraftregistration'], build_dateCode(row['scheduleddeparture'])), 0) + calculate_minutes(str(row['duration']))

    # Dictionary to aggregate final measures for the fact table
    daily_aggregates: Dict[Tuple[int, int], Dict[str, float]] = {}

    print("Aggregating daily flight operations...")
    for row in tqdm(flights_src, desc="Processing Flights"):
        try:
            # 1. Look up Surrogate Keys
            dep_date = row['scheduleddeparture']
            day_num, month_num, year = get_date(build_dateCode(dep_date))
            month_id = date_dim.lookup({'Month_Num': month_num, 'Year': year})
            day_id = date_dim.lookup({'Day_Num': day_num, 'Month_ID': month_id})
            aircraft_id = aircraft_dim.lookup({'Aircraft_Registration_Code': row['aircraftregistration']})
            
            fact_key = (day_id, aircraft_id)
            if fact_key not in daily_aggregates:
                daily_aggregates[fact_key] = {'FH': 0, 'Takeoffs': 0, 'OFC': 0, 'CFC': 0, 'TDM': 0}

            # 2. Handle cancelled flights
            if row['cancelled'] or not row['actualdeparture']:
                daily_aggregates[fact_key]['CFC'] += 1
                continue # Skip to the next flight

            # 3. Calculate base measures for non-cancelled flights
            fh = calculate_hours(str(row['actualdeparture']), str(row['actualarrival']))
            takeoffs = 1
            
            # --- CORRECT DELAY LOGIC ---
            delayed_flight_count = 0
            total_delay_minutes = 0
            
            # A flight is considered delayed if it has a delay code.
            if row['delaycode'] is not None:
                # This flight counts as one delayed flight.
                delayed_flight_count = 1
                # Use the pre-aggregated lookup to get the total minutes for this code.
                total_delay_minutes = tdm_lookup.get((row['delaycode'], row['aircraftregistration'], build_dateCode(row['scheduleddeparture'])), 0)

            # 4. Add all measures to the daily aggregates
            daily_aggregates[fact_key]['FH'] += fh
            daily_aggregates[fact_key]['Takeoffs'] += takeoffs
            daily_aggregates[fact_key]['OFC'] += delayed_flight_count # OFC is the count of delayed flights
            daily_aggregates[fact_key]['TDM'] += total_delay_minutes

        except (KeyError, TypeError) as e:
            logging.warning(f"Skipping flight record due to missing key or data: {e}. Row: {row}")

    # 5. Yield the final aggregated rows
    print("Yielding final daily flight facts...")
    for (day_id, aircraft_id), measures in tqdm(daily_aggregates.items(), desc="Finalizing Daily Facts"):
        yield {
            'Day_ID': day_id,
            'Aircraft_ID': aircraft_id,
            'FH': measures['FH'],
            'Takeoffs': int(measures['Takeoffs']),
            'OFC': int(measures['OFC']), # Operational Flight Count (Delayed Flights)
            'CFC': int(measures['CFC']), # Cancelled Flight Count
            'TDM': int(round(measures['TDM'])) # Total Delay Minutes
        }


def get_aircrafts_monthly_snapshot(
    maintenance_src: SQLSource, 
    date_dim: SnowflakedDimension, 
    aircraft_dim: CachedDimension
) -> Iterator[Dict[str, Any]]:
    """Aggregates maintenance data to create a monthly snapshot of aircraft service days."""
    
    monthly_out_of_service_data: Dict[Tuple[int, int], Dict[str, Any]] = {}

    print("Aggregating monthly aircraft snapshots...")
    for row in tqdm(maintenance_src, desc="Processing Maintenance"):
        try:
            # Assumes maintenance records have start and end timestamps
            start_date = row['scheduleddeparture']
            end_date = row['scheduledarrival']
            
            _, month_num, year = get_date(build_dateCode(start_date))
            month_id = date_dim.lookup({'Month_Num': month_num, 'Year': year})
            aircraft_id = aircraft_dim.lookup({'Aircraft_Registration_Code': row['aircraftregistration']})
            
            fact_key = (month_id, aircraft_id)
            if fact_key not in monthly_out_of_service_data:
                monthly_out_of_service_data[fact_key] = {
                    'scheduled_pct': 0.0, 'unscheduled_pct': 0.0,
                    'year': year, 'month_num': month_num
                }

            # Calculate duration as a percentage of a 24-hour day
            duration_hours = calculate_hours(str(start_date), str(end_date))
            pct_of_day = calculate_perct_of_day(duration_hours)
            
            if row['programmed']:
                monthly_out_of_service_data[fact_key]['scheduled_pct'] += pct_of_day
            else:
                monthly_out_of_service_data[fact_key]['unscheduled_pct'] += pct_of_day

        except (KeyError, TypeError) as e:
            logging.warning(f"Skipping maintenance record due to missing key or data: {e}. Row: {row}")

    print("Yielding final monthly aircraft facts...")
    for (month_id, aircraft_id), data in tqdm(monthly_out_of_service_data.items(), desc="Finalizing Monthly Facts"):
        _, num_days_in_month = calendar.monthrange(data['year'], data['month_num'])
        
        adoss = data['scheduled_pct']
        adosu = data['unscheduled_pct']
        
        # Total out-of-service time, represented as an equivalent number of days
        ados = adoss + adosu
        
        # In-service days is the total days in month minus the equivalent out-of-service days
        adis = num_days_in_month - ados
        
        yield {
            'Month_ID': month_id,
            'Aircraft_ID': aircraft_id,
            'ADIS': adis,
            'ADOSS': adoss,
            'ADOSU': adosu
        }


def get_logbooks(
    technical_logbooks_src: SQLSource, 
    date_dim: SnowflakedDimension, 
    aircraft_dim: CachedDimension, 
    reporter_dim: CachedDimension
) -> Iterator[Dict[str, Any]]:
    """Aggregates technical logbook entries by month, aircraft, and reporter."""
    log_counts: Dict[Tuple[int, int, int], int] = {}

    print("Aggregating logbook entries...")
    for row in tqdm(technical_logbooks_src, desc="Processing Logbooks"):
        try:
            rep_date = row['executiondate']
            _, month_num, year = get_date(build_dateCode(rep_date))
            month_id = date_dim.lookup({'Month_Num': month_num, 'Year': year})
            aircraft_id = aircraft_dim.lookup({'Aircraft_Registration_Code': row['aircraftregistration']})
            reporter_id = reporter_dim.lookup({'Reporter_Code': row['reporteurid']})
            
            fact_key = (month_id, aircraft_id, reporter_id)
            log_counts[fact_key] = log_counts.get(fact_key, 0) + 1

        except KeyError as e:
            logging.warning(f"Skipping logbook record due to missing dimension key: {e}. Row: {row}")

    print("Yielding final logbook facts...")
    for (month_id, aircraft_id, reporter_id), count in tqdm(log_counts.items(), desc="Finalizing Logbooks"):
        yield {
            'Month_ID': month_id,
            'Aircraft_ID': aircraft_id,
            'Reporter_ID': reporter_id,
            'Log_Count': count
        }


# =============================================================================
# Business Rules (BR) Cleaning Functions
# =============================================================================


def to_utc(dt: datetime) -> datetime:
    """Ensures a datetime object is timezone-aware and in UTC."""
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def check_and_fix_1st_and_2nd_BR(flights_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Applies BR1 and BR2 to flight data.
    - BR1: actualArrival must be after actualDeparture (swaps if needed).
    - BR2: Two non-cancelled flights of the same aircraft cannot overlap (ignores the earlier one).
    """
    # --- 1st BR ---
    for row in tqdm(flights_data, desc="Applying BR1 (Arrival/Departure Swap)"):
        arr = to_utc(row['actualarrival'])
        dep = to_utc(row['actualdeparture'])
        if arr and dep and arr < dep:
            logging.info(f"BR1 Violation: Swapping arrival and departure for flight {row.get('flight_id', 'N/A')}.")
            row['actualarrival'], row['actualdeparture'] = row['actualdeparture'], row['actualarrival']

    # --- 2nd BR ---
    aircraft_flights: Dict[str, List[Dict[str, Any]]] = {}
    for row in flights_data:
        if not row['cancelled']:
            aircraft_id = row['aircraftregistration']
            aircraft_flights.setdefault(aircraft_id, []).append(row)

    ignored_flights: Set[Any] = set()
    for aircraft_id, flights in tqdm(aircraft_flights.items(), desc="Applying BR2 (Flight Overlap)"):
        flights.sort(key=lambda x: to_utc(x['scheduleddeparture']))
        for i in range(len(flights) - 1):
            f1, f2 = flights[i], flights[i+1]
            if to_utc(f1['scheduledarrival']) > to_utc(f2['scheduleddeparture']):
                logging.info(f"BR2 Violation: Overlap for aircraft {aircraft_id}. Ignoring flight {f1.get('id', 'N/A')}.")
                ignored_flights.add(f1.get('id'))

    return [f for f in flights_data if f.get('id') not in ignored_flights]


def check_and_fix_3rd_BR(
    post_flights_reports: List[Dict[str, Any]], 
    aircrafts_src: CSVSource
) -> List[Dict[str, Any]]:
    """
    Applies BR3 to post-flight reports.
    - BR3: Aircraft registration must exist in the master aircraft list (ignores report if not).
    """
    aircraft_reg_codes: Set[str] = {row['aircraft_reg_code'] for row in aircrafts_src}
    aircrafts_src.reset() # Reset source in case it's used again

    valid_reports: List[Dict[str, Any]] = []
    for row in tqdm(post_flights_reports, desc="Applying BR3 (Aircraft Existence)"):
        if row['aircraftregistration'] in aircraft_reg_codes:
            valid_reports.append(row)
        else:
            logging.warning(f"BR3 Violation: Aircraft {row['aircraftregistration']} not found. Ignoring report {row.get('pfrid', 'N/A')}.")
            
    return valid_reports