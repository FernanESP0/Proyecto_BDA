from tqdm import tqdm # type: ignore
import logging
import pandas as pd
from datetime import timezone, datetime
from pygrametl.datasources import SQLSource, CSVSource
from pygrametl.tables import CachedDimension, SnowflakedDimension


# Configure logging
logging.basicConfig(
    filename='cleaning.log',   # Log file name
    level=logging.INFO,        # Logging level
    format='%(message)s'       # Log message format
)


def build_dateCode(date) -> str:
    return f"{date.year}-{date.month}-{date.day}"


def build_monthCode(date) -> str:
    return f"{date.year}{str(date.month).zfill(2)}"


# TODO: Implement here all transforming functions

def get_date(date_str: str) -> tuple[int, int, int]:
    """Convert a date string 'YYYY-MM-DD' into a tuple with Day_Num, Month_Num, and Year."""
    year = int(date_str[:4])
    month_num = int(date_str[5:7])
    day_num = int(date_str[8:10])
    return (day_num, month_num, year)


def calculate_minutes(time_str: str) -> int:
    """Convert a time string 'HH:MM:SS' into total minutes as an integer."""
    if not time_str:
        return 0  # Handle empty strings or None

    h, m, s = map(int, time_str.split(':'))
    total_minutes = h * 60 + m + (s / 60)
    return int(round(total_minutes))


def calculate_hours(actual_departure: str, actual_arrival: str) -> float:
    """Calculate the duration in hours between actual departure and arrival times."""
    if not actual_departure or not actual_arrival:
        return 0.0  # Handle empty strings or None

    # Parse the dates from string to datetime objects
    fmt = "%Y-%m-%d %H:%M:%S.%f"
    departure = datetime.strptime(actual_departure, fmt)
    arrival = datetime.strptime(actual_arrival, fmt)

    # Difference between both
    duration = arrival - departure

    # Convert to hours
    return duration.total_seconds() / 3600


def get_aircrafts(aircraft_src):
    """
    Transform aircraft manufacturer CSV rows into the structure
    expected for the Aircraft dimension in the DW.
    """
    for row in aircraft_src:
        yield {
            'Aircraft_Registration_Code': row['aircraft_reg_code'],
            'Manufacturer_Serial_Number': row['manufacturer_serial_number'],
            'Aircraft_Model': row['aircraft_model'],
            'Aircraft_Manufacturer_Class': row['aircraft_manufacturer']
        }


def get_reporters(reporter_src, maintenance_personnel_src):
    """
    Transform reporter CSV rows into the structure
    expected for the Reporter dimension in the DW, 
    and add the remaining reporters which are pilots from 
    the SQL source (table "AMOS".technicallogbookorders).
    """
    reporters_seen = set()
    
    # First, process maintenance personnel (MAREP)
    for row in maintenance_personnel_src:
        yield {
            'Reporter_Code': row['reporteurid'],
            'Report_Airport_Code': row['airport'],
            'Reporter_Class': 'MAREP'
        }
        reporters_seen.add(row['reporteurid'])
        
    # Then, process pilots (PIREP), avoiding duplicates
    for row in reporter_src:
        if row['reporteurid'] not in reporters_seen:
            reporters_seen.add(row['reporteurid'])
            yield {
            'Reporter_Code': row['reporteurid'],
            'Report_Airport_Code': row['executionplace'],
            'Reporter_Class': 'PIREP'
        }


def get_dates(flights_src, technical_logbooks_src):
    """
    Generate unique records for the Day and Month dimension
    from the scheduled departure and flight dates.
    """
    dates_seen = set()

    # Process the dates from the flights
    for row in flights_src:
        date = build_dateCode(row['scheduleddeparture'])  # e.g. '2025-10-06'
        day_num, month_num, year = get_date(date)

        if date not in dates_seen:
            dates_seen.add(date)
            yield {
                'Day_Num': day_num,
                'Month_Num': month_num,
                'Year': year
            }

    # Finally, process the dates from the technical logbooks, avoiding duplicates
    for row in technical_logbooks_src:
        date = build_dateCode(row['reportingdate'])  
        day_num, month_num, year = get_date(date)

        if date not in dates_seen:
            dates_seen.add(date)
            yield {
                'Day_Num': day_num,
                'Month_Num': month_num,
                'Year': year
            }


def get_flights_operations_daily(flights_src: SQLSource, operation_interruption_src: SQLSource, date_dim: SnowflakedDimension, aircraft_dim: CachedDimension):
    """
    Aggregates flight data by day and aircraft to create daily operational facts.

    This function iterates through flight records, looks up the surrogate keys for the 
    day and aircraft, calculates measures (Flight Hours, Takeoffs, Delays), and aggregates
    them into a dictionary. Finally, it yields one fact row for each unique 
    combination of Day_ID and Aircraft_ID.
    
    Args:
        flights_src: The source data containing individual flight records.
        date_dim: The populated snowflaked date dimension ('Day' and 'Month').
        aircraft_dim: The populated aircraft dimension.
    """
    # Dictionary to aggregate measures. Key: (Day_ID, Aircraft_ID), Value: dict of measures
    daily_aggregates = {}

    print("Aggregating daily flight operations...")
    for row in tqdm(flights_src):
        # We only care about flights that actually happened
        if row['cancelled'] or not row['actualdeparture']:
            continue

        try:
            # --- 1. Look up Surrogate Keys ---
            # Get date components from the scheduled departure
            dep_date = row['scheduleddeparture']
            day_num, month_num, year = get_date(build_dateCode(dep_date))
            
            # First lookup the Month_ID, then use it to look up the Day_ID
            month_id = date_dim.lookup({'Month_Num': month_num, 'Year': year})
            day_id = date_dim.lookup({'Day_Num': day_num, 'Month_ID': month_id})

            # Look up the Aircraft_ID
            aircraft_id = aircraft_dim.lookup({'Aircraft_Registration_Code': row['aircraftregistration']})
            
            # --- 2. Calculate Measures for this single flight ---
            fh = calculate_hours(str(row['actualdeparture']), str(row['actualarrival']))
            takeoffs = 1
            
            # Calculate Total Delay Minutes (TDM) from operation interruptions
            tdm = 0
            tdlc = 0
            if row['delaycode'] is not None:
                for delay_row in operation_interruption_src:
                    if delay_row['delaycode'] == row['delaycode']:
                        tdm += calculate_minutes(str(delay_row['duration']))
                        tdlc += 1
                        break  # Found the matching delay code, no need to continue
                        
            # --- 3. Aggregate the measures ---
            # Create the primary key tuple for our dictionary
            fact_key = (day_id, aircraft_id)

            # If we haven't seen this day/aircraft combo yet, initialize it
            if fact_key not in daily_aggregates:
                daily_aggregates[fact_key] = {
                    'FH': 0, 'Takeoffs': 0, 'OFC': 0, 'CFC': 0, 'TDM': 0
                }
            
            # Add the measures from this flight to the daily total
            daily_aggregates[fact_key]['FH'] += fh
            daily_aggregates[fact_key]['Takeoffs'] += takeoffs
            daily_aggregates[fact_key]['OFC'] += takeoffs # Assuming 1 OFC per takeoff
            daily_aggregates[fact_key]['CFC'] += takeoffs # Assuming 1 CFC per takeoff
            daily_aggregates[fact_key]['TDM'] += tdm

        except KeyError as e:
            # This can happen if a lookup fails (e.g., an aircraft in flights not in the dimension)
            logging.warning(f"Skipping flight record due to missing dimension key: {e}. Row: {row}")

    # --- 4. Yield the final aggregated rows ---
    print("Yielding final daily flight facts...")
    for (day_id, aircraft_id), measures in tqdm(daily_aggregates.items()):
        yield {
            'Day_ID': day_id,
            'Aircraft_ID': aircraft_id,
            'FH': measures['FH'],
            'Takeoffs': measures['Takeoffs'],
            'OFC': measures['OFC'],
            'CFC': measures['CFC'],
            'TDM': int(round(measures['TDM'])) # Return TDM as an integer
        }


def get_aircrafts_monthly_snapshot(flights_src: SQLSource, date_dim: SnowflakedDimension, aircraft_dim: CachedDimension):
    """
    Creates a monthly snapshot of aircraft service days.

    This function determines which days each aircraft was active (had at least one flight).
    It then aggregates this by month to count the "Aircraft Days In Service" (ADIS).
    The other measures (ADOS, ADOSS, ADOSU) are placeholders as the source data
    doesn't contain out-of-service information.
    
    Args:
        flights_src: The source data containing individual flight records.
        date_dim: The populated snowflaked date dimension ('Day' and 'Month').
        aircraft_dim: The populated aircraft dimension.
    """
    # Dictionary to store the unique days an aircraft was active.
    # Key: (Month_ID, Aircraft_ID), Value: set of day numbers
    monthly_active_days = {}

    print("Aggregating monthly aircraft snapshots...")
    for row in tqdm(flights_src):
        if row['cancelled'] or not row['actualdeparture']:
            continue
        
        try:
            # --- 1. Look up Surrogate Keys ---
            dep_date = row['scheduleddeparture']
            day_num, month_num, year = get_date(build_dateCode(dep_date))

            month_id = date_dim.lookup({'Month_Num': month_num, 'Year': year})
            aircraft_id = aircraft_dim.lookup({'Aircraft_Registration_Code': row['aircraftregistration']})
            
            fact_key = (month_id, aircraft_id)

            # --- 2. Aggregate by adding the active day to a set ---
            # Using a set automatically handles duplicates; we only care that it flew on a given day, not how many times.
            if fact_key not in monthly_active_days:
                monthly_active_days[fact_key] = set()
            
            monthly_active_days[fact_key].add(day_num)

        except KeyError as e:
            logging.warning(f"Skipping monthly snapshot record due to missing dimension key: {e}. Row: {row}")
            
    # --- 3. Yield the final aggregated rows ---
    print("Yielding final monthly aircraft facts...")
    for (month_id, aircraft_id), days_set in tqdm(monthly_active_days.items()):
        # The count of "Aircraft Days In Service" is the number of unique days in the set.
        adis = len(days_set)
        
        yield {
            'Month_ID': month_id,
            'Aircraft_ID': aircraft_id,
            'ADIS': adis,
            'ADOS': 0,    # No data available in source for these measures
            'ADOSS': 0,
            'ADOSU': 0
        }


def get_logbooks(technical_logbooks_src: SQLSource, date_dim: SnowflakedDimension, aircraft_dim: CachedDimension, reporter_dim: CachedDimension):
    """
    Aggregates technical logbook entries to count them by month, aircraft, and reporter.
    
    Args:
        technical_logbooks_src: The source data for technical logbooks.
        date_dim: The populated snowflaked date dimension ('Day' and 'Month').
        aircraft_dim: The populated aircraft dimension.
        reporter_dim: The populated reporter dimension.
    """
    # Dictionary to aggregate log counts. Key: (Month_ID, Aircraft_ID, Reporter_ID), Value: count
    log_counts = {}

    print("Aggregating logbook entries...")
    for row in tqdm(technical_logbooks_src):
        try:
            # --- 1. Look up Surrogate Keys ---
            rep_date = row['reportingdate']
            _, month_num, year = get_date(build_dateCode(rep_date))

            month_id = date_dim.lookup({'Month_Num': month_num, 'Year': year})
            aircraft_id = aircraft_dim.lookup({'Aircraft_Registration_Code': row['aircraftregistration']})
            reporter_id = reporter_dim.lookup({'Reporter_Code': row['reporteurid']})

            fact_key = (month_id, aircraft_id, reporter_id)

            # --- 2. Aggregate the measure (count) ---
            log_counts[fact_key] = log_counts.get(fact_key, 0) + 1

        except KeyError as e:
            logging.warning(f"Skipping logbook record due to missing dimension key: {e}. Row: {row}")

    # --- 3. Yield the final aggregated rows ---
    print("Yielding final logbook facts...")
    for (month_id, aircraft_id, reporter_id), count in tqdm(log_counts.items()):
        yield {
            'Month_ID': month_id,
            'Aircraft_ID': aircraft_id,
            'Reporter_ID': reporter_id,
            'Log_Count': count
        }
         

def to_utc(dt):
    """Ensure datetime is timezone-aware and in UTC."""
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def check_and_fix_1st_and_2nd_BR(flights_src):
    """
    1st BR: actualArrival must be after actualDeparture (swap if needed)
    2nd BR: Two non-cancelled flights of the same aircraft cannot overlap (ignore the first)
    """
    # --- 1st BR ---
    for row in flights_src:
        arr = to_utc(row['actualarrival'])
        dep = to_utc(row['actualdeparture'])
        if arr and dep and arr < dep:
            logging.info(f"Swapping actualArrival and actualDeparture for flight {row['flight_id']}.")
            row['actualarrival'], row['actualdeparture'] = row['actualdeparture'], row['actualarrival']

    # --- 2nd BR ---
    aircraft_flights = {}
    for row in flights_src:
        if not row['cancelled']:  # consider only non-cancelled flights
            aircraft_id = row['aircraftregistration']
            aircraft_flights.setdefault(aircraft_id, []).append(row)

    ignored_flights = set()

    for aircraft_id, flights in aircraft_flights.items():
        # Sort flights by scheduled departure time
        flights.sort(key=lambda x: to_utc(x['scheduleddeparture']))

        for i in range(len(flights) - 1):
            f1 = flights[i]
            f2 = flights[i + 1]

            # Check for overlap
            if to_utc(f1['scheduledarrival']) > to_utc(f2['scheduleddeparture']):
                logging.info(
                    f"Overlapping flights detected for aircraft {aircraft_id}: "
                    f"Ignoring flight {f1['id']}."
                )
                ignored_flights.add(f1['id'])  # Ignore the first flight

    # Return only valid flights
    cleaned_flights = [f for f in flights_src if f['id'] not in ignored_flights]

    return cleaned_flights


def check_and_fix_3rd_BR(post_flights_report, aircrafts_src):
    """
    Check and fix the 3rd Business Rule (BR) in the flights data.
    3rd BR: The aircraft registration in a post flight report must be an aircraft (Fix: Ignore the report, but record the row in a log file)
    """

    aircraft_ids = {row['aircraft_reg_code']: row for row in aircrafts_src}

    ignored_reports = set()
    
    for row in post_flights_report:
        aircraft_id = row['aircraftregistration']
        if aircraft_id not in aircraft_ids:
            logging.warning(f"Aircraft {aircraft_id} not found in aircrafts source.")
            ignored_reports.add(row['pfrid'])

    # Return only valid reports
    cleaned_reports = [r for r in post_flights_report if r['pfrid'] not in ignored_reports]
    return cleaned_reports