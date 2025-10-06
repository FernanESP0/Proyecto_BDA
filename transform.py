from tqdm import tqdm # type: ignore
import logging
import pandas as pd
from datetime import timezone


# Configure logging
logging.basicConfig(
    filename='cleaning.log',           # Log file name
    level=logging.INFO,           # Logging level
    format='%(message)s'  # Log message format
)


def build_dateCode(date) -> str:
    return f"{date.year}-{date.month}-{date.day}"


def build_monthCode(date) -> str:
    return f"{date.year}{str(date.month).zfill(2)}"


# TODO: Implement here all transforming functions

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
    the SQL source.
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
        year = int(date[:4])
        month_num = int(date[5:7])
        day_num = int(date[8:10])

        if date not in dates_seen:
            dates_seen.add(date)
            yield {
                'Day_Num': day_num,
                'Month_Num': month_num,
                'Year': year
            }

    # Finally, process the dates from the technical logbooks, avoiding duplicates
    for row in technical_logbooks_src:
        date = build_dateCode(row['flightdate'])
        year = int(date[:4])
        month_num = int(date[5:7])
        day_num = int(date[8:10])

        if date not in dates_seen:
            dates_seen.add(date)
            yield {
                'Day_Num': day_num,
                'Month_Num': month_num,
                'Year': year
            }


def get_flights_operations_daily(flights_src):
    
    




def get_aircrafts_monthly_snapshot(flights_src):
    
    
    

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