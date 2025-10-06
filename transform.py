from tqdm import tqdm # type: ignore
import logging
import pandas as pd


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
    for row in maintenance_personnel_src:
        yield {
            'Reporter_Code': row['reporteurid'],
            'Report_Airport_Code': row['airport'],
            'Reporter_Class': 'MAREP'
        }
        reporters_seen.add(row['reporteurid'])
    for row in reporter_src:
        if row['reporteurid'] not in reporters_seen:
            reporters_seen.add(row['reporteurid'])
            yield {
            'Reporter_Code': row['reporteurid'],
            'Report_Airport_Code': row['executionplace'],
            'Reporter_Class': 'PIREP'
        }


def get_date(flights_src):
    """
    Generate unique records for the Day and Month dimension
    from the scheduleddeparture dates.
    """
    seen = set()
    for row in flights_src:
        date = build_dateCode(row['scheduleddeparture']) 
        month_num = date.month
        year = date.year  
        if month_num not in seen:
            seen.add(month_id)
            yield {
                'Year': date.year,
                'Month_Num': date.month
            }

