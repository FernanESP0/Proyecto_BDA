"""
ETL Transformation and Cleaning Script

This module contains all the functions required to transform raw source data 
into the structures expected by the data warehouse dimensions and fact tables.
It also includes functions for cleaning data according to predefined business rules.
"""

from tqdm import tqdm # type: ignore
import logging
from datetime import datetime
from pygrametl.datasources import SQLSource, CSVSource # type: ignore
import calendar
from pygrametl.tables import CachedDimension # type: ignore
from typing import Iterator, Dict, Any, Tuple, Set


# Configure logging
logging.basicConfig(
    filename='cleaning.log',
    filemode='w',
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


# =============================================================================
# Dimension Table Transformations 
# =============================================================================

def get_aircrafts(aircraft_src: CSVSource) -> Iterator[Dict[str, str]]:
    """Transforms raw aircraft data for the Aircraft dimension."""
    for row in aircraft_src:
        yield {
            'Aircraft_Registration_Code': row['aircraft_reg_code'],
            'Manufacturer_Serial_Number': row['manufacturer_serial_number'],
            'Aircraft_Model': row['aircraft_model'],
            'Aircraft_Manufacturer_Class': row['aircraft_manufacturer']
        }


def get_reporters(reporter_src: SQLSource, maintenance_personnel_src: CSVSource) -> Iterator[Dict[str, str]]:
    """Merges and transforms pilots and maintenance personnel for the Reporter dimension."""
    reporters_seen: Set[tuple[str, str]] = set()
    
    for row in maintenance_personnel_src:
        if ('MAREP', row['airport']) not in reporters_seen:
            reporters_seen.add(('MAREP', row['airport']))
            yield {
                'Reporter_Class': 'MAREP',
                'Report_Airport_Code': row['airport'],
            }
            
    for row in reporter_src:
        if (row['reporteurclass'], row['executionplace']) not in reporters_seen:
            reporters_seen.add((row['reporteurclass'], row['executionplace']))
            yield {
                'Reporter_Class': row['reporteurclass'],
                'Report_Airport_Code': row['executionplace'],
            }


def generate_date_dimension_rows(all_dates_src: SQLSource) -> Iterator[Dict[str, Any]]:
    """
    Generates unique, enriched rows for the Date dimension.
    (Ahora usa la fuente 'all_dates_src' unificada).
    """
    dates_seen: Set[str] = set()
    
    for row in all_dates_src:
        date_obj = row['date']
        date_str = build_dateCode(date_obj) 
        if date_str not in dates_seen:
            dates_seen.add(date_str)
            day_num, month_num, year = get_date(date_str) 
            
            yield {
                'Full_Date': date_str,
                'Day_Num': day_num,
                'Month_Num': month_num,
                'Year': year,
            }


def generate_month_dimension_rows(all_dates_src: SQLSource) -> Iterator[Dict[str, Any]]:
    """
    Generates unique, efficient rows for the Month dimension.
    (Ahora usa la fuente 'all_dates_src' unificada).
    """
    months_seen: Set[Tuple[int, int]] = set()
    
    for row in all_dates_src:
        date_obj = row['date']
        month_num = date_obj.month
        year = date_obj.year
        
        if (year, month_num) not in months_seen:
            months_seen.add((year, month_num))
            yield {
                'Month_Num': month_num,
                'Year': year,
            }


# =============================================================================
# Fact Table Transformations 
# =============================================================================


def get_flights_operations_daily(
    aggregated_flights_src: SQLSource,  
    dates_dim: CachedDimension,
    aircrafts_dim: CachedDimension
) -> Iterator[Dict[str, Any]]:
    """
    Transforms the data from PRE-AGGREGATED daily flight operations.
    La agregación (SUM/COUNT) ya se hizo en la BD.
    """
    print("Transforming pre-aggregated daily flight facts...")
    for row in tqdm(aggregated_flights_src, desc="Transforming Daily Facts"):
        try:
            # 1. Look up Surrogate Keys
            date_str = build_dateCode(row['full_date'])
            date_id = dates_dim.lookup({'Full_Date': date_str})
            aircraft_id = aircrafts_dim.lookup({'Aircraft_Registration_Code': row['aircraftregistration']})

            # 2. Yield the row
            yield {
                'Date_ID': date_id,
                'Aircraft_ID': aircraft_id,
                'FH': row['fh'],
                'Takeoffs': int(row['takeoffs']),
                'DFC': int(row['dfc']),
                'CFC': int(row['cfc']),
                'TDM': int(round(row['tdm']))
            }
        except (KeyError, TypeError) as e:
            logging.warning(f"Skipping aggregated flight record due to missing key: {e}. Row: {row}")


def get_aircrafts_monthly_snapshot(
    aggregated_maintenance_src: SQLSource, 
    months_dim: CachedDimension, 
    aircrafts_dim: CachedDimension
) -> Iterator[Dict[str, Any]]:
    """
    Transforms the pre-aggregated monthly snapshot data.
    Applies the final business logic (ADIS) to the pre-calculated sums.
    """
    print("Transforming pre-aggregated monthly aircraft facts...")
    for row in tqdm(aggregated_maintenance_src, desc="Transforming Monthly Facts"):
        try:
            # 1. Look up Surrogate Keys
            year_val = int(row['year'])
            month_val = int(row['month_num'])
            
            month_id = months_dim.lookup({
                'Month_Num': month_val,
                'Year': year_val
            })
            aircraft_id = aircrafts_dim.lookup({'Aircraft_Registration_Code': row['aircraftregistration']})

            # 2. Apply business logic for ADIS
            _, num_days_in_month = calendar.monthrange(year_val, month_val)
            
            adoss = row['adoss_pct_total']
            adosu = row['adosu_pct_total']
            ados = adoss + adosu
            
            adis = num_days_in_month - ados

            # 3. Yield the row
            yield {
                'Month_ID': month_id,
                'Aircraft_ID': aircraft_id,
                'ADIS': adis,
                'ADOSS': adoss,
                'ADOSU': adosu
            }

        except (KeyError, TypeError) as e:
            logging.warning(f"Skipping aggregated maintenance record due to missing key: {e}. Row: {row}")


def get_logbooks(
    aggregated_logbooks_src: SQLSource, 
    months_dim: CachedDimension, 
    aircrafts_dim: CachedDimension, 
    reporters_dim: CachedDimension
) -> Iterator[Dict[str, Any]]:
    """
    Transforms the pre-aggregated logbook entries.
    The aggregation (COUNT) has already been done in the DB.
    """
    print("Transforming pre-aggregated logbook entries...")
    for row in tqdm(aggregated_logbooks_src, desc="Transforming Logbooks"):
        try:
            # 1. Look up Surrogate Keys
            month_id = months_dim.lookup({
                'Month_Num': int(row['month_num']), 
                'Year': int(row['year'])
            })
            aircraft_id = aircrafts_dim.lookup({'Aircraft_Registration_Code': row['aircraftregistration']})
            reporter_id = reporters_dim.lookup({
                'Reporter_Class': row['reporteurclass'], 
                'Report_Airport_Code': row['executionplace']
            })

            # 2. Yield the row
            yield {
                'Month_ID': month_id,
                'Aircraft_ID': aircraft_id,
                'Reporter_ID': reporter_id,
                'Log_Count': int(row['log_count'])
            }

        except KeyError as e:
            logging.warning(f"Skipping aggregated logbook record due to missing dimension key: {e}. Row: {row}")


# =============================================================================
# Business Rules (BR) Cleaning Functions (Con correcciones de lógica)
# =============================================================================


def check_and_fix_1st_BR(flights_data: SQLSource) -> Iterator[Dict[str, Any]]:
    """
    Applies BR1 by swapping arrival and departure times if they are in the wrong order.
    """
    for row in tqdm(flights_data, desc="Applying BR1 (Arrival/Departure Swap)"):
        arr = row['actualarrival']
        dep = row['actualdeparture']
        if arr and dep and arr < dep:
            logging.info(f"BR1 Violation: Swapping arrival and departure for flight {row['aircraftregistration']} with id: {row['id']}.")
            row['actualarrival'], row['actualdeparture'] = row['actualdeparture'], row['actualarrival']
        yield row


def check_and_fix_2nd_BR(flights_data: Iterator[Dict[str, Any]]) -> Iterator[Dict[str, Any]]:
    """
    Applies BR2 by checking for overlapping flights for the same aircraft.
    """
    aircraft_flights: Dict[str, list[Dict[str, Any]]] = {}
    print("Grouping flights by aircraft for BR2 check...")
    for row in flights_data:
        if not row['cancelled']:
            aircraft_id = row['aircraftregistration']
            aircraft_flights.setdefault(aircraft_id, []).append(row)
            
    ignored_flights: Set[Any] = set()
    for aircraft_id, flights in tqdm(aircraft_flights.items(), desc="Applying BR2 (Flight Overlap)"):
        flights.sort(key=lambda x: x['actualdeparture'])
        
        for i in range(len(flights) - 1):
            f1 = flights[i]
            f2 = flights[i+1]
            
            if f1['actualarrival'] > f2['actualdeparture']:
                logging.info(f"BR2 Violation: Overlap for {aircraft_id}. Ignoring flight {f1['id']} (keeping {f2['id']}).")
                ignored_flights.add(f1['id'])

    print("Yielding non-overlapping flights...")
    for aircraft_id, flights in aircraft_flights.items():
        for flight in flights:
            if flight['id'] not in ignored_flights:
                yield flight


def check_and_fix_3rd_BR(
    post_flights_reports: SQLSource, 
    aircrafts_src: CSVSource
) -> Iterator[Dict[str, Any]]:
    """
    Applies BR3 by filtering out reports linked to non-existent aircraft.
    """
    aircraft_reg_codes: set[str] = {row['aircraft_reg_code'] for row in aircrafts_src}
    
    for row in tqdm(post_flights_reports, desc="Applying BR3 (Aircraft Existence)"):
        if row['aircraftregistration'] in aircraft_reg_codes:
            yield row # El código de aeronave es válido, procesar la fila
        else:
            # La aeronave no existe, ignorar la fila
            logging.warning(f"BR3 Violation: Aircraft {row['aircraftregistration']} not found. Ignoring report {row['pfrid']}.")


def get_valid_technical_logbooks(
    post_flights_reports: Iterator[Dict[str, Any]],
    technical_logbooks: SQLSource
) -> Iterator[Dict[str, Any]]:
    """
    Filters technical logbooks to only include those linked to valid post-flight reports.
    """
    # Este conjunto contiene los 'tlborder' de reportes VÁLIDOS (después de BR3)
    valid_tlb_order_ids: set[int] = {row['tlborder'] for row in post_flights_reports if row['tlborder'] is not None}
    
    for row in tqdm(technical_logbooks, desc="Filtering Valid Technical Logbooks"):
        if row['workorderid'] in valid_tlb_order_ids: # La entrada es válida
            yield row
        else:
            # La entrada no es válida (o no está en un reporte válido), ignorar
            logging.warning(f"Invalid Technical Logbook: Work order ID {row['workorderid']} not found in valid post-flight reports. Ignoring logbook.")