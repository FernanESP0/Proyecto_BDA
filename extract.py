"""
ETL Extraction Module

Handles data extraction from PostgreSQL and CSV files.
Provides raw data sources for the transformation stage and executes
pre-defined baseline queries against the source database.
"""

from pathlib import Path
import psycopg2 # type: ignore
import pandas as pd
# https://pygrametl.org
from pygrametl.datasources import CSVSource, SQLSource  # type: ignore


# Connect to the PostgreSQL source
path = Path("db_conf.txt")
if not path.is_file():
    raise FileNotFoundError(f"Database configuration file '{path.absolute()}' not found.")
try:
    parameters = {}
    # Read the database configuration from the provided txt file, line by line
    with open(path, 'r') as f:
        lines = f.readlines()
        for line in lines:
            parameters[line.split('=', 1)[0]] = line.split('=', 1)[1].strip()
    conn = psycopg2.connect(
        dbname=parameters['dbname'],
        user=parameters['user'],
        password=parameters['password'],
        host=parameters['ip'],
        port=parameters['port']
    )
except psycopg2.Error as e:
    print(e)
    raise ValueError(f"Unable to connect to the database: {parameters}")
except Exception as e:
    print(e)
    raise ValueError(f"Database configuration file '{path.absolute()}' not properly formatted (check file 'db_conf.example.txt'.")


# ============================================================
#  Extracting functions of CSV files (Sin cambios)
# ============================================================

def get_aircraft_manufacturer_info() -> CSVSource:
    """
    Extract aircraft manufacturer information from the provided CSV file.
    """
    return CSVSource(
        open('aircraft-manufaturerinfo-lookup.csv', 'r', encoding='utf-8'),
        delimiter=','
    )


def get_maintenance_personnel() -> CSVSource:
    """ 
    Extract maintenance personnel information from the provided CSV file.
    """
    return CSVSource(
        open('maintenance_personnel.csv', 'r', encoding='utf-8'),
        delimiter=','
    )

# ============================================================
# Extracting function of PostgreSQL sources
# ============================================================


def get_aggregated_logbooks() -> SQLSource:
    """
    Extracts aggregated logbook entries by month, aircraft, and reporter.
    """
    query = """
        SELECT
            aircraftregistration,
            executionplace,
            reporteurclass,
            DATE_PART('year', reportingdate) AS "year",
            DATE_PART('month', reportingdate) AS "month_num",
            COUNT(*) AS log_count
        FROM "AMOS".technicallogbookorders
        WHERE reportingdate IS NOT NULL 
          AND aircraftregistration IS NOT NULL
          AND executionplace IS NOT NULL
          AND reporteurclass IS NOT NULL
        GROUP BY
            aircraftregistration,
            executionplace,
            reporteurclass,
            DATE_PART('year', reportingdate),
            DATE_PART('month', reportingdate)
    """
    return SQLSource(conn, query)


def get_reporters_info() -> SQLSource: 
    """
    Extracts unique combinations of reporters (for the dimension).
    """
    return SQLSource(conn, 'SELECT DISTINCT executionplace, reporteurclass FROM "AMOS".technicallogbookorders')


def get_all_dates() -> SQLSource:
    """
    Extracts all relevant dates from all tables in a single query.
    """
    query = """
        (SELECT reportingdate AS "date" FROM "AMOS".technicallogbookorders)
        UNION
        (SELECT scheduleddeparture AS "date" FROM "AIMS".flights)
        UNION
        (SELECT scheduleddeparture AS "date" FROM "AIMS".maintenance)
    """
    return SQLSource(conn, query)


def get_aggregated_flights() -> SQLSource:
    """
    Extracts aggregated flight data by day and aircraft.
    """
    query = """
        SELECT
            f.aircraftregistration,
            DATE(f.scheduleddeparture) AS "full_date",

            -- Sum of Flight Hours (FH)
            SUM(CASE WHEN f.cancelled = false THEN EXTRACT(EPOCH FROM f.actualarrival - f.actualdeparture) / 3600.0
                     ELSE 0 END) AS fh,

            -- Count of Takeoffs
            SUM(CASE WHEN f.cancelled = false THEN 1 ELSE 0 END) AS takeoffs,
            
            -- Count of Cancelled Flights
            SUM(CASE WHEN f.cancelled = true THEN 1 ELSE 0 END) AS cfc,

            -- Count of Delayed Flights
            SUM(CASE WHEN f.cancelled = false AND (EXTRACT(EPOCH FROM f.actualarrival - f.scheduledarrival) / 60 > 15)
                    THEN 1 ELSE 0 END) AS dfc,

            -- Sum of Delay Minutes (Total Delay Minutes)
            -- (Using the logic from the baseline query)
            SUM(CASE WHEN f.cancelled = false AND (EXTRACT(EPOCH FROM f.actualarrival - f.scheduledarrival) / 60 > 15)
                     THEN EXTRACT(EPOCH FROM f.actualarrival - f.scheduledarrival) / 60
                     ELSE 0 END) AS tdm
            
        FROM "AIMS".flights f
        GROUP BY
            f.aircraftregistration,
            DATE(f.scheduleddeparture)
    """
    return SQLSource(conn, query)


def get_aggregated_maintenance() -> SQLSource:
    """
    Extracts aggregated maintenance data by month and aircraft.
    Calculates the percentage of service days directly in the DB.
    """
    query = """
        SELECT
            m.aircraftregistration,
            DATE_PART('year', m.scheduleddeparture) AS "year",
            DATE_PART('month', m.scheduleddeparture) AS "month_num",

            -- Sum of percentage of days out-of-service scheduled (ADOSS)
            SUM(
                CASE WHEN m.programmed = true THEN
                    LEAST(
                        (EXTRACT(EPOCH FROM m.scheduledarrival - m.scheduleddeparture) / 3600.0) / 24.0,
                        1.0  -- LLimit of 1.0 (24h)
                    )
                ELSE 0 END
            ) AS adoss_pct_total,
            
            -- Sum of percentage of days out-of-service unscheduled (ADOSU)
            SUM(
                CASE WHEN m.programmed = false THEN
                    LEAST(
                        (EXTRACT(EPOCH FROM m.scheduledarrival - m.scheduleddeparture) / 3600.0) / 24.0,
                        1.0
                    )
                ELSE 0 END
            ) AS adosu_pct_total
            
        FROM "AIMS".maintenance m
        GROUP BY
            m.aircraftregistration,
            DATE_PART('month', m.scheduleddeparture),
            DATE_PART('year', m.scheduleddeparture)
    """
    return SQLSource(conn, query)


# =======================================================================================================
# Baseline queries
# =======================================================================================================


def get_aircrafts_per_manufacturer() -> dict[str, list[str]]:
    """
    Helper function to get a mapping of aircraft manufacturers to their respective aircraft registration codes.
    Used in baseline query implementations.
    """
    aircrafts: dict[str, list[str]] = {}
    for row in get_aircraft_manufacturer_info():
        manufacturer = row['aircraft_manufacturer']
        aircraft_id = row['aircraft_reg_code']
        if manufacturer not in aircrafts:
            aircrafts[manufacturer] = []
        aircrafts[manufacturer].append(aircraft_id)
    return aircrafts


def query_utilization_baseline():
    aircrafts = get_aircrafts_per_manufacturer()
    cur = conn.cursor()
    cur.execute(f"""
        WITH atomic_data AS (
            SELECT f.aircraftregistration,
                CASE 
                    WHEN f.aircraftregistration in ('{"','".join(aircrafts.get("Airbus", []))}') THEN 'Airbus'
                    WHEN f.aircraftregistration in ('{"','".join(aircrafts.get("Boeing", []))}') THEN 'Boeing'
                    ELSE f.aircraftregistration
                    END AS manufacturer, 
                DATE_PART('year', f.scheduleddeparture)::text AS year,
                CASE WHEN f.cancelled 
                    THEN 0
                    ELSE EXTRACT(EPOCH FROM f.actualarrival-f.actualdeparture) / 3600
                    END AS flightHours,
                CASE WHEN f.cancelled 
                    THEN 0
                    ELSE 1
                    END AS flightCycles,
                CASE WHEN f.cancelled
                    THEN 1
                    ELSE 0
                    END AS cancellations,
                CASE WHEN f.cancelled
                    THEN 0
                    ELSE CASE WHEN EXTRACT(EPOCH FROM f.actualarrival - f.scheduledarrival) / 60 > 15
                        THEN 1
                        ELSE 0
                        END
                    END AS delays,
                CASE WHEN f.cancelled
                    THEN 0
                    ELSE CASE WHEN EXTRACT(EPOCH FROM f.actualarrival - f.scheduledarrival) / 60 > 15
                        THEN EXTRACT(EPOCH FROM f.actualarrival - f.scheduledarrival) / 60
                        ELSE 0
                        END
                    END AS delayedMinutes,
                0 AS scheduledOutOfService,
                0 AS unScheduledOutOfService
            FROM "AIMS".flights f
            UNION ALL
            SELECT m.aircraftregistration,           
                CASE 
                    WHEN m.aircraftregistration in ('{"','".join(aircrafts.get("Airbus", []))}') THEN 'Airbus'
                    WHEN m.aircraftregistration in ('{"','".join(aircrafts.get("Boeing", []))}') THEN 'Boeing'
                    ELSE m.aircraftregistration
                    END AS manufacturer, 
                DATE_PART('year', m.scheduleddeparture)::text AS year,
                0 AS flightHours,
                0 AS flightCycles,
                0 AS cancellations,
                0 AS delays,
                0 AS delayedMinutes,
                CASE WHEN m.programmed
                    THEN EXTRACT(EPOCH FROM m.scheduledarrival-m.scheduleddeparture)/(24*3600)
                    ELSE 0
                    END AS scheduledOutOfService,
                CASE WHEN m.programmed
                    THEN 0
                    ELSE EXTRACT(EPOCH FROM m.scheduledarrival-m.scheduleddeparture)/(24*3600)
                    END AS unScheduledOutOfService
            FROM "AIMS".maintenance m
            )
        SELECT a.manufacturer, a.year, 
            ROUND(SUM(a.flightHours)/COUNT(DISTINCT a.aircraftregistration), 2) AS FH,
            ROUND(SUM(a.flightCycles)/COUNT(DISTINCT a.aircraftregistration), 2) AS TakeOff,
            ROUND(SUM(a.scheduledOutOfService)/COUNT(DISTINCT a.aircraftregistration), 2) AS ADOSS,
            ROUND(SUM(a.unscheduledOutOfService)/COUNT(DISTINCT a.aircraftregistration), 2) AS ADOSU,
            ROUND((SUM(a.scheduledOutOfService)+SUM(a.unscheduledOutOfService))/COUNT(DISTINCT a.aircraftregistration), 2) AS ADOS,
            365-ROUND((SUM(a.scheduledOutOfService)+SUM(a.unscheduledOutOfService))/COUNT(DISTINCT a.aircraftregistration), 2) AS ADIS, -- This assumes a period of one year (as in the group by)
            ROUND(ROUND(SUM(a.flightHours)/COUNT(DISTINCT a.aircraftregistration), 2)/((365-ROUND((SUM(a.scheduledOutOfService)+SUM(a.unscheduledOutOfService))/COUNT(DISTINCT a.aircraftregistration), 2))*24), 2) AS DU,
            ROUND(ROUND(SUM(a.flightCycles)/COUNT(DISTINCT a.aircraftregistration), 2)/(365-ROUND((SUM(a.scheduledOutOfService)+SUM(a.unscheduledOutOfService))/COUNT(DISTINCT a.aircraftregistration), 2)), 2) AS DC,
            100*ROUND(SUM(delays)/ROUND(SUM(a.flightCycles), 2), 4) AS DYR,
            100*ROUND(SUM(a.cancellations)/ROUND(SUM(a.flightCycles), 2), 4) AS CNR,
            100-ROUND(100*(SUM(delays)+SUM(cancellations))/SUM(a.flightCycles), 2) AS TDR,
            100*ROUND(SUM(delayedMinutes)/SUM(delays),2) AS ADD
        FROM atomic_data a
        GROUP BY a.manufacturer, a.year
        ORDER BY a.manufacturer, a.year;
        """)
    result = cur.fetchall()
    cur.close()
    return result


def query_reporting_baseline():
    aircrafts = get_aircrafts_per_manufacturer()
    cur = conn.cursor()
    cur.execute(f"""
        WITH 
            atomic_data_utilization AS (
                SELECT
                    CASE 
                        WHEN f.aircraftregistration in ('{"','".join(aircrafts.get("Airbus", []))}') THEN 'Airbus'
                        WHEN f.aircraftregistration in ('{"','".join(aircrafts.get("Boeing", []))}') THEN 'Boeing'
                        ELSE f.aircraftregistration
                        END AS manufacturer, 
                    DATE_PART('year', f.scheduleddeparture)::text AS year,
                    CAST(SUM(CASE WHEN f.cancelled 
                        THEN 0
                        ELSE EXTRACT(EPOCH FROM f.actualarrival-f.actualdeparture) / 3600
                        END) AS numeric) AS flightHours,
                    CAST(SUM(CASE WHEN f.cancelled 
                        THEN 0
                        ELSE 1
                        END) AS numeric) AS flightCycles
                FROM "AIMS".flights f
                GROUP BY manufacturer, YEAR
                ),
            atomic_data_reporting AS (
                SELECT
                    CASE 
                        WHEN f.aircraftregistration in ('{"','".join(aircrafts.get("Airbus", []))}') THEN 'Airbus'
                        WHEN f.aircraftregistration in ('{"','".join(aircrafts.get("Boeing", []))}') THEN 'Boeing'
                        ELSE f.aircraftregistration
                        END AS manufacturer, 
                    DATE_PART('year', f.reportingdate)::text AS year,
                    COUNT(*) AS counter
                FROM "AMOS".postflightreports f
                GROUP BY manufacturer, YEAR
                )
        SELECT f1.manufacturer, f1.year,
            1000*ROUND(f1.counter/f2.flightHours, 3) AS RRh,
            100*ROUND(f1.counter/f2.flightCycles, 2) AS RRc               
        FROM atomic_data_reporting f1
            JOIN atomic_data_utilization f2 ON f2.manufacturer = f1.manufacturer AND f1.year = f2.year
        ORDER BY f1.manufacturer, f1.YEAR;
        """)
    result = cur.fetchall()
    cur.close()
    return result


def query_reporting_per_role_baseline():
    aircrafts = get_aircrafts_per_manufacturer()
    cur = conn.cursor()
    cur.execute(f"""
        WITH 
            atomic_data_utilization AS (
                SELECT
                    CASE 
                        WHEN f.aircraftregistration in ('{"','".join(aircrafts.get("Airbus", []))}') THEN 'Airbus'
                        WHEN f.aircraftregistration in ('{"','".join(aircrafts.get("Boeing", []))}') THEN 'Boeing'
                        ELSE f.aircraftregistration
                        END AS manufacturer, 
                    DATE_PART('year', f.scheduleddeparture)::text AS year,
                    CAST(SUM(CASE WHEN f.cancelled 
                        THEN 0
                        ELSE EXTRACT(EPOCH FROM f.actualarrival-f.actualdeparture) / 3600
                        END) AS numeric) AS flightHours,
                    CAST(SUM(CASE WHEN f.cancelled 
                        THEN 0
                        ELSE 1
                        END) AS numeric) AS flightCycles
                FROM "AIMS".flights f
                GROUP BY manufacturer, YEAR
                ),
            atomic_data_reporting AS (
                SELECT
                    CASE 
                        WHEN f.aircraftregistration in ('{"','".join(aircrafts.get("Airbus", []))}') THEN 'Airbus'
                        WHEN f.aircraftregistration in ('{"','".join(aircrafts.get("Boeing", []))}') THEN 'Boeing'
                        ELSE f.aircraftregistration
                        END AS manufacturer, 
                    DATE_PART('year', f.reportingdate)::text AS year,
                    f.reporteurclass AS role,
                    COUNT(*) AS counter
                FROM "AMOS".postflightreports f
                GROUP BY manufacturer, year, role
                )
        SELECT f1.manufacturer, f1.year, f1.role,
            1000*ROUND(f1.counter/f2.flightHours, 3) AS RRh,
            100*ROUND(f1.counter/f2.flightCycles, 2) AS RRc              
        FROM atomic_data_reporting f1
            JOIN atomic_data_utilization f2 ON f2.manufacturer = f1.manufacturer AND f1.year = f2.year
        ORDER BY f1.manufacturer, f1.year, f1.role;
        """)
    result = cur.fetchall()
    cur.close()
    return result
