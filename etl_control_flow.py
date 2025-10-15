from dw import DW
import extract as extract
import transform as transform
import load as load


if __name__ == '__main__':
    dw = None
    try:
        # =====================================================================
        # 1. INITIALIZATION: Create DW and Extract all data sources
        # =====================================================================
        print("--- [PHASE 1] Initializing DW and Extracting from Sources ---")
        dw = DW(create=True)

        print("Extracting data from CSV sources")
        # CSV Sources
        aircraft_manuf_src = extract.get_aircraft_manufacturer_info()
        maint_personnel_src = extract.get_maintenance_personnel()
        print("CSV sources extracted.")

        print("Extracting data from PostgreSQL sources")
        # PostgreSQL Sources
        flights_src = extract.get_flights()
        flights_dates_src = extract.get_flight_dates()
        reporter_src = extract.get_reporters_info()
        reporting_dates_src = extract.get_reporting_dates()
        maintenance_src = extract.get_maintenance_info()
        maintenance_dates_src = extract.get_maintenance_dates()
        postflightreports_src = extract.get_postflightreports()
        delays_info_src = extract.get_delays_info()
        logbooks_src = extract.get_logbooks_info()
        print("PostgreSQL sources extracted.")
        
        # =====================================================================
        # 2. Proceed with Data Quality Checks
        # =====================================================================
        
        #print("Performing data quality checks...")
        
        #flights_src = transform.check_and_fix_1st_and_2nd_BR(flights_src)
        #postflightreports_src = transform.check_and_fix_3rd_BR(postflightreports_src, aircraft_manuf_src)
        
        # =====================================================================
        # 3. LOAD DIMENSIONS: Populate all dimension tables first
        # =====================================================================
        print("\n--- [PHASE 2] Loading Dimension Tables ---")

        # Load Aircraft Dimension
        aircraft_iterator = transform.get_aircrafts(aircraft_manuf_src)
        load.load_aircrafts(dw, aircraft_iterator)

        # Load Reporter Dimension
        reporter_iterator = transform.get_reporters(reporter_src, maint_personnel_src)
        load.load_reporters(dw, reporter_iterator)

        # Load Date Dimension (Month and Day tables)
        date_iterator = transform.get_dates(flights_dates_src, reporting_dates_src, maintenance_dates_src)
        load.load_dates(dw, date_iterator)

        # Commit after dimensions load for referential integrity in fact tables
        dw.conn_pygrametl.commit()
        print("Dimensions loaded and committed.")

        # =====================================================================
        # 4. LOAD FACT TABLES: Populate fact tables using populated dimensions
        # =====================================================================
        print("\n--- [PHASE 3] Loading Fact Tables ---")

        # Load Flight Operations Daily Fact Table
        # Note how we pass the populated dw.date_dim and dw.aircraft_dim
        #fod_iterator = transform.get_flights_operations_daily(
        #    flights_src,
        #    delays_info_src,
        #    dw.dates_dim,
        #    dw.aircrafts_dim
        #)
        #load.load_flights_operations_daily(dw, fod_iterator)

        # Load Aircraft Monthly Summary Fact Table
        #ams_iterator = transform.get_aircrafts_monthly_snapshot(
        #    maintenance_src,
        #    dw.months_dim,
        #    dw.aircrafts_dim
        #)
        #load.load_aircrafts_monthly_snapshot(dw, ams_iterator)

        # Load Logbooks Fact Table
        logbooks_iterator = transform.get_logbooks(
            logbooks_src,
            dw.months_dim,
            dw.aircrafts_dim,
            dw.reporters_dim
        )
        load.load_logbooks(dw, logbooks_iterator)

        # print("\nETL process completed successfully! ✅")

    except Exception as e:
        print(f"\nAn error occurred during the ETL process: {e}")
        # Optionally, you can add more detailed error logging here

    finally:
        # =====================================================================
        # 5. CLOSE: Ensure the database connection is always closed
        # =====================================================================
        if dw:
            print("Closing Data Warehouse connection...")
            dw.close()
            print("Connection closed.")