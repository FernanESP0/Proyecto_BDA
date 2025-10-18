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
        cleaning = True  # Set to True to perform data cleaning if desired

        print("Extracting data from CSV sources")
        # CSV Sources
        aircraft_manuf_src = extract.get_aircraft_manufacturer_info()
        aircraft_manuf_src_for_cleaning = extract.get_aircraft_manufacturer_info()
        maint_personnel_src = extract.get_maintenance_personnel()
        print("CSV sources extracted.")

        print("Extracting data from PostgreSQL sources")
        # PostgreSQL Sources
        flights_src = extract.get_flights()
        flights_dates_src_for_date = extract.get_flight_dates()
        flights_dates_src_for_month = extract.get_flight_dates() 
        reporter_src = extract.get_reporters_info()
        reporting_dates_src_for_date = extract.get_reporting_dates()
        reporting_dates_src_for_month = extract.get_reporting_dates()
        maintenance_src = extract.get_maintenance_info()
        maintenance_dates_src_for_date = extract.get_maintenance_dates()
        maintenance_dates_src_for_month = extract.get_maintenance_dates()
        postflightreports_src = extract.get_postflightreports()
        delays_info_src = extract.get_delays_info()
        logbooks_src = extract.get_logbooks_info()
        print("PostgreSQL sources extracted.")
        
        # =====================================================================
        # 2. Proceed with Data Quality Checks
        # =====================================================================
        if cleaning:
            print("\n--- [PHASE 2]  Performing Data Quality Checks ---")

            try:
                flights_src = transform.check_and_fix_1st_BR(flights_src)
                flights_src = transform.check_and_fix_2nd_BR(flights_src)
                postflightreports_src = transform.check_and_fix_3rd_BR(postflightreports_src, aircraft_manuf_src_for_cleaning)
                logbooks_src = transform.get_valid_technical_logbooks(postflightreports_src, logbooks_src)

            except Exception as e:
                print(f"Error occurred during data quality checks: {e}")

            print("Data quality checks completed.")
 
        
        # =====================================================================
        # 3. LOAD DIMENSIONS: Populate all dimension tables first
        # =====================================================================
        if cleaning:
            print("\n--- [PHASE 3] Loading Dimension Tables After Cleaning ---")
        else:
            print("\n--- [PHASE 2] Loading Dimension Tables Without Cleaning ---")

        # Load Aircrafts Dimension
        aircraft_iterator = transform.get_aircrafts(aircraft_manuf_src)
        load.load_aircrafts(dw, aircraft_iterator)

        # Load Reporters Dimension
        reporter_iterator = transform.get_reporters(reporter_src, maint_personnel_src)
        load.load_reporters(dw, reporter_iterator)

        # Load Dates Dimension
        date_iterator = transform.generate_date_dimension_rows(flights_dates_src_for_date, reporting_dates_src_for_date, maintenance_dates_src_for_date)
        load.load_dates(dw, date_iterator)
        
        # Load Months Dimension
        month_iterator = transform.generate_month_dimension_rows(flights_dates_src_for_month, reporting_dates_src_for_month, maintenance_dates_src_for_month)
        load.load_months(dw, month_iterator)

        # Commit after dimensions load for referential integrity in fact tables
        dw.conn_pygrametl.commit()
        print("Dimensions loaded and committed.")

        # =====================================================================
        # 4. LOAD FACT TABLES: Populate fact tables using populated dimensions
        # =====================================================================
        if cleaning:
            print("\n--- [PHASE 4] Loading Fact Tables After Cleaning ---")
        else:
            print("\n--- [PHASE 3] Loading Fact Tables Without Cleaning ---")

        # Load Flight Operations Daily Fact Table
        fod_iterator = transform.get_flights_operations_daily(
            flights_src,
            delays_info_src,
            dw.dates_dim,
            dw.aircrafts_dim
        )
        load.load_flights_operations_daily(dw, fod_iterator)

        # Load Aircraft Monthly Summary Fact Table
        ams_iterator = transform.get_aircrafts_monthly_snapshot(
            maintenance_src,
            dw.months_dim,
            dw.aircrafts_dim
        )
        load.load_aircrafts_monthly_snapshot(dw, ams_iterator)

        # Load Logbooks Fact Table
        logbooks_iterator = transform.get_logbooks(
            logbooks_src,
            dw.months_dim,
            dw.aircrafts_dim,
            dw.reporters_dim
        )
        load.load_logbooks(dw, logbooks_iterator)

        print("\nETL process completed successfully! ✅")

    except Exception as e:
        print(f"\nAn error occurred during the ETL process: {e}")

    finally:
        # =====================================================================
        # 5. CLOSE: Ensure the database connection is always closed
        # =====================================================================
        if dw:
            print("Closing Data Warehouse connection...")
            dw.close()
            print("Connection closed.")