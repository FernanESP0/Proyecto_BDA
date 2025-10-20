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

        print("Extracting data from CSV sources (Dimensions)")
        # CSV Sources
        aircraft_manuf_src = extract.get_aircraft_manufacturer_info()
        maint_personnel_src = extract.get_maintenance_personnel()
        print("CSV sources extracted.")

        print("Extracting data from PostgreSQL sources (Dimensions & Aggregated Facts)")
        # --- Dimension sources ---
        # Unified source for Dates and Months dimensions
        all_dates_src = extract.get_all_dates()
        all_dates_src_for_month = extract.get_all_dates() # Separate iterator for Months dimension

        # Unified source for Reporter dimension
        reporter_src = extract.get_reporters_info()

        # --- Unified sources for Facts (PRE-AGGREGATED) ---
        aggregated_flights_src = extract.get_aggregated_flights()
        aggregated_maintenance_src = extract.get_aggregated_maintenance()
        aggregated_logbooks_src = extract.get_aggregated_logbooks()
        print("PostgreSQL sources extracted.")
        
        # =====================================================================
        # 2. LOAD DIMENSIONS: Populate all dimension tables first
        # =====================================================================
        print("\n--- [PHASE 2] Loading Dimension Tables (Building Cache) ---")

        # Load Aircrafts Dimension
        aircraft_iterator = transform.get_aircrafts(aircraft_manuf_src)
        load.load_aircrafts(dw, aircraft_iterator)

        # Load Reporters Dimension
        reporter_iterator = transform.get_reporters(reporter_src, maint_personnel_src)
        load.load_reporters(dw, reporter_iterator)

        # Load Dates Dimension
        date_iterator = transform.generate_date_dimension_rows(all_dates_src)
        load.load_dates(dw, date_iterator)
        
        # Load Months Dimension 
        month_iterator = transform.generate_month_dimension_rows(all_dates_src_for_month)
        load.load_months(dw, month_iterator)

        print("Dimensions loaded and cache populated.")

        # =====================================================================
        # 3. LOAD FACT TABLES: Populate fact tables using populated dimensions
        # =====================================================================
        print("\n--- [PHASE 3] Loading Fact Tables (Using Cache) ---")

        # Load Flight Operations Daily Fact Table
        fod_iterator = transform.get_flights_operations_daily(
            aggregated_flights_src, 
            dw.dates_dim,
            dw.aircrafts_dim
        )
        load.load_flights_operations_daily(dw, fod_iterator)

        # Load Aircraft Monthly Summary Fact Table
        ams_iterator = transform.get_aircrafts_monthly_snapshot(
            aggregated_maintenance_src,  
            dw.months_dim,
            dw.aircrafts_dim
        )
        load.load_aircrafts_monthly_snapshot(dw, ams_iterator)

        # Load Logbooks Fact Table
        logbooks_iterator = transform.get_logbooks(
            aggregated_logbooks_src,
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
        # 4. CLOSE: Ensure the database connection is always closed
        # =====================================================================
        if dw:
            print("Closing Data Warehouse connection...")
            dw.close()
            print("Connection closed.")