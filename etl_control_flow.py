from dw import DW
import extract as extract
import transform as transform
import load as load
import warnings

# Suprime la advertencia específica de pandas/DBAPI
warnings.filterwarnings(
    "ignore",
    message="pandas only supports SQLAlchemy connectable.*",
    category=UserWarning
)

if __name__ == '__main__':
    dw = None
    try:
        # =====================================================================
        # 1. INITIALIZATION: Create DW and Extract Data to DataFrames
        # =====================================================================
        print("--- [PHASE 1] Initializing DW and Extracting ---")
        dw = DW(create=True)
        cleaning = False  # Set to True to run cleaning

        print("Extracting data from CSV sources...")
        aircraft_manuf_info = extract.get_aircraft_manufacturer_info()
        aircraft_manuf_info_cp = extract.get_aircraft_manufacturer_info()  # Copy for cleaning
        maint_personnel_info = extract.get_maintenance_personnel()
        print("CSV sources extracted.")

        print("Extracting data from PostgreSQL sources...")
        reporters_src = extract.get_reporters_info()
        flights_df = extract.get_flights_df()
        maintenance_df = extract.get_maintenance_info_df()
        logbooks_df = extract.get_logbooks_info_df()
        postflightreports_df = extract.get_postflightreports_df()
        print("PostgreSQL sources extracted.")

        # =====================================================================
        # 2. DATA QUALITY CHECKS AND CLEANING (Pandas)
        # =====================================================================
        if cleaning:
            print("\n--- [PHASE 2] Performing Data Quality Checks ---")
            try:
                # Apply Business Rules (BR) Cleaning Functions
                print("Applying Data Quality Checks and Cleaning...")
                flights_df = transform.check_and_fix_1st_BR(flights_df)
                flights_df = transform.check_and_fix_2nd_BR(flights_df)
                postflightreports_df = transform.check_and_fix_3rd_BR(
                    postflightreports_df, 
                    aircraft_manuf_info_cp
                )
                logbooks_df = transform.get_valid_technical_logbooks(
                    postflightreports_df, 
                    logbooks_df
                )
            
            except Exception as e:
                print(f"Error during data cleaning: {e}. Check 'cleaning.log'.")
                raise  # Stop ETL if cleaning fails
            
            print("Data quality checks completed.")
        
        else:
            print("\n--- [PHASE 2] Skipping Data Quality Checks ---")

        # =====================================================================
        # 3. LOAD DIMENSIONS TABLES
        # =====================================================================
        print("\n--- [PHASE 3] Loading Dimension Tables ---")

        # Load Aircrafts Dimension
        aircraft_iterator = transform.get_aircrafts(aircraft_manuf_info)
        load.load_aircrafts(dw, aircraft_iterator)

        # Load Reporters Dimension 
        reporter_iterator = transform.get_reporters(reporters_src, maint_personnel_info)
        load.load_reporters(dw, reporter_iterator)

        # Load Dates Dimension 
        date_iterator = transform.generate_date_dimension_rows(
            flights_df, logbooks_df, maintenance_df
        )
        load.load_dates(dw, date_iterator)

        # Load Months Dimension
        month_iterator = transform.generate_month_dimension_rows(
            flights_df, logbooks_df, maintenance_df
        )
        load.load_months(dw, month_iterator)

        # =====================================================================
        # 4. LOAD FACT TABLES
        # =====================================================================
        if cleaning:
            print("\n--- [PHASE 4] Loading Fact Tables After Cleaning ---")
        else:
            print("\n--- [PHASE 4] Loading Fact Tables Without Cleaning ---")

        # Load Flight Operations Daily Fact Table
        fod_iterator = transform.get_flights_operations_daily(
            flights_df,  # Can be cleaned or uncleaned depending on cleaning flag
            dw.dates_dim,
            dw.aircrafts_dim
        )
        load.load_flights_operations_daily(dw, fod_iterator)

        # Load Aircraft Monthly Summary Fact Table
        ams_iterator = transform.get_aircrafts_monthly_snapshot(
            maintenance_df, 
            dw.months_dim,
            dw.aircrafts_dim
        )
        load.load_aircrafts_monthly_snapshot(dw, ams_iterator)

        # Load Logbooks Fact Table
        logbooks_iterator = transform.get_logbooks(
            logbooks_df,  # Can be cleaned or uncleaned depending on cleaning flag
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
        # 5. CLOSE: Asegurar que la conexión a la BD siempre se cierre
        # =====================================================================
        if dw:
            dw.close()
            print("\nData Warehouse connection closed.")