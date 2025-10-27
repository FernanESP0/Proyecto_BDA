"""
ETL Orchestration Script

This script coordinates the end-to-end ETL pipeline:

1) Initialization: creates the DuckDB data warehouse schema (optionally resetting
    the database file) and establishes connections.
2) Extraction: pulls source data from PostgreSQL and CSV files into in-memory
    structures (DataFrames and pygrametl data sources).
3) Optional data quality checks: applies business-rule-based validation and
    cleaning on the extracted data using vectorized pandas operations.
4) Load: populates dimension tables first, then bulk-loads fact tables.
5) Finalization: safely closes DW connections regardless of success or failure.

Toggling data cleaning
- Set the "cleaning" flag below to True to enable BR-based cleaning; otherwise
  the pipeline loads the raw (baseline) datasets.
"""

from dw import DW
import extract as extract
import transform as transform
import load as load
import warnings

# Suppress a specific pandas/DBAPI warning about non-SQLAlchemy connections
warnings.filterwarnings(
    "ignore",
    message="pandas only supports SQLAlchemy connectable.*",
    category=UserWarning
)

if __name__ == '__main__':
    dw = None
    try:
        # =====================================================================
        # 1. INITIALIZATION: Create DW and extract data into memory
        # =====================================================================
        
        print("--- [PHASE 1] Initializing DW and Extracting ---")
        dw = DW(create=True)
        
        # --- User Prompt for Data Cleaning ---
        user_response = input("Do you want to apply cleaning of data? (yes/no): ")
        cleaning = user_response.lower().strip().startswith('y')

        if cleaning:
            print("User selected: YES. Data cleaning will be applied.")
        else:
            print("User selected: NO. Skipping data cleaning.")

        print("Extracting data from CSV sources...")
        aircraft_manuf_info = extract.get_aircraft_manufacturer_info()
        aircraft_manuf_info_cp = extract.get_aircraft_manufacturer_info()  # Copy for cleaning as it is an iterator
        maint_personnel_info = extract.get_maintenance_personnel()
        print("CSV sources extracted.")

        print("Extracting data from PostgreSQL sources...")
        flights_df = extract.get_flights_df()
        maintenance_df = extract.get_maintenance_df()
        postflightreports_df = extract.get_postflightreports_df()
        print("PostgreSQL sources extracted.")

        # =====================================================================
        # 2. DATA QUALITY CHECKS AND CLEANING (Pandas)
        # =====================================================================
        
        if cleaning:
            print("\n--- [PHASE 2] Performing Data Quality Checks ---")
            try:
                # Apply Business Rules (BR) cleaning functions
                print("Applying Data Quality Checks and Cleaning...")
                flights_df = transform.check_and_fix_1st_BR(flights_df)
                flights_df = transform.check_and_fix_2nd_BR(flights_df)
                postflightreports_df = transform.check_and_fix_3rd_BR(postflightreports_df, aircraft_manuf_info_cp)

            except Exception as e:
                print(f"Error during data cleaning: {e}. Check 'cleaning.log'.")
                raise  # Stop ETL if cleaning fails
            
            print("Data quality checks completed.")
        
        else:
            print("\n--- [PHASE 2] Skipping Data Quality Checks ---")

        # =====================================================================
        # 3. LOAD DIMENSION TABLES
        # =====================================================================
        
        print("\n--- [PHASE 3] Loading Dimension Tables ---")

        # Load Aircrafts Dimension
        aircraft_iterator = transform.get_aircrafts(aircraft_manuf_info, postflightreports_df)
        load.load_aircrafts(dw, aircraft_iterator)

        # Load Dates Dimension 
        date_iterator = transform.generate_date_dimension_rows(flights_df)
        load.load_dates(dw, date_iterator)

        # Load Months Dimension
        month_iterator = transform.generate_month_dimension_rows(postflightreports_df, maintenance_df)
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
            postflightreports_df,
            maint_personnel_info,
            dw.months_dim,
            dw.aircrafts_dim,
        )
        load.load_logbooks(dw, logbooks_iterator)

        print("\nETL process completed successfully! ✅")

    except Exception as e:
        print(f"\nAn error occurred during the ETL process: {e}")

    finally:
        # =====================================================================
        # 5. CLOSE: Ensure the DW connection is always closed
        # =====================================================================
        
        if dw:
            dw.close()
            print("\nData Warehouse connection closed.")