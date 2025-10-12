import os
import sys
import duckdb  # type: ignore
import pygrametl  # type: ignore
from pygrametl.tables import CachedDimension, SnowflakedDimension, FactTable # type: ignore


duckdb_filename = 'dw.duckdb'


class DW:
    def __init__(self, create=False):
        if create and os.path.exists(duckdb_filename):
            os.remove(duckdb_filename)
        try:
            self.conn_duckdb = duckdb.connect(duckdb_filename)
            print("Connection to the DW created successfully")
        except duckdb.Error as e:
            print(f"Unable to connect to DuckDB database '{duckdb_filename}':", e)
            sys.exit(1)

        if create:
            try:
                # TODO: Create the tables in the DW
                self.conn_duckdb.execute('''
                    CREATE TYPE Aircraft_Manufacturer AS ENUM ('Airbus', 'Boeing'); 

                    CREATE TABLE Aircraft (
                        Aircraft_ID                INT PRIMARY KEY, -- surrogate key
                        Aircraft_Registration_Code VARCHAR(10) NOT NULL UNIQUE,
                        Manufacturer_Serial_Number VARCHAR(20) NOT NULL,
                        Aircraft_Model             VARCHAR(50) NOT NULL,
                        Aircraft_Manufacturer_Class Aircraft_Manufacturer NOT NULL
                    );

                    CREATE TABLE Month (
                        Month_ID  INT PRIMARY KEY, -- surrogate key
                        Month_Num INT NOT NULL CHECK (Month_Num BETWEEN 1 AND 12),
                        Year      INT NOT NULL,
                        UNIQUE (Month_Num, Year)     
                    );

                    CREATE TABLE Day (
                        Day_ID   INT PRIMARY KEY, -- surrogate key
                        Day_Num  INT NOT NULL CHECK (Day_Num BETWEEN 1 AND 31),
                        Month_ID INT NOT NULL,
                        FOREIGN KEY (Month_ID) REFERENCES Month(Month_ID)
                    );

                    CREATE TYPE ReportKind AS ENUM ('PIREP', 'MAREP'); 

                    CREATE TABLE Reporter (
                        Reporter_ID         INT PRIMARY KEY, -- surrogate key
                        Reporter_Code       INT NOT NULL,
                        Report_Airport_Code CHAR(3),
                        Reporter_Class ReportKind NOT NULL,
                        UNIQUE (Reporter_Code)
                    );

                    -- ===========================
                    -- Fact tables
                    -- ===========================

                    CREATE TABLE Flight_operations_Daily (
                        Day_ID      INT NOT NULL,
                        Aircraft_ID INT NOT NULL,
                        FH          FLOAT NOT NULL CHECK (FH > 0),
                        Takeoffs    INT   NOT NULL CHECK (Takeoffs > 0),
                        OFC         INT   NOT NULL CHECK (OFC > 0),
                        CFC         INT   NOT NULL CHECK (CFC > 0),
                        TDM         INT   NOT NULL CHECK (TDM > 0),
                        PRIMARY KEY (Day_ID, Aircraft_ID),
                        FOREIGN KEY (Day_ID) REFERENCES Day(Day_ID),
                        FOREIGN KEY (Aircraft_ID) REFERENCES Aircraft(Aircraft_ID)
                    );

                    CREATE TABLE Aircraft_Monthly_Summary (
                        Month_ID    INT NOT NULL,
                        Aircraft_ID INT NOT NULL,
                        ADIS        INT NOT NULL CHECK (ADIS > 0),
                        ADOS        INT NOT NULL CHECK (ADOS > 0),
                        ADOSS       INT NOT NULL CHECK (ADOSS > 0),
                        ADOSU       INT NOT NULL CHECK (ADOSU > 0),
                        PRIMARY KEY (Month_ID, Aircraft_ID),
                        FOREIGN KEY (Month_ID) REFERENCES Month(Month_ID),
                        FOREIGN KEY (Aircraft_ID) REFERENCES Aircraft(Aircraft_ID)
                    );

                    CREATE TABLE Logbooks (
                        Month_ID    INT NOT NULL,
                        Aircraft_ID INT NOT NULL,
                        Reporter_ID INT NOT NULL,
                        Log_Count   INT NOT NULL CHECK (Log_Count > 0),
                        PRIMARY KEY (Month_ID, Aircraft_ID, Reporter_ID),
                        FOREIGN KEY (Month_ID) REFERENCES Month(Month_ID),
                        FOREIGN KEY (Aircraft_ID) REFERENCES Aircraft(Aircraft_ID),
                        FOREIGN KEY (Reporter_ID) REFERENCES Reporter(Reporter_ID)
                    );
                ''')
                print("DW tables created successfully")
            except duckdb.Error as e:
                print("Error creating the DW tables:", e)
                sys.exit(2)

        # Link DuckDB and pygrametl
        self.conn_pygrametl = pygrametl.ConnectionWrapper(self.conn_duckdb)

        # ======================================================================================================= Dimension and fact table objects
        # TODO: Declare the dimensions and facts for pygrametl
        
        # =======================================================================================================
        # Dimensions
        # =======================================================================================================

        self.aircraft_dim = CachedDimension(
            name='Aircraft',
            key='Aircraft_ID',
            attributes=[
                'Aircraft_Registration_Code',
                'Manufacturer_Serial_Number',
                'Aircraft_Model',
                'Aircraft_Manufacturer_Class'
            ],
            lookupatts=['Aircraft_Registration_Code']
        )

        self.month_dim = CachedDimension(
            name='Month',
            key='Month_ID',
            attributes=['Month_Num', 'Year'],
            lookupatts=['Month_Num', 'Year']
        )

        self.day_dim = CachedDimension(
            name='Day',
            key='Day_ID',
            attributes=['Day_Num', 'Month_ID'],
            lookupatts=['Day_Num', 'Month_ID']
        )
        
        self.date_dim = SnowflakedDimension(
           [(self.day_dim, self.month_dim)]
        )

        self.reporter_dim = CachedDimension(
            name='Reporter',
            key='Reporter_ID',
            attributes=['Reporter_Code', 'Report_Airport_Code', 'Reporter_Class'],
            lookupatts=['Reporter_Code']
        )

        # =======================================================================================================
        # Fact Tables
        # =======================================================================================================

        self.flight_fact = FactTable(
            name='Flight_operations_Daily',
            keyrefs=['Day_ID', 'Aircraft_ID'],
            measures=['FH', 'Takeoffs', 'OFC', 'CFC', 'TDM']
        )

        self.aircraft_monthly_fact = FactTable(
            name='Aircraft_Monthly_Summary',
            keyrefs=['Month_ID', 'Aircraft_ID'],
            measures=['ADIS', 'ADOS', 'ADOSS', 'ADOSU']
        )

        self.logbook_fact = FactTable(
            name='Logbooks',
            keyrefs=['Month_ID', 'Aircraft_ID', 'Reporter_ID'],
            measures=['Log_Count']
        )


    # TODO: Rewrite the queries exemplified in "extract.py"
    def query_utilization(self):
        result = self.conn_duckdb.execute("""
            SELECT ...
            """).fetchall()
        return result

    def query_reporting(self):
        result = self.conn_duckdb.execute("""
            SELECT ...
            """).fetchall()
        return result

    def query_reporting_per_role(self):
        result = self.conn_duckdb.execute("""
            SELECT ...
            """).fetchall()
        return result

    def close(self):
        self.conn_pygrametl.commit()
        self.conn_pygrametl.close()

# dw.py - Corrected proves() function
dates = ["2023-01-15", "2024-01-15"]

# dw.py - Corrected proves() function

def proves():
    # crea una instancia de DW y crea las tablas si no existen
    dw = DW(create=True)

    print("--- Inserting data using SnowflakedDimension ---")
    
    # Llenar las dimensiones de fecha
    for date_str in dates:
        # 1. Extract ALL natural keys for the entire snowflake
        year = int(date_str[:4])
        month_num = int(date_str[5:7])
        day_num = int(date_str[8:10])

        # 2. Combine all natural keys into a single row dictionary
        full_date_key_data = {
            # Keys for the Root dimension ('Day')
            'Day_Num': day_num, 
            # Keys for the Snowflaked dimension ('Month')
            'Month_Num': month_num,
            'Year': year
        }

        # 3. Use the SnowflakedDimension's ensure() method.
        # This single call handles insertion/lookup for BOTH Month and Day tables.
        # It ensures that the Day table row gets the correct foreign key (Month_ID).
        dw.date_dim.ensure(full_date_key_data)
        

    # --- Confirmation Queries ---
    print("\n--- Confirming Data in DuckDB ---")
    print("Month Table (Month_ID, Month_Num, Year):")
    # Expected output: (1, 1, 2023), (2, 1, 2024)
    print(dw.conn_duckdb.execute("SELECT * FROM Month ORDER BY Year, Month_Num").fetchall())
    print("\nDay Table (Day_ID, Day_Num, Month_ID):")
    # Expected output: (1, 15, 1), (2, 15, 2) where the last number is Month_ID
    print(dw.conn_duckdb.execute("SELECT * FROM Day ORDER BY Month_ID, Day_Num").fetchall())

    dw.close()

if __name__ == '__main__':
    # Ensure there is no trailing dot here
    proves()