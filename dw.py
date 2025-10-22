"""
Data Warehouse (DW) interface and schema definition

Exposes a single DW class that encapsulates:
- DuckDB database connection and schema DDL (dimensions and facts)
- pygrametl integration (ConnectionWrapper, CachedDimensions, FactTables)

The class aims to provide a simple surface for the ETL layers: callers can
instantiate DW(create=True) to reset and create the schema, or DW() to reuse
an existing database file.
"""

import os
import sys
import duckdb  # type: ignore
import pygrametl  # type: ignore
from pygrametl.tables import CachedDimension, FactTable # type: ignore


duckdb_filename = 'dw.duckdb'


class DW:
    """
    Data Warehouse facade wrapping DuckDB and pygrametl constructs.

    Parameters
    - create: When True, removes any existing DuckDB file and recreates schema.

    Attributes
    - conn_duckdb: Native DuckDB connection used for DDL/DML and bulk inserts.
    - conn_pygrametl: pygrametl ConnectionWrapper bound to conn_duckdb.
    - <*_dim>: CachedDimension instances for all dimension tables.
    - <*_fact>: FactTable instances for all fact tables.
    """
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
                self.conn_duckdb.execute('''
                    CREATE TYPE Aircraft_Manufacturer AS ENUM ('Airbus', 'Boeing'); 

                    CREATE TABLE Aircrafts (
                        Aircraft_ID                   INT PRIMARY KEY, -- surrogate key
                        Aircraft_Registration_Code    VARCHAR(10) NOT NULL UNIQUE,
                        Manufacturer_Serial_Number    VARCHAR(20) NOT NULL,
                        Aircraft_Model                VARCHAR(50) NOT NULL,
                        Aircraft_Manufacturer_Class   Aircraft_Manufacturer NOT NULL
                    );

                    CREATE TABLE Dates (
                        Date_ID             INT PRIMARY KEY, -- surrogate key 
                        Full_Date           DATE NOT NULL UNIQUE,  -- 'YYYY-MM-DD'
                        Day_Num             INT NOT NULL,
                        Month_Num           INT NOT NULL,
                        Year                INT NOT NULL,
                        UNIQUE (Day_Num, Month_Num, Year)
                    );

                    CREATE TABLE Months (
                        Month_ID   INT PRIMARY KEY, -- surrogate key
                        Month_Num  INT NOT NULL CHECK (Month_Num BETWEEN 1 AND 12),
                        Year       INT NOT NULL,
                        UNIQUE (Year, Month_Num)
                    );
                    
                    CREATE TYPE ReportKind AS ENUM ('PIREP', 'MAREP'); 

                    CREATE TABLE Reporters (
                        Reporter_ID           INT PRIMARY KEY, -- surrogate key
                        Reporter_Class        ReportKind NOT NULL,
                        Report_Airport_Code   CHAR(3),
                        UNIQUE (Reporter_Class, Report_Airport_Code)
                    );

                    -- ===========================
                    -- Fact tables
                    -- ===========================

                    CREATE TABLE Flight_operations_Daily (
                        Date_ID     INT NOT NULL, 
                        Aircraft_ID INT NOT NULL,
                        FH          FLOAT NOT NULL,
                        Takeoffs    INT   NOT NULL CHECK (Takeoffs >= 0),
                        DFC         INT   NOT NULL CHECK (DFC >= 0),
                        CFC         INT   NOT NULL CHECK (CFC >= 0),
                        TDM         FLOAT NOT NULL CHECK (TDM >= 0),
                        PRIMARY KEY (Date_ID, Aircraft_ID), 
                        FOREIGN KEY (Date_ID) REFERENCES Dates(Date_ID), 
                        FOREIGN KEY (Aircraft_ID) REFERENCES Aircrafts(Aircraft_ID)
                    );

                    CREATE TABLE Aircraft_Monthly_Summary (
                        Month_ID    INT NOT NULL,
                        Aircraft_ID INT NOT NULL,
                        ADIS        FLOAT NOT NULL CHECK (ADIS >= 0),
                        ADOSS       FLOAT NOT NULL CHECK (ADOSS >= 0),
                        ADOSU       FLOAT NOT NULL CHECK (ADOSU >= 0),
                        PRIMARY KEY (Month_ID, Aircraft_ID),
                        FOREIGN KEY (Month_ID) REFERENCES Months(Month_ID),
                        FOREIGN KEY (Aircraft_ID) REFERENCES Aircrafts(Aircraft_ID)
                    );

                    CREATE TABLE Logbooks (
                        Month_ID    INT NOT NULL,
                        Aircraft_ID INT NOT NULL,
                        Reporter_ID INT NOT NULL,
                        Log_Count   INT NOT NULL CHECK (Log_Count > 0),
                        PRIMARY KEY (Month_ID, Aircraft_ID, Reporter_ID),
                        FOREIGN KEY (Month_ID) REFERENCES Months(Month_ID),
                        FOREIGN KEY (Aircraft_ID) REFERENCES Aircrafts(Aircraft_ID),
                        FOREIGN KEY (Reporter_ID) REFERENCES Reporters(Reporter_ID)
                    );
                ''')
                print("DW tables created successfully")
            except duckdb.Error as e:
                print("Error creating the DW tables:", e)
                sys.exit(2)

    # Link DuckDB and pygrametl
        self.conn_pygrametl = pygrametl.ConnectionWrapper(self.conn_duckdb)

        # =======================================================================================================
        # Dimensions
        # =======================================================================================================

        self.aircrafts_dim = CachedDimension( 
            name='Aircrafts',
            key='Aircraft_ID',
            attributes=[
                'Aircraft_Registration_Code',
                'Manufacturer_Serial_Number',
                'Aircraft_Model',
                'Aircraft_Manufacturer_Class'
            ],
            lookupatts=['Aircraft_Registration_Code'], 
        )

        self.dates_dim = CachedDimension(
            name='Dates',
            key='Date_ID',
            attributes=[
                'Full_Date', 'Day_Num', 'Month_Num', 'Year'
            ],
            lookupatts=['Full_Date'],
        )

        self.months_dim = CachedDimension(
            name='Months',
            key='Month_ID',
            attributes=['Month_Num', 'Year'],
            lookupatts=['Month_Num', 'Year'],
        )
        
        self.reporters_dim = CachedDimension(
            name='Reporters',
            key='Reporter_ID',
            attributes=['Reporter_Class', 'Report_Airport_Code'],
            lookupatts=['Reporter_Class', 'Report_Airport_Code'], 
        )

        # =====================================================================
        # Fact Tables
        # =====================================================================
        
        # FactTables also rely on the pygrametl connection wrapper
        self.flight_fact = FactTable(
            name='Flight_operations_Daily',
            keyrefs=['Date_ID', 'Aircraft_ID'],
            measures=['FH', 'Takeoffs', 'DFC', 'CFC', 'TDM'], 
        )

        self.aircraft_monthly_fact = FactTable(
            name='Aircraft_Monthly_Summary',
            keyrefs=['Month_ID', 'Aircraft_ID'],
            measures=['ADIS', 'ADOSS', 'ADOSU'], 
        )

        self.logbook_fact = FactTable(
            name='Logbooks',
            keyrefs=['Month_ID', 'Aircraft_ID', 'Reporter_ID'],
            measures=['Log_Count'],
        )
        
    # Example query methods for analysis
    def query_utilization(self):
        """Placeholder: Example utilization query against the DW schema."""
        result = self.conn_duckdb.execute("""
            SELECT ...
            """).fetchall()
        return result

    def query_reporting(self):
        """Placeholder: Example reporting query against the DW schema."""
        result = self.conn_duckdb.execute("""
            SELECT ...
            """).fetchall()
        return result

    def query_reporting_per_role(self):
        """Placeholder: Example reporting per role query against the DW schema."""
        result = self.conn_duckdb.execute("""
            SELECT ...
            """).fetchall()
        return result

    def close(self):
        """
        Flush pending transactions and close both pygrametl and DuckDB connections.
        Safe to call multiple times.
        """
        self.conn_pygrametl.commit()
        self.conn_pygrametl.close()

