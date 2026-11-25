import duckdb
from pathlib import Path

DB_PATH = Path("/data/warehouse/warehouse.duckdb")


def main():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    conn = duckdb.connect(str(DB_PATH))

    conn.execute("""
        CREATE TABLE IF NOT EXISTS Dim_Measure (
            Measure_ID INTEGER PRIMARY KEY,
            Measure_Code VARCHAR,
            Measure_Name VARCHAR,
            Source VARCHAR,
            Min_Value DOUBLE,
            Max_Value DOUBLE,
            Better_When VARCHAR,
            Is_Overall BOOLEAN
        );
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS Dim_Level (
            Level_ID INTEGER PRIMARY KEY,
            Scale VARCHAR,
            Level_Value INTEGER,
            Level_Text VARCHAR,
            Is_Rated BOOLEAN
        );
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS Dim_Date (
            Date_ID INTEGER PRIMARY KEY,
            Date DATE,
            Year INTEGER,
            Quarter INTEGER,
            Month INTEGER,
            Day_Of_Week VARCHAR,
            Is_Holiday BOOLEAN
        );
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS Dim_Geography (
            Geo_ID INTEGER PRIMARY KEY,
            Postcode_Sector VARCHAR,
            Local_Authority_Code VARCHAR,
            Region VARCHAR
        );
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS Dim_Institution (
            Institution_ID INTEGER PRIMARY KEY,
            Source_ID VARCHAR,
            Name VARCHAR,
            Type VARCHAR,
            Business_Type VARCHAR,
            Postcode VARCHAR,
            Local_Authority VARCHAR,
            Latitude DOUBLE,
            Longitude DOUBLE,
            Status VARCHAR,
            Valid_From DATE,
            Valid_To DATE,
            Is_Current BOOLEAN
        );
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS Fact_Inspection_Measure (
            Inspection_ID VARCHAR NOT NULL,
            Institution_ID INTEGER NOT NULL,
            Measure_ID INTEGER NOT NULL,
            Level_ID INTEGER NOT NULL,
            Date_ID INTEGER NOT NULL,
            Geo_ID INTEGER NOT NULL,
            Raw_Score DOUBLE,
            Normalised_Score DOUBLE,

            FOREIGN KEY (Institution_ID) REFERENCES Dim_Institution(Institution_ID),
            FOREIGN KEY (Measure_ID) REFERENCES Dim_Measure(Measure_ID),
            FOREIGN KEY (Level_ID) REFERENCES Dim_Level(Level_ID),
            FOREIGN KEY (Date_ID) REFERENCES Dim_Date(Date_ID),
            FOREIGN KEY (Geo_ID) REFERENCES Dim_Geography(Geo_ID)
        );
    """)
    

    conn.close()

    print("Warehouse database initialized successfully.")


if __name__ == "__main__":
    main()