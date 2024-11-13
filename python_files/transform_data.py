import sqlite3
import csv

# This file should take the cvs data and convert it into a database, or .db file
# performs the CREATE from CRUD operations


# Loads the CSV file and transforms it into a new SQLite3 database
def transform(
    dataset="data/nypd_shooting.csv",
    db_name="nypd_shooting.db",
    table_name="nypd_shooting",
):
    """Transforms and Loads data into the local SQLite3 database"""

    try:
        # Open the CSV file
        with open(dataset, newline="", encoding="ISO-8859-1") as csvfile:
            payload = csv.reader(csvfile, delimiter=",")

            # Connect to the SQLite database (or create it if it doesn't exist)
            conn = sqlite3.connect(db_name)
            c = conn.cursor()

            # Drop the table if it already exists, then create a new one
            c.execute(f"DROP TABLE IF EXISTS {table_name}")
            c.execute(
                f"""
            CREATE TABLE {table_name} (
                Incident_Key INTEGER,
                Occur_Date TEXT,
                Occur_Time TEXT, 
                Boro TEXT,
                Loc_of_occur_desc TEXT, 
                Precinct NUMBER,
                Jurisdiction_Code INTEGER,
                Location_Class_Desc TEXT,
                Loc_Desc TEXT,
                Stat_Murder_Flag BOOL,
                Perp_Age_Group TEXT,
                Perp_Sex TEXT,
                Perp_Race TEXT,
                Vicitm_Age_Group TEXT,
                Victim_Sex TEXT,
                Victim_Race TEXT,
                X_Coord TEXT,
                Y_Coord TEXT,
                Latitide_Coord FLOAT,
                Longitude_Coord FLOAT,
                Long_Lat FLOAT
            )
            """
            )

            # Skip the header
            next(payload)

            # Prepare and sanitize data before inserting
            sanitized_payload = [
                tuple(map(lambda x: x.strip() if isinstance(x, str) else x, row))
                for row in payload
            ]

            # Insert all rows into the table
            c.executemany(
                f"INSERT INTO {table_name} VALUES ("
                "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                sanitized_payload,
            )

            # Commit the changes and close the connection
            conn.commit()
            print(f"Data loaded successfully into {table_name} table.")
        c.close()
        conn.close()

    except Exception as e:
        print(f"An error occurred: {e}")

    return db_name


if __name__ == "__main__":
    transform()
