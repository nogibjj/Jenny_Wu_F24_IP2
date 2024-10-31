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
                incident_Key INTEGER,
                occur_date TEXT,
                occur_time TEXT, 
                boro TEXT,
                loc_of_occur_desc TEXT, 
                precinct NUMBER,
                jurisdiction_code INTEGER,
                location_class_desc TEXT,
                loc_desc TEXT,
                stat_murder_flag BOOL,
                perp_age_group TEXT,
                perp_sex TEXT,
                perp_race TEXT,
                vicitm_age_group TEXT,
                victim_sex TEXT,
                victim_race TEXT,
                x_coord TEXT,
                y_coord TEXT,
                latitide_coord FLOAT,
                longitude_coord FLOAT,
                long_lat FLOAT
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