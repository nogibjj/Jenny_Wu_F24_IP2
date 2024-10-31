import sqlite3

"""This file sets all of the query functions."""


def query_create(database, table, colnames, values):
    """Creates an entry into the specified table"""
    conn = sqlite3.connect(database)
    cursor = conn.cursor()
    cursor.execute(f"INSERT INTO {table} ({colnames}) VALUES ({values})")
    conn.commit()
    cursor.close()
    conn.close()
    return "Success"


def query_read(database, table):
    """Query the database to read the top 5 rows of the specified table"""
    conn = sqlite3.connect(database)
    cursor = conn.cursor()
    cursor.execute(f"SELECT * FROM {table} LIMIT 5")
    print(f"Top 5 rows of the {table} table:")
    rows = cursor.fetchall()
    for row in rows:
        print(row)
    cursor.close()
    conn.close()
    return rows


def query_update(database, table, column, new_value, Incident_Key):
    """Update a specific column in a row based on incident key"""
    conn = sqlite3.connect(database)
    cursor = conn.cursor()
    query = f"UPDATE {table} SET {column} = ? WHERE Incident_Key= ?"
    cursor.execute(query, (new_value, Incident_Key))
    print("Updated")
    conn.commit()
    cursor.close()
    conn.close()


def query_delete(database, table, Incident_Key):
    """Deletes a specific column in a row based on incident key"""
    conn = sqlite3.connect(database)
    cursor = conn.cursor()
    query = f"DELETE FROM {table} WHERE Incident_Key= ?"
    cursor.execute(query, (Incident_Key,))
    print("Deleted")
    conn.commit()
    cursor.close()
    conn.close()


def query_1(database, table):
    """Queries the db for 10 Incidents Against a F/BLK Victim
    in the Streets of the Bronx"""
    conn = sqlite3.connect(database)
    cursor = conn.cursor()

    # Corrected SQL query
    query = f"""
        SELECT Incident_Key 
        FROM {table} 
        WHERE Boro = 'BRONX' 
        AND Location_Class_Desc = 'STREET' 
        AND Victim_Sex = 'F' 
        AND Victim_Race = 'BLACK' 
        LIMIT 10
    """

    cursor.execute(query)
    print("10 Incidents Against a F/BLK Victim in the Streets of the Bronx:")
    query_1_result = cursor.fetchall()
    print(query_1_result)
    cursor.close()
    conn.close()
    return "Query is complete!"


def query_2(database, table):
    """Queries the db for all incidences on 4/19/2008"""
    conn = sqlite3.connect(database)
    cursor = conn.cursor()

    # Corrected SQL query
    query = f"""
        SELECT Incident_Key, Boro, Perp_Sex, Perp_Race, Victim_Sex, Victim_Race
        FROM {table} 
        WHERE Occur_Date= '04/19/2008' 
    """
    cursor.execute(query)
    print("Incidents on 04/19/2008")
    query_2_result = cursor.fetchall()
    print(query_2_result)
    cursor.close()
    conn.close()
    return "Query is complete!"