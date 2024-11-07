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
    """Queries the db for all incidences on 2023-12-29"""
    conn = sqlite3.connect(database)
    cursor = conn.cursor()

    # Corrected SQL query
    query = f"""
        SELECT *
        FROM {table} 
        WHERE Occur_Date= '2023-12-29T00:00:00.000' 
    """
    cursor.execute(query)
    print("Incidents on 2023-12-29")
    query_1_result = cursor.fetchall()
    print(query_1_result)
    cursor.close()
    conn.close()
    return "Query is complete!"
