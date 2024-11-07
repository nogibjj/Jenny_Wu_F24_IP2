use csv::ReaderBuilder; // For loading from CSV
use rusqlite::{params, Connection, Result};
use std::error::Error;
use std::fs::File; // For loading CSV and capturing errors

//load data from a file path to a table
pub fn load_data(
    conn: &Connection,
    table_name: &str,
    file_path: &str,
) -> Result<(), Box<dyn Error>> {
    let file = File::open(file_path)?;
    let mut rdr = ReaderBuilder::new().from_reader(file);

    let insert_query = format!(
        "INSERT INTO {} (incident_key, occur_date, boro, vic_sex, vic_race) VALUES (?, ?, ?, ?, ?)",
        table_name
    );

    for result in rdr.records() {
        let record = result?;
        let incident_key: i32 = record[0].trim().parse()?;
        let occur_date: String = record[1].trim().to_string();
        let boro: String = record[2].trim().to_string();
        let vic_sex: String = record[3].trim().to_string();
        let vic_race: String = record[4].trim().to_string();

        println!(
            "incident_key: {}, Occur Date: {}, Boro: {}, Victim Sex: {}, Victim Race: {}",
            incident_key, occur_date, boro, vic_sex, vic_race,
        );

        conn.execute(
            &insert_query,
            params![incident_key, occur_date, boro, vic_sex, vic_race],
        )?;
    }

    println!(
        "Data loaded successfully from '{}' into table '{}'.",
        file_path, table_name
    );
    Ok(())
}

// Create a table
pub fn create_table(conn: &Connection, table_name: &str) -> Result<()> {
    let create_query = format!(
        "CREATE TABLE IF NOT EXISTS {} (
            incident_key INTEGER,
            occur_date STRING,
            boro TEXT,
            vic_sex TEXT,
            vic_race TEXT
        )",
        table_name
    );
    println!("{:?}", create_query);
    conn.execute(&create_query, [])?;
    println!("Table '{}' created successfully.", table_name);
    Ok(())
}

// Read and query the data from the table
pub fn query(conn: &Connection, query_string: &str) -> Result<()> {
    // Prepare the query and iterate over the rows returned
    let mut stmt = conn.prepare(query_string)?;

    // Use query_map to handle multiple rows
    let rows = stmt.query_map([], |row| {
        // Retrieve each column's data
        let incident_key: i32 = row.get(0)?;
        let occur_date: String = row.get(1)?;
        let boro: String = row.get(2)?;
        let vic_sex: String = row.get(3)?;
        let vic_race: String = row.get(4)?;
        Ok((incident_key, occur_date, boro, vic_sex, vic_race))
    })?;

    // Iterate over the rows and print the results
    for row in rows {
        let (incident_key, occur_date, boro, vic_sex, vic_race) = row?;

        println!(
            "incident_key: {}, Occur Date: {}, Boro: {}, Victim Sex: {}, Victim Race: {}",
            incident_key, occur_date, boro, vic_sex, vic_race,
        );
    }

    Ok(())
}

// Update a specific column in a row based on the `Incident_Key`
pub fn query_update(
    conn: &Connection,
    table_name: &str,
    column_name: &str,
    new_value: &str,
    incident_key: &i32,
) -> Result<(), rusqlite::Error> {
    // Prepare the SQL update statement with placeholders
    let query = format!(
        "UPDATE {} SET {} = ? WHERE incident_key = ?",
        table_name, column_name
    );

    // Execute the update query with `new_value` and `incident_key` as parameters
    conn.execute(&query, params![new_value, incident_key])?;
    println!("Updated successfully.");
    Ok(())
}

// Delete a specific row based on the `Incident_Key` from the specified table
pub fn query_delete(conn: &Connection, table_name: &str, incident_key: &i32) -> Result<()> {
    let query = format!(
        "DELETE FROM {} WHERE Incident_Key = {}",
        table_name, incident_key
    );
    conn.execute(&query, [])?;
    println!("Deleted successfully.");
    Ok(())
}
