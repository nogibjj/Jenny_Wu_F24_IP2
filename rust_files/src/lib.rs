use csv::ReaderBuilder; //for loading from csv
use rusqlite::{params, Connection, Result};
use std::error::Error;
use std::fs::File; //for loading csv //for capturing errors from loading
                   // Here we will have a function for each of the commands

// Create a table
pub fn create_table(conn: &Connection, table_name: &str) -> Result<()> {
    let create_query = format!(
        "CREATE TABLE IF NOT EXISTS {}
                Incident_Key INTEGER,
                Occur_Date TEXT,
                Occur_Time TEXT, 
                Boro TEXT,
                Loc_of_occur_desc TEXT, 
                Precinct INTEGER,
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
                Latitide_Coord REAL,
                Longitude_Coord REAL,
                Long_Lat REAL
        )",
        table_name
    );
    conn.execute(&create_query, [])?;
    println!("Table '{}' created successfully.", table_name);
    Ok(()) //returns nothing except an error if it occurs
}


//Read
pub fn query_exec(conn: &Connection, query_string: &str) -> Result<()> {
    // Prepare the query and iterate over the rows returned

    let mut stmt = conn.prepare(query_string)?;

    // Use query_map to handle multiple rows
    let rows = stmt.query_map([], |row| {
        // Assuming the `users` table has an `id` and `name` column
        let indicent_key: i32 = row.get(0)?;
        let occur_date: str = row.get(1)?;
        let occur_time: str = row.get(2)?;
        let boro: str = row.get(3)?;
        let loc_of_occur_desc: str = row.get(4)?;
        let precinct: i32 = row.get(5)?;
        let jurisdiction_code: i32 = row.get(6)?;
        let location_class_desc: str = row.get(7)?;
        let loc_desc: str = row.get(8)?;
        let stat_murder_flag: bool = row.get(9)?;
        let perp_age_group: str = row.get(10)?;
        let perp_sex: str = row.get(11)?;
        let perp_race: str = row.get(12)?;
        let vicitm_age_group: str = row.get(13)?;
        let victim_sex: str = row.get(14)?;
        let victim_race: str = row.get(15)?;
        let x_coord: str = row.get(16)?;
        let y_coord: str = row.get(17)?;
        let latitide_coord: f64 = row.get(18)?;
        let longitude_coord: f64 = row.get(19)?;
        let long_lat: f64 = row.get(20)?;
        Ok((
            indicent_key,
            occur_date,
            occur_time,
            boro,
            loc_of_occur_desc,
            precinct,
            jurisdiction_code,
            location_class_desc,
            loc_desc,
            stat_murder_flag,
            perp_age_group,
            perp_sex,
            perp_race,
            vicitm_age_group,
            victim_sex,
            victim_race,
            x_coord,
            y_coord,
            latitide_coord,
            longitude_coord,
            long_lat,
        ))
    })?;

    // Iterate over the rows and print the results
    for row in rows {
        let (incident_key, occur_date, occur_time, boro, loc_of_occur_desc, 
            precinct, jurisdiction_code, location_class_desc, loc_desc, 
            stat_murder_flag, perp_age_group, perp_sex, perp_race, 
            vicitm_age_group, victim_sex, victim_race, x_coord, y_coord, 
            latitide_coord, longitude_coord, long_lat) =
            row?;
        println!("incident_key: {}, Occur Date: {}, Occur Time: {}, 
        Boro: {}, Location of Occur: {}, Precinct: {}, Jurisdiction Code: {}, 
        Location Class Desc: {}, Location Desc: {}, Stat Murder Flag: {}, Perp Age Group:{},    
        Perp Sex: {}, Perp Race: {}, Vicitm Age Group: {}, Victim Sex: {}, 
        Victim Race: {}, X Coord: {}, Y Coord: {}, Latitide Coord: {}, 
        Longitude Coord: {}, Long Lat: {}", incident_key, occur_date, occur_time, boro, 
        loc_of_occur_desc, precinct, jurisdiction_code, location_class_desc, loc_desc, 
        stat_murder_flag, perp_age_group, perp_sex, perp_race, vicitm_age_group, victim_sex, 
        victim_race, x_coord, y_coord, latitide_coord, longitude_coord, long_lat);
    }

    // Close the statement
    stmt.close();
    Ok(())
}

/// Updates a specific column in a row based on the `Incident_Key`.
pub fn query_update(database: &str, table: &str, column: &str, new_value: &str, incident_key: i32) -> Result<()> {
    let conn = Connection::open(database)?;
    // Construct the SQL query with the table and column names
    let query = format!("UPDATE {} SET {} = ? WHERE Incident_Key = ?", table, column);

    // Execute the update query with `new_value` and `incident_key` as parameters
    conn.execute(&query, params![new_value, incident_key])?;

    println!("Updated");
    Ok(())
}


/// Deletes a specific row based on the `Incident_Key` from the specified table in the database.
pub fn query_delete(database: &str, table: &str, incident_key: i32) -> Result<()> {
    let conn = Connection::open(database)?;
    // Create the query string
    let query = format!("DELETE FROM {} WHERE Incident_Key = ?", table);
    conn.execute(&query, params![incident_key])?;
    println!("Deleted");
    Ok(())
}




