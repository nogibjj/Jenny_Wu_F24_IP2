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
    let file = File::open(file_path).expect("failed to open the file path");
    let mut rdr = ReaderBuilder::new().from_reader(file);

    let insert_query = format!(
        "INSERT INTO {} (incident_key,
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
            long_lat,) 
            VALUES 
            (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        table_name
    );
    //this is a loop that expects a specific schema, you will need to change this if you have a different schema
    for result in rdr.records() {
        let record = result.expect("failed to parse a record");
        let incident_key: i32 = record[0].trim().parse()?;
        let occur_date: String = record[1].trim().to_string();
        let occur_time: String = record[2].trim().to_string();
        let boro: String = record[3].trim().to_string();
        let loc_of_occur_desc: String = record[4].trim().to_string();
        let precinct: i32 = record[5].trim().parse().expect("failed to parse precinct");
        let jurisdiction_code: i32 = record[6]
            .trim()
            .parse()
            .expect("failed to parse jurisdiction code");
        let location_class_desc: String = record[7].trim().to_string();
        let loc_desc: String = record[8].trim().to_string();
        let stat_murder_flag: String = record[9].trim().to_string();
        let perp_age_group: String = record[10].trim().to_string();
        let perp_sex: String = record[11].trim().to_string();
        let perp_race: String = record[12].trim().to_string();
        let vicitm_age_group: String = record[13].trim().to_string();
        let victim_sex: String = record[14].trim().to_string();
        let victim_race: String = record[15].trim().to_string();
        let x_coord: i32 = record[16].trim().parse()?;
        let y_coord: i32 = record[17].trim().parse()?;
        let latitide_coord: f64 = record[18].trim().parse()?;
        let longitude_coord: f64 = record[19].trim().parse()?;
        let long_lat: f64 = record[20].trim().parse()?;
        println!(
            "incident_key: {}, Occur Date: {}, Occur Time: {}, Boro: {}, 
        Location of Occur: {}, Precinct: {}, Jurisdiction Code: {}, 
        Location Class Desc: {}, Location Desc: {}, Stat Murder Flag: {}, 
        Perp Age Group: {}, Perp Sex: {}, Perp Race: {}, Vicitm Age Group: {}, 
        Victim Sex: {}, Victim Race: {}, X Coord: {}, Y Coord: {}, 
        Latitide Coord: {}, Longitude Coord: {}, Long Lat: {}",
            incident_key,
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
            long_lat
        );

        conn.execute(
            &insert_query,
            params![
                incident_key,
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
                long_lat
            ],
        )
        .expect("failed to execute data into db table");
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
            occur_date TEXT,
            occur_time TEXT, 
            boro TEXT,
            loc_of_occur_desc TEXT, 
            precinct INTEGER,
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
            x_coord INTEGER,
            y_coord INTEGER,
            latitide_coord REAL,
            longitude_coord REAL,
            long_lat TEXT,
        )",
        table_name
    );
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
        let occur_time: String = row.get(2)?;
        let boro: String = row.get(3)?;
        let loc_of_occur_desc: String = row.get(4)?;
        let precinct: i32 = row.get(5)?;
        let jurisdiction_code: i32 = row.get(6)?;
        let location_class_desc: String = row.get(7)?;
        let loc_desc: String = row.get(8)?;
        let stat_murder_flag: bool = row.get(9)?;
        let perp_age_group: String = row.get(10)?;
        let perp_sex: String = row.get(11)?;
        let perp_race: String = row.get(12)?;
        let vicitm_age_group: String = row.get(13)?;
        let victim_sex: String = row.get(14)?;
        let victim_race: String = row.get(15)?;
        let x_coord: i32 = row.get(16)?;
        let y_coord: i32 = row.get(17)?;
        let latitide_coord: f64 = row.get(18)?;
        let longitude_coord: f64 = row.get(19)?;
        let long_lat: f64 = row.get(20)?;
        Ok((
            incident_key,
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
        let (
            incident_key,
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
        ) = row?;

        println!(
            "incident_key: {}, Occur Date: {}, Occur Time: {}, Boro: {}, 
        Location of Occur: {}, Precinct: {}, Jurisdiction Code: {}, 
        Location Class Desc: {}, Location Desc: {}, Stat Murder Flag: {}, 
        Perp Age Group: {}, Perp Sex: {}, Perp Race: {}, Vicitm Age Group: {}, 
        Victim Sex: {}, Victim Race: {}, X Coord: {}, Y Coord: {}, 
        Latitide Coord: {}, Longitude Coord: {}, Long Lat: {}",
            incident_key,
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
            long_lat
        );
    }

    Ok(())
}

// Update a specific column in a row based on the `Incident_Key`
pub fn query_update(
    conn: &Connection,
    table_name: &str,
    new_value_condition: &str,
    incident_key: i32,
) -> Result<()> {
    let query = format!(
        "UPDATE {} SET {} WHERE Incident_Key {}",
        table_name, new_value_condition, incident_key
    );
    // Execute the update query with `new_value` and `incident_key` as parameters
    conn.execute(&query, [])?;
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
