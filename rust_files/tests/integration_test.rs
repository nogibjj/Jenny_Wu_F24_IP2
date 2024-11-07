// tests/integration_test.rs
use rusqlite::Connection;
use std::io::Write;
use tempfile::NamedTempFile;

// Import functions from your main library
use rust_files::{create_table, load_data, query, query_delete, query_update};

fn setup_test_db() -> Connection {
    // Create an in-memory database for testing purposes
    Connection::open_in_memory().expect("Failed to open a test database")
}

#[test]
fn test_create_table() {
    let conn = setup_test_db();
    let table_name = "test_table";

    // Test creating a table
    assert!(
        create_table(&conn, table_name).is_ok(),
        "Failed to create table"
    );
}

#[test]
fn test_load_data() {
    let conn = setup_test_db();
    let table_name = "test_table";

    // First, create the table to load data into
    create_table(&conn, table_name).expect("Failed to create table for loading data");

    // Create a temporary CSV file with test data
    let mut temp_csv = NamedTempFile::new().expect("Failed to create a temporary file");
    writeln!(
        temp_csv,
        "1,2023-01-01,Brooklyn,M,Black\n2,2023-02-01,Queens,F,White"
    )
    .expect("Failed to write test data to CSV");

    // Test loading data from the temporary CSV
    let file_path = temp_csv.path().to_str().unwrap();
    assert!(
        load_data(&conn, table_name, file_path).is_ok(),
        "Failed to load data from CSV"
    );
}

#[test]
fn test_query() {
    let conn = setup_test_db();
    let table_name = "test_table";

    // Create the table and insert test data
    create_table(&conn, table_name).expect("Failed to create table");
    conn.execute(
        &format!("INSERT INTO {} (incident_key, occur_date, boro, vic_sex, vic_race) VALUES (1, '2023-01-01', 'Brooklyn', 'M', 'Black')", table_name),
        [],
    ).expect("Failed to insert test data");

    // Run a test query and check for successful execution
    let query_string = format!(
        "SELECT incident_key, occur_date, boro, vic_sex, vic_race FROM {}",
        table_name
    );
    assert!(
        query(&conn, &query_string).is_ok(),
        "Failed to execute query"
    );
}

#[test]
fn test_update() {
    let conn = setup_test_db();
    let table_name = "test_table";

    // Create table
    create_table(&conn, table_name).expect("Failed to create table");

    // Insert initial data into the table
    conn.execute(
        &format!(
            "INSERT INTO {} (incident_key, occur_date, boro, vic_sex, vic_race) VALUES (?, ?, ?, ?, ?)",
            table_name
        ),
        rusqlite::params![1, "2023-01-01", "Brooklyn", "M", "Black"],
    )
    .expect("Failed to insert initial data");

    // Assert initial data is correct
    let mut stmt = conn
        .prepare(&format!(
            "SELECT vic_race FROM {} WHERE incident_key = 1",
            table_name
        ))
        .expect("Failed to prepare select statement");

    let initial_race: String = stmt
        .query_row([], |row| row.get(0))
        .expect("Failed to query initial row");

    assert_eq!(initial_race, "Black", "Initial value should be 'Black'");

    // Update the 'vic_race' column where incident_key = 1
    query_update(&conn, table_name, "vic_race", "White", &1).expect("Failed to update table");

    // Verify that the update was applied
    let mut stmt = conn
        .prepare(&format!(
            "SELECT vic_race FROM {} WHERE incident_key = 1",
            table_name
        ))
        .expect("Failed to prepare select statement");

    let updated_race: String = stmt
        .query_row([], |row| row.get(0))
        .expect("Failed to query updated row");

    assert_eq!(updated_race, "White", "Update was not applied correctly");
}

#[test]
fn test_query_delete() {
    let conn = setup_test_db();
    let table_name = "test_table";

    // Set up table and initial data
    create_table(&conn, table_name).expect("Failed to create table");
    conn.execute(
        &format!("INSERT INTO {} (incident_key, occur_date, boro, vic_sex, vic_race) VALUES (1, '2023-01-01', 'Brooklyn', 'M', 'Black')", table_name),
        [],
    ).expect("Failed to insert test data");

    // Delete test row
    let incident_key = 1;
    assert!(
        query_delete(&conn, table_name, &incident_key).is_ok(),
        "Failed to delete data"
    );

    // Verify deletion
    let mut stmt = conn
        .prepare(&format!(
            "SELECT COUNT(*) FROM {} WHERE incident_key = 1",
            table_name
        ))
        .expect("Failed to prepare select statement");
    let count: i32 = stmt
        .query_row([], |row| row.get(0))
        .expect("Failed to query row count");
    assert_eq!(count, 0, "Delete was not applied correctly");
}
