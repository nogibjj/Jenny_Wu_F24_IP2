//this will be the CLI portion of the project where we accept
//user defined arguments and call lib.rs logic to handle them
use clap::{Parser, Subcommand};
use rusqlite::{Connection, Result};
use rust_files::{create_table, load_data, query, query_delete, query_update}; //import library logic
use std::time::Instant;
// sys.info for timing tests
//use std::time::{Duration, Instant};
//Here we define a struct (or object) to hold our CLI arguments
//for #[STUFF HERE] syntax, these are called attributes. Dont worry about them
//for now, they define behavior for elements in rust.

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
//Think of a struct as a class which makes objects in python
//This is designed to generate an object out of the CLI inputs
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

//An enum is a type in rust that can have multiple exauhstive and mutually exclusive options
//We also know that the command can be 1 of 4 (really 3) options
//Create, Read and Update (query), Delete

#[derive(Debug, Subcommand)]
//By separating out the commands as enum types we can easily match what the user is
//trying to do in main
enum Commands {
    ///Pass a table name and a file path to load data from csv
    /// sqlite -l table_name file_path
    #[command(alias = "l", short_flag = 'l')]
    Load {
        table_name: String,
        file_path: String,
    },
    ///Pass a table name to create a table
    #[command(alias = "c", short_flag = 'c')]
    Create { table_name: String },
    ///Pass a query string to execute Read or Update operations
    #[command(alias = "q", short_flag = 'q')]
    Query { query_string: String },
    ///Pass a table name, a set clause, and a condition to update a row in the table
    /// sqlite -u table_name set_clause condition
    #[command(alias = "u", short_flag = 'u')]
    Update {
        table_name: String,
        column_name: String,
        new_value: String,
        incident_key: i32,
    },
    ///Pass a table name to drop
    #[command(alias = "d", short_flag = 'd')]
    Delete {
        table_name: String,
        incident_key: i32,
    },
    /// Creating a speed test
    #[command(alias = "t", short_flag = 't')]
    SpeedTest { query_string: String },
}

fn main() -> Result<()> {
    //Here we parse the CLI arguments and store them in the args object
    let args = Cli::parse();
    //generate connection
    let conn = Connection::open("nypd_shooting.db")?;

    //Here we can match the behavior on the subcommand and call our lib logic
    match args.command {
        Commands::Load {
            table_name,
            file_path,
        } => {
            println!(
                "Loading data into table '{}' from '{}'",
                table_name, file_path
            );
            load_data(&conn, &table_name, &file_path).expect("Failed to load data from csv");
        }
        Commands::Create { table_name } => {
            println!("Creating Table {}", table_name);
            create_table(&conn, &table_name).expect("Failed to create table");
        }
        Commands::Query { query_string } => {
            println!("Query: {}", query_string);
            query(&conn, &query_string).expect("Failed to execute query");
        }
        Commands::Update {
            table_name,
            new_value,
            incident_key,
            column_name,
        } => {
            println!(
                "Updating table '{}' with '{}' where {}",
                table_name, new_value, incident_key
            );
            query_update(&conn, &table_name, &column_name, &new_value, &incident_key)
                .expect("Failed to update table");
        }
        Commands::Delete {
            table_name,
            incident_key,
        } => {
            println!("Deleting {} from {} ", incident_key, table_name);
            query_delete(&conn, &table_name, &incident_key).expect("Failed to drop incident");
        }
        Commands::SpeedTest { query_string } => {
            println!("Starting Rust speed test...");

            // Measure the time for get_mean
            let start = Instant::now();

            let _ = query(&conn, &query_string);

            let duration = start.elapsed();

            println!(
                "Rust took: {} microseconds to complete the load and save operation.",
                duration.as_micros()
            )
        }
    }
    Ok(())
}
