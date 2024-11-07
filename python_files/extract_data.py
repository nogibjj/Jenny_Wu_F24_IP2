import requests
import pandas as pd

"""
Extract a dataset from a URL and place it into a database

NYPD Shooting dataset
"""


def extract(
    url,
    file_path,
):
    """ "Extract a url to a file path"""
    with requests.get(url) as r:
        with open(file_path, "wb") as f:
            f.write(r.content)
    return file_path


extract(
    "https://data.cityofnewyork.us/resource/833y-fsy8.csv",
    "data/nypd_shooting_base.csv",
)


def cut_csv(input_file, output_file, columns_to_keep):
    try:
        # Load the CSV file into a DataFrame
        df = pd.read_csv(input_file)

        # Select only the columns we want to keep
        df_filtered = df[columns_to_keep]

        # Save the filtered DataFrame to a new CSV file
        df_filtered.to_csv(output_file, index=False)
        print(
            f"CSV file saved successfully with columns {columns_to_keep} in {output_file}."
        )
    except KeyError as e:
        print(f"Error: One or more columns not found in the input file. {e}")
    except FileNotFoundError:
        print(f"Error: The file '{input_file}' was not found.")
    except Exception as e:
        print(f"An error occurred: {e}")


input_file = "python_files/data/nypd_shooting_base.csv"
output_file = "python_files/data/nypd_shooting.csv"
columns_to_keep = ["incident_key", "occur_date", "boro", "vic_sex", "vic_race"]

cut_csv(input_file, output_file, columns_to_keep)
