import requests
#import pandas as pd

"""
Extract a dataset from a URL and place it into a database

NYPD Shooting dataset
"""


def extract(
    url="https://data.cityofnewyork.us/api/views/833y-fsy8/rows.csv?accessType=DOWNLOAD",
    file_path="data/nypd_shooting.csv",
):
    """ "Extract a url to a file path"""
    with requests.get(url) as r:
        with open(file_path, "wb") as f:
            f.write(r.content)
    return file_path


extract()

# def cut_csv(input_file, output_file, columns_to_keep):
#      # Load the CSV file into a DataFrame
#     df = pd.read_csv(input_file)

#     # Select only the columns we want to keep
#     df_filtered = df[columns_to_keep]

#     # Save the filtered DataFrame to a new CSV file
#     df_filtered.to_csv(output_file, index=False)

# input_file = "data/nypd_shooting_base.csv"
# output_file = "data/nypd_shooting.csv"
# columns_to_keep_2 = ["incident_key", "occur_date", "boro", "vic_sex", "vic_race"]

# cut_csv(input_file, output_file, columns_to_keep_2)
