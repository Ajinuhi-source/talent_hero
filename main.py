#! /usr/bin/env python
from collections import Counter
import datetime
import numpy as np
import subprocess
import glob
import os
import time
import pandas as pd
from functools import reduce
import tldextract


def rename_csv_files(folder_path):
    for root, dirs, files in os.walk(folder_path):
        for file in files:
            if file.endswith(".csv"):
                file_path = os.path.join(root, file)
                df = pd.read_csv(file_path, sep="\t", encoding="utf-16")
                url_col = df["URL"].astype(str)  # Convert the 'URL' column to strings
                for url in url_col:
                    if "http" in url:
                        url = tldextract.extract(url).domain
                        new_file_name = url + ".csv"
                        # create a new column named 'project' and set all the values to url
                        df["project"] = url
                        df.to_csv(file_path, index=False, sep="\t", encoding="utf-16")
                        break  # Only need the first matching URL
                else:
                    continue  # No matching URL found, skip renaming
                new_file_path = os.path.join(root, new_file_name)
                if file_path != new_file_path and not os.path.exists(new_file_path):
                    os.rename(file_path, new_file_path)
                    file_path = (
                        new_file_path  # Update the file path for the second loop below
                    )

                # Append folder name to the end of the filename
                folder_name = os.path.basename(root)
                file_name = os.path.basename(file_path)
                new_file_name = (
                    file_name.replace(".csv", "") + "_" + folder_name + ".csv"
                )
                new_file_path = os.path.join(root, new_file_name)
                if file_path != new_file_path and not os.path.exists(new_file_path):
                    os.rename(file_path, new_file_path)


def add_folder_name_to_csv(folder_path):
    for root, dirs, files in os.walk(folder_path):
        for file in files:
            if file.endswith(".csv"):
                file_path = os.path.join(root, file)
                df = pd.read_csv(file_path, sep="\t", encoding="utf-16")
                folder_name = os.path.basename(root)
                df["date_scraped"] = folder_name
                df.to_csv(file_path, index=False, sep="\t", encoding="utf-16")


def process_csv_files(folder_path):
    for file in glob.glob(os.path.join(folder_path, "**/*.csv"), recursive=True):
        df = pd.read_csv(file, sep="\t", encoding="utf-16")
        # try rename colum "position" to "rank"
        try:
            df = df.rename(columns={"Position": "Rank"})
        except:
            pass

        df = df[
            [
                "Keyword",
                "Rank",
                "URL",
                "Location",
                "Volume",
                "Tags",
                "date_scraped",
                "project",
            ]
        ]
        # sort alphabetically by keyword column 'tags' then 'keyword' then 'location'
        df = df.sort_values(by=["Tags", "Keyword", "Location"])
        # overwrite the original file with the sorted data
        df.to_csv(file, index=False, sep="\t", encoding="utf-16")


def extract_project_names(folder_path):
    project_names = []
    for file in glob.glob(os.path.join(folder_path, "**/*.csv"), recursive=True):
        df = pd.read_csv(file, sep="\t", encoding="utf-16")
        project_names.append(df["project"][0])
    # remove duplicates
    project_names = list(set(project_names))

    # sort alphabetically
    project_names = project_names.sort()
    return project_names


def merge_csv_files(folder, search_string):
    # Find all files in the folder and subfolders that contain .csv files
    files = glob.glob(os.path.join(folder, "**/*.csv"), recursive=True)
    # Merge all the CSV files into one dataframe
    df_list = []
    for file in files:
        df = pd.read_csv(file, sep="\t", encoding="utf-16")
        df_list.append(df)
    merged_df = pd.concat(df_list, ignore_index=True)

    # Filter the dataframe to only include rows that column "project" contains the search string
    merged_df = merged_df[merged_df["project"].str.contains(search_string, na=False)]

    # remove duplicates rows
    merged_df = merged_df.drop_duplicates()

    return merged_df


def calculate_days_between_dates(dates, max_diff):
    # Convert the list of dates to numpy.datetime64 objects
    dates = np.array(dates, dtype="datetime64[s]")

    # Convert numpy.datetime64 objects to datetime.datetime objects
    dates = [np.datetime64(date).astype(datetime.datetime) for date in dates]

    # Initialize variables
    num_days = []
    i = 0

    while i < len(dates) - 1:
        # Calculate the number of days between consecutive dates
        diff = (dates[i] - dates[i + 1]).days

        # If the difference is less than the maximum allowed difference, remove the second date and recalculate
        if diff < max_diff:
            dates = np.delete(dates, i + 1)
        else:
            num_days.append(diff)
            i += 1

    # convert datetime.datetime objects to a list of dates
    dates = [date.strftime("%Y-%m-%d") for date in dates]
    # Return the list of remaining dates
    return dates


def important_dates_filter(project_df, max_dates):
    # extract the unique "date_scraped" values
    unique_dates = project_df["date_scraped"].unique()

    #  extract the unique "date_scraped" values
    unique_dates = calculate_days_between_dates(unique_dates, 10)

    # only keep the first 3 elements in the list, if there are more than 3 elements
    if len(unique_dates) > max_dates:
        unique_dates = unique_dates[:max_dates]

    # filter the dataframe to only in column date_scraped valuest from unique_dates
    project_df = project_df[project_df["date_scraped"].isin(unique_dates)]

    # sort the dataframe by "scrapped_date" from newest to oldest
    project_df = project_df.sort_values("date_scraped", ascending=False)

    return project_df


def pivot_rank_tracker(df):
    # Convert date_scraped to datetime and format it
    df["date_scraped"] = pd.to_datetime(df.date_scraped).dt.strftime("%Y-%m-%d")

    # Pivot the dataframe on the date_scraped column and reset the index
    df = df[["Keyword", "date_scraped", "Location", "Rank", "URL", "Volume", "Tags"]]
    df = df.pivot(
        index=["Keyword", "Location", "URL", "Volume", "Tags"],
        columns="date_scraped",
        values="Rank",
    )
    df = df.reset_index()
    df.columns.name = None

    # Rename columns that start with 'Rank'
    df.columns = [
        "{}_{}".format("Rank", col) if "-" in col else col for col in df.columns
    ]

    # Rearrange the columns so that the 'Rank' columns come after the first column
    col_filtered_final = [col for col in df.columns if not "Rank" in col]
    col_rank = [col for col in df.columns if "Rank" in col]

    col_rank.reverse()
    col_filtered_final[1:1] = col_rank
    df = df[col_filtered_final]

    return df


# Define the path to the folder containing the csv files
folder_path = "data/ahrefs/rank_tracker"
# # rename to human readable names
rename_csv_files(folder_path)
# # add date column
add_folder_name_to_csv(folder_path)
# # remove unnecessary columns
process_csv_files(folder_path)

# for all csv files in folder_path and subfolders, extract all values from column "project" and save them to a list
project_list = extract_project_names(folder_path)

# show all columns  when printing
pd.set_option("display.max_columns", None)

# enumerate through the list of projects
for project in project_list:
    project_df = merge_csv_files(folder_path, project)

    project_df = important_dates_filter(project_df, 3)

    # pivot by rank
    project_df = pivot_rank_tracker(project_df)

    # save project_df to csv
    project_df.to_csv(f"data/ahrefs/{project}.csv", index=False)
