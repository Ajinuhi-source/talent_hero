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


def rename_csv_files(id):
    date = time.strftime("%Y-%m-%d")

    # Step 1: Rename file to lower case and replace " " with "_"
    for file in glob.glob("*.csv"):
        os.rename(file, file.lower().replace(" ", "_"))

    # Step 2: Rename file to include the date in YYYY-MM-DD format and an id
    for file in glob.glob("*.csv"):
        os.rename(file, file.replace(".csv", "-" + time.strftime("%Y-%m-%d") + "_" +id + ".csv"))

    # Step 3: Add a colum named "date" using pandas and the date in YYYY-MM-DD format
    for file in glob.glob("*.csv"):
        df = pd.read_csv(file,sep='\t', encoding='utf-16')
        df['date_scraped'] = time.strftime("%Y-%m-%d")
        df.to_csv(file, index=False, sep='\t', encoding='utf-16')

    # Step 4: Remove all columns except for the following: Keyword, Position, URL, Location, Volume, Tags, date_scraped
    for file in glob.glob("*.csv"):
        df = pd.read_csv(file,sep='\t', encoding='utf-16')
        df = df[['Keyword','Position', 'URL', 'Location', 'Volume', 'Tags', 'date_scraped']]
        # rename colum 'position' to 'Rank + Date'
        df = df.rename(columns={'Position': 'Rank '+ date})
        # sort alphabetically by keyword column 'tags' then 'keyword' then 'location'
        df = df.sort_values(by=['Tags', 'Keyword', 'Location'])
        df.to_csv(file, index=False, sep='\t', encoding='utf-16')


    # Step 5: If folder does not exist, create it
    if not os.path.exists("data/ahrefs/rank_tracker"):
        os.makedirs("data/ahrefs/rank_tracker")

    # Step 6: Find the first csv file in the current folder and move it to the folder "data/ahrefs/rank_tracker"
    for file in glob.glob("*.csv"):
        os.rename(file, os.path.join("data/ahrefs/rank_tracker", file))
        break

    # Step 7: Return file path and name
    for file in glob.glob("*.csv"):
        return os.path.join("data/ahrefs/rank_tracker", file)


def launch_ahrefs_rank_tracker_scraper(ahrefs_email, ahrefs_password, ahrefs_rank_tracker_url):
    #cleanup python delete all .csv files in current folder
    for file in glob.glob("*.csv"):
        os.remove(file)
    _bash_command = f'node main.js "{ahrefs_email}" "{ahrefs_password}" "{ahrefs_rank_tracker_url}"'
    subprocess.call(_bash_command, shell=True)
    #extract id after the last "/" in the ahrefs_rank_tracker_url
    ahrefs_rank_tracker_id = ahrefs_rank_tracker_url.split("/")[-1]
    rename_csv_files(id=ahrefs_rank_tracker_id)

def launch_ahrefs_rank_tracker_scrapers(scrapers):
    date = time.strftime("%Y-%m-%d")
    for i, scraper_args in enumerate(scrapers):
        file_name = f"data/ahrefs/rank_tracker/*{date}*{scraper_args[2].split('/')[-1]}*"
        file_exists = bool(glob.glob(file_name))
        if not file_exists:
            for j in range(3):
                launch_ahrefs_rank_tracker_scraper(*scraper_args)
                file_exists = bool(glob.glob(file_name))
                if file_exists:
                    print(f"Scraper {i+1}/{len(scrapers)} completed")
                    break
                else:
                    print(f"Scraper {i+1}/{len(scrapers)} failed")
        else:
            print(f"Scraper {i+1}/{len(scrapers)} skipped")

def merge_csv_files(folder, search_string):
    # Find all files in the folder that match the search string
    files = glob.glob(os.path.join(folder, search_string))

    # Merge all the CSV files into one dataframe
    df_list = []
    for file in files:
        df = pd.read_csv(file,sep='\t', encoding='utf-16')
        df_list.append(df)
    merged_df = pd.concat(df_list, ignore_index=True)
    
    return merged_df


def calculate_days_between_dates(dates, max_diff):
    # Convert the list of dates to numpy.datetime64 objects
    dates = np.array(dates, dtype='datetime64[s]')
    
    # Convert numpy.datetime64 objects to datetime.datetime objects
    dates = [np.datetime64(date).astype(datetime.datetime) for date in dates]

    # Initialize variables
    num_days = []
    i = 0

    while i < len(dates)-1:
        # Calculate the number of days between consecutive dates
        diff = (dates[i] - dates[i + 1]).days

        # If the difference is less than the maximum allowed difference, remove the second date and recalculate
        if diff < max_diff:
            dates = np.delete(dates, i+1)
        else:
            num_days.append(diff)
            i += 1

    # Return the list of remaining dates

    # Sort dates from newest to oldest
    dates = dates[::-1]
    return dates.tolist()

def pivot_rank_tracker(df):
    # Convert date_scraped to datetime and format it
    df['date_scraped'] = pd.to_datetime(df.date_scraped).dt.strftime("%Y-%m-%d")
    
    # Pivot the dataframe on the date_scraped column and reset the index
    df = df[['Keyword', 'date_scraped', 'Location', 'Rank', 'URL', 'Volume', 'Tags']]
    df = df.pivot(index=['Keyword', 'Location', 'URL', 'Volume', 'Tags'], columns='date_scraped', values='Rank')
    df = df.reset_index()
    df.columns.name = None

    # Rename columns that start with 'Rank'
    df.columns = ['{}_{}'.format('Rank', col) if '-' in col else col for col in df.columns]

    # Rearrange the columns so that the 'Rank' columns come after the first column
    col_filtered_final = [col for col in df.columns if not 'Rank' in col]
    col_rank = [col for col in df.columns if 'Rank' in col]
    col_filtered_final[1:1] = col_rank
    df = df[col_filtered_final]

    return df


ahrefs_email_1 = 'energy@talentheromedia.com'
ahrefs_password_1 = '#Uc*8bObLBun3ste+Lb4'

ahrefs_email_2 = 'zack@lucidwebgroup.com'
ahrefs_password_2 = 'Luc!dWeb1'

#ahrefs_rank_tracker_url = 'https://app.ahrefs.com/rank-tracker/overview/4086622'



scrapers = [
    (ahrefs_email_1, ahrefs_password_1, "https://app.ahrefs.com/rank-tracker/overview/4086622")
    # (ahrefs_email_1, ahrefs_password_1, "https://app.ahrefs.com/rank-tracker/overview/4450926"),
    # (ahrefs_email_1, ahrefs_password_1, "https://app.ahrefs.com/rank-tracker/overview/4467808"),
    # (ahrefs_email_2, ahrefs_password_2, "https://app.ahrefs.com/rank-tracker/overview/517624"),
    # (ahrefs_email_2, ahrefs_password_2, "https://app.ahrefs.com/rank-tracker/overview/1545607"),
    # (ahrefs_email_2, ahrefs_password_2, "https://app.ahrefs.com/rank-tracker/overview/1568215"),
    # (ahrefs_email_2, ahrefs_password_2, "https://app.ahrefs.com/rank-tracker/overview/1655818"),
    # (ahrefs_email_2, ahrefs_password_2, "https://app.ahrefs.com/rank-tracker/overview/1782440"),
    # (ahrefs_email_2, ahrefs_password_2, "https://app.ahrefs.com/rank-tracker/overview/2031974"),
    # (ahrefs_email_2, ahrefs_password_2, "https://app.ahrefs.com/rank-tracker/overview/2126592"),
    # (ahrefs_email_2, ahrefs_password_2, "https://app.ahrefs.com/rank-tracker/overview/2195336"),
    # (ahrefs_email_2, ahrefs_password_2, "https://app.ahrefs.com/rank-tracker/overview/2225463"),
    # (ahrefs_email_2, ahrefs_password_2, "https://app.ahrefs.com/rank-tracker/overview/2343332"),
    # (ahrefs_email_2, ahrefs_password_2, "https://app.ahrefs.com/rank-tracker/overview/2375716"),
    # (ahrefs_email_2, ahrefs_password_2, "https://app.ahrefs.com/rank-tracker/overview/2437880"),
    # (ahrefs_email_2, ahrefs_password_2, "https://app.ahrefs.com/rank-tracker/overview/2678958"),
    # (ahrefs_email_2, ahrefs_password_2, "https://app.ahrefs.com/rank-tracker/overview/2944880"),
    # (ahrefs_email_2, ahrefs_password_2, "https://app.ahrefs.com/rank-tracker/overview/3482655"),
    # (ahrefs_email_2, ahrefs_password_2, "https://app.ahrefs.com/rank-tracker/overview/3521687"),
    # (ahrefs_email_2, ahrefs_password_2, "https://app.ahrefs.com/rank-tracker/overview/830326"),
    #(ahrefs_email_2, ahrefs_password_2, "https://app.ahrefs.com/rank-tracker/overview/3660056")
]

#launch_ahrefs_rank_tracker_scrapers(scrapers)
folder = "data/ahrefs/rank_tracker"
search_string = "*4086622*.csv"
merged_df = merge_csv_files(folder, search_string)
# sort by scraped date relative to current date
merged_df['date_scraped'] = pd.to_datetime(merged_df['date_scraped'])
merged_df = merged_df.sort_values('date_scraped', ascending=False)

# extract the unique "date_scraped" values
unique_dates = merged_df['date_scraped'].unique()

unique_dates = calculate_days_between_dates(unique_dates, 10)

print(unique_dates)

# filter the dataframe to only include the unique dates
merged_df = merged_df[merged_df['date_scraped'].isin(unique_dates)]

# sort the dataframe by "scrapped_date" from newest to oldest
merged_df = merged_df.sort_values('date_scraped', ascending=False)

merged_df = pivot_rank_tracker(merged_df)

# save merged_df to csv
merged_df.to_csv('data/ahrefs/rank_tracker/merged_rank_tracker.csv', index=False)

