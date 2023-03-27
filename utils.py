import pandas as pd
import datetime
import functools
import os
import requests


@functools.lru_cache(maxsize=None)
def get_date_ranges() -> list:
    """
    Returns a list of tuples, where each tuple contains two datetime objects that represent the start and end of a date range.

    Returns:
    list: A list of tuples, where each tuple contains two datetime objects that represent the start and end of a date range.
    """

    # Get today's date as a Pandas Timestamp object
    today = pd.Timestamp.today()

    # Get the first day of the current month as a Pandas Timestamp object
    first_day_of_month = pd.Timestamp(today.year, today.month, 1)

    # Define date value ranges

    date_ranges = [
        (first_day_of_month, first_day_of_month - datetime.timedelta(days=29)),
        (
            first_day_of_month - datetime.timedelta(days=29),
            first_day_of_month - datetime.timedelta(days=57),
        ),
        (
            first_day_of_month - datetime.timedelta(days=57),
            first_day_of_month - datetime.timedelta(days=85),
        ),
        (
            first_day_of_month - datetime.timedelta(days=85),
            first_day_of_month - datetime.timedelta(days=113),
        ),
        (
            first_day_of_month - datetime.timedelta(days=113),
            first_day_of_month - datetime.timedelta(days=141),
        ),
        (
            first_day_of_month - datetime.timedelta(days=141),
            first_day_of_month - datetime.timedelta(days=169),
        ),
    ]

    return date_ranges


def download_gsheet(url: str, path: str = "/path/downloaded_content.csv"):
    # Check if the URL is a valid Google Sheets URL
    if "docs.google.com" in url:
        # Extract the document ID and sheet ID from the URL
        document_id = url.split("/")[5]
        sheet_id = url.split("/edit#gid=")[1]

        # Construct the CSV download URL
        csv_url = f"https://docs.google.com/spreadsheets/d/{document_id}/export?format=csv&gid={sheet_id}"

        # Download the CSV file from the URL
        response = requests.get(csv_url)

        # Create directories if the path does not exist
        dir_path = os.path.dirname(path)
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)

        # Write the CSV file to the specified path
        with open(path, "wb") as file:
            file.write(response.content)
    else:
        raise ValueError("The provided URL is not a valid Google Sheets URL.")
