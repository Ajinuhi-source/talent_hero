import pandas as pd
import datetime
import functools
import os
import requests
import tldextract


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


def convert_to_datetime(df: pd.DataFrame) -> pd.DataFrame:
    """
    Converts a column of dates in a pandas DataFrame to datetime format.

    Parameters:
    -----------
    df : pandas.DataFrame
        The DataFrame containing the dates.

    Returns:
    --------
    pandas.DataFrame
        The DataFrame with the converted dates.
    """
    df["date"] = pd.to_datetime(df["date"])
    return df


def add_date_range_column(date_ranges: list, input_df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds a "date_range" column to a pandas DataFrame based on a list of date ranges.

    Parameters:
    -----------
    date_ranges : list
        A list of tuples, where each tuple contains two datetime objects that represent the start and end of a date range.
    input_df : pandas.DataFrame
        The DataFrame to add the "date_range" column to.

    Returns:
    --------
    pandas.DataFrame
        The DataFrame with the added "date_range" column.
    """

    def get_date_range(date, date_ranges):
        for date_range in date_ranges:
            if date >= date_range[1] and date <= date_range[0]:
                return f"{date_range[1].date()} - {date_range[0].date()}"
        return "N/A"

    input_df["date_range"] = input_df["date"].apply(
        lambda date: get_date_range(date, date_ranges)
    )
    return input_df[input_df["date_range"] != "N/A"]


def drop_date_column(df: pd.DataFrame) -> pd.DataFrame:
    """
    Removes a column of dates from a pandas DataFrame.

    Parameters:
    -----------
    df : pandas.DataFrame
        The DataFrame containing the dates.

    Returns:
    --------
    pandas.DataFrame
        The DataFrame with the date column removed.
    """
    return df.drop(columns=["date"])


def add_date_range_column_and_clean(
    df: pd.DataFrame, date_ranges: list
) -> pd.DataFrame:
    """
    Adds a "date_range" column to a pandas DataFrame based on a list of date ranges, and removes the original date column.

    Parameters:
    -----------
    date_ranges : list
        A list of tuples, where each tuple contains two datetime objects that represent the start and end of a date range.
    df : pandas.DataFrame
        The DataFrame to add the "date_range" column to.
    date_column : str
        The name of the column in the DataFrame that contains the dates.

    Returns:
    --------
    pandas.DataFrame
        The DataFrame with the added "date_range" column and the original date column removed.
    """
    df = convert_to_datetime(df)
    df = add_date_range_column(date_ranges, df)
    df = drop_date_column(df)

    return df


def add_domain_tld_column(df, url_column_name="page", new_column_name="domain"):
    """
    Extracts the domain from the URL in the given column and puts it in a new column with the given name.

    Parameters:
    df (pandas.DataFrame): The DataFrame containing the URLs
    url_column_name (str): The name of the column containing the URLs. Default is 'page'.
    new_column_name (str): The name of the new column to be added with domain+TLD. Default is 'domain_tld'.

    Returns:
    pandas.DataFrame: The updated DataFrame with the new column for domain+TLD
    """
    # Apply the lambda function to extract the domain and TLD from the URL and add it to the new column
    df[new_column_name] = df[url_column_name].apply(
        lambda x: tldextract.extract(x).domain + "." + tldextract.extract(x).suffix
    )

    return df
