import pandas as pd
import numpy as np
from utils import download_gsheet
from gsc import get_gsc_data_df
from click_tracking import get_click_data_df
from stqdm import stqdm


# This function takes a row from the db_df DataFrame and the entire filter_df DataFrame as input arguments. It checks whether the row from db_df matches any of the filtering rules in filter_df. If a match is found, it returns True or False based on the filter type. If no match is found, the function returns False, meaning the row should not be kept.
def apply_filter(row, filter_df):
    # Loop through each row in the filter_df DataFrame
    for _, filter_row in filter_df.iterrows():
        # Extract the keyword, domain, and filter type from the filter_row
        keyword = filter_row["Keyword"]
        domain = filter_row["Domain"]
        filter_type = filter_row["Filter Type"]

        # Check if the keyword is in the 'query' column of the current row
        # and if the domain matches or if the domain is set to 'All'
        if keyword in row["query"] and (domain == "All" or domain == row["domain"]):
            # If the filter type is 'Blacklist', return False (do not keep the row)
            if filter_type == "Blacklist":
                return False
            # If the filter type is 'Whitelist', return True (keep the row)
            elif filter_type == "Whitelist":
                return True

    # If none of the filtering rules matched, return False (do not keep the row)
    return False


# create a function that merges gsc_df and click_data_df on query,page,country,start_date, end_date, domain
def merge_gsc_and_click_data(gsc_df, click_data_df) -> pd.DataFrame:
    """
    Merges the Google Search Console data and the click tracking data.

    Args:
        gsc_df (pd.DataFrame): A DataFrame containing Google Search Console data.
        click_data_df (pd.DataFrame): A DataFrame containing click tracking data.

    Returns:
        pd.DataFrame: A merged DataFrame.

    """

    merged_df = pd.merge(
        gsc_df,
        click_data_df,
        how="outer",
        on=["query", "page", "country", "date_range", "domain"],
    )

    return merged_df


def aggregate_clicks_impressions_by_query_page_country(df):
    # Pivot the dataframe to aggregate clicks and impressions by query, page, country, and date range.
    click_impressions_by_query_page_country = df.pivot(
        index=["query", "page", "country"],
        columns="date_range",
        values=["position"],
    ).reset_index()

    # Rename the columns to include the metric (clicks or impressions) and the date range.
    click_impressions_by_query_page_country.columns = [
        "_".join(str(column).strip() for column in col if column)
        for col in click_impressions_by_query_page_country.columns
    ]

    # move the 2nd column to the end
    click_impressions_by_query_page_country = click_impressions_by_query_page_country[
        click_impressions_by_query_page_country.columns[[0, 8, 7, 6, 5, 4, 3, 1, 2]]
    ]

    return click_impressions_by_query_page_country


def get_adjusted_clicks(df):
    # create a column "adjusted_clicks" which is the difference between "clicks","in_house_clicks",	"serpclix_clicks". If the value is NaN, treat it as 0.
    df["adjusted_clicks"] = df.apply(
        lambda row: row["clicks"] - row["in_house_clicks"] - row["serpclix_clicks"]
        if not pd.isnull(row["in_house_clicks"])
        and not pd.isnull(row["serpclix_clicks"])
        else row["clicks"],
        axis=1,
    )
    return df


def fill_na_with_zero(df):
    # fill the NaN with 0 for the collumns "clicks","in_house_clicks",	"serpclix_clicks"
    df[["clicks", "in_house_clicks", "serpclix_clicks", "impressions"]] = df[
        ["clicks", "in_house_clicks", "serpclix_clicks", "impressions"]
    ].fillna(0)
    return df


def remove_root_domain_rows(df):
    # remove the rows with the root domain
    # if page contains less or equal than 3 "/", remove the row
    df = df[df["page"].str.count("/") > 3]
    return df


def create_first_rank_column(df):
    # Check if all required columns are present in df
    required_cols = [
        "current_rank",
        "previous_rank_1",
        "previous_rank_2",
        "previous_rank_3",
        "previous_rank_4",
        "previous_rank_5",
    ]
    if not set(required_cols).issubset(df.columns):
        raise ValueError("Input dataframe is missing one or more required columns")

    # Define a function to extract the first non-NaN value from a list of values
    def get_first_rank(row):
        for col in required_cols:
            if not pd.isnull(row[col]):
                return row[col]
        return np.nan

    # Create the "first_rank" column
    df["first_rank"] = df.apply(get_first_rank, axis=1)

    # Handle cases where "first_rank" column contains only NaN values
    if df["first_rank"].isnull().all():
        raise ValueError("All previous rank columns contain NaN values")

    return df


def combine_merged_df_with_pivoted(merged_df, pivoted_df):
    # merge on query, page, country, position (for merged_df) and 4th column on pivoted_df
    merged_df = pd.merge(
        merged_df,
        pivoted_df,
        how="right",
        left_on=["query", "page", "country", "position"],
        right_on=["query", "page", "country", "first_rank"],
    )
    return merged_df


def drop_columns(df, columns_to_drop):
    """
    Drop one or more columns from a dataframe.

    Parameters:
    -----------
    df : pandas.DataFrame
        The DataFrame to drop columns from.
    columns_to_drop : str or list of str
        The name(s) of the column(s) to drop from the DataFrame.

    Returns:
    --------
    pandas.DataFrame
        The DataFrame with the specified column(s) dropped.
    """
    # Ensure that the columns_to_drop argument is a list
    if isinstance(columns_to_drop, str):
        columns_to_drop = [columns_to_drop]

    # Attempt to drop the specified columns from the DataFrame
    try:
        df = df.drop(columns_to_drop, axis=1)
        # print(f"Columns {', '.join(columns_to_drop)} dropped successfully.")
    except KeyError as e:
        print(f"Error: {e} not found in DataFrame.")

    return df


def rename_columns(df):
    """
    Renames 9 columns using iloc and a list of desired column names.

    Parameters:
    -----------
    df : pandas.DataFrame
        The DataFrame to rename columns in.

    Returns:
    --------
    pandas.DataFrame
        The DataFrame with renamed columns.
    """
    new_column_names = [
        "query",
        "current_rank",
        "previous_rank_1",
        "previous_rank_2",
        "previous_rank_3",
        "previous_rank_4",
        "previous_rank_5",
        "page",
        "country",
    ]
    df.columns = new_column_names + list(df.columns[9:])

    return df


def reorder_dataframe(df):
    # Define the desired column order
    column_order = [
        "query",
        "page",
        "current_rank",
        "previous_rank_1",
        "previous_rank_2",
        "previous_rank_3",
        "previous_rank_4",
        "previous_rank_5",
        "impressions",
        "clicks",
        "in_house_clicks",
        "serpclix_clicks",
        "adjusted_clicks",
        "country",
        "date_range",
        "domain",
    ]
    return df[column_order]


def order_rows_by_adjusted_clicks(df):
    # order from highest to lowest value of column "adjusted_clicks"
    df = df.sort_values(by="adjusted_clicks", ascending=False)
    return df


def pretty_rename(df):
    df = df.rename(
        columns={
            "query": "Keyword",
            "current_rank": "Current Rank",
            "previous_rank_1": "Previous Rank 1",
            "previous_rank_2": "Previous Rank 2",
            "previous_rank_3": "Previous Rank 3",
            "previous_rank_4": "Previous Rank 4",
            "previous_rank_5": "Previous Rank 5",
            "impressions": "Impressions",
            "clicks": "Clicks",
            "in_house_clicks": "In House Clicks",
            "serpclix_clicks": "SerpClix",
            "adjusted_clicks": "Adjusted Clicks",
            "page": "Page",
            "country": "Country",
            "date_range": "Date Last Updated Interval",
            "domain": "Domain",
        }
    )
    return df


def gen_db_df():
    gsc_df = get_gsc_data_df()
    click_data_df = get_click_data_df()

    # Add a stqdm for the number of steps
    with stqdm(total=12) as pbar:
        pbar.set_description("Processing data")

        # Step 1: Merge GSC and Click Data
        pbar.set_description("Step 1: Merging GSC and Click Data")
        merged_df = merge_gsc_and_click_data(gsc_df, click_data_df)
        pbar.update(1)

        # Step 2: Remove root domain rows
        pbar.set_description("Step 2: Removing root domain rows")
        merged_df = remove_root_domain_rows(merged_df)
        pbar.update(1)

        # Step 3: Download domain list from Google Sheet
        pbar.set_description("Step 3: Download domain list from Google Sheet")
        filter_rules = "https://docs.google.com/spreadsheets/d/1uBsysJd1XTtOftpD04W_vlWDRXczzeESmbS51DP0U_0/edit#gid=0"
        download_gsheet(filter_rules, "gsheet/filter_rules.csv")
        filter_rules_df = pd.read_csv(
            "gsheet/filter_rules.csv", sep=",", encoding="utf-8"
        )
        pbar.update(1)

        # Step 4: Apply filter rules to merged_df
        pbar.set_description("Step 4: Applying filter rules to merged data")
        merged_df = merged_df[
            merged_df.apply(lambda row: apply_filter(row, filter_rules_df), axis=1)
        ]
        pbar.update(1)
        # Step 5: Fill missing values with 0
        pbar.set_description("Step 5: Filling missing values with 0")
        merged_df = fill_na_with_zero(merged_df)
        pbar.update(1)

        # Step 6: Get adjusted clicks for each row
        pbar.set_description("Step 6: Getting adjusted clicks for each row")
        merged_df = get_adjusted_clicks(merged_df)
        pbar.update(1)

        # Step 7: Aggregate clicks and impressions by query, page and country
        pbar.set_description(
            "Step 7: Aggregating clicks and impressions by query, page and country"
        )
        pivoted_df = aggregate_clicks_impressions_by_query_page_country(merged_df)
        pbar.update(1)

        # Step 8: Rename columns in pivoted_df
        pbar.set_description("Step 8: Renaming columns")
        pivoted_df = rename_columns(pivoted_df)
        pbar.update(1)

        # Step 9: Create first_rank column in pivoted_df
        pbar.set_description("Step 9: Creating first_rank column")
        pivoted_df = create_first_rank_column(pivoted_df)
        pbar.update(1)

        # Step 10: Combine merged_df with pivoted_df
        pbar.set_description("Step 10: Combining merged_df with pivoted_df")
        final_df = combine_merged_df_with_pivoted(merged_df, pivoted_df)
        final_df = drop_columns(final_df, ["first_rank", "position", "source"])
        pbar.update(1)

        # Step 11: Reorder columns in final_df
        pbar.set_description("Step 11: Reordering columns")
        final_df = reorder_dataframe(final_df)
        pbar.update(1)

        # Step 12: Rename columns in final_df
        pbar.set_description("Step 12: Renaming columns to be more readable")
        final_df = pretty_rename(final_df)
        final_df.to_csv("gsheet/final_12_df.csv", index=False)
        pbar.update(1)

    return final_df


# gen_db_df()
