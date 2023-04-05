import pandas as pd
import numpy as np
from utils import download_gsheet
from gsc import get_gsc_data_df
from click_tracking import get_click_data_df


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
    df[["clicks", "in_house_clicks", "serpclix_clicks"]] = df[
        ["clicks", "in_house_clicks", "serpclix_clicks"]
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
        "page",
        "country",
        "date_range",
        "domain",
    ]
    return df[column_order]


def order_rows_by_adjusted_clicks(df):
    # order from highest to lowest value of column "adjusted_clicks"
    df = df.sort_values(by="adjusted_clicks", ascending=False)
    return df


def gen_db_df():
    gsc_df = get_gsc_data_df()
    # gsc_df.to_csv("test/gsc.csv", index=False, encoding="utf-8", sep="\t")

    click_data_df = get_click_data_df()
    # click_data_df.to_csv("test/click_data.csv", index=False, encoding="utf-8", sep="\t")

    merged_df = merge_gsc_and_click_data(gsc_df, click_data_df)
    # merged_df.to_csv("test/1merged_df.csv", index=False, encoding="utf-8", sep="\t")

    merged_df = remove_root_domain_rows(merged_df)
    # merged_df.to_csv(
    #     "test/2merged_df_no_root.csv", index=False, encoding="utf-8", sep="\t"
    # )

    # Download domain list from Google Sheet
    filter_rules = "https://docs.google.com/spreadsheets/d/1uBsysJd1XTtOftpD04W_vlWDRXczzeESmbS51DP0U_0/edit#gid=0"
    download_gsheet(filter_rules, "gsheet/filter_rules.csv")

    filter_rules_df = pd.read_csv("gsheet/filter_rules.csv", sep=",", encoding="utf-8")

    merged_df = merged_df[
        merged_df.apply(lambda row: apply_filter(row, filter_rules_df), axis=1)
    ]
    # merged_df.to_csv(
    #     "test/3merged_df_filtered.csv", index=False, encoding="utf-8", sep="\t"
    # )

    merged_df = fill_na_with_zero(merged_df)
    # merged_df.to_csv(
    #     "test/4merged_df_na_zero.csv", index=False, encoding="utf-8", sep="\t"
    # )

    merged_df = get_adjusted_clicks(merged_df)
    # merged_df.to_csv(
    #     "test/5merged_df_adjusted.csv", index=False, encoding="utf-8", sep="\t"
    # )

    pivoted_df = aggregate_clicks_impressions_by_query_page_country(merged_df)
    # pivoted_df.to_csv("test/6pivoted.csv", index=False, encoding="utf-8", sep="\t")

    pivoted_df = rename_columns(pivoted_df)
    # pivoted_df.to_csv(
    #     "test/7pivoted_renamed.csv", index=False, encoding="utf-8", sep="\t"
    # )

    pivoted_df = create_first_rank_column(pivoted_df)
    # pivoted_df.to_csv(
    #     "test/8pivoted_first_rank.csv", index=False, encoding="utf-8", sep="\t"
    # )

    final_df = combine_merged_df_with_pivoted(merged_df, pivoted_df)
    # final_df.to_csv("test/9final.csv", index=False, encoding="utf-8", sep="\t")

    final_df = drop_columns(final_df, ["first_rank", "position", "source"])
    # final_df.to_csv(
    #     "test/10final_drop_pos.csv", index=False, encoding="utf-8", sep="\t"
    # )
    final_df = reorder_dataframe(final_df)

    final_df = order_rows_by_adjusted_clicks(final_df)

    # final_df.to_csv(
    #     "test/11final_reordered.csv", index=False, encoding="utf-8", sep="\t"
    # )

    return final_df


# gen_db_df()
