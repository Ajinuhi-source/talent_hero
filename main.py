import pandas as pd
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

    # save to csv
    gsc_df.to_csv("test/gsc.csv", index=False, encoding="utf-8", sep="\t")
    click_data_df.to_csv("test/click_data.csv", index=False, encoding="utf-8", sep="\t")

    merged_df = pd.merge(
        gsc_df,
        click_data_df,
        how="outer",
        on=["query", "page", "country", "date_range", "domain"],
    )

    # # create a new column "final clicks" with the diffrence between "clicks" and the "simulated clicks", if "simulated clicks" is NaN, then "final clicks" = "clicks"
    # merged_df["adjusted_clicks"] = merged_df.apply(
    #     lambda row: row["clicks"] - row["simulated clicks"]
    #     if not pd.isnull(row["simulated clicks"])
    #     else row["clicks"],
    #     axis=1,
    # )

    return merged_df


def aggregate_clicks_impressions_by_query_page_country(df):
    """
    Aggregates clicks and impressions by query, page, country, and date range.

    :param df: A Pandas DataFrame containing data to pivot.
    :return: A pivoted DataFrame with aggregated metrics for each query, page, country, and date range.
    """

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

    return click_impressions_by_query_page_country


def run():
    gsc_df = get_gsc_data_df()
    gsc_df.to_csv("test/gsc.csv", index=False, encoding="utf-8", sep="\t")

    click_data_df = get_click_data_df()
    click_data_df.to_csv("test/click_data.csv", index=False, encoding="utf-8", sep="\t")

    merged_df = merge_gsc_and_click_data(gsc_df, click_data_df)
    merged_df.to_csv("test/merged_df.csv", index=False, encoding="utf-8", sep="\t")

    pivoted_df = aggregate_clicks_impressions_by_query_page_country(merged_df)
    pivoted_df.to_csv("test/pivoted.csv", index=False, encoding="utf-8", sep="\t")

    # intersect pivoted_df with merged_df


run()


# open merged_df.csv on column date_range and extract create a list of all unique values
# create a list of all unique values in the column date_range

# merged_df = pd.read_csv("merged_df.csv", sep="\t", encoding="utf-8")
# date_range_list = merged_df["date_range"].unique().tolist()
# # strip the list at : and keep the first value
# date_range_list = [date_range.split(" - ")[0] for date_range in date_range_list]
# # order dates from newest to oldest
# date_range_list.sort(reverse=True)
# newest_date = date_range_list[0]
# print(newest_date)

# in merged_df, drop all rows where date_range does not contain the string newest_date
# merged_df_newest_date = merged_df[merged_df["date_range"].str.contains(newest_date)]
# merged_df_newest_date.to_csv(
#     "merged_df_newest_date.csv", index=False, encoding="utf-8", sep="\t"
# )


# def filter_domains_by_country(df1, df2):
#     """
#     Filter rows in df1 based on matching values in df2 for domain and country.
#     """
#     # Create a copy of df1 to avoid modifying the original DataFrame
#     filtered_df = df1.copy()

#     # Split countries column into a list
#     filtered_df["countries"] = filtered_df["countries"].str.split(",")

#     # Iterate over each row of df1
#     for index, row in df1.iterrows():
#         # Check if there are any matching rows in df2 for this domain and any country in the list
#         is_match = False
#         domain = row["domain"]
#         for country in row["countries"]:
#             if not df2.loc[
#                 (df2["domain"] == domain) & (df2["country"] == country)
#             ].empty:
#                 is_match = True
#                 break

#         # If no match found, delete this row from the resulting dataframe
#         if not is_match:
#             filtered_df.drop(index, inplace=True)

#     return filtered_df
