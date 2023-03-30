import country_converter as coco
from searchconsole import authenticate
from pathlib import Path
import pandas as pd
import tldextract
import pandas as pd
import tldextract
from tqdm import tqdm
import pycountry_convert as pc
from utils import download_gsheet
from serpclix_tracking import process_click_tracking_data_serpclix
from in_house_tracking import process_in_house_link_clicking_df
from utils import add_domain_tld_column
from utils import add_date_range_column_and_clean
from utils import get_date_ranges


def count_in_house_clicks(click_data_df: pd.DataFrame) -> pd.DataFrame:
    """
    Counts the number of in-house clicks for each keyword, country, and date range.

    Args:
        click_data_df (pd.DataFrame): A DataFrame containing click tracking data.

    Returns:
        pd.DataFrame: A DataFrame containing the number of in-house clicks for each keyword, country, and date range.
    """
    # Group the DataFrame by "Keyword", "Country", "Date", and "Source" and count the number of clicks
    in_house_clicks_df = click_data_df.groupby(
        ["query", "country", "page", "date_range", "source"]
    )["page"].count()

    # Convert the Series to a DataFrame
    in_house_clicks_df = in_house_clicks_df.to_frame()

    # Create a new column "in_house_clicks"
    in_house_clicks_df = in_house_clicks_df.rename(columns={"page": "in_house_clicks"})

    # Reset the index
    in_house_clicks_df = in_house_clicks_df.reset_index()

    # make column "in_house_clicks" an integer
    in_house_clicks_df["in_house_clicks"] = in_house_clicks_df[
        "in_house_clicks"
    ].astype(int)

    # # Return the DataFrame
    return in_house_clicks_df


def count_serpclix_clicks(click_data_df: pd.DataFrame) -> pd.DataFrame:
    """
    Counts the number of in-house clicks for each keyword, country, and date range.

    Args:
        click_data_df (pd.DataFrame): A DataFrame containing click tracking data.

    Returns:
        pd.DataFrame: A DataFrame containing the number of in-house clicks for each keyword, country, and date range.
    """
    # Group the DataFrame by "Keyword", "Country", "Date", and "Source" and count the number of clicks
    inclix_df = click_data_df.groupby(
        ["query", "country", "page", "date_range", "source"]
    )["page"].count()

    # Convert the Series to a DataFrame
    inclix_df = inclix_df.to_frame()

    # Create a new column "in_house_clicks"
    inclix_df = inclix_df.rename(columns={"page": "serpclix_clicks"})

    # Reset the index
    inclix_df = inclix_df.reset_index()

    # # Return the DataFrame
    return inclix_df


def clean_date_range_df(click_data_df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans and transforms the click tracking data DataFrame.

    Args:
        click_data_df (pd.DataFrame): A DataFrame containing click tracking data.

    Returns:
        pd.DataFrame: A cleaned and transformed DataFrame.
    """
    # Count all duplicate rows where "Keyword", "Country", "Date", and "Link" are the same and create a new column with the count named "simulated_clicks"
    click_data_df["simulated_clicks"] = click_data_df.groupby(
        ["Keyword", "Country", "date_range", "Link"]
    )["Link"].transform("count")

    # Drop duplicates from the DataFrame based on the columns ["Keyword", "Country", "Date", "Link"]
    click_data_df = click_data_df.drop_duplicates(
        subset=["Keyword", "Country", "date_range", "Link"]
    )

    # Remove column "Source"
    click_data_df = click_data_df.drop(columns=["Source"])

    # Drop all rows where "date_range" is "N/A"
    click_data_df = click_data_df[click_data_df["date_range"] != "N/A"]

    # Create a new column "domain" with root domain + extension of the "Link" column using tldextract
    click_data_df["domain"] = click_data_df["Link"].apply(
        lambda link: tldextract.extract(link).registered_domain
    )

    # Split the column date_range into two columns "start_date" and "end_date" and drop the column "date_range"
    click_data_df[["start_date", "end_date"]] = click_data_df["date_range"].str.split(
        " - ", expand=True
    )
    click_data_df = click_data_df.drop(columns=["date_range"])

    # convert the start_date and end_date columns to datetime64[ns]
    click_data_df["start_date"] = pd.to_datetime(click_data_df["start_date"])
    click_data_df["end_date"] = pd.to_datetime(click_data_df["end_date"])

    # Rename "Keyword" column to "query", "Link" column to "page", "Country" column to "country", "click_count" column to "simulated_clicks"
    click_data_df = click_data_df.rename(
        columns={
            "Keyword": "query",
            "Link": "page",
            "Country": "country",
            "simulated_clicks": "simulated clicks",
        }
    )

    # Reorder the columns query,page,country,clicks,start_date,end_date,domain
    click_data_df = click_data_df[
        [
            "query",
            "page",
            "country",
            "simulated clicks",
            "start_date",
            "end_date",
            "domain",
        ]
    ]

    return click_data_df


def combine_source_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Combines the source_x and source_y columns into a single column named source.

    Args:
        df (pd.DataFrame): A DataFrame containing the source_x and source_y columns.

    Returns:
        pd.DataFrame: A DataFrame with the source_x and source_y columns combined into a single column named source.
    """
    # combine source_x and source_y columns
    df["source"] = df["source_x"].fillna("") + df["source_y"].fillna("")

    # remove source_x and source_y columns
    df = df.drop(columns=["source_x", "source_y"])

    # save to csv
    # df.to_csv("combined_source_columns.csv", index=False, sep="\t", encoding="utf-8")

    return df


# def sum_click_data(df):
#     # sum between in-house and serpclix clicks, where not null, convert to int
#     df["adjusted_clicks"] = df["in_house_clicks"].fillna(0).astype(int) + df[
#         "serpclix_clicks"
#     ].fillna(0).astype(int)
#     return df


def merge_inhouse_serpclix_dfs(df1, df2):
    # merge the two click tracking dataframes on 'query', 'country', 'page', 'date_range'
    merged_click_tracking_df = pd.merge(
        df1,
        df2,
        on=["query", "country", "page", "date_range"],
        how="outer",
    )
    return merged_click_tracking_df


def get_click_data_df() -> pd.DataFrame:
    date_ranges = get_date_ranges()

    # print(f"Date ranges:{date_ranges}")

    in_house_link_clicking = "https://docs.google.com/spreadsheets/d/124YEzAPOtR3UFT-KfG9IAWrcJr67icSPU475opYSaw8/edit#gid=1743911824"
    serpclix_link_clicking = "https://docs.google.com/spreadsheets/d/186V5aIS4cNqhlFI_--0uqSQUMzrzjp_OtVCXZ1xVVoE/edit#gid=0"

    download_gsheet(in_house_link_clicking, "gsheet/in_house_link_clicking.csv")
    download_gsheet(serpclix_link_clicking, "gsheet/serpclix_link_clicking.csv")

    in_house_link_clicking_df = pd.read_csv(
        "gsheet/in_house_link_clicking.csv", sep=",", encoding="utf-8"
    )
    serpclix_link_clicking_df = pd.read_csv(
        "gsheet/serpclix_link_clicking.csv", sep=",", encoding="utf-8"
    )

    #### Manual Click Tracking Data Extraction

    in_house_link_clicking_df = process_in_house_link_clicking_df(
        in_house_link_clicking_df
    )

    in_house_link_clicking_df = add_date_range_column_and_clean(
        in_house_link_clicking_df, date_ranges
    )
    in_house_link_clicking_df = count_in_house_clicks(in_house_link_clicking_df)

    # save to csv
    in_house_link_clicking_df.to_csv(
        "in_house_link_clicking.csv", index=False, sep="\t", encoding="utf-8"
    )

    #### Serpclix Click Tracking Data Extraction

    click_tracking_data_serpclix_df = process_click_tracking_data_serpclix(
        serpclix_link_clicking_df, date_ranges
    )

    click_tracking_data_serpclix_df = add_date_range_column_and_clean(
        click_tracking_data_serpclix_df, date_ranges
    )

    click_tracking_data_serpclix_df = count_serpclix_clicks(
        click_tracking_data_serpclix_df
    )
    # save to csv
    click_tracking_data_serpclix_df.to_csv(
        "serpclix_link_clicking.csv", index=False, sep="\t", encoding="utf-8"
    )
    #### Merge Click Tracking Data

    merged_click_tracking_df = merge_inhouse_serpclix_dfs(
        in_house_link_clicking_df, click_tracking_data_serpclix_df
    )
    merged_click_tracking_df = combine_source_columns(merged_click_tracking_df)

    merged_click_tracking_df = add_domain_tld_column(merged_click_tracking_df)

    # # save to csv
    # merged_click_tracking_df.to_csv(
    #     "merged_click_tracking_df.csv", index=False, sep="\t", encoding="utf-8"
    # )

    return merged_click_tracking_df


# get_click_data_df()
