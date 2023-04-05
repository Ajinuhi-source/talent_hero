import country_converter as coco
from searchconsole import authenticate
from pathlib import Path
import pandas as pd


def add_source_column_serpclix(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds a column "Source" with the value "SerpClix".

    Args:
        df (pd.DataFrame): A DataFrame containing click tracking data.

    Returns:
        pd.DataFrame: A DataFrame with the added "Source" column.
    """
    df["source"] = "serpclix"
    return df


def rename_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Renames the columns "Clicker Country" to "country", "Timestamp" to "date", "URL" to "page",
    "Keyword" to "query".

    Args:
        df (pd.DataFrame): A DataFrame containing click tracking data.

    Returns:
        pd.DataFrame: A DataFrame with the renamed columns.
    """
    df = df.rename(
        columns={
            "Clicker Country": "country",
            "Timestamp": "date",
            "URL": "page",
            "Keyword": "query",
        }
    )
    return df


def convert_country_to_iso2(df: pd.DataFrame) -> pd.DataFrame:
    """
    Converts all values from column "country" into ISO2 codes.

    Args:
        df (pd.DataFrame): A DataFrame containing click tracking data.

    Returns:
        pd.DataFrame: A DataFrame with the converted "country" values.
    """

    df["country"] = df["country"].apply(
        lambda x: coco.convert(names=x, to="ISO2") if x else x
    )
    return df


def convert_date_format(df: pd.DataFrame) -> pd.DataFrame:
    """
    Converts "date" column to datetime values in YYYY-MM-DD format.

    Args:
        df (pd.DataFrame): A DataFrame containing click tracking data.

    Returns:
        pd.DataFrame: A DataFrame with the converted "date" column.
    """
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    return df


def add_slash_to_page_column(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds a "/" to the end of the "page" column if it doesn't already end with one.

    Args:
        df (pd.DataFrame): A DataFrame containing click tracking data.

    Returns:
        pd.DataFrame: A DataFrame with the modified "page" column.
    """
    df.loc[~df["page"].str.endswith("/"), "page"] += "/"
    return df


def add_https_to_page_column(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds "https://" to "page" values that do not have "http" or "https".

    Args:
        df (pd.DataFrame): A DataFrame containing click tracking data.

    Returns:
        pd.DataFrame: A DataFrame with the modified "page" column.
    """
    df["page"] = df["page"].apply(
        lambda x: "https://" + x if not x.startswith(("http", "https")) else x
    )
    return df


def add_slash_to_end_of_page_column(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds "/" to the end of "page" values that do not end with "/".

    Args:
        df (pd.DataFrame): A DataFrame containing click tracking data.

    Returns:
        pd.DataFrame: A DataFrame with the modified "page" column.
    """
    df.loc[~df["page"].str.endswith("/"), "page"] += "/"
    return df


def select_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Selects only the “query”, “country”, “date”, “page”, and “source” columns.

    Args:
        df (pd.DataFrame): A DataFrame containing click tracking data.

    Returns:
        pd.DataFrame: A DataFrame with only the selected columns.
    """

    df = df[["query", "country", "date", "page", "source"]]
    return df


def drop_na_rows(df: pd.DataFrame) -> pd.DataFrame:
    """
    Drops rows with NaN values in “date” column.

    Args:
        df (pd.DataFrame): A DataFrame containing click tracking data.

    Returns:
        pd.DataFrame: A DataFrame with NaN rows dropped.
    """
    df_copy = df.copy()
    df_copy.dropna(subset=["date"], inplace=True)
    return df_copy


def process_click_tracking_data_serpclix(
    click_tracking_df: pd.DataFrame, date_ranges: list
) -> pd.DataFrame:
    """
    Processes the click tracking data for SerpClix and returns a cleaned DataFrame.

    Args:
        click_tracking_df (pd.DataFrame): A DataFrame containing click tracking data for SerpClix.

    Returns:
        pd.DataFrame: A cleaned DataFrame containing only the "query", "country", "date", "page", and "source" columns.
    """
    click_tracking_df = add_source_column_serpclix(click_tracking_df)
    # click_tracking_df.to_csv("serpclix_1.csv", index=False, encoding="utf-8", sep="\t")

    click_tracking_df = rename_columns(click_tracking_df)
    # click_tracking_df.to_csv("serpclix_2.csv", index=False, encoding="utf-8", sep="\t")

    click_tracking_df = convert_country_to_iso2(click_tracking_df)
    # click_tracking_df.to_csv("serpclix_3.csv", index=False, encoding="utf-8", sep="\t")

    click_tracking_df = convert_date_format(click_tracking_df)
    # click_tracking_df.to_csv("serpclix_4.csv", index=False, encoding="utf-8", sep="\t")

    click_tracking_df = add_slash_to_page_column(click_tracking_df)
    # click_tracking_df.to_csv("serpclix_5.csv", index=False, encoding="utf-8", sep="\t")

    click_tracking_df = add_https_to_page_column(click_tracking_df)
    # click_tracking_df.to_csv("serpclix_6.csv", index=False, encoding="utf-8", sep="\t")

    click_tracking_df = add_slash_to_end_of_page_column(click_tracking_df)
    # click_tracking_df.to_csv("serpclix_7.csv", index=False, encoding="utf-8", sep="\t")

    click_tracking_df = select_columns(click_tracking_df)
    # click_tracking_df.to_csv("serpclix_8.csv", index=False, encoding="utf-8", sep="\t")

    click_tracking_df = drop_na_rows(click_tracking_df)
    # click_tracking_df.to_csv("serpclix_9.csv", index=False, encoding="utf-8", sep="\t")

    # click_tracking_df = add_date_range_column_and_clean(click_tracking_df, date_ranges)
    return click_tracking_df


# date_ranges = utils.DATE_RANGES
# serpclix_link_clicking = "https://docs.google.com/spreadsheets/d/186V5aIS4cNqhlFI_--0uqSQUMzrzjp_OtVCXZ1xVVoE/edit#gid=0"
# download_gsheet(serpclix_link_clicking, "gsheet/serpclix_link_clicking.csv")
# df = pd.read_csv("gsheet/serpclix_link_clicking.csv", sep=",", encoding="utf-8")
# df = process_click_tracking_data_serpclix(df, date_ranges)
# df.to_csv("serpclix_10.csv", index=False, encoding="utf-8", sep="\t")
