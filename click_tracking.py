import country_converter as coco
from searchconsole import authenticate
from pathlib import Path
import pandas as pd
import tldextract
import pandas as pd
import tldextract
from tqdm import tqdm
from utils import get_date_ranges
from utils import download_gsheet


def process_in_house_link_clicking_df(
    click_tracking_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Process click tracking data from in house source.

    Parameters:
    -----------
    click_tracking_df : pandas.DataFrame
        The click tracking data as a pandas DataFrame.

    Returns:
        pd.DataFrame: A cleaned DataFrame containing only the "Keyword", "Country", "Date", "Link", and "Source" columns.
    """
    # Extract country from VPN column
    click_tracking_df["Country"] = (
        click_tracking_df["VPN"].str.split(" ", expand=True)[0].str.lower()
    )

    # Add Source column with value 'Manual'
    click_tracking_df["Source"] = "Manual"

    # Convert Ranking column to numeric
    click_tracking_df["Ranking"] = pd.to_numeric(
        click_tracking_df["Ranking"], errors="coerce"
    )

    # Apply filters to the DataFrame
    click_tracking_df = click_tracking_df[
        (click_tracking_df["Ranking"].notnull())
        & (click_tracking_df["Country"].str.len() <= 2)
        & (click_tracking_df["Link"].str.count("/") > 2)
        & (
            ~click_tracking_df["Link"]
            .fillna("")
            .str.endswith(
                (
                    ".com/",
                    ".net/",
                    ".org/",
                    ".ca/",
                    ".info/",
                    ".biz/",
                    ".us/",
                    ".uk/",
                    ".co.uk/",
                    ".de/",
                    ".jp/",
                    ".fr/",
                    ".au/",
                )
            )
        )
        & (click_tracking_df["Link"].str.endswith("/"))
    ].loc[:, ["Keyword", "Country", "Date", "Link", "Source"]]

    # Convert country names to ISO2 codes
    cc = coco.CountryConverter()
    click_tracking_df["Country"] = click_tracking_df["Country"].apply(
        cc.convert, to="ISO2"
    )

    # Return the processed DataFrame
    return click_tracking_df


def process_click_tracking_data_serpclix(
    click_tracking_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Processes the click tracking data for SerpClix and returns a cleaned DataFrame.

    Args:
        click_tracking_df (pd.DataFrame): A DataFrame containing click tracking data for SerpClix.

    Returns:
        pd.DataFrame: A cleaned DataFrame containing only the "Keyword", "Country", "Date", "Link", and "Source" columns.
    """

    # Create a column "Source" with the value "SerpClix"
    click_tracking_df["Source"] = "SerpClix"

    # Rename "Clicker Country" column to "Country"
    click_tracking_df = click_tracking_df.rename(columns={"Clicker Country": "Country"})

    # Convert all values from column Country into ISO2 codes
    country_converter = coco.CountryConverter()
    click_tracking_df.loc[:, "Country"] = click_tracking_df["Country"].apply(
        country_converter.convert, to="ISO2"
    )

    # Rename column "Timestamp" to "Date"
    click_tracking_df = click_tracking_df.rename(columns={"Timestamp": "Date"})

    # Rename column "URL" to "Link"
    click_tracking_df = click_tracking_df.rename(columns={"URL": "Link"})

    # Convert "Date" column to datetime values YYYY-MM-DD
    click_tracking_df["Date"] = pd.to_datetime(
        click_tracking_df["Date"], errors="coerce"
    )

    # Convert "Date" column to string values YYYY-MM-DD
    click_tracking_df["Date"] = click_tracking_df["Date"].dt.strftime("%Y-%m-%d")

    # Drop rows if "Link" values do not contain "/"
    click_tracking_df = click_tracking_df.loc[
        click_tracking_df["Link"].str.contains("/")
    ]

    # Add a "/" to the end of the "Link" column if it doesn't already end with one
    click_tracking_df.loc[~click_tracking_df["Link"].str.endswith("/"), "Link"] += "/"

    # Add "http://" to "Link" values that do not have "http" or "https"
    click_tracking_df["Link"] = click_tracking_df["Link"].apply(
        lambda x: "https://" + x if "http" not in x else x
    )

    # Add "/" to the end of "Link" values that do not end with "/"
    click_tracking_df["Link"] = click_tracking_df["Link"].apply(
        lambda x: x + "/" if x[-1] != "/" else x
    )

    # Drop columns other than "Keyword", "Country", "Date", "Link", and "Source"
    click_tracking_df = click_tracking_df[
        ["Keyword", "Country", "Date", "Link", "Source"]
    ]

    # Drop rows with NaN values in Date column
    click_tracking_df = click_tracking_df.dropna(subset=["Date"])

    # Return the cleaned DataFrame
    return click_tracking_df


def add_date_range_column(
    date_ranges: list, df: pd.DataFrame, date_column: str
) -> pd.DataFrame:
    """
    Adds a date_range column to a pandas DataFrame based on a list of date ranges.

    Parameters:
    -----------
    date_ranges : list
        A list of tuples, where each tuple contains two datetime objects that represent the start and end of a date range.
    df : pandas.DataFrame
        The DataFrame to add the date_range column to.
    date_column : str
        The name of the column in the DataFrame that contains the dates.

    Returns:
    --------
    pandas.DataFrame
        The DataFrame with the added date_range column.
    """

    df[date_column] = pd.to_datetime(
        df[date_column]
    )  # parse date_column as a date type

    df["date_range"] = df[date_column].apply(
        lambda date: next(
            (
                f"{date_range[1].date()} - {date_range[0].date()}"
                for date_range in date_ranges
                if date >= date_range[1] and date <= date_range[0]
            ),
            "N/A",
        )
    )

    return df


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


def get_click_data_df() -> pd.DataFrame:
    date_ranges = get_date_ranges()

    # ahrefs_domain = "https://docs.google.com/spreadsheets/d/1K7RfT4x8rjZyEN6M3pmwZspUpL1QFqnjsSgLQyqXFYs/edit#gid=437054005"
    # filter_rules = "https://docs.google.com/spreadsheets/d/1uBsysJd1XTtOftpD04W_vlWDRXczzeESmbS51DP0U_0/edit#gid=0"
    in_house_link_clicking = "https://docs.google.com/spreadsheets/d/124YEzAPOtR3UFT-KfG9IAWrcJr67icSPU475opYSaw8/edit#gid=1743911824"
    serpclix_link_clicking = "https://docs.google.com/spreadsheets/d/186V5aIS4cNqhlFI_--0uqSQUMzrzjp_OtVCXZ1xVVoE/edit#gid=0"

    # download_gsheet(ahrefs_domain, "gsheet/ahrefs_domain.csv")
    # download_gsheet(filter_rules, "gsheet/filter_rules.csv")
    download_gsheet(in_house_link_clicking, "gsheet/in_house_link_clicking.csv")
    download_gsheet(serpclix_link_clicking, "gsheet/serpclix_link_clicking.csv")

    # ahrefs_domain_df = pd.read_csv("gsheet/ahrefs_domain.csv", sep=",", encoding="utf-8")
    # filter_rules_df = pd.read_csv("gsheet/filter_rules.csv", sep=",", encoding="utf-8")
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

    #### Serpclix Click Tracking Data Extraction

    click_tracking_data_serpclix = process_click_tracking_data_serpclix(
        serpclix_link_clicking_df
    )

    #### Merge Click Tracking Data

    # merge the two click tracking dataframes
    merged_click_tracking_df = pd.concat(
        [in_house_link_clicking_df, click_tracking_data_serpclix]
    )

    click_data_df = add_date_range_column(date_ranges, merged_click_tracking_df, "Date")

    ### Generate click data
    click_data_df = clean_date_range_df(click_data_df)

    # click_data_df.to_csv("click_data.csv", index=False)

    return click_data_df


# get_click_data_df()
