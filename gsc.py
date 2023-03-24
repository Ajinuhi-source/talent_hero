import datetime
import time
from click import DateTime
import country_converter as coco
from searchconsole import authenticate
from pathlib import Path
import pandas as pd
import tldextract
import pandas as pd
import tldextract
import os
import requests
from tqdm import tqdm


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


def authenticate_account(creds_path: str):
    """Authenticate a Google Search Console account with the provided credentials file path.

    Args:
        creds_path (str): The file path to the credentials file.

    Returns:
        authenticated account: An authenticated account object that can be used to make requests to the Search Console API.
    """
    if Path(creds_path).is_file():
        account = authenticate(
            client_config="api/client_secrets.json", credentials=creds_path
        )
    else:
        account = authenticate(
            client_config="api/client_secrets.json", serialize=creds_path
        )

    return account


def find_matching_webproperty(domain, gsc_webpropriety_list):
    """Find matching webproperty for a domain"""
    for wp in gsc_webpropriety_list:
        if f"sc-domain:{domain}" in wp.url:
            return f"sc-domain:{domain}"
    for wp in gsc_webpropriety_list:
        if f"https://{domain}/" in wp.url:
            return f"https://{domain}/"
    for wp in gsc_webpropriety_list:
        if f"http://{domain}/" in wp.url:
            return f"http://{domain}/"
    # print(f"No matching webproperty found for {domain}")
    return None


def get_viable_gsc_webpropriety_list(gsc_webpropriety_list, domain_list):
    viable_gsc_domains_list = []
    i = 0
    for i in range(12):
        for domain in domain_list:
            wp = find_matching_webproperty(domain, gsc_webpropriety_list)
            if wp:
                viable_gsc_domains_list.append(wp)
    # sleep 1 second to avoid hitting the API rate limit
    time.sleep(1)
    i += 1
    # remove duplicates elements from the list
    viable_gsc_domains_list = list(dict.fromkeys(viable_gsc_domains_list))

    return viable_gsc_domains_list


def get_gsc_dataframes(account, date_ranges, viable_gsc_domain):
    # get GSC dataframes for each date range and append start and end date columns
    gsc_dfs = []
    webproperty = account[viable_gsc_domain]
    for i, date_range in enumerate(date_ranges):
        start_date, end_date = date_range
        gsc_df = (
            webproperty.query.search_type("web")
            .range(start_date, days=-28)
            .dimension("query", "page", "country")
            .limit(100)
            .get()
            .to_dataframe()
        )
        gsc_df["start_date"] = start_date
        gsc_df["end_date"] = end_date
        gsc_dfs.append(gsc_df)

    # merge all dataframes into one
    gsc_df = pd.concat(gsc_dfs)

    # remove duplicates when 'page' and 'start date' and 'end date' are the same
    gsc_df = gsc_df.drop_duplicates(
        subset=["query", "page", "country", "start_date", "end_date"], keep="first"
    )

    # using tldextract extract the domain from the url and put it in a new column named "domain"+ "tld"
    gsc_df["domain"] = gsc_df["page"].apply(
        lambda x: tldextract.extract(x).domain + "." + tldextract.extract(x).suffix
    )

    # drop rows where colum country is "zzz"
    gsc_df = gsc_df[gsc_df["country"] != "zzz"]
    # drop rows where colum country is "xkk" Kosovo
    gsc_df = gsc_df[gsc_df["country"] != "xkk"]

    cc = coco.CountryConverter()

    gsc_df["country"] = gsc_df["country"].apply(cc.convert, to="ISO2")

    return gsc_df


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


def explode_country_column_lists(df) -> pd.DataFrame:
    """
    Split the country column into separate rows and strip whitespace.

    Args:
        df (pandas.DataFrame): The input dataframe.
    Returns:
        pandas.DataFrame: The transformed dataframe.
    """

    country_column = "Country"

    # Convert to to list on "," and strip whitespace
    df[country_column] = (
        df[country_column].str.split(",").apply(lambda x: [item.strip() for item in x])
    )
    # Split countries into separate rows
    df = df.explode(country_column)

    return df


# function to transform the country column to iso2 using country_converter as coco
def transform_country_column_to_iso2(df) -> pd.DataFrame:
    """
    Transform the country column to iso2 using country_converter as coco.

    Args:
        df (pandas.DataFrame): The input dataframe.
    Returns:
        pandas.DataFrame: The transformed dataframe.
    """

    country_column = "Country"

    # Transform country to iso2
    df[country_column] = df[country_column].apply(lambda x: coco.convert(x, to="ISO2"))

    return df


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
        ["Keyword", "Country", "Date", "Link"]
    )["Link"].transform("count")

    # Drop duplicates from the DataFrame based on the columns ["Keyword", "Country", "Date", "Link"]
    click_data_df = click_data_df.drop_duplicates(
        subset=["Keyword", "Country", "Date", "Link"]
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
        how="left",
        on=["query", "page", "country", "start_date", "end_date", "domain"],
    )

    # create a new column "final clicks" with the diffrence between "clicks" and the "simulated clicks", if "simulated clicks" is NaN, then "final clicks" = "clicks"
    merged_df["final clicks"] = merged_df.apply(
        lambda row: row["clicks"] - row["simulated clicks"]
        if not pd.isnull(row["simulated clicks"])
        else row["clicks"],
        axis=1,
    )

    return merged_df


def nomalize_domains_country_filter_df(df):
    explode_country_column_df = explode_country_column_lists(df)
    transform_country_column_to_iso2_df = transform_country_column_to_iso2(
        explode_country_column_df
    )
    return transform_country_column_to_iso2_df


def extract_domain(df):
    df = nomalize_domains_country_filter_df(df)
    df = df[["Domain"]]
    df = df.drop_duplicates()
    df = df.values.tolist()
    df = [item for sublist in df for item in sublist]
    return df


def extract_gsc_accounts_webpropriety_list(account):
    return list(account)


#### Download needed Google Sheets
ahrefs_domain = "https://docs.google.com/spreadsheets/d/1K7RfT4x8rjZyEN6M3pmwZspUpL1QFqnjsSgLQyqXFYs/edit#gid=437054005"
filter_rules = "https://docs.google.com/spreadsheets/d/1uBsysJd1XTtOftpD04W_vlWDRXczzeESmbS51DP0U_0/edit#gid=0"
in_house_link_clicking = "https://docs.google.com/spreadsheets/d/124YEzAPOtR3UFT-KfG9IAWrcJr67icSPU475opYSaw8/edit#gid=1743911824"
serpclix_link_clicking = "https://docs.google.com/spreadsheets/d/186V5aIS4cNqhlFI_--0uqSQUMzrzjp_OtVCXZ1xVVoE/edit#gid=0"

download_gsheet(ahrefs_domain, "gsheet/ahrefs_domain.csv")
download_gsheet(filter_rules, "gsheet/filter_rules.csv")
download_gsheet(in_house_link_clicking, "gsheet/in_house_link_clicking.csv")
download_gsheet(serpclix_link_clicking, "gsheet/serpclix_link_clicking.csv")

ahrefs_domain_df = pd.read_csv("gsheet/ahrefs_domain.csv", sep=",", encoding="utf-8")
filter_rules_df = pd.read_csv("gsheet/filter_rules.csv", sep=",", encoding="utf-8")

####GSC DF Extraction

date_ranges = get_date_ranges()

creds_path = "api/credentials.json"
account = authenticate_account(creds_path)

domains = pd.read_csv("gsc/domains/domains.csv", sep=",", encoding="utf-8")
domain_list = extract_domain(domains)

gsc_webpropriety_list = extract_gsc_accounts_webpropriety_list(account)

viable_gsc_domains_list = []
viable_gsc_domains_list = get_viable_gsc_webpropriety_list(
    gsc_webpropriety_list, domain_list
)


print(viable_gsc_domains_list)

domains_df = []
for viable_gsc_domain in tqdm(viable_gsc_domains_list):
    domains_df.append(get_gsc_dataframes(account, date_ranges, viable_gsc_domain))

if len(domains_df) > 0:
    gsc_df = pd.concat(domains_df)
else:
    print("Domains not found in GSC")

# save to csv
gsc_df.to_csv("gsc/gsc.csv", index=False, encoding="utf-8", sep="\t")


gsc_df_filtered = gsc_df[
    gsc_df.apply(lambda row: apply_filter(row, filter_rules_df), axis=1)
]

# save to csv
gsc_df_filtered.to_csv("gsc/gsc_filtered.csv", index=False, encoding="utf-8", sep="\t")

#### Manual Click Tracking Data Extraction

in_house_link_clicking_df = pd.read_csv(
    "gsheet/in_house_link_clicking.csv", sep=",", encoding="utf-8"
)
in_house_link_clicking_df = process_in_house_link_clicking_df(in_house_link_clicking_df)
# save to csv
in_house_link_clicking_df.to_csv(
    "click_data/processed_in_house_link_clicking.csv",
    index=False,
    encoding="utf-8",
    sep="\t",
)

#### Serpclix Click Tracking Data Extraction

serpclix_link_clicking_df = pd.read_csv(
    "gsheet/serpclix_link_clicking.csv", sep=",", encoding="utf-8"
)
click_tracking_data_serpclix = process_click_tracking_data_serpclix(
    serpclix_link_clicking_df
)
# save to csv
click_tracking_data_serpclix.to_csv(
    "click_data/processed_serpclix_link_clicking.csv",
    index=False,
    encoding="utf-8",
    sep="\t",
)

#### Merge Click Tracking Data

# merge the two click tracking dataframes
merged_click_tracking_df = pd.concat(
    [in_house_link_clicking_df, click_tracking_data_serpclix]
)
# save to csv
merged_click_tracking_df.to_csv(
    "click_data/processed_click_tracking_data.csv",
    index=False,
    encoding="utf-8",
    sep="\t",
)


click_data_df = add_date_range_column(date_ranges, merged_click_tracking_df, "Date")


### Generate click data
click_data_df = clean_date_range_df(click_data_df)

# save to csv
click_data_df.to_csv(
    f"click_data/click_data_range_clean.csv", index=False, encoding="utf-8", sep="\t"
)

### Merge GSC and Click Data

merged_df = merge_gsc_and_click_data(gsc_df_filtered, click_data_df)

# save to csv
merged_df.to_csv("merged_df.csv", index=False, encoding="utf-8", sep="\t")

# convert start_date and end_date to string
merged_df["start_date"] = merged_df["start_date"].astype(str)
merged_df["end_date"] = merged_df["end_date"].astype(str)

# combine start_date and end_date into a single column separated by a :
merged_df["date_range"] = merged_df["start_date"] + ":" + merged_df["end_date"]
# remove the start_date and end_date columns
merged_df = merged_df.drop(["start_date", "end_date"], axis=1)


# save to csv
merged_df.to_csv("merged_df.csv", index=False, encoding="utf-8", sep="\t")
