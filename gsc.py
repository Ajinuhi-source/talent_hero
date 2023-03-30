import time
import country_converter as coco
from searchconsole import authenticate
from pathlib import Path
import pandas as pd
import tldextract
from tqdm import tqdm
from utils import get_date_ranges
from utils import download_gsheet
from country_converter import CountryConverter
from utils import add_domain_tld_column
from utils import add_date_range_column_and_clean


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


def find_matching_webproperty(domain, gsc_webproperty_list):
    """
    Find matching web property for a domain.
    The search is performed in the following order:
    1. sc-domain:<domain>
    2. https://<domain>/
    3. http://<domain>/
    """
    search_conditions = [
        f"sc-domain:{domain}",
        f"https://{domain}/",
        f"http://{domain}/",
    ]

    for condition in search_conditions:
        matching_properties = [wp for wp in gsc_webproperty_list if condition in wp]
        if matching_properties:
            return matching_properties[0]

    print(f"No matching web property found for {domain}")
    return None


def get_viable_gsc_webpropriety_list(gsc_webproperty_list, domain_list):
    """
    Find viable Google Search Console web properties for a list of domains.
    Performs up to 12 attempts to find web properties for each domain.
    Deduplicates the list of viable web properties.
    """
    viable_web_properties = []
    for _ in range(1):
        for domain in domain_list:
            web_property = find_matching_webproperty(domain, gsc_webproperty_list)
            if web_property:
                viable_web_properties.append(web_property)
        # Sleep for 1 second to avoid hitting the API rate limit
        time.sleep(1)
    # Deduplicate the list while preserving order
    viable_web_properties = list(dict.fromkeys(viable_web_properties))
    return viable_web_properties


def gsc_viable_web_proprieties(viable_web_properties, ahrefs_domains_df):
    # create a column from viable_web_properties named "gsc_web_property"
    gsc_helper_df = pd.DataFrame(viable_web_properties, columns=["gsc_web_property"])

    # create a colum 'domain' that extracts the domain from the 'gsc_web_property' column using tldextract
    gsc_helper_df["domain"] = gsc_helper_df["gsc_web_property"].apply(
        lambda x: tldextract.extract(x).domain
    )

    return gsc_helper_df


def add_date_range_to_ahrefs_domains(df, date_ranges):
    # convert each element in the date_ranges to YYYY-MM-DD

    date_ranges = [
        f"{date_range[0].strftime('%Y-%m-%d')} - {date_range[1].strftime('%Y-%m-%d')}"
        for date_range in date_ranges
    ]

    # convert to string
    date_ranges = [str(date_range) for date_range in date_ranges]
    date_ranges = str(date_ranges)
    date_ranges = date_ranges.replace("]", "")
    date_ranges = date_ranges.replace("[", "")
    date_ranges = date_ranges.replace("'", "")

    # create a colum 'date_range' that has the value 'date_ranges'
    df["date_range"] = str(date_ranges)

    # Split the 'date_range' column by comma and explode the resulting list
    df = df.assign(date_range=df["date_range"].str.split(",")).explode("date_range")

    # split column date_range into two columns "start_date" and "end_date". Split by ' - '
    df[["start_date", "end_date"]] = df["date_range"].str.split(" - ", expand=True)

    # remove empty spaces from the 'start_date' and 'end_date' columns
    df["start_date"] = df["start_date"].str.strip()

    # drop the 'date_range' column
    df = df.drop(columns=["date_range"])

    return df


def explode_date_range(df):
    # explode the 'date_range' column
    df = df.explode("date_range")

    return df


def merge_viable_web_proprieties_with_ahrefs_domains(gsc_helper_df, ahrefs_domains_df):
    # merge the gsc_helper_df with the ahrefs_domains_df on the 'domain' column
    merged_df = pd.merge(
        gsc_helper_df,
        ahrefs_domains_df,
        how="right",
        on="domain",
    )

    return merged_df


def explode_countries(data_frame):
    """
    This function takes a DataFrame with a 'Country' column
    and returns a new DataFrame where each row has only one country.
    """
    # Remove whitespace from the 'Country' column
    data_frame["Country"] = data_frame["Country"].str.strip()

    # Split the 'Country' column by comma and explode the resulting list
    exploded_df = data_frame.assign(
        Country=data_frame["Country"].str.split(",")
    ).explode("Country")

    return exploded_df


def get_gsc_dataframes(
    account, web_property, start_date, end_date, country, includingRegex, excludingRegex
):
    """
    Get Google Search Console dataframes for a given account, date ranges, and web property.
    Performs a GSC query for each date range and appends start and end date columns.
    Removes duplicates, extracts the domain from the page URL, removes invalid country codes, and converts country names to ISO2 codes.

    Args:
        account (google.oauth2.service_account.Credentials): The GSC account credentials.
        date_ranges (list): A list of date range tuples (start_date, end_date) to query.
        viable_gsc_domain (str): The viable web property to query.
    Returns:
        pandas.DataFrame: The concatenated and transformed GSC dataframes.
    """
    web_property = account[web_property]

    gsc_df = (
        web_property.query.search_type("web")
        .range(start_date, days=-28)
        .dimension("query", "page", "country")
        .filter("country", country, "equals")
        .filter(
            "query",
            "(job|energy resourcing|archipro|vetted|180|bemana|bristol|caltek|energists|lock|mangrum|nexus|pender|quantum|redfish|summit|surf search)",
            "excludingRegex",
        )
        .filter(
            "query",
            "(staff|recruit|energy|oil|gas|power|renewable|mining|chemical|construction|technology|architect|interior design|landscape|engineer|project|program|database|helpdesk|system admin|systems admin)",
            "includingRegex",
        )
        .limit(100)
        .get()
        .to_dataframe()
    )
    gsc_df["start_date"] = start_date
    gsc_df["end_date"] = end_date

    print(f"Number of rows: {len(gsc_df)}")

    return gsc_df


def drop_fake_countries(df):
    # Filter out rows containing "zzz" or "xkk" in the "country" column
    df = df[~df["country"].str.contains("zzz|xkk")]

    # Return the filtered DataFrame
    return df


def drop_duplicates_based_on_columns(df, columns_to_drop):
    # Drop duplicate rows based on specified columns
    df = df.drop_duplicates(subset=columns_to_drop, keep="first")

    # Return the updated DataFrame
    return df


def convert_country_to_format(df, format="ISO2", column_name="country"):
    """
    Converts country names to ISO2 codes using country_converter.

    Parameters:
    df (pandas.DataFrame): The DataFrame containing country names
    column_name (str): The name of the column containg country names. Default is 'country'.

    Returns:
    pandas.DataFrame: The updated DataFrame with the converted country names in the specified column.
    """
    # Create a CountryConverter object
    cc = CountryConverter()

    # Use apply() method to convert country names to ISO2 codes
    df[column_name] = df[column_name].apply(cc.convert, to=format)

    return df


def combine_start_date_end_date(df):
    """
    Combine start_date and end_date columns into a single column.
    """
    df["start_date"] = pd.to_datetime(df["start_date"])
    df["end_date"] = pd.to_datetime(df["end_date"])
    df["date_range"] = (
        df["start_date"].dt.strftime("%Y-%m-%d")
        + " - "
        + df["end_date"].dt.strftime("%Y-%m-%d")
    )
    return df


def drop_start_date_end_date(df):
    """
    Drop start_date and end_date columns.
    """
    df = df.drop(columns=["start_date", "end_date"])
    return df


def drop_ctr(df):
    """
    Drop ctr column.
    """
    df = df.drop(columns=["ctr"])
    return df


def extract_gsc_web_properties(account):
    """
    Extracts the list of web properties from a Google Search Console account.

    Args:
        account (GoogleSearchConsoleClient): A GoogleSearchConsoleClient instance representing the GSC account.

    Returns:
        List: A list of web properties in the GSC account.
    """
    gsc_web_property_list = []
    for i in tqdm(range(10), desc="Extracting web properties..."):
        gsc_web_property_list += extract_gsc_accounts_webproperty_list(account)

    # Remove duplicates from the list of web properties
    gsc_web_property_list = list(set(gsc_web_property_list))

    return gsc_web_property_list


def extract_gsc_accounts_webproperty_list(account_list):
    """
    Extract a list of web properties from a list of Google Search Console accounts.
    Removes duplicate web properties from the list.
    """
    web_property_list = [account.url for account in account_list]
    # Remove duplicates from the list while preserving order
    web_property_list = list(dict.fromkeys(web_property_list))
    return web_property_list


def extract_unique_ahrefs_domains(df):
    """
    Extracts a list of unique domains from the Ahrefs DataFrame.
    """
    df = df["Domain"].unique()
    return df


def create_gsc_webpropriety_df(gsc_webpropriety_list):
    """
    This function takes a list of web properties from Google Search Console (GSC),
    creates a DataFrame with 'web_property' and 'domain' columns,
    extracts domain and extension using tldextract library,
    and saves it to CSV file named 'gsc_webpropriety_df.csv'
    """
    # Create a DataFrame with 'web_property' and 'domain' columns
    gsc_webpropriety_df = pd.DataFrame(
        {"web_property": gsc_webpropriety_list, "domain": ""}
    )

    # Remove prefix string 'sc-domain:' from 'web_property' column
    gsc_webpropriety_df["domain"] = gsc_webpropriety_df["web_property"].str.replace(
        "sc-domain:", ""
    )

    # Extract domain and extension from 'domain' column using tldextract library
    gsc_webpropriety_df["domain"] = gsc_webpropriety_df["domain"].apply(
        lambda x: tldextract.extract(x).domain + "." + tldextract.extract(x).suffix
    )

    # Save the DataFrame to CSV file without index column
    gsc_webpropriety_df.to_csv("gsc_webpropriety_df.csv", index=False)

    return gsc_webpropriety_df


def merge_and_clean_data(ahrefs_domains_df, gsc_webpropriety_df):
    """
    This function takes two DataFrames, 'ahrefs_domains_df' and 'gsc_webpropriety_df',
    merges them on 'Domain' and 'domain' columns,
    drops the 'domain' column, and removes rows where 'web_property' column is empty.
    It returns a new cleaned DataFrame 'merged_webpropriety_df'.
    """
    # Merge 'ahrefs_domains_df' and 'gsc_webpropriety_df'
    merged_webpropriety_df = pd.merge(
        ahrefs_domains_df,
        gsc_webpropriety_df,
        how="left",
        left_on="Domain",
        right_on="domain",
    )

    # Drop the 'domain' column
    merged_webpropriety_df = merged_webpropriety_df.drop(columns=["domain"])

    # Remove rows with empty 'web_property' column
    merged_webpropriety_df = merged_webpropriety_df[
        merged_webpropriety_df["web_property"].notna()
    ]

    return merged_webpropriety_df


def extract_rows_as_lists(df, column_names):
    return df[column_names].values.tolist()


def convert_to_numbers(df):
    # convert column 'clicks', 'impressions' to int
    df["clicks"] = df["clicks"].astype(int)
    df["impressions"] = df["impressions"].astype(int)

    # convert column 'position' to float with 1 decimal place
    df["position"] = df["position"].astype(float).round(1)

    return df


def get_gsc_data_df():
    # Get date ranges
    date_ranges = get_date_ranges()
    # print(f"Date ranges: {date_ranges}")

    # Authenticate Google Search Console account
    creds_path = "api/credentials.json"
    account = authenticate_account(creds_path)

    # Download domain list from Google Sheet
    ahrefs_domains = "https://docs.google.com/spreadsheets/d/1K7RfT4x8rjZyEN6M3pmwZspUpL1QFqnjsSgLQyqXFYs/edit#gid=437054005"
    download_gsheet(ahrefs_domains, "gsheet/ahrefs_domain.csv")

    ahrefs_domains_df = pd.read_csv(
        "gsheet/ahrefs_domain.csv", sep=",", encoding="utf-8"
    )
    ahrefs_domains_df = explode_countries(ahrefs_domains_df)

    ahrefs_domains_df = add_date_range_to_ahrefs_domains(ahrefs_domains_df, date_ranges)

    ahrefs_domains_df = convert_country_to_format(
        ahrefs_domains_df, format="ISO3", column_name="Country"
    )

    # saveto csv
    ahrefs_domains_df.to_csv("ahrefs_domain_processed.csv", index=False)

    domain_list = extract_unique_ahrefs_domains(ahrefs_domains_df)

    # Extract Google Search Console web properties
    gsc_webpropriety_list = []
    for i in tqdm(range(10), desc="Extracting web properties..."):
        gsc_webpropriety_list += extract_gsc_accounts_webproperty_list(account)

    # Remove duplicates from the list of web properties
    gsc_webpropriety_list = list(set(gsc_webpropriety_list))
    print(gsc_webpropriety_list)

    gsc_webpropriety_df = create_gsc_webpropriety_df(gsc_webpropriety_list)

    viable_gsc_domains_df = merge_and_clean_data(ahrefs_domains_df, gsc_webpropriety_df)

    # save to csv
    viable_gsc_domains_df.to_csv("viable_gsc_domains_df.csv", index=False)

    lists_from_rows = extract_rows_as_lists(
        viable_gsc_domains_df, ["web_property", "start_date", "end_date", "Country"]
    )

    # Extract dataframes for each viable web property
    domains_df = []
    for viable_gsc_domain in tqdm(lists_from_rows, desc="Extracting dataframes..."):
        domains_df.append(
            get_gsc_dataframes(
                account,
                web_property=viable_gsc_domain[0],
                start_date=viable_gsc_domain[1],
                end_date=viable_gsc_domain[2],
                country=viable_gsc_domain[3],
            )
        )

    # Concatenate dataframes and drop duplicates
    if len(domains_df) > 0:
        gsc_df = pd.concat(domains_df)

        # Save dataframe to csv
        gsc_df.to_csv("gsc_df_raw.csv", index=False, sep="\t", encoding="utf-8")

        gsc_df = drop_duplicates_based_on_columns(
            gsc_df, ["page", "start_date", "end_date", "country"]
        )

        # Clean up data
        gsc_df = drop_fake_countries(gsc_df)
        gsc_df = convert_country_to_format(gsc_df, format="ISO2", column_name="country")
        gsc_df = add_domain_tld_column(
            gsc_df, url_column_name="page", new_column_name="domain"
        )

        gsc_df = drop_ctr(gsc_df)
        gsc_df = convert_to_numbers(gsc_df)
        gsc_df = combine_start_date_end_date(gsc_df)
        gsc_df = drop_start_date_end_date(gsc_df)

        # Save dataframe to csv
        gsc_df.to_csv("gsc_df.csv", index=False, sep="\t", encoding="utf-8")
    else:
        print("Domains not found in GSC")
        gsc_df = None

    return gsc_df


# get_gsc_data_df()


def explode_all_domains(df):
    # Get all unique domain values
    unique_domains = df["domain"].unique()

    # If "all" is in unique_domains, explode it into all unique domains
    if "all" in unique_domains:
        df = df.drop(columns="domain").merge(
            pd.DataFrame({"domain": unique_domains[unique_domains != "All"]}),
            how="right",
        )

    return df


# Download domain list from Google Sheet
filter_rules = "https://docs.google.com/spreadsheets/d/1uBsysJd1XTtOftpD04W_vlWDRXczzeESmbS51DP0U_0/edit#gid=0"
download_gsheet(filter_rules, "gsheet/filter_rules.csv")

filter_rules_df = pd.read_csv("gsheet/filter_rules.csv", sep=",", encoding="utf-8")
# convert all columns to lowercase
filter_rules_df.columns = filter_rules_df.columns.str.lower()

# remove epmty space at beggining of string in all columns
filter_rules_df = filter_rules_df.apply(
    lambda x: x.str.strip() if x.dtype == "object" else x
)

# in column "domain" if value is exactly "all" then replace with empty string then
filter_rules_df = explode_all_domains(filter_rules_df)

# save to csv
filter_rules_df.to_csv("filter_rules_df.csv", sep="\t", index=False, encoding="utf-8")
