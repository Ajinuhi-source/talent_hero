import time
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
from country_converter import CountryConverter


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


def get_gsc_dataframes(account, date_ranges, viable_gsc_domain):
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
    gsc_dfs = []
    web_property = account[viable_gsc_domain]

    for start_date, end_date in date_ranges:
        gsc_df = (
            web_property.query.search_type("web")
            .range(start_date, days=-28)
            .dimension("query", "page", "country")
            .limit(1000)
            .get()
            .to_dataframe()
        )
        gsc_df["start_date"] = start_date
        gsc_df["end_date"] = end_date
        gsc_dfs.append(gsc_df)

    gsc_df = pd.concat(gsc_dfs)

    # drop rows in "country" column that countain the string "zz" and "xkk"
    gsc_df = gsc_df[~gsc_df["country"].str.contains("zzz|xkk")]

    # Drop duplicates when 'page', 'start_date', 'end_date', and 'country' are the same
    gsc_df = gsc_df.drop_duplicates(
        subset=["page", "start_date", "end_date", "country"], keep="first"
    )

    # Extract the domain from the URL and put it in a new column named "domain"+"tld"
    gsc_df["domain"] = gsc_df["page"].apply(
        lambda x: tldextract.extract(x).domain + "." + tldextract.extract(x).suffix
    )

    # Remove rows where the country column is invalid or Kosovo
    # valid_country_codes = get_valid_country_codes()
    # gsc_df = gsc_df[gsc_df["country"].isin(valid_country_codes)]

    # Convert country names to ISO2 codes using country_converter
    cc = CountryConverter()
    gsc_df["country"] = gsc_df["country"].apply(cc.convert, to="ISO2")

    return gsc_df


def extract_gsc_accounts_webproperty_list(account_list):
    """
    Extract a list of web properties from a list of Google Search Console accounts.
    Removes duplicate web properties from the list.
    """
    web_property_list = [account.url for account in account_list]
    # Remove duplicates from the list while preserving order
    web_property_list = list(dict.fromkeys(web_property_list))
    return web_property_list


def nomalize_domains_country_filter_df(df):
    """
    Normalize the country column in a dataframe by splitting it into separate rows
    and transforming country names to ISO2 codes using country_converter.

    Args:
        df (pandas.DataFrame): The input dataframe.
    Returns:
        pandas.DataFrame: The transformed dataframe.
    """
    df = _explode_country_column(df)
    df = _transform_country_to_iso2(df)
    return df


def _explode_country_column(df) -> pd.DataFrame:
    """
    Split the country column into separate rows and strip whitespace.

    Args:
        df (pandas.DataFrame): The input dataframe.
    Returns:
        pandas.DataFrame: The transformed dataframe.
    """
    country_column = "Country"

    # Split comma-separated countries into separate lists
    df[country_column] = df[country_column].str.split(",")
    # Strip whitespace from each country name
    df[country_column] = df[country_column].apply(
        lambda x: [item.strip() for item in x]
    )

    # Explode the country column into separate rows
    df = df.explode(country_column)

    # Save df to CSV for debugging purposes
    df.to_csv("explode_country.csv", index=False)

    return df


def _transform_country_to_iso2(df) -> pd.DataFrame:
    """
    Transform the country column to ISO2 codes using country_converter.

    Args:
        df (pandas.DataFrame): The input dataframe.
    Returns:
        pandas.DataFrame: The transformed dataframe.
    """
    country_column = "Country"
    # Use country_converter to transform country names to ISO2 codes
    df[country_column] = coco.convert(names=df[country_column], to="ISO2")
    return df


def get_gsc_data_df():
    date_ranges = get_date_ranges()

    creds_path = "api/credentials.json"
    account = authenticate_account(creds_path)

    ahrefs_domains = "https://docs.google.com/spreadsheets/d/1K7RfT4x8rjZyEN6M3pmwZspUpL1QFqnjsSgLQyqXFYs/edit#gid=437054005"
    download_gsheet(ahrefs_domains, "gsheet/ahrefs_domain.csv")

    domain_list = pd.read_csv("gsheet/ahrefs_domain.csv")

    # Select the "Domain" column, remove duplicates, and convert to a list
    domain_list = domain_list["Domain"].tolist()

    print(domain_list)

    gsc_webpropriety_list1 = extract_gsc_accounts_webproperty_list(account)
    gsc_webpropriety_list2 = extract_gsc_accounts_webproperty_list(account)
    gsc_webpropriety_list3 = extract_gsc_accounts_webproperty_list(account)
    gsc_webpropriety_list4 = extract_gsc_accounts_webproperty_list(account)
    gsc_webpropriety_list5 = extract_gsc_accounts_webproperty_list(account)
    gsc_webpropriety_list6 = extract_gsc_accounts_webproperty_list(account)
    gsc_webpropriety_list7 = extract_gsc_accounts_webproperty_list(account)
    gsc_webpropriety_list8 = extract_gsc_accounts_webproperty_list(account)
    gsc_webpropriety_list9 = extract_gsc_accounts_webproperty_list(account)
    gsc_webpropriety_list10 = extract_gsc_accounts_webproperty_list(account)

    # merge all dataframes into one
    gsc_webpropriety_list = (
        gsc_webpropriety_list1
        + gsc_webpropriety_list2
        + gsc_webpropriety_list3
        + gsc_webpropriety_list4
        + gsc_webpropriety_list5
        + gsc_webpropriety_list6
        + gsc_webpropriety_list7
        + gsc_webpropriety_list8
        + gsc_webpropriety_list9
        + gsc_webpropriety_list10
    )

    gsc_webpropriety_list = list(dict.fromkeys(gsc_webpropriety_list))

    print("GSC Webproperty List: ", gsc_webpropriety_list)

    viable_gsc_domains_list = []
    viable_gsc_domains_list = get_viable_gsc_webpropriety_list(
        gsc_webpropriety_list, domain_list
    )

    domains_df = []
    for viable_gsc_domain in tqdm(viable_gsc_domains_list):
        domains_df.append(get_gsc_dataframes(account, date_ranges, viable_gsc_domain))

    if len(domains_df) > 0:
        gsc_df = pd.concat(domains_df)
    else:
        print("Domains not found in GSC")

    # save the dataframe to csv
    gsc_df.to_csv("gsc_df.csv", index=False)

    return gsc_df


# get_gsc_data_df()
