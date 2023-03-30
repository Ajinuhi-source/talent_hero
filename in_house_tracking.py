import country_converter as coco
import pandas as pd


# Constants
INVALID_URL_ENDINGS = (
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


def extract_country_from_vpn_column(df):
    """
    Extracts country from VPN column.
    """
    df["Country"] = df["VPN"].str.split(" ", expand=True)[0].str.lower()
    return df


def add_source_column_in_house(df):
    """
    Adds source column with value 'clicks_in_house'.
    """
    df["source"] = "clicks_in_house"
    return df


def convert_ranking_to_numeric(df):
    """
    Converts Ranking column to numeric.
    """
    df["Ranking"] = pd.to_numeric(df["Ranking"], errors="coerce")
    return df


def apply_filters(df):
    """
    Applies filters to the DataFrame.
    """
    filtered_df = df[
        (df["Ranking"].notnull())
        & (df["Country"].str.len() <= 2)
        & (df["Link"].str.count("/") > 2)
        & (~df["Link"].fillna("").str.endswith(INVALID_URL_ENDINGS))
        & (df["Link"].str.endswith("/"))
    ].loc[:, ["Keyword", "Country", "Date", "Link", "source"]]
    return filtered_df


def convert_country_names_to_iso2(df):
    """
    Converts country names to ISO2 codes.
    """
    # Convert all values from column Country into ISO2 codes
    country_converter = coco.CountryConverter()
    df.loc[:, "Country"] = df["Country"].apply(country_converter.convert, to="ISO2")
    return df


def remove_empty_space_in_keyword_column(df):
    """
    Removes empty space at the end of the Keyword column.
    """
    df["Keyword"] = df["Keyword"].str.rstrip()
    return df


def rename_columns(df):
    """
    Renames the columns "Keyword" to "query", "Country" to "country", "Date" to "date", "Link" to "page".
    """
    df = df.rename(
        columns={
            "Keyword": "query",
            "Country": "country",
            "Date": "date",
            "Link": "page",
        }
    )
    return df


def process_in_house_link_clicking_df(df):
    """
    Processes click tracking data from in house source.

    Parameters:
    -----------
    df : pandas.DataFrame
        The click tracking data as a pandas DataFrame.

    Returns:
        pd.DataFrame: A cleaned DataFrame containing only the "Keyword", "Country", "Date", "Link", and "Source" columns.
    """
    # Function calls
    df = extract_country_from_vpn_column(df)
    df = add_source_column_in_house(df)
    df = convert_ranking_to_numeric(df)
    df = apply_filters(df)
    df = convert_country_names_to_iso2(df)
    df = remove_empty_space_in_keyword_column(df)
    df = rename_columns(df)

    # save to csv
    # df.to_csv("in_house_clicks.csv", index=False)

    # Return the processed DataFrame
    return df


# in_house_link_clicking = "https://docs.google.com/spreadsheets/d/124YEzAPOtR3UFT-KfG9IAWrcJr67icSPU475opYSaw8/edit#gid=1743911824"
# df = download_gsheet(in_house_link_clicking, "gsheet/in_house_link_clicking.csv")
# # read df
# df = pd.read_csv("gsheet/in_house_link_clicking.csv")
# process_in_house_link_clicking_df(df)
