from pivoted_db import gen_db_df
import streamlit as st
import pandas as pd

from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build


def get_credentials():
    SCOPES = ["https://www.googleapis.com/auth/webmasters.readonly"]
    cred = None
    try:
        cred = Credentials.from_authorized_user_file("credentials.json", SCOPES)
    except:
        pass

    if not cred:
        flow = InstalledAppFlow.from_client_secrets_file(
            "api/client_secrets.json", SCOPES
        )
        cred = flow.run_local_server(port=0)
        with open("api/credentials.json", "w") as f:
            f.write(cred.to_json())
    return cred


def sign_in():
    st.set_page_config(page_title="GSC Viewer - Sign In")
    st.title("Sign In with Google")

    # Create an OAuth 2.0 flow object
flow = InstalledAppFlow.from_client_config(
    creds.client_config, scopes=SCOPES, redirect_uri=creds.client_config["redirect_uris"][0]
)

    # Check if user is already authorized
    try:
        credentials = Credentials.from_authorized_user_info(info=None, scopes=API_SCOPES)
    except google.auth.exceptions.DefaultCredentialsError:
        credentials = None

    # If user is already authorized, redirect to main app page
    if credentials:
        st.experimental_set_query_params(google_auth_state=flow._state)
        return st.redirect('/app')

    # If user is not authorized, display sign-in button
    else:
        st.write("Click the button below to sign in with Google.")
        if st.button("Sign in"):
            auth_url, _ = flow.authorization_url(prompt="consent")
            st.experimental_set_query_params(google_auth_state=flow._state)
            return st.redirect(auth_url)

def app():
    st.set_page_config(page_title="GSC Viewer - App")
    st.title("Google Search Console Viewer")

    # Check if user is authorized
    try:
        credentials = Credentials.from_authorized_user_info(info=None, scopes=API_SCOPES)
    except google.auth.exceptions.DefaultCredentialsError:
        st.error("You need to sign in with Google to use this app.")
        return

    # If user is authorized, display GSC data
    st.write("You are signed in with Google.")
    st.write("Here is your Google Search Console data:")

    # Make GSC API requests using the authorized credentials
    # ...

def main():
    if st.experimental_get_query_params().get('google_auth_state'):
        state = st.experimental_get_query_params()['google_auth_state'][0]
        flow = Flow.from_client_config(
            {
                "installed": {
                    "client_id": CLIENT_ID,
                    "client_secret": CLIENT_SECRET,
                    "redirect_uris": [REDIRECT_URI],
                    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                    "token_uri": "https://accounts.google.com/o/oauth2/token",
                }
            },
            scopes=API_SCOPES,
            state=state,
        )
        flow.fetch_token(authorization_response=st.request.url)
        credentials = flow.credentials
        st.experimental_set_query_params(google_auth_state=None)
    else:
        credentials = None

    if st.session_state.current_page == 'sign_in':
        sign_in()
    elif st.session_state.current_page == 'app':
        app()
    else:
        st.session_state.current_page = 'sign_in'
        sign_in()

if __name__ == "__main__":
    st.session_state


# Define the function to generate the dataframe
@st.cache_data
def generate_dataframe():
    """Generates the dataframe by calling the `gen_db_df()` function."""
    return gen_db_df()


@st.cache_data
def convert_dataframe_to_csv(dataframe):
    """Converts the given dataframe to a CSV file."""
    return dataframe.to_csv(index=False, encoding="utf-8", sep="\t").encode("utf-8")


import streamlit as st


def filter_dataframe_with_sliders(dataframe, *column_names):
    """Filters the given dataframe based on selected ranges of multiple columns."""
    ranges = []
    for column_name in column_names:
        # Get range values for the column
        column_min_value = int(dataframe[column_name].min())
        column_max_value = int(dataframe[column_name].max())

        # If the minimum and maximum values are equal, don't filter on that column
        if column_min_value == column_max_value:
            ranges.append((column_min_value, column_max_value))
        else:
            val_range = st.sidebar.slider(
                f"{column_name.capitalize()} Range",
                min_value=column_min_value,
                max_value=column_max_value,
                value=(column_min_value, column_max_value),
            )
            ranges.append(val_range)

    # Filter dataframe based on selected slider ranges
    filter_conditions = []
    for i, column_name in enumerate(column_names):
        if ranges[i][0] != ranges[i][1]:
            filter_conditions.append(
                dataframe[column_name].between(ranges[i][0], ranges[i][1])
            )
    if len(filter_conditions) > 0:
        filtered_dataframe = dataframe[filter_conditions[0]]
        for condition in filter_conditions[1:]:
            filtered_dataframe = filtered_dataframe[condition]
    else:
        filtered_dataframe = dataframe.copy()

    return filtered_dataframe


def regenerate_dataframe_on_button_press(dataframe):
    """Regenerates the dataframe when the button is pressed."""
    if st.sidebar.button("Regenerate DataFrame"):
        dataframe = generate_dataframe()
        domains = dataframe["Domain"].unique()
        st.sidebar.success("DataFrame regenerated")

    return dataframe


# log in logic to google
st.set_page_config(layout="wide")
pd.set_option("display.max_rows", 1000)

# Generate the dataframe
df = generate_dataframe()
# gen mpty dataframe
filtered_dataframe = pd.DataFrame()

# Regenerate the dataframe on button press
df = regenerate_dataframe_on_button_press(df)

# Get domain names for select box
domains = df["Domain"].unique()

# Display a select box of domain options
if len(domains) > 0:
    selected_domain = st.sidebar.selectbox("Select a domain", sorted(domains))
else:
    selected_domain = None

# Filter the dataframe by the selected domain and display it
if df is not None and selected_domain is not None:
    # Set header text
    st.header(f"{selected_domain.capitalize()} Data")

    # Only generate filtered dataframe if it doesn't already exist
    if "filtered_dataframe" not in st.session_state:
        st.session_state.filtered_dataframe = pd.DataFrame()
    if (
        not st.session_state.filtered_dataframe.empty
        and st.session_state.filtered_dataframe["Domain"].iloc[0] == selected_domain
    ):
        filtered_dataframe = st.session_state.filtered_dataframe
    else:
        filtered_dataframe = df[df["Domain"] == selected_domain]
        st.session_state.filtered_dataframe = filtered_dataframe

    filtered_dataframe = filter_dataframe_with_sliders(
        filtered_dataframe, "Adjusted Clicks", "Impressions"
    )
    # Display filtered dataframe
    st.write(filtered_dataframe)

    # Add a button to download the filtered dataframe as a CSV file
    # csv_data = convert_dataframe_to_csv(filtered_dataframe)
    # st.download_button(
    #     label="Download Report as CSV",
    #     data=csv_data,
    #     file_name=f"{selected_domain}_report.csv",
    #     mime="text/csv",
    # )

else:
    st.warning("Please generate the DataFrame and select a domain")
