from pivoted_db import gen_db_df
import streamlit as st
import pandas as pd

from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
import google.auth
from googleapiclient.discovery import build


@st.cache_data
def get_credentials():
    SCOPES = ["https://www.googleapis.com/auth/webmasters.readonly"]
    cred = None
    try:
        cred = Credentials.from_authorized_user_file("api/credentials2.json", SCOPES)
    except:
        pass

    if not cred:
        flow = InstalledAppFlow.from_client_secrets_file(
            "api/client_secrets.json", SCOPES
        )
        cred = flow.run_local_server(port=0)
        with open("api/credentials2.json", "w") as f:
            f.write(cred.to_json())
    return cred


# Define the function to generate the dataframe
@st.cache_data
def generate_dataframe():
    """Generates the dataframe by calling the `gen_db_df()` function."""
    return gen_db_df()


@st.cache_data
def convert_dataframe_to_csv(dataframe):
    """Converts the given dataframe to a CSV file."""
    return dataframe.to_csv(index=False, encoding="utf-8", sep="\t").encode("utf-8")


def filter_dataframe_by_adjusted_clicks(dataframe, column_name):
    """Filters the given dataframe based on selected range of adjusted clicks."""
    # Get range values for adjusted_clicks column
    min_value = int(dataframe[column_name].min())
    max_value = int(dataframe[column_name].max())

    if min_value < max_value:
        # Display slider for the adjusted_clicks column
        val_range = st.slider(
            f"{column_name.capitalize()} range",
            min_value=min_value,
            max_value=max_value,
            value=(min_value, max_value),
        )

        # Filter dataframe based on selected slider range
        filtered_dataframe = dataframe[
            (dataframe[column_name] >= val_range[0])
            & (dataframe[column_name] <= val_range[1])
        ]
    else:
        # If the minimum value is equal to or greater than the maximum value, return the original dataframe
        filtered_dataframe = dataframe

    return filtered_dataframe


def regenerate_dataframe_on_button_press(dataframe):
    """Regenerates the dataframe when the button is pressed."""
    if st.sidebar.button("Regenerate DataFrame"):
        dataframe = generate_dataframe()
        domains = dataframe["domain"].unique()
        st.sidebar.success("DataFrame regenerated")

    return dataframe


def main():
    # Check if we have credentials before displaying the log in button
    creds = get_credentials()
    if creds:
        st.write("Logged in successfully!")

        # Generate the dataframe
        df = generate_dataframe()
        # gen mpty dataframe
        filtered_dataframe = pd.DataFrame()

        # Regenerate the dataframe on button press
        df = regenerate_dataframe_on_button_press(df)

        # Get domain names for select box
        domains = df["domain"].unique()

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
                and st.session_state.filtered_dataframe["domain"].iloc[0]
                == selected_domain
            ):
                filtered_dataframe = st.session_state.filtered_dataframe
            else:
                filtered_dataframe = df[df["domain"] == selected_domain]
                st.session_state.filtered_dataframe = filtered_dataframe

            filtered_dataframe = filter_dataframe_by_adjusted_clicks(
                filtered_dataframe, "adjusted_clicks"
            )
            # Display filtered dataframe
            st.write(filtered_dataframe)

            # Add a button to download the filtered dataframe as a CSV file
            csv_data = convert_dataframe_to_csv(filtered_dataframe)
            st.download_button(
                label="Download Report as CSV",
                data=csv_data,
                file_name=f"{selected_domain}_report.csv",
                mime="text/csv",
            )
        else:
            st.warning("Please generate the DataFrame and select a domain")
    else:
        st.warning("Please log in with OAuth to use this app.")

    # Add log in button at the beginning
    st.title("Google Search Console Login")
    if not creds:
        if st.button("Log In"):
            creds = get_credentials()
            if creds:
                st.write("Logged in successfully!")
                st.experimental_rerun()  # Rerun the app to display the filtered data
            else:
                st.write("Could not log in. Please try again.")
        else:
            st.warning("Please log in with OAuth to use this app.")


if __name__ == "__main__":
    main()
