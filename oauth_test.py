import streamlit as st
from streamlit_oauth import OAuth2Component
import os

# Load environment variables from .env file
from dotenv import load_dotenv

load_dotenv()

# Set environment variables
AUTHORIZE_URL = os.environ.get("https://accounts.google.com/o/oauth2/auth")
TOKEN_URL = os.environ.get("https://oauth2.googleapis.com/token")
REFRESH_TOKEN_URL = os.environ.get(
    "1//093iEipWQO4xfCgYIARAAGAkSNwF-L9IrQNecuUiL71drDRYC4BJ4_dORzKkHD_N04QcudiRqgavUpVb0QlZ2HGYOlrpCgNW8wNo"
)
REVOKE_TOKEN_URL = os.environ.get("REVOKE_TOKEN_URL")
CLIENT_ID = os.environ.get(
    "674795694378-vl9uqohnji0a86mdedv1nogl7fqmo5uo.apps.googleusercontent.com"
)
CLIENT_SECRET = os.environ.get("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")
REDIRECT_URI = os.environ.get("http://localhost")
SCOPE = os.environ.get("https://www.googleapis.com/auth/webmasters.readonly ")

# Create OAuth2Component instance
oauth2 = OAuth2Component(
    CLIENT_ID,
    CLIENT_SECRET,
    AUTHORIZE_URL,
    TOKEN_URL,
    REFRESH_TOKEN_URL,
    REVOKE_TOKEN_URL,
)

# Check if token exists in session state
if "token" not in st.session_state:
    # If not, show authorize button
    result = oauth2.authorize_button("Authorize", REDIRECT_URI, SCOPE)
    if result and "token" in result:
        # If authorization successful, save token in session state
        st.session_state.token = result.get("token")
        st.experimental_rerun()
else:
    # If token exists in session state, show the token
    token = st.session_state["token"]
    st.json(token)
    if st.button("Refresh Token"):
        # If refresh token button is clicked, refresh the token
        token = oauth2.refresh_token(token)
        st.session_state.token = token
        st.experimental_rerun()
