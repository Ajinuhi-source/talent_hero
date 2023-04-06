import streamlit as st
from streamlit_oauth import OAuth2Component
import os

# Load environment variables from .env file
from dotenv import load_dotenv

load_dotenv()

# Set environment variables
AUTHORIZE_URL = "https://accounts.google.com/o/oauth2/auth"
TOKEN_URL = "ya29.a0Ael9sCOKelMNfpOfA_Zs2Kh00MI9FO0UaE1aIvB0pQrc1B7eDXWNhTe4Gcn5168FkCTWa_ZDxckVAgpFBnoyyAqiw9N9SYlYH52xHhDjutEjTl5BNPrSwRES1EzAcAv3MaPfTA-aq10GHgKIYwfoRAut8qNmaCgYKAbISARASFQF4udJh5zKTYwdmfZnm8nl3UzA2iQ0163"
REFRESH_TOKEN_URL = "1//093iEipWQO4xfCgYIARAAGAkSNwF-L9IrQNecuUiL71drDRYC4BJ4_dORzKkHD_N04QcudiRqgavUpVb0QlZ2HGYOlrpCgNW8wNo"
REVOKE_TOKEN_URL = "REVOKE_TOKEN_URL"
CLIENT_ID = "674795694378-vl9uqohnji0a86mdedv1nogl7fqmo5uo.apps.googleusercontent.com"
CLIENT_SECRET = "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"
REDIRECT_URI = "https://cefege-talent-hero-oauth-regesz.streamlit.app/"
SCOPE = "https://www.googleapis.com/auth/webmasters.readonly"

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
