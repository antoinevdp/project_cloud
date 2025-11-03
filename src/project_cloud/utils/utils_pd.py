import pandas as pd
import requests
import streamlit as st

@st.cache_data
def get_df_from_api(endpoint):
    url = f"https://sqrujqh495.execute-api.us-east-1.amazonaws.com/prod/{endpoint}"
    response = requests.get(url)
    return pd.json_normalize(response.json())