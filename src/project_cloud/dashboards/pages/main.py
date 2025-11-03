import streamlit as st
import pandas as pd
import requests

def get_parkings_df():
    url = "https://sqrujqh495.execute-api.us-east-1.amazonaws.com/prod/parkings"
    response = requests.get(url)
    return pd.json_normalize(response.json())

def get_traffic_df():
    url = "https://sqrujqh495.execute-api.us-east-1.amazonaws.com/prod/traffic"
    response = requests.get(url)
    return pd.json_normalize(response.json())

def main():
    st.set_page_config(
        page_title="Parking Dashboard",
    )
    st.title("Parking Dashboard")
    parkings_df = get_parkings_df()
    parkings_df = parkings_df.dropna(subset=['latitude', 'longitude'])
    st.write(parkings_df)
    st.map(parkings_df)

    st.title("Traffic Dashboard")
    traffic_df = get_traffic_df()
    traffic_df = traffic_df.dropna(subset=['coordinates'])
    st.write(traffic_df)

if __name__ == "__main__":
    main()