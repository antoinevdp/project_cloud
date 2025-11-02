import streamlit as st
import pydeck as pdk
import pandas as pd
import requests
from dotenv import load_dotenv
import os

# --- Configuration ---
# Make sure to replace this with your actual API Gateway endpoint for traffic
API_ENDPOINT = "https://sqrujqh495.execute-api.us-east-1.amazonaws.com/prod/traffic"

load_dotenv()
MAPBOX_API_KEY = os.getenv('MAPBOX_API_KEY')

st.set_page_config(layout="wide")
st.title("Live Traffic Data Visualization")


# --- Data Fetching ---
@st.cache_data
def get_traffic_data(api_url):
    """Fetches and caches the traffic data from your API."""
    try:
        response = requests.get(api_url, timeout=60)
        # This will raise an error for non-200 status codes
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        st.error(f"Error fetching data from API: {e}")
        return None


def get_color(state):
    if state == "R":
        return [255, 0, 0]
    elif state == "O":
        return [255, 165, 0]
    elif state == "V":
        return [0, 128, 0]
    elif state == "G":
        return [128, 128, 128]
    elif state == "N":
        return [0, 0, 0]
    return [128, 128, 128]


# --- Main App ---
traffic_data = get_traffic_data(API_ENDPOINT)

if traffic_data:
    st.write(f"Found {len(traffic_data)} traffic segments.")

    all_items = []
    for i, item in enumerate(traffic_data):
        if "coordinates" in item and item["coordinates"]:
            item_copy = item.copy()
            item_copy['color'] = get_color(item["etat"])
            item_copy['path'] = item['coordinates']
            all_items.append(item_copy)

    if all_items:
        df = pd.DataFrame(all_items)

        # Set the initial map view based on the first coordinate we find
        initial_view_state = pdk.ViewState(
            latitude=df["coordinates"][0][0][1],
            longitude=df["coordinates"][0][0][0],
            zoom=11,
            pitch=45,
        )

        path_layer = pdk.Layer(
            "PathLayer",
            data=df,
            get_path="path",
            get_width=5,
            get_color='color',
            width_min_pixels=2,
            pickable=True,
            auto_highlight=True,
        )

        deck = pdk.Deck(
            layers=[path_layer],
            initial_view_state=initial_view_state,
            map_provider="mapbox",
            map_style=pdk.map_styles.MAPBOX_ROAD,
            api_keys={"mapbox": st.secrets["MAPBOX_API_KEY"]},
            tooltip={"text": "gid: {gid}\nEtat: {etat}\nVitesse: {vitesse}"},
        )

        st.pydeck_chart(deck)

        st.write("---")
        st.write("### Raw Data Inspector")
        st.write(pd.json_normalize(traffic_data))
    else:
        st.warning("No traffic segments with coordinates were found in the API response.")
else:
    st.warning("Could not retrieve data from the API endpoint.")

