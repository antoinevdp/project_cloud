import streamlit as st
import pydeck as pdk
import pandas as pd
import requests
from dotenv import load_dotenv
import os

# --- Configuration ---
# Make sure to replace this with your actual API Gateway endpoint for traffic
TRAFFIC_ENDPOINT = "https://sqrujqh495.execute-api.us-east-1.amazonaws.com/prod/traffic"
PARKINGS_ENDPOINT = "https://sqrujqh495.execute-api.us-east-1.amazonaws.com/prod/parkings"
DEPARTURES_ENDPOINT = "https://sqrujqh495.execute-api.us-east-1.amazonaws.com/prod/departures"

load_dotenv()
MAPBOX_API_KEY = os.getenv('MAPBOX_API_KEY')

st.set_page_config(
    layout="wide",
    page_title="Traffic Dashboard",
)
st.title("Live Traffic Data Visualization")


# --- Data Fetching ---
@st.cache_data
def get_data(api_url):
    """Fetches and caches the data from your API."""
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

def main():
    # --- Main App ---
    traffic_data = get_data(TRAFFIC_ENDPOINT)
    parkings_data = get_data(PARKINGS_ENDPOINT)
    departures_data = get_data(DEPARTURES_ENDPOINT)

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

            layers = [path_layer]

            if parkings_data:
                st.write(f"Found {len(parkings_data)} parkings.")
                parking_items = []
                for item in parkings_data:
                    if "longitude" in item and "latitude" in item:
                        parking_items.append(item)

                if parking_items:
                    parking_df = pd.DataFrame(parking_items)
                    parking_layer = pdk.Layer(
                        "ScatterplotLayer",
                        data=parking_df,
                        get_position=['longitude', 'latitude'],
                        get_color=[200, 30, 0, 160],
                        get_radius=100,
                        pickable=True,
                        auto_highlight=True,
                    )
                    layers.append(parking_layer)

            deck = pdk.Deck(
                layers=layers,
                initial_view_state=initial_view_state,
                map_provider="mapbox",
                map_style=pdk.map_styles.MAPBOX_ROAD,
                api_keys={"mapbox": st.secrets["MAPBOX_API_KEY"]},
                tooltip={"text": "gid: {gid}\nEtat: {etat}\nNom: {nom}\nDispo: {places_disponibles}"},
            )

            st.pydeck_chart(deck)

            st.write("---")
            st.write("### Raw Data Inspector")
            st.write(pd.json_normalize(traffic_data))
        else:
            st.warning("No traffic segments with coordinates were found in the API response.")
    else:
        st.warning("Could not retrieve data from the API endpoint.")

if __name__ == "__main__":
    main()
