import streamlit as st

from project_cloud.utils.utils_pd import get_df_from_api

tables = ["departures", "aggregation_departures_by_network", "aggregation_departures_top_destinations","aggregation_departures_total"]

def main():
    st.set_page_config(
        page_title="Departures Dashboard",
    )
    st.title("Departures Dashboard")

    for table in tables:
        st.write(f"---")
        st.write(f"### {table}")
        st.write(get_df_from_api(table))



if __name__ == "__main__":
    main()