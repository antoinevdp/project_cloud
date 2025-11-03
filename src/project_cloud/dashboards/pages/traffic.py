import streamlit as st

from project_cloud.utils.utils_pd import get_df_from_api

tables = [
    "traffic", "aggregation_traffic_congestion_index",
    "aggregation_traffic_critical_segments","aggregation_traffic_fluidity_by_zone"
]

def main():
    st.set_page_config(
        page_title="Traffic Dashboard",
    )
    st.title("Traffic Dashboard")

    for table in tables:
        st.write(f"---")
        st.write(f"### {table}")
        st.write(get_df_from_api(table))



if __name__ == "__main__":
    main()