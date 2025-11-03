import streamlit as st

from project_cloud.utils.utils_pd import get_df_from_api

tables = [
    "parkings", "aggregation_average_availability_parking",
    "aggregation_number_of_parkings_in_operation","aggregation_overall_occupancy_rate","aggregation_reference_pricing"
]

def main():
    st.set_page_config(
        page_title="Parkings Dashboard",
    )
    st.title("Parkings Dashboard")

    for table in tables:
        st.write(f"---")
        st.write(f"### {table}")
        st.write(get_df_from_api(table))



if __name__ == "__main__":
    main()