#TODO: Test that is still works when there are no known users
#TODO: Add eCommerece 

import streamlit as st
import pandas as pd
import os
import json
import tempfile
import logging
import pytz 

from datetime import datetime
from google.cloud import bigquery
from google.api_core.exceptions import NotFound, BadRequest, GoogleAPICallError
from io import StringIO

# Configure logging
logging.basicConfig(level=logging.INFO, filename='script.log', filemode='w', format='%(name)s - %(levelname)s - %(message)s')

# Create a dictionary with country name and corresponding timezone
timezone_dict = {
    "North America": {
        "United States": "America/New_York",
        "Canada": "America/Toronto",
        "Mexico": "America/Mexico_City",
        "Jamaica": "America/Jamaica",
        "Costa Rica": "America/Costa_Rica",
        "Bahamas": "America/Nassau",
        "Honduras": "America/Tegucigalpa",
        "Cuba": "America/Havana",
        "Dominican Republic": "America/Santo_Domingo"
    },
    "South America": {
        "Brazil": "America/Sao_Paulo",
        "Argentina": "America/Argentina/Buenos_Aires",
        "Chile": "America/Santiago",
        "Colombia": "America/Bogota",
        "Peru": "America/Lima",
        "Uruguay": "America/Montevideo",
        "Ecuador": "America/Guayaquil",
        "Bolivia": "America/La_Paz",
        "Paraguay": "America/Asuncion",
        "Venezuela": "America/Caracas"
    },
    "Europe": {
        "United Kingdom": "Europe/London",
        "France": "Europe/Paris",
        "Germany": "Europe/Berlin",
        "Italy": "Europe/Rome",
        "Spain": "Europe/Madrid",
        "Russia": "Europe/Moscow",
        "Turkey": "Europe/Istanbul",
        "Greece": "Europe/Athens",
        "Poland": "Europe/Warsaw",
        "Ukraine": "Europe/Kiev"
    },
    "Asia": {
        "India": "Asia/Kolkata",
        "Japan": "Asia/Tokyo",
        "China": "Asia/Shanghai",
        "Saudi Arabia": "Asia/Riyadh",
        "South Korea": "Asia/Seoul",
        "Indonesia": "Asia/Jakarta",
        "Malaysia": "Asia/Kuala_Lumpur",
        "Vietnam": "Asia/Ho_Chi_Minh",
        "Philippines": "Asia/Manila",
        "Thailand": "Asia/Bangkok"
    },
    "Oceania": {
        "Australia": "Australia/Sydney",
        "New Zealand": "Pacific/Auckland",
        "Fiji": "Pacific/Fiji",
        "Papua New Guinea": "Pacific/Port_Moresby",
        "Samoa": "Pacific/Apia",
        "Tonga": "Pacific/Tongatapu",
        "Solomon Islands": "Pacific/Guadalcanal",
        "Vanuatu": "Pacific/Efate",
        "Kiribati": "Pacific/Tarawa",
        "New Caledonia": "Pacific/Noumea"
    }
}

# Create a list of continents
continents = ["North America", "South America", "Europe", "Asia", "Oceania"]

# 
def get_unique_keys_and_types(client, project_id, dataset_id, event_table_patterns):
    st.write("Getting unique keys and their types...")
    union_subqueries = [
        f"""
        SELECT key, 
               IF(ep.value.string_value IS NOT NULL, 'string', 
                  IF(ep.value.int_value IS NOT NULL, 'int', 
                     IF(ep.value.float_value IS NOT NULL, 'float', NULL)
                  )
               ) AS value_type
        FROM `{project_id}.{dataset_id}.{table_pattern}`,
        UNNEST(event_params) AS ep
        """
        for table_pattern in event_table_patterns
    ]
    query = " UNION ALL ".join(union_subqueries) + " GROUP BY key, value_type"
    query_job = client.query(query)
    keys_and_types = query_job.result()
    logging.info("keys and types")
    logging.info(query)
    st.write("Unique keys and types retrieved successfully.")
    return {row.key: row.value_type for row in keys_and_types}

# 
def generate_event_table_query(keys_and_types, project_id, dataset_id, event_table_patterns, utc_ts):
    logging.info("Generating the event table query...")
    
    pivot_sections = []
    for key, value_type in keys_and_types.items():
        column_alias = "event_param_" + key.replace("-", "_")  # Ensure valid SQL identifier
        if value_type == 'string':
            pivot_sections.append(f"MAX(IF(key = '{key}', string_value, NULL)) AS {column_alias}")
        elif value_type == 'int':
            pivot_sections.append(f"MAX(IF(key = '{key}', int_value, NULL)) AS {column_alias}")
        elif value_type == 'float':
            pivot_sections.append(f"MAX(IF(key = '{key}', float_value, NULL)) AS {column_alias}")

    pivot_sql = ",\n".join(pivot_sections)
    
    union_subqueries = [
        f"""
        SELECT
            sub.ueid,
            FORMAT_DATETIME("%F %T {utc_ts}", DATETIME(TIMESTAMP_MICROS(sub.event_timestamp), "{utc_ts}")) AS event_timezone,
            sub.event_timestamp AS event_timestamp,
            sub.event_date,
            sub.user_id, 
            sub.user_pseudo_id,
            sub.event_name,
            sub.platform AS event_platform,
            sub.stream_id AS event_stream_id,
            sub.traffic_source.source AS traffic_source,
            sub.traffic_source.medium AS traffic_medium,
            sub.traffic_source.name AS traffic_name,
            sub.geo.country AS event_geo_country,
            sub.geo.region AS event_geo_region,
            sub.geo.city AS event_geo_city,
            sub.geo.sub_continent AS event_geo_sub_continent,
            sub.geo.metro AS event_geo_metro,
            sub.geo.continent AS event_geo_continent,
            sub.device.browser AS event_device_browser,
            sub.device.language AS event_device_language,
            sub.device.is_limited_ad_tracking AS event_device_is_limited_ad_tracking,
            sub.device.mobile_model_name AS event_device_mobile_model_name,
            sub.device.mobile_marketing_name AS event_device_mobile_marketing_name,
            sub.device.mobile_os_hardware_model AS event_device_mobile_os_hardware_model,
            sub.device.operating_system AS event_device_operating_system,
            sub.device.operating_system_version AS event_device_operating_system_version,
            sub.device.category AS event_device_category,
            sub.device.mobile_brand_name AS event_device_mobile_brand_name,
            sub.user_first_touch_timestamp AS event_user_first_touch_timestamp,
            sub.user_ltv.revenue AS event_user_ltv_revenue,
            sub.user_ltv.currency AS event_user_ltv_currency,
            sub.device.web_info.browser AS web_info_browser,
            sub.device.web_info.browser_version AS web_info_browser_version,
            sub.device.web_info.hostname AS web_info_hostname,
            ep.key AS key,
            ep.value.string_value AS string_value,
            ep.value.int_value AS int_value,
            ep.value.float_value AS float_value
        FROM (
            SELECT
                GENERATE_UUID() as ueid,
                *
            FROM 
                `{project_id}.{dataset_id}.{table_pattern}`
        ) sub
        CROSS JOIN UNNEST(sub.event_params) AS ep
        """
        for table_pattern in event_table_patterns
    ]

    sql_query = f"""
    WITH expanded AS (
        {" UNION ALL ".join(union_subqueries)}
    ),
    pivot_table AS (
        SELECT 
            COUNT(DISTINCT ueid) AS ueid_dcount,
            event_timezone,
            event_timestamp,
            event_date,
            user_id,
            user_pseudo_id,
            event_name,
            event_platform,
            event_stream_id,
            traffic_source,
            traffic_medium,
            traffic_name,
            event_geo_country,
            event_geo_region,
            event_geo_city,
            event_geo_sub_continent,
            event_geo_metro,
            event_geo_continent,
            event_device_browser,
            event_device_language,
            event_device_is_limited_ad_tracking,
            event_device_mobile_model_name,
            event_device_mobile_marketing_name,
            event_device_mobile_os_hardware_model,
            event_device_operating_system,
            event_device_operating_system_version,
            event_device_category,
            event_device_mobile_brand_name,
            event_user_first_touch_timestamp,
            event_user_ltv_revenue,
            event_user_ltv_currency,
            web_info_browser,
            web_info_browser_version,
            web_info_hostname,
            {pivot_sql}
        FROM 
            expanded
       GROUP BY 
    event_timezone, event_timestamp, event_date, user_id, user_pseudo_id, event_name, event_platform, event_stream_id, traffic_source, traffic_medium, traffic_name, event_geo_country, event_geo_region, event_geo_city, event_geo_sub_continent, event_geo_metro, event_geo_continent, event_device_browser, event_device_language, event_device_is_limited_ad_tracking, event_device_mobile_model_name, event_device_mobile_marketing_name, event_device_mobile_os_hardware_model, event_device_operating_system, event_device_operating_system_version, event_device_category, event_device_mobile_brand_name, event_user_first_touch_timestamp, event_user_ltv_revenue, event_user_ltv_currency, web_info_browser, web_info_browser_version, web_info_hostname
)
    SELECT 
        * 
    FROM 
        pivot_table
    """

    logging.info(sql_query)

    return sql_query

    logging.info("Event table query generated successfully...")

def generate_user_table_query(project_id, dataset_id, user_table_pattern, utc_ts):

    union_subqueries = []

    for pattern in user_table_pattern:
        if pattern == "pseudonymous_users_*":
            subquery = f"""
            SELECT
                pseudo_user_id AS user_id,
                FORMAT_DATETIME("%F %T {utc_ts}", DATETIME(TIMESTAMP_MICROS(user_info.last_active_timestamp_micros), "{utc_ts}")) AS user_last_active_timestamp,
                FORMAT_DATETIME("%F %T {utc_ts}", DATETIME(TIMESTAMP_MICROS(user_info.user_first_touch_timestamp_micros), "{utc_ts}")) AS user_first_touch_timestamp,
                user_info.first_purchase_date AS user_first_purchase_date,
                device.operating_system AS user_device_operating_system,
                device.category AS user_device_category,
                device.mobile_brand_name AS user_device_mobile_brand_name,
                device.mobile_model_name AS user_device_mobile_model_name,
                device.unified_screen_name AS user_device_unified_screen_name,
                geo.city AS user_geo_city,
                geo.country AS user_geo_country,
                geo.continent AS user_geo_continent,
                geo.region AS user_geo_region,
                user_ltv.revenue_in_usd AS user_ltv_revenue_in_usd,
                user_ltv.sessions AS user_ltv_sessions,
                user_ltv.engagement_time_millis AS user_ltv_engagement_time,
                user_ltv.purchases AS user_ltv_purchases,
                user_ltv.engaged_sessions AS user_ltv_engaged_sessions,
                user_ltv.session_duration_micros AS user_ltv_session_duration,
                predictions.in_app_purchase_score_7d AS user_prediction_in_app_purchase_score_7d,
                predictions.purchase_score_7d AS user_prediction_purchase_score_7d,
                predictions.churn_score_7d AS user_prediction_churn_score_7d,
                predictions.revenue_28d_in_usd AS user_prediction_revenue_28d,
                occurrence_date AS user_occurrence_date,
                last_updated_date AS user_last_updated_date,
            FROM 
                `{project_id}.{dataset_id}.{pattern}`
            """
            union_subqueries.append(subquery)
        elif pattern == "users_*":
            subquery = f"""
            SELECT
                user_id AS user_id,
                FORMAT_DATETIME("%F %T {utc_ts}", DATETIME(TIMESTAMP_MICROS(user_info.last_active_timestamp_micros), "{utc_ts}")) AS user_last_active_timestamp,
                FORMAT_DATETIME("%F %T {utc_ts}", DATETIME(TIMESTAMP_MICROS(user_info.user_first_touch_timestamp_micros), "{utc_ts}")) AS user_first_touch_timestamp,
                user_info.first_purchase_date AS user_first_purchase_date,
                device.operating_system AS user_device_operating_system,
                device.category AS user_device_category,
                device.mobile_brand_name AS user_device_mobile_brand_name,
                device.mobile_model_name AS user_device_mobile_model_name,
                device.unified_screen_name AS user_device_unified_screen_name,
                geo.city AS user_geo_city,
                geo.country AS user_geo_country,
                geo.continent AS user_geo_continent,
                geo.region AS user_geo_region,
                user_ltv.revenue_in_usd AS user_ltv_revenue_in_usd,
                user_ltv.sessions AS user_ltv_sessions,
                user_ltv.engagement_time_millis AS user_ltv_engagement_time,
                user_ltv.purchases AS user_ltv_purchases,
                user_ltv.engaged_sessions AS user_ltv_engaged_sessions,
                user_ltv.session_duration_micros AS user_ltv_session_duration,
                predictions.in_app_purchase_score_7d AS user_prediction_in_app_purchase_score_7d,
                predictions.purchase_score_7d AS user_prediction_purchase_score_7d,
                predictions.churn_score_7d AS user_prediction_churn_score_7d,
                predictions.revenue_28d_in_usd AS user_prediction_revenue_28d,
                occurrence_date AS user_occurrence_date,
                last_updated_date AS user_last_updated_date,
            FROM 
                `{project_id}.{dataset_id}.{pattern}`
            """
            union_subqueries.append(subquery)

    # Join the individual subqueries with a "UNION ALL"
    combined_subqueries = " UNION ALL ".join(union_subqueries)

    sql_query = f"""
    WITH expanded AS (
        {combined_subqueries}
    )
    SELECT 
        * 
    FROM 
        expanded
    """
    return sql_query

    logging.info("User table query generated successfully...")

# Function to retrieve schema columns
def get_schema_columns(client, project_id, dataset_id, table_name):
    query = f"SELECT column_name FROM `{project_id}.{dataset_id}.INFORMATION_SCHEMA.COLUMNS` WHERE table_name = '{table_name}'"
    query_job = client.query(query)
    return [row for row in query_job.result()]

def get_distinct_counts(client, project_id, dataset_id, view_name):
    try:
        # Retrieve the column names in the view
        view_columns = [column.column_name for column in get_schema_columns(client, project_id, dataset_id, view_name)]
        if not view_columns:
            logging.error(f"No columns found in the view: {view_name}")
            st.error(f"No columns found in a view")
            return {}

        # Create a query to get distinct counts for each column
        distinct_count_query = ", ".join([f"COUNT(DISTINCT {col}) AS {col}" for col in view_columns])
        query = f"""
        SELECT
            {distinct_count_query}
        FROM
            `{project_id}.{dataset_id}.{view_name}`
        """
        
        query_job = client.query(query)
        result = query_job.result()

        # Convert result to a dictionary
        for row in result:
            distinct_counts = dict(row.items())
            logging.info(f"Distinct counts for view {view_name}: {distinct_counts}")
            return distinct_counts
    except Exception as e:
        logging.error(f"An error occurred while getting distinct counts for {view_name}: {e}")
        st.error(f"An error occurred while getting distinct counts")
        return {}

def identify_useless_columns(distinct_counts):
    useless_columns = [column for column, count in distinct_counts.items() if count in (0, 1)]
    logging.info(f"Identified columns with 0 or 1 distinct values: {useless_columns}")
    return useless_columns

def create_updated_view(client, project_id, dataset_id, view_name, columns_to_exclude):
    try:
        if columns_to_exclude:
            # Retrieve all column names
            view_columns = [column.column_name for column in get_schema_columns(client, project_id, dataset_id, view_name)]
            
            # Generate the SELECT statement for the updated view excluding the identified columns
            select_statement = ", ".join([col for col in view_columns if col not in columns_to_exclude])

            # Create or replace the view with the updated SELECT statement
            query = f"""
            CREATE OR REPLACE VIEW `{project_id}.{dataset_id}.{view_name}_mini` AS
            SELECT
                {select_statement}
            FROM
                `{project_id}.{dataset_id}.{view_name}`
            """
            query_job = client.query(query)
            query_job.result()
            logging.info(f"Excluded columns with unique counts of 0 or 1 from the view: {', '.join(columns_to_exclude)}")
        else:
            logging.info(f"No columns to exclude in the view: {view_name}")
    except Exception as e:
        logging.error(f"An error occurred while creating the updated view for {view_name}: {e}")
        st.error(f"An error occurred while creating or updating views")

# Your create_summary_statistics function remains the same

def create_summary_statistics(client, project_id, dataset_id, view_names):
    logging.info("Creating summary statistics...")

    for view_name in view_names:
        logging.info(f"Creating summary statistics for view: {view_name}...")
        distinct_counts = get_distinct_counts(client, project_id, dataset_id, view_name)
        columns_to_exclude = identify_useless_columns(distinct_counts)
        create_updated_view(client, project_id, dataset_id, view_name, columns_to_exclude)


def create_or_replace_view(client, project_id, dataset_id, view_name, query):
    logging.info(f"Creating/Modifying view: {view_name}...")
    view_id = f"{project_id}.{dataset_id}.{view_name}"

    view = bigquery.Table(view_id)
    view.view_query = query

    try:
        # Check if the view already exists
        client.get_table(view_id)
        view_exists = True
    except NotFound:
        view_exists = False

    try:
        if view_exists:
            view = client.update_table(view, ["view_query"])
            logging.info(f"Modified view {view_id} successfully.")
        else:
            view = client.create_table(view)
            logging.info(f"Created view {view_id} successfully.")
    except BadRequest as e:
        logging.error("Error: Bad request (e.g., schema or query issue). Details: %s", e)
        st.error("Error: Bad request (e.g., schema or query issue). Check your logs")
    except GoogleAPICallError as e:
        logging.error("Error: API call failed. Details: %s", e)
        st.error("Error: API call failed. Check your logs")
    except Exception as e:
        logging.error("An unexpected error occurred: %s", e)
        st.error("Error: An unexpected error occurred. Check your logs")

def create_user_table_view(client, project_id, dataset_id, user_table_pattern, utc_ts):
    user_table_query = generate_user_table_query(project_id, dataset_id, user_table_pattern, utc_ts)
    create_or_replace_view(client, project_id, dataset_id, "user_table_view", user_table_query)

def create_event_table_view(client, project_id, dataset_id, event_table_patterns, keys_and_types):
    event_table_query = generate_event_table_query(keys_and_types, project_id, dataset_id, event_table_patterns, utc_ts)
    create_or_replace_view(client, project_id, dataset_id, "event_table_view", event_table_query)

view_names = "user_table_view", "event_table_view"
event_table_patterns = "events_*", "events_intraday_*"
user_table_pattern = "users_*", "pseudonymous_users_*"

############################################################################################################################################################
# Streamlit Layout
############################################################################################################################################################


st.set_page_config(layout="wide", page_icon=":unlock:", page_title="GA4 Data Transformer and Dashboard")
st.title("Unlocking your GA4 data :unlock:")

# Get IDs for Project before continuing

tab1, tab2 = st.tabs(["Main", "Instructions"])

#Explain what is the point of this tool
with tab1:
    st.write('''
             ## Explanation
             ### For those curious about the technical aspects: 
             This Streamlit app is designed to analyse data from Google Analytics 4 (GA4) stored in BigQuery. The instructions will ensure that GA4 data is sent to BigQuery, and that the app can securely access this data. When you upload the JSON config file, you're providing the app with a set of credentials. These credentials act as a key, giving the app permission to read the GA4 data from BigQuery. It's a secure way to allow access without sharing sensitive information. Once the dataset is selected and the app runs, it processes the data using Python scripts, for non-technical users, think of this app as a bridge between your GA4 data in BigQuery and actionable insights. By providing the necessary setup and credentials, you're allowing the app to fetch, process, and visualize the data in a way that's meaningful and valuable to you.
            ### Combining Daily and Intra-Daily Feeds for Both Event and User Data:
            GA4 exports data to BigQuery in two primary feeds: daily and intra-daily, both for event data and user data. For event data, the daily feed gives a full snapshot of the previous day's events, while the intra-daily feed provides near real-time event data throughout the day. For user data, the daily feed captures a daily snapshot of user-level information, while the intra-daily feed provides incremental updates on user interactions. The code consolidates these feeds, ensuring that the combined data view is up-to-date, comprehensive, and consistent across both events and users.
            1.	Assessing Data Types and Quality for Both Tables:  
            Both the event and user tables in BigQuery can have various data types. The code assesses each column's data type in both tables to ensure consistency and accuracy. It evaluates data quality by pinpointing columns with significant missing data or inconsistent values, ensuring the integrity of both event and user data.
            2.	Flattening Tables and Removing Nesting:  
            Both GA4 event and user data in BigQuery are often structured in nested records. The code transforms these nested structures into flat tables, making them more accessible. For instance, nested event parameters in the event table and user properties in the user table are converted into individual columns.
            3.	Dropping Unnecessary Columns:  
            Not all columns in the GA4 datasets (both event and user tables) are relevant for every analysis. The code identifies and excludes columns with redundant or irrelevant data, streamlining both tables for easier and more efficient querying.
            4.	Producing a Near Real-Time, Easily Reportable View for Both Tables:  
            By integrating and cleaning both the event and user data, the code provides a unified and accessible view that's: Near Real-Time: Thanks to the incorporation of intra-daily feed data. Easily Reportable: The flat structure and optimized columns make querying straightforward. Contrast with GA4 UI: While the GA4 user interface is beneficial for general insights, the Streamlit app offers several advantages: Customization: The GA4 UI presents predefined reports. In contrast, accessing BigQuery data via the Streamlit app allows for tailored analyses specific to individual needs, leveraging both event and user-level data. Granularity: The Streamlit app provides a granular look into data, offering insights into specific event parameters or individual user behaviors, something the GA4 UI may not provide in-depth. Data Access: Users can interact directly with the raw data in BigQuery via the app, offering more flexibility in analyses than the GA4 UI, especially when correlating event and user data. In summary, the Streamlit app offers a deeper, more customizable dive into GA4 data by meticulously processing and presenting both event and user tables, ensuring comprehensive insights beyond what the standard GA4 UI might offer.
             
            Things to do:
             - Allow it to work with GA4 instances without known users
             - Add in eCommerce data
            ''')

# Step by step instructions and running
with tab2:
    st.write('''
            ## Step-by-Step Guide
            ### Set timezone adjustment:
            **What this does:** The drop downs below allow for your Google Analytics 4 data to show time in your timezone rather than default of UTC .
             ''')
    # Create a dropdown to select a continent
    continent = st.selectbox("1. Select a continent", continents)

    # Create a dropdown to select a country within the selected continent
    countries = list(timezone_dict[continent].keys())
    country = st.selectbox("2. Select a country", countries)
    
    # Display the selected UTC offset for confirmation
    timezone = timezone_dict[continent][country]
    utc_offset = datetime.now(pytz.timezone(timezone)).strftime('%z')
    utc_ts = utc_offset[:-2]+':'+utc_offset[-2:]
    st.markdown(f"> :earth_americas:  **{country}** time zone is **UTC{utc_ts}**")

    st.write('''
            ### Connect Google Analytics 4 (GA4) to BigQuery:
            **What this does:** This step ensures that your Google Analytics 4 data is being sent to BigQuery, making it accessible for further analysis.
            1. Go to your GA4 Property.
            2. Navigate to Integrations.
            3. Find BigQuery in the list and click on Link.
            4. Follow the on-screen instructions to complete the linking process.  
             ''')
    st.markdown("![Alt Text](https://github.com/team-circle-tech/GA4toBQ/blob/main/gifs/Link_GABQ.gif?raw=true)")
    st.write('''
        ### Generate JSON Config File for GCP API Access:
        **What this does:** This JSON file is a key that allows applications (like our Streamlit app) to access your BigQuery data on your behalf. It's essential to keep this file secure. By uploading the JSON, you're giving the Streamlit app the credentials it needs to access and process your BigQuery data.
        1. Go to the Google Cloud Console
        2. Select the project linked with your GA4.
        3. Navigate to IAM & Admin > Service accounts.
        4. Click on Create Service Account and give it a descriptive name.
        5. Grant appropriate roles. For BigQuery, roles like BigQuery Data Viewer and BigQuery User are essential.
        6. Click Continue and then Done.
        7. Now, find the service account you just created in the list, click on the three-dot menu, and select Manage keys.
        8. Click on Add Key and choose JSON. This will download a JSON file to your computer.
        9. Upload the file below.  
        ''')
    json_file = st.file_uploader("Drop your JSON here", type="json")
    if not json_file :
        st.info("Upload GA JSON Authenticator to continue")
        st.stop()

    if json_file :
        with tempfile.NamedTemporaryFile(delete=False) as fp:
            fp.write(json_file.getvalue())
        try:
            os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = fp.name          
            with open(fp.name,'rb') as a:
                client = bigquery.Client()
        finally:
            if os.path.isfile(fp.name):
                os.unlink(fp.name)
    
    if "PROJECT_ID" in st.secrets:
        project_id = st.secrets["PROJECT_ID"]
    else:
        st.write('''
            ### Select Project and Dataset:
            **What this does:** The app will now use the provided credentials to access the selected project/dataset in BigQuery and perform the analysis. Once you have done this the project will run
            1. Type in the project ID that you want the app to use
            2. Type in the dataset ID that you want the app to use
            ''')
        project_id = st.text_input("Project ID")
    if not project_id:
        st.info("Enter a Project ID to continue")
        st.stop()

    if "DATASET_ID" in st.secrets:
        dataset_id = st.secrets["DATASET_ID"]
    else:
        st.write("Give us your dataset id from balah blah")
        dataset_id = st.text_input("Dataset ID")
    if not dataset_id:
        st.info("Enter a Dataset ID to continue")
        st.stop()

    #This is where things are run
    keys_and_types = get_unique_keys_and_types(client, project_id, dataset_id, event_table_patterns)
    if keys_and_types:
        st.write("Retrieved keys and types:")#, keys_and_types)
        generate_event_table_query(keys_and_types, project_id, dataset_id, event_table_patterns, utc_ts)
        st.write("generate_event_table_query")
        create_user_table_view(client, project_id, dataset_id, user_table_pattern, utc_ts)
        st.write("create_user_table_view")
        create_event_table_view(client, project_id, dataset_id, event_table_patterns, keys_and_types)
        st.write("create_event_table_view")
        create_summary_statistics(client, project_id, dataset_id, view_names)
        st.write("create_summary_statistics")
        st.write("FINISHED!")
        st.write('''
            ### Notes
            [Link to looker dashboard](https://lookerstudio.google.com/reporting/b774ca26-720c-4b23-999c-5e5cec53bca3/preview)
            ''')
    else:
        st.write("Failed to retrieve keys and types.")


