import streamlit as st
import pandas as pd
import os
import json
import tempfile
import logging
import pytz 
import sys

from datetime import datetime, timedelta
from google.cloud import bigquery
from google.api_core.exceptions import NotFound, BadRequest, GoogleAPICallError
from io import StringIO

from ga4queries import *

# Configure logging
logging.basicConfig(level=logging.INFO, filename='script.log', filemode='w', format='%(name)s - %(levelname)s - %(message)s')

# Create a dictionary with country name and corresponding timezone
timezone_dict = {
    "Oceania": {
        "New Zealand": "Pacific/Auckland",
        "Australia": "Australia/Sydney",
        "Fiji": "Pacific/Fiji",
        "Papua New Guinea": "Pacific/Port_Moresby",
        "Samoa": "Pacific/Apia",
        "Tonga": "Pacific/Tongatapu",
        "Solomon Islands": "Pacific/Guadalcanal",
        "Vanuatu": "Pacific/Efate",
        "Kiribati": "Pacific/Tarawa",
        "New Caledonia": "Pacific/Noumea"
    },
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
    }
}

# Create a list of continents
continents = ["Oceania","North America", "South America", "Europe", "Asia"]

view_names = "user_table_view", "event_table_view", "item_table_view"
event_table_patterns = "events_*", "events_intraday_*"

##############################################################################################################################################
# Streamlit Layout
##############################################################################################################################################
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
             - Error checking and notification needs to be improved.
             - If there is no traffic on the website in the day you are running this it will crash as the intraday table has not been generated. Will add a check for this but you can just visit the site to generate that table.  

            If you have any questions or feedback do not heistate to contact us at **howdy@teamcircle.tech**
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
    utc_tz = utc_offset[:-2]+':'+utc_offset[-2:]
    utc_ts = timezone
    st.markdown(f"> :earth_americas:  **{country}** time zone is **UTC{utc_tz}** and in Timezone: **{timezone}**")

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
    st.markdown("![Alt Text](https://github.com/team-circle-tech/GA4toBQ/blob/main/gifs/Config_JSON.gif?raw=true)")
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

    # Generate table list to account for known users, traffic for the day
    tables = client.list_tables(project_id+'.'+dataset_id)
    table_names = set(table.table_id for table in tables)
    today = datetime.now()
    formatted_today = today.strftime('%Y%m%d')
    yesterday = today - timedelta(days=1)
    formatted_yesterday = yesterday.strftime('%Y%m%d')

    # Checking if the events table exists if not error out
    st.write("Checking for events yesterday today")
    events_intra = "events_"+formatted_yesterday
    if events_intra not in table_names:
        st.error("You have no events on your site for yesterday, do you have streaming turned on: https://support.google.com/analytics/answer/9823238")
        sys.exit()
    else:
        st.write("Events for yesterday found")

    # Checking if the today events_intraday table exists, if not it errors our
    st.write("Checking for traffic today")
    events_intra = "events_intraday_"+formatted_today
    if events_intra not in table_names:
        st.error("You have no traffic on your site for today, please generate some")
        sys.exit()
    else:
        st.write("Events for today found")

    #Check if there are known users 
    st.write("Checking for known users")
    user_table = "users_"+formatted_yesterday
    # Check if the 'users' table exists
    if user_table in table_names:
        st.write("Table "+user_table+" does exist.")
        user_table_pattern = "users_*", "pseudonymous_users_*"
        userid_sub = "sub.user_id, sub.user_pseudo_id,"
    else:
        st.write("Table "+user_table+" does not exist.")
        user_table_pattern = "pseudonymous_users_*",""
        userid_sub = "sub.user_pseudo_id,"

    #This is where things are run
    keys_and_types = get_unique_keys_and_types(client, project_id, dataset_id, event_table_patterns)
    if keys_and_types:

        st.write("Retrieved keys and types:")#, keys_and_types)
        create_user_table_view(client, project_id, dataset_id, user_table_pattern, utc_ts)
        st.write("create_user_table_view")

        create_event_table_view(client, project_id, dataset_id, event_table_patterns, userid_sub, keys_and_types, utc_ts)
        st.write("create_event_table_view")

        #check if there are any items 
        itemcheckquery = f"""
        SELECT it.item_id AS item_id, FROM ( SELECT GENERATE_UUID() as ueid, * FROM `{project_id}.{dataset_id}.events_*`) sub CROSS JOIN UNNEST(sub.items) AS it LIMIT 1
        """
        query_job = client.query(itemcheckquery)
        # Attempt to fetch one row
        try:
            # Get the results
            results = query_job.result()  # Waits for the job to complete

            # Fetch one row
            row = next(results, None)

            # Check if the query returned at least one result
            if row:
                create_item_table_view(client, project_id, dataset_id, event_table_patterns, userid_sub, keys_and_types, utc_ts)
                st.write("create_items_table_view")
            else:
                st.write("No items found in event table")
                view_names = "user_table_view", "event_table_view"

        except Exception as e:
            st.error(f"Error occurred: {e}")

        create_summary_statistics(client, project_id, dataset_id, view_names)
        st.write("create_summary_statistics")
        st.write("FINISHED!")
        st.write('''
            ### Notes
            [Link to looker dashboard](https://lookerstudio.google.com/reporting/b774ca26-720c-4b23-999c-5e5cec53bca3/preview)
            ''')
    else:
        st.write("Failed to retrieve keys and types.")


