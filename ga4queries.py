import streamlit as st
import pandas as pd
import os
import json
import tempfile
import logging
import pytz 

from datetime import datetime, timedelta
from google.cloud import bigquery
from google.api_core.exceptions import NotFound, BadRequest, GoogleAPICallError
from io import StringIO

# Configure logging
logging.basicConfig(level=logging.INFO, filename='script.log', filemode='w', format='%(name)s - %(levelname)s - %(message)s')

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
def generate_event_table_query(keys_and_types, project_id, dataset_id, event_table_patterns, userid_sub, utc_ts):
    logging.info("Generating the event table query...")

    userid_q = userid_sub.replace("sub.", "")
    
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
            DATETIME(TIMESTAMP_MICROS(sub.event_timestamp), "{utc_ts}") AS event_timezone,
            sub.event_timestamp AS event_timestamp,
            sub.event_date,
            {userid_sub}
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
            sub.ecommerce.total_item_quantity AS total_item_quantity,
            sub.ecommerce.purchase_revenue_in_usd AS purchase_revenue_in_usd,
            sub.ecommerce.purchase_revenue AS purchase_revenue,			
            sub.ecommerce.refund_value_in_usd AS refund_value_in_usd, 
            sub.ecommerce.refund_value AS refund_value,	
            sub.ecommerce.shipping_value_in_usd AS shipping_value_in_usd,		
            sub.ecommerce.shipping_value AS shipping_value, 
            sub.ecommerce.tax_value_in_usd AS tax_value_in_usd,		
            sub.ecommerce.tax_value AS tax_value,
            sub.ecommerce.unique_items AS unique_items,	
            sub.ecommerce.transaction_id AS transaction_id,
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
            {userid_q}
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
            total_item_quantity,
            purchase_revenue_in_usd,
            purchase_revenue,			
            refund_value_in_usd, 
            refund_value,	
            shipping_value_in_usd,		
            shipping_value, 
            tax_value_in_usd,		
            tax_value,
            unique_items,	
            transaction_id,
            {pivot_sql}
        FROM 
            expanded
       GROUP BY 
    event_timezone, event_timestamp, event_date, {userid_q} event_name, event_platform, event_stream_id, traffic_source, traffic_medium, traffic_name, event_geo_country, event_geo_region, event_geo_city, event_geo_sub_continent, event_geo_metro, event_geo_continent, event_device_browser, event_device_language, event_device_is_limited_ad_tracking, event_device_mobile_model_name, event_device_mobile_marketing_name, event_device_mobile_os_hardware_model, event_device_operating_system, event_device_operating_system_version, event_device_category, event_device_mobile_brand_name, event_user_first_touch_timestamp, event_user_ltv_revenue, event_user_ltv_currency, web_info_browser, web_info_browser_version, web_info_hostname, total_item_quantity, purchase_revenue_in_usd, purchase_revenue, refund_value_in_usd, refund_value, shipping_value_in_usd,shipping_value, tax_value_in_usd, tax_value,unique_items, transaction_id
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
                DATETIME(TIMESTAMP_MICROS(user_info.last_active_timestamp_micros), "{utc_ts}") AS user_last_active_timestamp,
                DATETIME(TIMESTAMP_MICROS(user_info.user_first_touch_timestamp_micros), "{utc_ts}") AS user_first_touch_timestamp,
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
                DATETIME(TIMESTAMP_MICROS(user_info.last_active_timestamp_micros), "{utc_ts}") AS user_last_active_timestamp,
                DATETIME(TIMESTAMP_MICROS(user_info.user_first_touch_timestamp_micros), "{utc_ts}") AS user_first_touch_timestamp,
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

def generate_item_table_query(keys_and_types, project_id, dataset_id, event_table_patterns, userid_sub, utc_ts):
    logging.info("Generating the item table query...")

    union_subqueries = [
        f"""
        SELECT
            sub.ueid,
            DATETIME(TIMESTAMP_MICROS(sub.event_timestamp), "{utc_ts}") AS event_timezone,
            sub.event_timestamp AS event_timestamp,
            sub.event_date,
            {userid_sub}
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
            sub.ecommerce.total_item_quantity AS total_item_quantity,
            sub.ecommerce.purchase_revenue_in_usd AS purchase_revenue_in_usd,
            sub.ecommerce.purchase_revenue AS purchase_revenue,			
            sub.ecommerce.refund_value_in_usd AS refund_value_in_usd, 
            sub.ecommerce.refund_value AS refund_value,	
            sub.ecommerce.shipping_value_in_usd AS shipping_value_in_usd,		
            sub.ecommerce.shipping_value AS shipping_value, 
            sub.ecommerce.tax_value_in_usd AS tax_value_in_usd,		
            sub.ecommerce.tax_value AS tax_value,
            sub.ecommerce.unique_items AS unique_items,	
            sub.ecommerce.transaction_id AS transaction_id,
            it.item_id AS item_id,
            it.item_name AS item_name,
            it.item_brand AS item_brand,
            it.item_variant AS item_variant,
            it.item_category AS item_category,
            it.item_category2 AS item_category2,
            it.item_category3 AS item_category3,
            it.item_category4 AS item_category4,
            it.item_category5 AS item_category5,
            it.price_in_usd AS price_in_usd,
            it.price AS price,
            it.quantity AS quantity,
            it.item_revenue_in_usd AS item_revenue_in_usd,
            it.item_revenue AS item_revenue,
            it.item_refund_in_usd AS item_refund_in_usd,
            it.item_refund AS item_refund,
            it.coupon AS coupon,
            it.affiliation AS affiliation,
            it.location_id AS location_id,
            it.item_list_id AS item_list_id,
            it.item_list_name AS item_list_name,
            it.item_list_index AS item_list_index,
            it.promotion_id AS promotion_id,
            it.promotion_name AS promotion_name,
            it.creative_name AS creative_name,
            it.creative_slot AS creative_slot,
        FROM (
            SELECT
                GENERATE_UUID() as ueid,
                *
            FROM 
                `{project_id}.{dataset_id}.{table_pattern}`
        ) sub
        CROSS JOIN UNNEST(sub.items) AS it
        """
        for table_pattern in event_table_patterns
    ]

    sql_query = f"""
    WITH expanded AS (
        {" UNION ALL ".join(union_subqueries)}
    )

    SELECT 
        * 
    FROM 
        expanded
    """

    logging.info(sql_query)

    return sql_query

    logging.info("Event table items generated successfully...")

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

def create_event_table_view(client, project_id, dataset_id, event_table_patterns, userid_sub, keys_and_types, utc_ts):
    event_table_query = generate_event_table_query(keys_and_types, project_id, dataset_id, event_table_patterns, userid_sub, utc_ts)
    create_or_replace_view(client, project_id, dataset_id, "event_table_view", event_table_query)

def create_item_table_view(client, project_id, dataset_id, event_table_patterns, userid_sub, keys_and_types, utc_ts):
    item_table_query = generate_item_table_query(keys_and_types, project_id, dataset_id, event_table_patterns, userid_sub, utc_ts)
    logging.info(item_table_query)
    create_or_replace_view(client, project_id, dataset_id, "item_table_view", item_table_query)