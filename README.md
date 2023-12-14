# Unlocking your GA4 data

Harness your GA4 data using Google Cloud Platform with a flattened table structure in BigQuery and same day reporting in Looker Studio. The code creates views in your bigquery instance that can used for both hisotrical and same day reporting. It also has a Looker Studio template 

Link to app: [Streamlit App](https://ga4tobq.streamlit.app/)

### You will need
- Your GA4 data streaming to BQ
- JSON key for the tool to access your instance, which can be deactivated once the process is run

## How To...

### Connect Google Analytics 4 (GA4) to BigQuery:
**What this does:** This step ensures that your Google Analytics 4 data is being sent to BigQuery, making it accessible for further analysis.
1. Go to your GA4 Property.
2. Navigate to Integrations.
3. Find BigQuery in the list and click on Link.
4. Follow the on-screen instructions to complete the linking process.
![](https://github.com/team-circle-tech/GA4toBQ/blob/main/gifs/Link_GABQ.gif)

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
![](https://github.com/team-circle-tech/GA4toBQ/blob/main/gifs/Config_JSON.gif)



## FAQ

WIP
