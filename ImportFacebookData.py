import requests
import pyodbc
from tqdm import tqdm

# Set up connection to SQL Server
conn = pyodbc.connect(
    "Driver=ODBC Driver 17 for SQL Server;"
    "Server=PW-SQL-BI;"
    "Database=ActiveCampaign;"
    "Trusted_Connection=yes;"
)

# Define the API endpoint and query parameters
url = "https://connectors.windsor.ai/facebook"
params = {
    "api_key": "e8abfc04375dd42edb2043f9dc9d45ae8f97",
    "date_from": "2021-01-01",
    "date_to": "2021-01-01",
    "fields": "ad_id,actions_lead,ad_name,adset_name,campaign,clicks,cost_per_action_type_lead,ctr,date,spend"
}

# Send the HTTP request and fetch the response
response = requests.get(url, params=params)
data = response.json()["data"]

# Insert or update the data in the SQL Server table
cursor = conn.cursor()
total_rows = len(data)
created_rows = 0
updated_rows = 0
with tqdm(total=total_rows, desc="Processing rows") as pbar:
    try:
        for row in data:
            ad_id = row['ad_id']
            date = row['date']
            query = f"SELECT COUNT(*) FROM facebook_ads_data WHERE ad_id = '{ad_id}' AND date = '{date}'"
            cursor.execute(query)
            row_count = cursor.fetchone()[0]
            if row_count > 0:
                # If the row already exists, update it
                actions_lead = row['actions_lead'] if row['actions_lead'] is not None else 'NULL'
                cost_per_action_type_lead = row['cost_per_action_type_lead'] if row['cost_per_action_type_lead'] is not None else 'NULL'
                ad_name = row['ad_name'].replace("'", "''") if row['ad_name'] is not None else 'NULL' # Escape single quotes in ad_name
                adset_name = row['adset_name'].replace("'", "''") if row['adset_name'] is not None else 'NULL' # Escape single quotes in adset_name
                campaign = row['campaign'].replace("'", "''") if row['campaign'] is not None else 'NULL' # Escape single quotes in campaign
                query = f"UPDATE facebook_ads_data SET leads = {actions_lead}, ad_name = '{ad_name}', adset_name = '{adset_name}', campaign = '{campaign}', clicks = {row['clicks']}, cost_per_lead = {cost_per_action_type_lead}, ctr = {row['ctr']}, spend = {row['spend']} WHERE ad_id = '{ad_id}' AND date = '{date}'"
                cursor.execute(query)
                updated_rows += 1
            else:
                # If the row doesn't exist, insert it
                actions_lead = row['actions_lead'] if row['actions_lead'] is not None else 'NULL'
                cost_per_action_type_lead = row['cost_per_action_type_lead'] if row['cost_per_action_type_lead'] is not None else 'NULL'
                ad_name = row['ad_name'].replace("'", "''") if row['ad_name'] is not None else 'NULL' # Escape single quotes in ad_name
                adset_name = row['adset_name'].replace("'", "''") if row['adset_name'] is not None else 'NULL' # Escape single quotes in adset_name
                campaign = row['campaign'].replace("'", "''") if row['campaign'] is not None else 'NULL' # Escape single quotes in campaign
                query = f"INSERT INTO facebook_ads_data (leads, ad_name, adset_name, campaign, clicks, cost_per_lead, ctr, date, spend, ad_id) VALUES ({actions_lead}, '{ad_name}', '{adset_name}', '{campaign}', {row['clicks']}, {cost_per_action_type_lead}, {row['ctr']}, '{row['date']}', {row['spend']}, '{ad_id}')"
                cursor.execute(query)
                created_rows += 1
            pbar.update(1)
        conn.commit()
    except Exception as e:
        print(f"An error occurred: {str(e)}")

# Print the number of updated and created rows
print(f"Updated {updated_rows} rows.")
print(f"Created {created_rows} rows.")
