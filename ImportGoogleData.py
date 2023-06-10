import requests
import pyodbc
import json
from tqdm import tqdm

# Set up connection to SQL Server
conn = pyodbc.connect(
    "Driver=ODBC Driver 17 for SQL Server;"
    "Server=PW-SQL-BI;"
    "Database=ActiveCampaign;"
    "Trusted_Connection=yes;"
)

# Define the API endpoint and query parameters
url = "https://connectors.windsor.ai/google_ads"
params = {
    "api_key": "e8abfc04375dd42edb2043f9dc9d45ae8f97",
    "date_from": "2021-01-01",
    "date_to": "2021-12-31",
    "fields": "account_name,ad_group_name,ad_id,campaign,clicks,conversion_rate,ctr,date,impressions,search_term,spend,transactionrevenue,transactions"
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
            query = f"SELECT COUNT(*) FROM google_ads_data WHERE ad_id = '{ad_id}' AND date = '{date}'"
            cursor.execute(query)
            row_count = cursor.fetchone()[0]
            if row_count > 0:
                # If the row already exists, update it
                query = f"UPDATE google_ads_data SET account_name = {row['account_name']}, ad_group_name = '{row['ad_group_name']}', ad_id = '{row['ad_id']}', campaign = '{row['campaign']}', clicks = {row['clicks']}, conversion_rate = {row['conversion_rate']}, ctr = {row['ctr']}, date = {row['date']}, impressions = {row['impressions']}, search_term = {row['search_term']}, spend = {row['spend']}, transactionrevenue = {row['transactionrevenue']}, transactions = {row['transactions']} WHERE ad_id = '{ad_id}' AND date = '{date}'"
                cursor.execute(query)
                updated_rows += 1
            else:
                # If the row doesn't exist, insert it
                query = f"INSERT google_ads_data SET account_name = {row['account_name']}, ad_group_name = '{row['ad_group_name']}', ad_id = '{row['ad_id']}', campaign = '{row['campaign']}', clicks = {row['clicks']}, conversion_rate = {row['conversion_rate']}, ctr = {row['ctr']}, date = {row['date']}, impressions = {row['impressions']}, search_term = {row['search_term']}, spend = {row['spend']}, transactionrevenue = {row['transactionrevenue']}, transactions = {row['transactions']} WHERE ad_id = '{ad_id}' AND date = '{date}'"
                cursor.execute(query)
                created_rows += 1
            pbar.update(1)
        conn.commit()
    except Exception as e:
        print(f"An error occurred: {str(e)}")

# Print the number of updated and created rows
print(f"Updated {updated_rows} rows.")
print(f"Created {created_rows} rows.")
