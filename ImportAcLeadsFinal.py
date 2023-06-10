import requests
import json
import pyodbc
import datetime

# Set up connection to SQL Server
conn = pyodbc.connect(
    "Driver={SQL Server};"
    "Server=PW-SQL-BI;"
    "Database=ActiveCampaign;"
    "Trusted_Connection=yes;"
)

# Create a cursor object
cursor = conn.cursor()

# Make request to API to get contacts
url = "https://philipsonwine.api-us1.com/api/3/contacts&segmentid=3962"
params = {
    "segmentid": 3962,
    "fields[fieldvalues]": "63,65,67,68,69,71,73,74",  # Include all fields you want to extract here
    "limit": "100",
    "offset": "0",
}
headers = {
    "Api-Token": "1148dfaaa29966d494529f210f3a93970c7ad0bfaba9d267e18d1ec32a58f59a65527607",
    "accept": "application/json"
}

# Get the input for the date to filter by inclusive to only get today write "today" if yesterday and today write "yesterday" else date that is inclusive
input_date = "today"

# Update the "updated_after" parameter based on the input date
if input_date.lower() == "today":
    updated_after_date = datetime.datetime.now().strftime("%d-%m-%Y")
else:
    updated_after_date = input_date

params["filters[updated_after]"] = updated_after_date

# Set max_offset to None to disable it
max_offset = None

# Loop through requests until there is no more data
offset = 0
while True:
    params['offset'] = str(offset)
    response = requests.get(url, params=params, headers=headers)
    json_response = response.json()
    
    # process contacts here
    for contact in json_response['contacts']:
        email = contact['email']
        ActiveCampaignID = contact['id']
        created_timestamp = contact['created_timestamp']
        updated_timestamp = contact['updated_timestamp']
        fieldValuesURL = contact['links']['fieldValues']

        # Make request to API to get field values for this contact
        field_values_response = requests.get(fieldValuesURL, headers=headers)
        field_values_json_response = field_values_response.json()

        # Find the values for each field
        Country = None
        AdId = None
        AdName = None
        AdsetName = None
        CampaignName = None
        Platform = None
        leadad_timestamp = None
        from_leadad = None

        for field in field_values_json_response['fieldValues']:
            if field['field'] == '65':
                Country = field['value']
            elif field['field'] == '67':
                AdId = field['value']
            elif field['field'] == '68':
                AdName = field['value']
            elif field['field'] == '69':
                AdsetName = field['value']
            elif field['field'] == '71':
                Platform = field['value']
            elif field['field'] == '73':
                leadad_timestamp = field['value']
            elif field['field'] == '74':
                from_leadad = field['value']
            elif field['field'] == '60':
                 CampaignName = field['value']
                

        # Check if email already exists in the table
        select_query = "SELECT email FROM active_campaign_purchase_leads WHERE email = ?"
        cursor.execute(select_query, email)
        result = cursor.fetchone()

        if result is None:
            # Insert the data into the SQL Server table
            insert_query = "INSERT INTO active_campaign_purchase_leads (email, ActiveCampaignID, created_timestamp, updated_timestamp, fieldValuesURL, Country, AdId, AdName, AdsetName, CampaignName, Platform, leadad_timestamp, from_leadad) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
            cursor.execute(insert_query, email, ActiveCampaignID, created_timestamp, updated_timestamp, fieldValuesURL, Country, AdId, AdName, AdsetName, CampaignName, Platform, leadad_timestamp, from_leadad)

        else:
            print(f"Skipped record for email: {email}")

    # Check if we have exceeded the maximum offset
    if max_offset is not None and offset >= max_offset:
        break
        
    # Check if we have reached the end of the contacts
    if len(json_response['contacts']) < 1:
        break
    
    # Increment the offset by 100
    offset += 100



# Commit the transaction and close the connection
cursor.commit()
conn.close()
