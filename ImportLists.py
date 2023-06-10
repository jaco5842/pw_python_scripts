import requests
import json
import pyodbc
import datetime
from tqdm import tqdm

# List of IDs
ids = [326, 294, 325, 329, 327, 295, 328, 330, 296]

url = "https://philipsonwine.api-us1.com/api/3/lists/"
contacts_url = "https://philipsonwine.api-us1.com/api/3/contacts"

headers = {
    "accept": "application/json",
    "Api-Token": "1148dfaaa29966d494529f210f3a93970c7ad0bfaba9d267e18d1ec32a58f59a65527607"
}

# Set up connection to SQL Server
conn = pyodbc.connect(
    "Driver=ODBC Driver 17 for SQL Server;"
    "Server=PW-SQL-BI;"
    "Database=ActiveCampaign;"
    "Trusted_Connection=yes;"
)

# Create a cursor for executing SQL queries
cursor = conn.cursor()

# Create a tqdm progress bar
progress_bar = tqdm(ids, desc="Processing Lists", unit="List")

for id in progress_bar:
    request_url = url + str(id)
    response = requests.get(request_url, headers=headers)

    if response.status_code == 200:
        data = response.json()
        required_data = {
            "name": data["list"]["name"],
            "id": data["list"]["id"],
        }
        beautified_response = json.dumps(required_data, indent=2)
        #print(beautified_response)

        # Perform additional request to count emails associated with the list
        contacts_params = {
            "listid": id,
            "status": "+1"
        }
        contacts_response = requests.get(contacts_url, headers=headers, params=contacts_params)

        if contacts_response.status_code == 200:
            contacts_data = contacts_response.json()
            total_contacts = contacts_data["meta"]["total"]
            #print("Total Contacts:", total_contacts)

            # Insert the data into the SQL table if it doesn't already exist
            insert_query = """
                INSERT INTO active_campaign_list_subscribers (name, id, contact_count, date, company)
                SELECT ?, ?, ?, ?,?
                WHERE NOT EXISTS (
                    SELECT 1 FROM active_campaign_list_subscribers WHERE id = ? AND date = ?
                )
            """
            current_date = datetime.date.today()
            company = "Philipson Wine"
            cursor.execute(insert_query, (required_data["name"], required_data["id"], total_contacts, current_date, company, required_data["id"], current_date))
            conn.commit()

        else:
            print("Request for contacts count failed with status code:", contacts_response.status_code)
            print("Error message:", contacts_response.text)

    else:
        print("Request for ID", id, "failed with status code:", response.status_code)
        print("Error message:", response.text)

# Close the cursor and connection
cursor.close()
conn.close()
