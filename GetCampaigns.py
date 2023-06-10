import requests
import json
import pyodbc
import datetime
import math
from tqdm import tqdm

# Set up connection to SQL Server
conn = pyodbc.connect(
    "Driver=ODBC Driver 17 for SQL Server;"
    "Server=PW-SQL-BI;"
    "Database=ActiveCampaign;"
    "Trusted_Connection=yes;"
)

url = "https://philipsonwine.api-us1.com/api/3/campaigns"
params = {
    "orders[sdate]": "DESC",
    "limit": 100,  # Adjust the limit per page as per your needs
    "offset": 0
}

headers = {
    "accept": "application/json",
    "Api-Token": "1148dfaaa29966d494529f210f3a93970c7ad0bfaba9d267e18d1ec32a58f59a65527607"
}

# Fetch campaign data from the API
response = requests.get(url, headers=headers, params=params)

if response.status_code == 200:
    data = response.json()
    campaigns = data["campaigns"]
    all_campaigns = campaigns[:]  # Create a copy of the campaigns from the first page

    # Check if there are more pages
    if "meta" in data and "total" in data["meta"]:
        total_campaigns = int(data["meta"]["total"])
        total_pages = math.ceil(total_campaigns / params["limit"])
        total_pages = 2  # for testing
        current_page = 1  # Start from the second page

        while current_page <= total_pages:
            params["offset"] = (current_page) * params["limit"]
            response = requests.get(url, headers=headers, params=params)

            if response.status_code == 200:
                data = response.json()
                campaigns = data["campaigns"]

                if len(campaigns) == 0:
                    break  # No more campaigns, break out of the loop

                all_campaigns.extend(campaigns)  # Append the campaigns from the current page to the list

                current_page += 1

            else:
                print("Request for page", current_page, "failed with status code:", response.status_code)

    campaign_counter = len(all_campaigns)  # Count the total number of campaigns

    cursor = conn.cursor()

    for campaign in all_campaigns:  # Iterate over all_campaigns, not campaigns
        # Extract the fields you want to print or insert into your table
        name = campaign['name']
        type = campaign['type']
        userid = campaign['userid']
        segmentid = campaign['segmentid']
        basetemplateid = campaign['basetemplateid']
        source = campaign['source']
        cdate = campaign['cdate']
        sdate = campaign['sdate']
        send_amt = campaign['send_amt']
        total_amt = campaign['total_amt']
        opens = campaign['opens']
        uniqueopens = campaign['uniqueopens']
        linkclicks = campaign['linkclicks']
        uniquelinkclicks = campaign['uniquelinkclicks']
        hardbounces = campaign['hardbounces']
        softbounces = campaign['softbounces']
        unsubscribes = campaign['unsubscribes']
        mail_send = campaign['mail_send']
        analytics_campaign_name = campaign['analytics_campaign_name']
        campaign_message_id = campaign['campaign_message_id']
        segmentname = campaign['segmentname']
        id = campaign['id']
        user = campaign['user']
        automation = campaign['automation']

        # Check if the campaign already exists in the table
        cursor.execute("SELECT * FROM active_campaign_campaigns WHERE id = ?", id)
        existing_campaign = cursor.fetchone()

        if existing_campaign:
            # Update the existing campaign
            cursor.execute(
                """
                UPDATE active_campaign_campaigns
                SET name=?, [type]=?, userid=?, segmentid=?, basetemplateid=?, source=?, cdate=?, sdate=?, send_amt=?,
                total_amt=?, opens=?, uniqueopens=?, linkclicks=?, uniquelinkclicks=?, hardbounces=?, softbounces=?,
                unsubscribes=?, mail_send=?, analytics_campaign_name=?, campaign_message_id=?, segmentname=?, [user]=?,
                automation=?
                WHERE id=?
                """,
                name, type, userid, segmentid, basetemplateid, source, cdate, sdate, send_amt, total_amt, opens,
                uniqueopens, linkclicks, uniquelinkclicks, hardbounces, softbounces, unsubscribes, mail_send,
                analytics_campaign_name, campaign_message_id, segmentname, user, automation, id
            )
        else:
            # Insert the new campaign
            cursor.execute(
                """
                INSERT INTO active_campaign_campaigns
                (name, [type], userid, segmentid, basetemplateid, source, cdate, sdate, send_amt, total_amt, opens,
                uniqueopens, linkclicks, uniquelinkclicks, hardbounces, softbounces, unsubscribes, mail_send,
                analytics_campaign_name, campaign_message_id, segmentname, [user], automation, id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                name, type, userid, segmentid, basetemplateid, source, cdate, sdate, send_amt, total_amt, opens,
                uniqueopens, linkclicks, uniquelinkclicks, hardbounces, softbounces, unsubscribes, mail_send,
                analytics_campaign_name, campaign_message_id, segmentname, user, automation, id
            )

        conn.commit()

    print("Total campaigns:", campaign_counter)
    print("Total pages:", current_page)
    print("Data insertion completed.")

else:
    print("Failed to fetch campaign data. Status code:", response.status_code)
