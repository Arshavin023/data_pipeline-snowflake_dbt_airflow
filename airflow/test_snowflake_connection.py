import snowflake.connector
import datetime

# conn = snowflake.connector.connect(
#     user="ARSHAVIN",
#     password="Neymar02349yahoo",
#     account="pd73040.us-east-2",  # Update this if needed
#     warehouse="dbt_wh",
#     database="dbt_db",
#     schema="staging"
# )

# cur = conn.cursor()
# cur.execute("SELECT CURRENT_VERSION();")
# print(cur.fetchone())  # Should print Snowflake version if successful

# conn.close()

print(datetime.datetime.now())