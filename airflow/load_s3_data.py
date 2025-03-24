# import boto3
import pandas as pd 
from faker import Faker 
import random 
from io import StringIO 
from datetime import datetime
from snowflake import connector
from sqlalchemy import create_engine



# # Initialize Fake and AWS S3 client
fake = Faker()
# s3 = boto3.client()

def generate_cdc_order_data(num_rows=5000):
    data = []
    for _ in range(num_rows):
        order = {
            'order_id': fake.uuid4(),
            'customer_id': fake.uuid4(),
            'order_date': fake.date_this_year(),
            'status': random.choice(['CREATED', 'SHIPPED', 'DELIVERED', 'CANCELED']),
            'product_id': fake.uuid4(),
            'quantity': random.randint(1,50),
            'price': round(random.uniform(10.0, 500.0),2),
            'total_amount': 0.0, # This will be calculated
            'cdc_timestamp': datetime.now() #Simulate CDC timestamp
        }
        order['total_amount'] = round(order['quantity'] * order['price'], 2)
        data.append(order)
    # Convert to DataFrame
    df = pd.DataFrame(data)
    return df

second_transaction = generate_cdc_order_data(5000)
second_transaction.to_csv('second_transaction.csv',header=True,index=False)

# # Connect to Snowflake
# conn = connector.connect(
#     user="ARSHAVIN",
#     password="Neymar02349yahoo",
#     account="pd73040.us-east-2.aws",
#     warehouse = "dbt_wh",
#     database = "dbt_db",
#     schema="staging"
# )

# user="ARSHAVIN",
# password="Neymar02349yahoo",
# account="pd73040.us-east-2.aws",
# warehouse = "dbt_wh",
# database = "dbt_db",
# schema="staging"

# # Create SQLAlchemy engine for Snowflake
# engine = create_engine(
#     f"snowflake://{user}:{password}@{account}/{database}/{schema}?warehouse={warehouse}&role=ACCOUNTADMIN"
# )
# # Upload DataFrame to Snowflake
# # first_transaction.to_sql("raw_orders", con=conn, if_exists="append", index=False)
# # first_transaction.to_sql("raw_orders", con=engine, if_exists="append", index=False)

# conn = engine.connect()
# print("Connection successful!")
# conn.close()

