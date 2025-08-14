import pyodbc
import pandas as pd
import boto3
from io import BytesIO

# SQL Server connection
conn = pyodbc.connect(
    r"Driver={ODBC Driver 17 for SQL Server};"
    r"Server=SANDEEPSURE;"
    r"Database=CompanyDB;"  # Replace with your DB name
    r"Trusted_Connection=yes;"
)

# Step 1: Read data from SQL Server
query = "SELECT * FROM Emp"  # Replace with your table name
df = pd.read_sql(query, conn)

print("📦 Data fetched from SQL Server:", df.shape)

# Step 2: Ensure timestamp column is in datetime format
df['LoginTime'] = pd.to_datetime(df['LoginTime'])  # Replace with your column name

# Step 3: Extract date
df['date_only'] = df['LoginTime'].dt.date

# Step 4: S3 client
s3 = boto3.client(
    's3',
    aws_access_key_id='',
    aws_secret_access_key='',
    region_name='us-east-2'
)

bucket_name = "sandy-1947"

# Step 5: Loop through each date and upload separately
for date_value, group in df.groupby('date_only'):
    buffer = BytesIO()
    group.drop(columns=['date_only'], inplace=True)
    group.to_parquet(buffer, index=False)
    buffer.seek(0)

    folder_path = f"transformed/{date_value}/data.parquet"
    s3.upload_fileobj(buffer, bucket_name, folder_path)

    print(f"✅ Uploaded {len(group)} rows to s3://{bucket_name}/{folder_path}")

# Close SQL connection
conn.close()

