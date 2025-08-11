import pandas as pd
import boto3
import os

# S3 bucket and prefix

staging_bucket = 'siva-staging'
staging_prefix = ''  # This is the folder containing your .parquet files

curated_bucket = 'siva-curated'
curated_key = 'siva-curated/merged_dataset.parquet'

# Create S3 client
s3 = boto3.client('s3')

# Get list of Parquet files from staging
objects = s3.list_objects_v2(Bucket=staging_bucket, Prefix=staging_prefix)
parquet_files = [obj['Key'] for obj in objects.get('Contents', []) if obj['Key'].endswith('.parquet')]

# Read and concatenate Parquet files
df_list = []
for file_key in parquet_files:
    s3_path = f's3://{staging_bucket}/{file_key}'
    print(f"Reading: {s3_path}")
    df = pd.read_parquet(s3_path, engine='pyarrow')
    df_list.append(df)

merged_df = pd.concat(df_list, ignore_index=True)

# 🧪 --- Apply your transformations here ---
# Example transformation: drop duplicates and add a calculated column
merged_df.drop_duplicates(inplace=True)


# Save to curated bucket
output_path = f's3://{curated_bucket}/{curated_key}'
merged_df.to_parquet(output_path, engine='pyarrow', index=False)
print(f"✅ Output saved to {output_path}")

