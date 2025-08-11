import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Set up Glue context
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Sample: Read from S3
input_path = "s3://sandy-1947/data/input/"
df = spark.read.csv(input_path, header=True, inferSchema=True)

# Sample Transformation
filtered_df = df.filter(df["amount"] > 1000)
# Sample: Add a new column
filtered_df = filtered_df.withColumn(
    "high_value", filtered_df["amount"] > 5000)  # Sample: Write to S3
output_path = "s3://sandy-1947/data/output/"
filtered_df.write.mode("overwrite").csv(output_path)

# Commit Glue job
job.commit(