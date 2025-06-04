import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Get job parameters
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'source_path', 'target_path'])

# Initialize Spark and Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Print Hello World message with job information
print("="*80)
print("HELLO WORLD FROM AWS GLUE!")
print("="*80)
print(f"Job Name: {args['JOB_NAME']}")
print(f"Source Path: {args['source_path']}")
print(f"Target Path: {args['target_path']}")
print("="*80)

# Create a simple DataFrame as a demonstration
data = [("Hello", 1), ("World", 2), ("Glue", 3), ("Job", 4)]
columns = ["word", "count"]
df = spark.createDataFrame(data, columns)

# Show the DataFrame
print("Sample DataFrame:")
df.show()

# Write the DataFrame to the target location
print(f"Writing data to: {args['target_path']}")
df.write.mode("overwrite").parquet(args['target_path'])

# Complete the job
job.commit()
print("Glue job completed successfully!")