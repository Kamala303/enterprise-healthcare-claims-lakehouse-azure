"""
Bronze Members Ingestion Notebook
Loads raw members data from ADLS into Bronze Delta table with metadata.
"""

# Databricks notebook source
# MAGIC %md
# MAGIC ## Bronze Members Ingestion
# MAGIC 
# MAGIC This notebook:
# MAGIC 1. Reads raw members CSV files from ADLS raw zone
# MAGIC 2. Validates basic schema compliance
# MAGIC 3. Adds metadata columns (ingestion_timestamp, source_file_name, pipeline_run_id)
# MAGIC 4. Writes to Bronze Delta table in append-only mode
# MAGIC 5. Records audit metrics

# COMMAND ----------

# Import required libraries
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.utils import AnalysisException
import uuid
from datetime import datetime

# Import custom utilities
import sys
sys.path.append('/Workspace/src')
from src.config.config import Config
from src.utils.spark_session import SparkSessionManager, add_metadata_columns

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Configuration and Setup

# COMMAND ----------

# Initialize configuration
pipeline_run_id = f"MEMBERS_BRONZE_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{str(uuid.uuid4())[:8]}"
source_path = Config.get_full_path(Config.MEMBERS_RAW_PATH)
target_path = Config.get_full_path(Config.BRONZE_MEMBERS_PATH)

print(f"Pipeline Run ID: {pipeline_run_id}")
print(f"Source Path: {source_path}")
print(f"Target Path: {target_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Read Raw Members Data

# COMMAND ----------

try:
    # Read CSV files with members schema
    members_schema = SparkSessionManager.get_healthcare_schema("members")
    
    # Read all CSV files from the raw members directory
    raw_members_df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "false")
        .schema(members_schema)
        .load(source_path)
    )
    
    print(f"Successfully read {raw_members_df.count()} records from raw members")
    
    # Show sample data
    print("Sample raw members data:")
    raw_members_df.show(5, truncate=False)
    
    # Display schema
    print("Members schema:")
    raw_members_df.printSchema()
    
except Exception as e:
    print(f"Error reading raw members data: {str(e)}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Basic Data Validation

# COMMAND ----------

# Check for required columns and basic data quality
validation_results = {}

# Check if DataFrame is empty
if raw_members_df.count() == 0:
    raise ValueError("No records found in raw members data")

# Check for null values in critical fields
critical_fields = ["member_id", "member_first_name", "member_last_name", "dob", "gender"]
for field in critical_fields:
    null_count = raw_members_df.filter(col(field).isNull()).count()
    validation_results[f"null_{field}"] = null_count
    print(f"Null {field}: {null_count}")

# Check for valid gender values
invalid_genders = raw_members_df.filter(~col("gender").isin("M", "F")).count()
validation_results["invalid_genders"] = invalid_genders
print(f"Invalid gender values: {invalid_genders}")

# Check for future dates (DOB after current date)
from pyspark.sql.functions import current_date
future_dob = raw_members_df.filter(col("dob") > current_date()).count()
validation_results["future_dob"] = future_dob
print(f"Future dates of birth: {future_dob}")

# Check for duplicate member IDs
total_records = raw_members_df.count()
unique_members = raw_members_df.select("member_id").distinct().count()
duplicate_members = total_records - unique_members
validation_results["duplicate_members"] = duplicate_members
print(f"Duplicate member IDs: {duplicate_members}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Add Metadata Columns

# COMMAND ----------

# Add metadata columns
bronze_members_df = add_metadata_columns(
    raw_members_df,
    source_file_name="members_raw.csv",
    pipeline_run_id=pipeline_run_id
)

# Add additional processing metadata
bronze_members_df = bronze_members_df.withColumn(
    "record_hash", 
    md5(concat_ws("|", *raw_members_df.columns))
)

# Add age calculation for reference
bronze_members_df = bronze_members_df.withColumn(
    "calculated_age",
    floor(months_between(current_date(), col("dob")) / 12)
)

print("Added metadata columns:")
bronze_members_df.select(
    "member_id", "member_first_name", "member_last_name", 
    "ingestion_timestamp", "source_file_name", "pipeline_run_id", 
    "record_hash", "calculated_age"
).show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. Write to Bronze Delta Table

# COMMAND ----------

try:
    # Configure Delta table options
    delta_options = {
        "mergeSchema": "true",
        "overwriteSchema": "false"
    }
    
    # Write to Bronze Delta table in append mode
    (
        bronze_members_df.write
        .format("delta")
        .mode("append")
        .options(**delta_options)
        .option("delta.autoOptimize.optimizeWrite", "true")
        .option("delta.autoOptimize.autoCompact", "true")
        .save(target_path)
    )
    
    print(f"Successfully wrote {bronze_members_df.count()} records to Bronze members table")
    
    # Optimize the table for performance
    spark.sql(f"OPTIMIZE delta.`{target_path}`")
    
    # Vacuum old files (keep 7 days)
    spark.sql(f"VACUUM delta.`{target_path}` RETAIN 168 HOURS")
    
except Exception as e:
    print(f"Error writing to Bronze members table: {str(e)}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6. Record Audit Metrics

# COMMAND ----------

# Create audit record
audit_df = spark.createDataFrame([
    {
        "pipeline_run_id": pipeline_run_id,
        "pipeline_name": "bronze_members_ingestion",
        "source_system": "members_raw_files",
        "target_table": "bronze_members",
        "start_time": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        "status": "SUCCESS",
        "source_row_count": raw_members_df.count(),
        "target_row_count": bronze_members_df.count(),
        "validation_results": str(validation_results),
        "error_message": None
    }
])

# Write audit record
audit_path = Config.get_full_path(Config.PIPELINE_RUNS_PATH)
(
    audit_df.write
    .format("delta")
    .mode("append")
    .partitionBy("pipeline_name")
    .save(audit_path)
)

print("Audit record written successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7. Summary

# COMMAND ----------

print("=" * 60)
print("BRONZE MEMBERS INGESTION SUMMARY")
print("=" * 60)
print(f"Pipeline Run ID: {pipeline_run_id}")
print(f"Source Records: {raw_members_df.count()}")
print(f"Target Records: {bronze_members_df.count()}")
print(f"Validation Results: {validation_results}")
print(f"Target Path: {target_path}")
print(f"Status: SUCCESS")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8. Data Quality Check Results

# COMMAND ----------

# Display final Bronze table statistics
try:
    bronze_table_df = spark.read.format("delta").load(target_path)
    
    print("Bronze Members Table Statistics:")
    print(f"Total Records: {bronze_table_df.count()}")
    print(f"Unique Member IDs: {bronze_table_df.select('member_id').distinct().count()}")
    
    # Age distribution
    bronze_table_df.select(
        floor(months_between(current_date(), col("dob")) / 12).alias("age")
    ).describe().show()
    
    # Gender distribution
    bronze_table_df.groupBy("gender").count().show()
    
    # Show sample records
    print("Sample Bronze Members Records:")
    bronze_table_df.select(
        "member_id", "member_first_name", "member_last_name", 
        "gender", "city", "state", "ingestion_timestamp"
    ).show(5, truncate=False)
    
except Exception as e:
    print(f"Error reading Bronze table for validation: {str(e)}")
