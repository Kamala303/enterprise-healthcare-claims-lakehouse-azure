"""
Bronze Claims Ingestion Notebook
Loads raw claims data from ADLS into Bronze Delta table with metadata.
"""

# Databricks notebook source
# MAGIC %md
# MAGIC ## Bronze Claims Ingestion
# MAGIC 
# MAGIC This notebook:
# MAGIC 1. Reads raw claims CSV files from ADLS raw zone
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
pipeline_run_id = f"CLAIMS_BRONZE_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{str(uuid.uuid4())[:8]}"
source_path = Config.get_full_path(Config.CLAIMS_RAW_PATH)
target_path = Config.get_full_path(Config.BRONZE_CLAIMS_PATH)

print(f"Pipeline Run ID: {pipeline_run_id}")
print(f"Source Path: {source_path}")
print(f"Target Path: {target_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Read Raw Claims Data

# COMMAND ----------

try:
    # Read CSV files with claims schema
    claims_schema = SparkSessionManager.get_healthcare_schema("claims")
    
    # Read all CSV files from the raw claims directory
    raw_claims_df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "false")
        .schema(claims_schema)
        .load(source_path)
    )
    
    print(f"Successfully read {raw_claims_df.count()} records from raw claims")
    
    # Show sample data
    print("Sample raw claims data:")
    raw_claims_df.show(5, truncate=False)
    
    # Display schema
    print("Claims schema:")
    raw_claims_df.printSchema()
    
except Exception as e:
    print(f"Error reading raw claims data: {str(e)}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Basic Data Validation

# COMMAND ----------

# Check for required columns and basic data quality
validation_results = {}

# Check if DataFrame is empty
if raw_claims_df.count() == 0:
    raise ValueError("No records found in raw claims data")

# Check for null values in critical fields
critical_fields = ["claim_id", "claim_line_id", "member_id", "provider_id", "claim_amount"]
for field in critical_fields:
    null_count = raw_claims_df.filter(col(field).isNull()).count()
    validation_results[f"null_{field}"] = null_count
    print(f"Null {field}: {null_count}")

# Check for negative claim amounts
negative_amounts = raw_claims_df.filter(col("claim_amount") < 0).count()
validation_results["negative_claim_amounts"] = negative_amounts
print(f"Negative claim amounts: {negative_amounts}")

# Check for duplicate claim_id + claim_line_id combinations
total_records = raw_claims_df.count()
unique_records = raw_claims_df.select("claim_id", "claim_line_id").distinct().count()
duplicates = total_records - unique_records
validation_results["duplicate_claim_lines"] = duplicates
print(f"Duplicate claim lines: {duplicates}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Add Metadata Columns

# COMMAND ----------

# Add metadata columns
bronze_claims_df = add_metadata_columns(
    raw_claims_df,
    source_file_name="claims_raw.csv",
    pipeline_run_id=pipeline_run_id
)

# Add additional processing metadata
bronze_claims_df = bronze_claims_df.withColumn(
    "record_hash", 
    md5(concat_ws("|", *raw_claims_df.columns))
)

print("Added metadata columns:")
bronze_claims_df.select(
    "claim_id", "claim_line_id", "ingestion_timestamp", 
    "source_file_name", "pipeline_run_id", "record_hash"
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
        bronze_claims_df.write
        .format("delta")
        .mode("append")
        .options(**delta_options)
        .option("delta.autoOptimize.optimizeWrite", "true")
        .option("delta.autoOptimize.autoCompact", "true")
        .save(target_path)
    )
    
    print(f"Successfully wrote {bronze_claims_df.count()} records to Bronze claims table")
    
    # Optimize the table for performance
    spark.sql(f"OPTIMIZE delta.`{target_path}`")
    
    # Vacuum old files (keep 7 days)
    spark.sql(f"VACUUM delta.`{target_path}` RETAIN 168 HOURS")
    
except Exception as e:
    print(f"Error writing to Bronze claims table: {str(e)}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6. Record Audit Metrics

# COMMAND ----------

# Create audit record
audit_df = spark.createDataFrame([
    {
        "pipeline_run_id": pipeline_run_id,
        "pipeline_name": "bronze_claims_ingestion",
        "source_system": "claims_raw_files",
        "target_table": "bronze_claims",
        "start_time": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        "status": "SUCCESS",
        "source_row_count": raw_claims_df.count(),
        "target_row_count": bronze_claims_df.count(),
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
print("BRONZE CLAIMS INGESTION SUMMARY")
print("=" * 60)
print(f"Pipeline Run ID: {pipeline_run_id}")
print(f"Source Records: {raw_claims_df.count()}")
print(f"Target Records: {bronze_claims_df.count()}")
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
    
    print("Bronze Claims Table Statistics:")
    print(f"Total Records: {bronze_table_df.count()}")
    print(f"Unique Claim IDs: {bronze_table_df.select('claim_id').distinct().count()}")
    print(f"Unique Claim Lines: {bronze_table_df.select('claim_id', 'claim_line_id').distinct().count()}")
    
    # Show sample records
    print("Sample Bronze Claims Records:")
    bronze_table_df.select(
        "claim_id", "claim_line_id", "member_id", "provider_id", 
        "claim_amount", "claim_status", "ingestion_timestamp"
    ).show(5, truncate=False)
    
except Exception as e:
    print(f"Error reading Bronze table for validation: {str(e)}")
