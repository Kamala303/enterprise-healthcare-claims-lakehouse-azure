"""
Bronze Providers Ingestion Notebook
Loads raw providers data from ADLS into Bronze Delta table with metadata.
"""

# Databricks notebook source
# MAGIC %md
# MAGIC ## Bronze Providers Ingestion
# MAGIC 
# MAGIC This notebook:
# MAGIC 1. Reads raw providers CSV files from ADLS raw zone
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
pipeline_run_id = f"PROVIDERS_BRONZE_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{str(uuid.uuid4())[:8]}"
source_path = Config.get_full_path(Config.PROVIDERS_RAW_PATH)
target_path = Config.get_full_path(Config.BRONZE_PROVIDERS_PATH)

print(f"Pipeline Run ID: {pipeline_run_id}")
print(f"Source Path: {source_path}")
print(f"Target Path: {target_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Read Raw Providers Data

# COMMAND ----------

try:
    # Read CSV files with providers schema
    providers_schema = SparkSessionManager.get_healthcare_schema("providers")
    
    # Read all CSV files from the raw providers directory
    raw_providers_df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "false")
        .schema(providers_schema)
        .load(source_path)
    )
    
    print(f"Successfully read {raw_providers_df.count()} records from raw providers")
    
    # Show sample data
    print("Sample raw providers data:")
    raw_providers_df.show(5, truncate=False)
    
    # Display schema
    print("Providers schema:")
    raw_providers_df.printSchema()
    
except Exception as e:
    print(f"Error reading raw providers data: {str(e)}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Basic Data Validation

# COMMAND ----------

# Check for required columns and basic data quality
validation_results = {}

# Check if DataFrame is empty
if raw_providers_df.count() == 0:
    raise ValueError("No records found in raw providers data")

# Check for null values in critical fields
critical_fields = ["provider_id", "provider_name", "specialty", "network_status"]
for field in critical_fields:
    null_count = raw_providers_df.filter(col(field).isNull()).count()
    validation_results[f"null_{field}"] = null_count
    print(f"Null {field}: {null_count}")

# Check for valid network status values
valid_network_statuses = ["IN_NETWORK", "OUT_OF_NETWORK"]
invalid_network_status = raw_providers_df.filter(
    ~col("network_status").isin(valid_network_statuses)
).count()
validation_results["invalid_network_status"] = invalid_network_status
print(f"Invalid network status values: {invalid_network_status}")

# Check for duplicate provider IDs
total_records = raw_providers_df.count()
unique_providers = raw_providers_df.select("provider_id").distinct().count()
duplicate_providers = total_records - unique_providers
validation_results["duplicate_providers"] = duplicate_providers
print(f"Duplicate provider IDs: {duplicate_providers}")

# Check for providers with termination date before effective date
invalid_dates = raw_providers_df.filter(
    col("termination_date").isNotNull() & 
    col("effective_date").isNotNull() &
    (col("termination_date") < col("effective_date"))
).count()
validation_results["invalid_dates"] = invalid_dates
print(f"Providers with invalid date ranges: {invalid_dates}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Add Metadata Columns

# COMMAND ----------

# Add metadata columns
bronze_providers_df = add_metadata_columns(
    raw_providers_df,
    source_file_name="providers_raw.csv",
    pipeline_run_id=pipeline_run_id
)

# Add additional processing metadata
bronze_providers_df = bronze_providers_df.withColumn(
    "record_hash", 
    md5(concat_ws("|", *raw_providers_df.columns))
)

# Add provider status flag (active/inactive based on dates)
bronze_providers_df = bronze_providers_df.withColumn(
    "is_active_provider",
    when(
        col("termination_date").isNull() | (col("termination_date") > current_date()),
        lit(True)
    ).otherwise(lit(False))
)

print("Added metadata columns:")
bronze_providers_df.select(
    "provider_id", "provider_name", "specialty", "network_status",
    "ingestion_timestamp", "source_file_name", "pipeline_run_id", 
    "record_hash", "is_active_provider"
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
        bronze_providers_df.write
        .format("delta")
        .mode("append")
        .options(**delta_options)
        .option("delta.autoOptimize.optimizeWrite", "true")
        .option("delta.autoOptimize.autoCompact", "true")
        .save(target_path)
    )
    
    print(f"Successfully wrote {bronze_providers_df.count()} records to Bronze providers table")
    
    # Optimize the table for performance
    spark.sql(f"OPTIMIZE delta.`{target_path}`")
    
    # Vacuum old files (keep 7 days)
    spark.sql(f"VACUUM delta.`{target_path}` RETAIN 168 HOURS")
    
except Exception as e:
    print(f"Error writing to Bronze providers table: {str(e)}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6. Record Audit Metrics

# COMMAND ----------

# Create audit record
audit_df = spark.createDataFrame([
    {
        "pipeline_run_id": pipeline_run_id,
        "pipeline_name": "bronze_providers_ingestion",
        "source_system": "providers_raw_files",
        "target_table": "bronze_providers",
        "start_time": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        "status": "SUCCESS",
        "source_row_count": raw_providers_df.count(),
        "target_row_count": bronze_providers_df.count(),
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
print("BRONZE PROVIDERS INGESTION SUMMARY")
print("=" * 60)
print(f"Pipeline Run ID: {pipeline_run_id}")
print(f"Source Records: {raw_providers_df.count()}")
print(f"Target Records: {bronze_providers_df.count()}")
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
    
    print("Bronze Providers Table Statistics:")
    print(f"Total Records: {bronze_table_df.count()}")
    print(f"Unique Provider IDs: {bronze_table_df.select('provider_id').distinct().count()}")
    
    # Network status distribution
    bronze_table_df.groupBy("network_status").count().show()
    
    # Specialty distribution
    bronze_table_df.groupBy("specialty").count().orderBy(desc("count")).show(10)
    
    # Active vs inactive providers
    bronze_table_df.groupBy("is_active_provider").count().show()
    
    # Show sample records
    print("Sample Bronze Providers Records:")
    bronze_table_df.select(
        "provider_id", "provider_name", "specialty", "city", 
        "state", "network_status", "is_active_provider", "ingestion_timestamp"
    ).show(5, truncate=False)
    
except Exception as e:
    print(f"Error reading Bronze table for validation: {str(e)}")
