"""
Bronze Reference Data Ingestion Notebook
Loads reference data (diagnosis, procedure, payer) from ADLS into Bronze Delta tables.
"""

# Databricks notebook source
# MAGIC %md
# MAGIC ## Bronze Reference Data Ingestion
# MAGIC 
# MAGIC This notebook:
# MAGIC 1. Reads reference data CSV files from ADLS raw zone
# MAGIC 2. Validates schema compliance for each reference dataset
# MAGIC 3. Adds metadata columns (ingestion_timestamp, source_file_name, pipeline_run_id)
# MAGIC 4. Writes to Bronze Delta tables in append-only mode
# MAGIC 5. Records audit metrics for each reference dataset

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
pipeline_run_id = f"REFERENCE_BRONZE_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{str(uuid.uuid4())[:8]}"

# Reference data paths
diagnosis_source_path = Config.get_full_path(Config.DIAGNOSIS_REF_PATH)
procedure_source_path = Config.get_full_path(Config.PROCEDURE_REF_PATH)
payer_source_path = Config.get_full_path(Config.PAYER_REF_PATH)

# Target paths
diagnosis_target_path = Config.get_full_path(Config.BRONZE_DIAGNOSIS_REF_PATH)
procedure_target_path = Config.get_full_path(Config.BRONZE_PROCEDURE_REF_PATH)
payer_target_path = Config.get_full_path(Config.BRONZE_PAYER_REF_PATH)

print(f"Pipeline Run ID: {pipeline_run_id}")
print(f"Diagnosis Source: {diagnosis_source_path} -> Target: {diagnosis_target_path}")
print(f"Procedure Source: {procedure_source_path} -> Target: {procedure_target_path}")
print(f"Payer Source: {payer_source_path} -> Target: {payer_target_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Process Diagnosis Reference Data

# COMMAND ----------

def process_reference_data(source_path, target_path, table_name, schema, pipeline_run_id):
    """Generic function to process reference data."""
    
    print(f"\n{'='*40}")
    print(f"Processing {table_name}")
    print(f"{'='*40}")
    
    validation_results = {}
    
    try:
        # Read raw data
        raw_df = (
            spark.read.format("csv")
            .option("header", "true")
            .option("inferSchema", "false")
            .schema(schema)
            .load(source_path)
        )
        
        print(f"Successfully read {raw_df.count()} records from {table_name}")
        raw_df.show(5, truncate=False)
        
        # Basic validation
        if raw_df.count() == 0:
            raise ValueError(f"No records found in {table_name}")
        
        # Check for duplicates in the key field
        key_field = raw_df.columns[0]  # First column is typically the key
        total_records = raw_df.count()
        unique_records = raw_df.select(key_field).distinct().count()
        duplicates = total_records - unique_records
        validation_results[f"duplicate_{key_field}"] = duplicates
        print(f"Duplicate {key_field}: {duplicates}")
        
        # Add metadata
        bronze_df = add_metadata_columns(
            raw_df,
            source_file_name=f"{table_name}_raw.csv",
            pipeline_run_id=pipeline_run_id
        )
        
        bronze_df = bronze_df.withColumn(
            "record_hash", 
            md5(concat_ws("|", *raw_df.columns))
        )
        
        # Write to Delta
        (
            bronze_df.write
            .format("delta")
            .mode("append")
            .option("delta.autoOptimize.optimizeWrite", "true")
            .option("delta.autoOptimize.autoCompact", "true")
            .save(target_path)
        )
        
        print(f"Successfully wrote {bronze_df.count()} records to Bronze {table_name}")
        
        # Optimize table
        spark.sql(f"OPTIMIZE delta.`{target_path}`")
        
        # Create audit record
        audit_df = spark.createDataFrame([
            {
                "pipeline_run_id": pipeline_run_id,
                "pipeline_name": f"bronze_{table_name}_ingestion",
                "source_system": f"{table_name}_raw_files",
                "target_table": f"bronze_{table_name}",
                "start_time": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                "status": "SUCCESS",
                "source_row_count": raw_df.count(),
                "target_row_count": bronze_df.count(),
                "validation_results": str(validation_results),
                "error_message": None
            }
        ])
        
        return {
            "status": "SUCCESS",
            "source_count": raw_df.count(),
            "target_count": bronze_df.count(),
            "validation_results": validation_results,
            "audit_df": audit_df
        }
        
    except Exception as e:
        print(f"Error processing {table_name}: {str(e)}")
        return {
            "status": "ERROR",
            "error": str(e),
            "validation_results": validation_results
        }

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Process All Reference Data

# COMMAND ----------

# Process diagnosis reference data
diagnosis_schema = SparkSessionManager.get_healthcare_schema("diagnosis_ref")
diagnosis_result = process_reference_data(
    diagnosis_source_path, diagnosis_target_path, 
    "diagnosis_ref", diagnosis_schema, pipeline_run_id
)

# Process procedure reference data
procedure_schema = SparkSessionManager.get_healthcare_schema("procedure_ref")
procedure_result = process_reference_data(
    procedure_source_path, procedure_target_path, 
    "procedure_ref", procedure_schema, pipeline_run_id
)

# Process payer reference data
payer_schema = SparkSessionManager.get_healthcare_schema("payer_ref")
payer_result = process_reference_data(
    payer_source_path, payer_target_path, 
    "payer_ref", payer_schema, pipeline_run_id
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Write Audit Records

# COMMAND ----------

# Collect all audit DataFrames
audit_dfs = []
for result in [diagnosis_result, procedure_result, payer_result]:
    if result["status"] == "SUCCESS" and "audit_df" in result:
        audit_dfs.append(result["audit_df"])

# Write all audit records
if audit_dfs:
    audit_path = Config.get_full_path(Config.PIPELINE_RUNS_PATH)
    for audit_df in audit_dfs:
        (
            audit_df.write
            .format("delta")
            .mode("append")
            .partitionBy("pipeline_name")
            .save(audit_path)
        )
    print("All audit records written successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. Summary Report

# COMMAND ----------

print("=" * 80)
print("BRONZE REFERENCE DATA INGESTION SUMMARY")
print("=" * 80)
print(f"Pipeline Run ID: {pipeline_run_id}")
print()

print("Diagnosis Reference:")
print(f"  Status: {diagnosis_result['status']}")
if diagnosis_result["status"] == "SUCCESS":
    print(f"  Records: {diagnosis_result['source_count']} -> {diagnosis_result['target_count']}")
    print(f"  Validation: {diagnosis_result['validation_results']}")
else:
    print(f"  Error: {diagnosis_result.get('error', 'Unknown error')}")

print()
print("Procedure Reference:")
print(f"  Status: {procedure_result['status']}")
if procedure_result["status"] == "SUCCESS":
    print(f"  Records: {procedure_result['source_count']} -> {procedure_result['target_count']}")
    print(f"  Validation: {procedure_result['validation_results']}")
else:
    print(f"  Error: {procedure_result.get('error', 'Unknown error')}")

print()
print("Payer Reference:")
print(f"  Status: {payer_result['status']}")
if payer_result["status"] == "SUCCESS":
    print(f"  Records: {payer_result['source_count']} -> {payer_result['target_count']}")
    print(f"  Validation: {payer_result['validation_results']}")
else:
    print(f"  Error: {payer_result.get('error', 'Unknown error')}")

print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6. Data Quality Verification

# COMMAND ----------

# Verify Bronze tables were created successfully
reference_tables = [
    ("Diagnosis", diagnosis_target_path),
    ("Procedure", procedure_target_path),
    ("Payer", payer_target_path)
]

for table_name, table_path in reference_tables:
    try:
        df = spark.read.format("delta").load(table_path)
        print(f"\n{table_name} Bronze Table:")
        print(f"  Total Records: {df.count()}")
        print(f"  Sample Records:")
        df.show(3, truncate=False)
    except Exception as e:
        print(f"Error reading {table_name} Bronze table: {str(e)}")
