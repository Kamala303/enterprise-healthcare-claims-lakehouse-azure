"""
Silver Claims Transformation Notebook
Transforms Bronze claims data into clean, validated Silver layer with data quality checks.
"""

# Databricks notebook source
# MAGIC %md
# MAGIC ## Silver Claims Transformation
# MAGIC 
# MAGIC This notebook:
# MAGIC 1. Reads Bronze claims data
# MAGIC 2. Performs data cleaning and standardization
# MAGIC 3. Validates against quality rules
# MAGIC 4. Separates clean records from quarantine
# MAGIC 5. Enriches with reference data
# MAGIC 6. Writes to Silver tables (current, history, quarantine)
# MAGIC 7. Records data quality results

# COMMAND ----------

# Import required libraries
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import uuid
from datetime import datetime
from functools import reduce

# Import custom utilities
import sys
sys.path.append('/Workspace/src')
from src.config.config import Config
from src.utils.spark_session import SparkSessionManager
from src.quality.rules import DataQualityRules

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Configuration and Setup

# COMMAND ----------

# Initialize configuration
pipeline_run_id = f"CLAIMS_SILVER_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{str(uuid.uuid4())[:8]}"

# Source and target paths
bronze_claims_path = Config.get_full_path(Config.BRONZE_CLAIMS_PATH)
silver_claims_current_path = Config.get_full_path(Config.SILVER_CLAIMS_CURRENT_PATH)
silver_claims_history_path = Config.get_full_path(Config.SILVER_CLAIMS_HISTORY_PATH)
silver_quarantine_path = Config.get_full_path(Config.SILVER_QUARANTINE_PATH)
silver_data_quality_path = Config.get_full_path(Config.SILVER_DATA_QUALITY_PATH)

# Reference data paths
bronze_diagnosis_path = Config.get_full_path(Config.BRONZE_DIAGNOSIS_REF_PATH)
bronze_procedure_path = Config.get_full_path(Config.BRONZE_PROCEDURE_REF_PATH)
bronze_payer_path = Config.get_full_path(Config.BRONZE_PAYER_REF_PATH)

print(f"Pipeline Run ID: {pipeline_run_id}")
print(f"Source: {bronze_claims_path}")
print(f"Targets:")
print(f"  Current: {silver_claims_current_path}")
print(f"  History: {silver_claims_history_path}")
print(f"  Quarantine: {silver_quarantine_path}")
print(f"  Quality: {silver_data_quality_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Read Bronze Claims Data

# COMMAND ----------

try:
    # Read Bronze claims data
    bronze_claims_df = spark.read.format("delta").load(bronze_claims_path)
    
    print(f"Successfully read {bronze_claims_df.count()} records from Bronze claims")
    
    # Show sample data
    print("Sample Bronze claims data:")
    bronze_claims_df.select(
        "claim_id", "claim_line_id", "member_id", "provider_id",
        "diagnosis_code", "procedure_code", "claim_amount", "claim_status",
        "service_date", "submission_date", "ingestion_timestamp"
    ).show(5, truncate=False)
    
    # Get latest ingestion timestamp to process only new records
    latest_timestamp = bronze_claims_df.agg(max("ingestion_timestamp")).collect()[0][0]
    print(f"Latest ingestion timestamp: {latest_timestamp}")
    
except Exception as e:
    print(f"Error reading Bronze claims data: {str(e)}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Read Reference Data for Enrichment

# COMMAND ----------

# Read reference data
try:
    diagnosis_ref_df = spark.read.format("delta").load(bronze_diagnosis_path)
    procedure_ref_df = spark.read.format("delta").load(bronze_procedure_path)
    payer_ref_df = spark.read.format("delta").load(bronze_payer_path)
    
    print(f"Reference data loaded:")
    print(f"  Diagnosis codes: {diagnosis_ref_df.count()}")
    print(f"  Procedure codes: {procedure_ref_df.count()}")
    print(f"  Payer codes: {payer_ref_df.count()}")
    
except Exception as e:
    print(f"Error reading reference data: {str(e)}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Data Cleaning and Standardization

# COMMAND ----------

# Create a working copy for transformations
claims_clean_df = bronze_claims_df.alias("claims")

# Standardize text fields (uppercase, trim)
claims_clean_df = claims_clean_df.withColumn("claim_id", upper(trim(col("claim_id"))))
claims_clean_df = claims_clean_df.withColumn("claim_line_id", upper(trim(col("claim_line_id"))))
claims_clean_df = claims_clean_df.withColumn("member_id", upper(trim(col("member_id"))))
claims_clean_df = claims_clean_df.withColumn("provider_id", upper(trim(col("provider_id"))))
claims_clean_df = claims_clean_df.withColumn("diagnosis_code", upper(trim(col("diagnosis_code"))))
claims_clean_df = claims_clean_df.withColumn("procedure_code", upper(trim(col("procedure_code"))))
claims_clean_df = claims_clean_df.withColumn("payer_id", upper(trim(col("payer_id"))))
claims_clean_df = claims_clean_df.withColumn("claim_status", upper(trim(col("claim_status"))))
claims_clean_df = claims_clean_df.withColumn("place_of_service", trim(col("place_of_service")))

# Standardize date formats (ensure proper date types)
claims_clean_df = claims_clean_df.withColumn("service_date", to_date(col("service_date")))
claims_clean_df = claims_clean_df.withColumn("submission_date", to_date(col("submission_date")))

# Remove exact duplicates based on business key
claims_deduped_df = claims_clean_df.dropDuplicates(["claim_id", "claim_line_id"])

print(f"After deduplication: {claims_deduped_df.count()} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. Data Quality Validation

# COMMAND ----------

# Initialize data quality rules
quality_rules = DataQualityRules(Config)

# Validate claims data
clean_claims_df, quarantine_claims_df, quality_results = quality_rules.validate_claims(claims_deduped_df)

print("Data Quality Results:")
for rule, result in quality_results.items():
    print(f"  {rule}: {result}")

print(f"\nClean records: {clean_claims_df.count()}")
print(f"Quarantine records: {quarantine_claims_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6. Enrich with Reference Data

# COMMAND ----------

# Enrich clean claims with reference data
enriched_claims_df = clean_claims_df.alias("claims") \
    .join(diagnosis_ref_df.alias("diagnosis"), 
          col("claims.diagnosis_code") == col("diagnosis.diagnosis_code"), "left") \
    .join(procedure_ref_df.alias("procedure"), 
          col("claims.procedure_code") == col("procedure.procedure_code"), "left") \
    .join(payer_ref_df.alias("payer"), 
          col("claims.payer_id") == col("payer.payer_id"), "left")

# Add reference data columns with prefixes
enriched_claims_df = enriched_claims_df.select(
    col("claims.*"),
    col("diagnosis.diagnosis_desc").alias("diagnosis_desc"),
    col("diagnosis.diagnosis_category").alias("diagnosis_category"),
    col("procedure.procedure_desc").alias("procedure_desc"),
    col("procedure.procedure_category").alias("procedure_category"),
    col("payer.payer_name").alias("payer_name"),
    col("payer.payer_type").alias("payer_type")
)

# Add processing metadata
enriched_claims_df = enriched_claims_df.withColumn("silver_processing_timestamp", current_timestamp())
enriched_claims_df = enriched_claims_df.withColumn("silver_pipeline_run_id", lit(pipeline_run_id))

print(f"Enriched claims: {enriched_claims_df.count()} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7. Handle CDC - Create Current and History Tables

# COMMAND ----------

# Read existing Silver current table if it exists
try:
    existing_current_df = spark.read.format("delta").load(silver_claims_current_path)
    print(f"Existing Silver current records: {existing_current_df.count()}")
except:
    existing_current_df = None
    print("No existing Silver current table found")

if existing_current_df is not None:
    # Identify changed records
    existing_keys = existing_current_df.select("claim_id", "claim_line_id")
    new_keys = enriched_claims_df.select("claim_id", "claim_line_id")
    
    # Find records that exist in both but have changed
    changed_records = enriched_claims_df.join(existing_keys, ["claim_id", "claim_line_id"], "inner") \
        .join(existing_current_df, ["claim_id", "claim_line_id"], "inner") \
        .where(
            (col("claims.claim_status") != col("existing_current_df.claim_status")) |
            (col("claims.claim_amount") != col("existing_current_df.claim_amount")) |
            (col("claims.service_date") != col("existing_current_df.service_date"))
        )
    
    # Move old versions to history
    if changed_records.count() > 0:
        history_records = existing_current_df.join(
            changed_records.select("claim_id", "claim_line_id"), 
            ["claim_id", "claim_line_id"], "inner"
        ).withColumn("history_effective_end_date", current_timestamp())
        
        # Write to history
        (
            history_records.write
            .format("delta")
            .mode("append")
            .save(silver_claims_history_path)
        )
        
        print(f"Moved {history_records.count()} records to history")
    
    # Get final current records (new + unchanged existing)
    final_current_df = enriched_claims_df.unionByName(
        existing_current_df.join(
            new_keys, ["claim_id", "claim_line_id"], "left_anti"
        )
    )
else:
    # First time load - all records go to current
    final_current_df = enriched_claims_df

print(f"Final current records: {final_current_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8. Write Silver Tables

# COMMAND ----------

# Write Silver current table (overwrite for full refresh)
(
    final_current_df.write
    .format("delta")
    .mode("overwrite")
    .partitionBy("claim_status", "submission_date")
    .option("overwriteSchema", "true")
    .option("delta.autoOptimize.optimizeWrite", "true")
    .option("delta.autoOptimize.autoCompact", "true")
    .save(silver_claims_current_path)
)

print(f"Written {final_current_df.count()} records to Silver current table")

# Write quarantine records
quarantine_with_metadata = quarantine_claims_df.withColumn("silver_processing_timestamp", current_timestamp()) \
    .withColumn("silver_pipeline_run_id", lit(pipeline_run_id))

(
    quarantine_with_metadata.write
    .format("delta")
    .mode("append")
    .partitionBy("pipeline_run_id")
    .save(silver_quarantine_path)
)

print(f"Written {quarantine_with_metadata.count()} records to quarantine table")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 9. Record Data Quality Results

# COMMAND ----------

# Create data quality results record
quality_results_df = spark.createDataFrame([
    {
        "pipeline_run_id": pipeline_run_id,
        "table_name": "silver_claims",
        "rule_name": "claims_validation",
        "status": "SUCCESS",
        "failed_count": quarantine_claims_df.count(),
        "total_count": claims_deduped_df.count(),
        "pass_rate": (clean_claims_df.count() / claims_deduped_df.count()) * 100 if claims_deduped_df.count() > 0 else 0,
        "run_timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        "validation_details": str(quality_results)
    }
])

(
    quality_results_df.write
    .format("delta")
    .mode("append")
    .save(silver_data_quality_path)
)

print("Data quality results recorded")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 10. Summary

# COMMAND ----------

print("=" * 80)
print("SILVER CLAIMS TRANSFORMATION SUMMARY")
print("=" * 80)
print(f"Pipeline Run ID: {pipeline_run_id}")
print(f"Source Records (Bronze): {bronze_claims_df.count()}")
print(f"After Deduplication: {claims_deduped_df.count()}")
print(f"Clean Records: {clean_claims_df.count()}")
print(f"Quarantine Records: {quarantine_claims_df.count()}")
print(f"Final Current Records: {final_current_df.count()}")
print(f"Quality Pass Rate: {(clean_claims_df.count() / claims_deduped_df.count()) * 100:.2f}%")
print()
print("Quality Validation Results:")
for rule, result in quality_results.items():
    print(f"  {rule}: {result}")
print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 11. Data Quality Verification

# COMMAND ----------

# Verify Silver tables
try:
    # Current table verification
    current_df = spark.read.format("delta").load(silver_claims_current_path)
    print(f"Silver Current Table: {current_df.count()} records")
    
    # Show distribution by status
    current_df.groupBy("claim_status").count().orderBy(desc("count")).show()
    
    # Show sample enriched records
    current_df.select(
        "claim_id", "member_id", "provider_id", "claim_amount", 
        "claim_status", "diagnosis_desc", "procedure_desc", "payer_type"
    ).show(5, truncate=False)
    
    # Quarantine table verification
    quarantine_df = spark.read.format("delta").load(silver_quarantine_path)
    print(f"Quarantine Table: {quarantine_df.count()} records")
    
    if quarantine_df.count() > 0:
        quarantine_df.groupBy("quarantine_reasons").count().orderBy(desc("count")).show(5)
    
except Exception as e:
    print(f"Error verifying Silver tables: {str(e)}")
