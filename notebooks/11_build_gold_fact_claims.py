"""
Gold Fact Claims Builder Notebook
Creates the central fact table by joining Silver claims with Gold dimensions.
"""

# Databricks notebook source
# MAGIC %md
# MAGIC ## Gold Fact Claims Builder
# MAGIC 
# MAGIC This notebook:
# MAGIC 1. Reads Silver claims data
# MAGIC 2. Joins with Gold dimensions using surrogate keys
# MAGIC 3. Creates the central fact_claims table
# MAGIC 4. Adds calculated measures and derived columns
# MAGIC 5. Implements proper grain (one row per claim line)
# MAGIC 6. Writes Gold fact table with optimal partitioning

# COMMAND ----------

# Import required libraries
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import uuid
from datetime import datetime

# Import custom utilities
import sys
sys.path.append('/Workspace/src')
from src.config.config import Config
from src.utils.spark_session import SparkSessionManager

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Configuration and Setup

# COMMAND ----------

# Initialize configuration
pipeline_run_id = f"FACT_CLAIMS_GOLD_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{str(uuid.uuid4())[:8]}"

# Source paths
silver_claims_current_path = Config.get_full_path(Config.SILVER_CLAIMS_CURRENT_PATH)

# Gold dimension paths
gold_dim_member_path = Config.get_full_path(Config.GOLD_DIM_MEMBER_PATH)
gold_dim_provider_path = Config.get_full_path(Config.GOLD_DIM_PROVIDER_PATH)
gold_dim_diagnosis_path = Config.get_full_path(Config.GOLD_DIM_DIAGNOSIS_PATH)
gold_dim_procedure_path = Config.get_full_path(Config.GOLD_DIM_PROCEDURE_PATH)
gold_dim_payer_path = Config.get_full_path(Config.GOLD_DIM_PAYER_PATH)
gold_dim_date_path = Config.get_full_path(Config.GOLD_DIM_DATE_PATH)

# Target path
gold_fact_claims_path = Config.get_full_path(Config.GOLD_FACT_CLAIMS_PATH)

print(f"Pipeline Run ID: {pipeline_run_id}")
print(f"Source Claims: {silver_claims_current_path}")
print(f"Target Fact: {gold_fact_claims_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Read Source Data

# COMMAND ----------

try:
    # Read Silver claims data
    silver_claims_df = spark.read.format("delta").load(silver_claims_current_path)
    print(f"Silver claims: {silver_claims_df.count()} records")
    
    # Read Gold dimensions
    gold_member_df = spark.read.format("delta").load(gold_dim_member_path)
    gold_provider_df = spark.read.format("delta").load(gold_dim_provider_path)
    gold_diagnosis_df = spark.read.format("delta").load(gold_dim_diagnosis_path)
    gold_procedure_df = spark.read.format("delta").load(gold_dim_procedure_path)
    gold_payer_df = spark.read.format("delta").load(gold_dim_payer_path)
    gold_date_df = spark.read.format("delta").load(gold_dim_date_path)
    
    print(f"Gold dimensions loaded:")
    print(f"  Member: {gold_member_df.count()} records")
    print(f"  Provider: {gold_provider_df.count()} records")
    print(f"  Diagnosis: {gold_diagnosis_df.count()} records")
    print(f"  Procedure: {gold_procedure_df.count()} records")
    print(f"  Payer: {gold_payer_df.count()} records")
    print(f"  Date: {gold_date_df.count()} records")
    
except Exception as e:
    print(f"Error reading source data: {str(e)}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Join Claims with Dimensions

# COMMAND ----------

# Create the fact table by joining claims with dimensions
# Start with claims as the base
fact_claims_df = silver_claims_df.alias("claims")

# Join with Member dimension
fact_claims_df = fact_claims_df.join(
    gold_member_df.alias("member").filter(col("member.is_current") == True),
    fact_claims_df["member_id"] == col("member.member_id"),
    "left"
)

# Join with Provider dimension (get current record)
fact_claims_df = fact_claims_df.join(
    gold_provider_df.alias("provider").filter(col("provider.is_current") == True),
    fact_claims_df["provider_id"] == col("provider.provider_id"),
    "left"
)

# Join with Diagnosis dimension
fact_claims_df = fact_claims_df.join(
    gold_diagnosis_df.alias("diagnosis"),
    fact_claims_df["diagnosis_code"] == col("diagnosis.diagnosis_code"),
    "left"
)

# Join with Procedure dimension
fact_claims_df = fact_claims_df.join(
    gold_procedure_df.alias("procedure"),
    fact_claims_df["procedure_code"] == col("procedure.procedure_code"),
    "left"
)

# Join with Payer dimension
fact_claims_df = fact_claims_df.join(
    gold_payer_df.alias("payer"),
    fact_claims_df["payer_id"] == col("payer.payer_id"),
    "left"
)

# Join with Date dimension for service date
fact_claims_df = fact_claims_df.join(
    gold_date_df.alias("service_date_dim"),
    fact_claims_df["service_date"] == col("service_date_dim.date"),
    "left"
)

# Join with Date dimension for submission date
fact_claims_df = fact_claims_df.join(
    gold_date_df.alias("submission_date_dim"),
    fact_claims_df["submission_date"] == col("submission_date_dim.date"),
    "left"
)

print(f"After all joins: {fact_claims_df.count()} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Build Fact Table with Surrogate Keys and Measures

# COMMAND ----------

# Create the final fact table structure
final_fact_df = fact_claims_df.select(
    # Business keys
    col("claims.claim_id"),
    col("claims.claim_line_id"),
    
    # Surrogate keys (foreign keys to dimensions)
    col("member.member_sk").alias("member_sk"),
    col("provider.provider_sk").alias("provider_sk"),
    col("diagnosis.diagnosis_sk").alias("diagnosis_sk"),
    col("procedure.procedure_sk").alias("procedure_sk"),
    col("payer.payer_sk").alias("payer_sk"),
    col("service_date_dim.date_key").alias("service_date_key"),
    col("submission_date_dim.date_key").alias("submission_date_key"),
    
    # Measures
    col("claims.claim_amount").alias("claim_amount"),
    
    # Attributes (denormalized for convenience)
    col("claims.claim_status").alias("claim_status"),
    col("claims.place_of_service").alias("place_of_service"),
    col("claims.diagnosis_code").alias("diagnosis_code"),
    col("claims.procedure_code").alias("procedure_code"),
    col("claims.payer_id").alias("payer_id"),
    
    # Member attributes
    col("member.member_age").alias("member_age"),
    col("member.member_age_group").alias("member_age_group"),
    col("member.gender").alias("member_gender"),
    col("member.state").alias("member_state"),
    
    # Provider attributes
    col("provider.specialty").alias("provider_specialty"),
    col("provider.specialty_category").alias("provider_specialty_category"),
    col("provider.network_status").alias("provider_network_status"),
    col("provider.provider_region").alias("provider_region"),
    
    # Payer attributes
    col("payer.payer_type").alias("payer_type"),
    
    # Date attributes
    col("service_date_dim.year").alias("service_year"),
    col("service_date_dim.quarter").alias("service_quarter"),
    col("service_date_dim.month").alias("service_month"),
    col("service_date_dim.season").alias("service_season"),
    col("submission_date_dim.year").alias("submission_year"),
    col("submission_date_dim.quarter").alias("submission_quarter"),
    
    # Original dates
    col("claims.service_date").alias("service_date"),
    col("claims.submission_date").alias("submission_date")
)

# Add calculated measures
final_fact_df = final_fact_df.withColumn(
    "is_paid_claim",
    col("claim_status") == "PAID"
).withColumn(
    "is_denied_claim",
    col("claim_status") == "DENIED"
).withColumn(
    "is_pending_claim",
    col("claim_status") == "PENDING"
).withColumn(
    "is_adjusted_claim",
    col("claim_status") == "ADJUSTED"
).withColumn(
    "paid_amount",
    when(col("is_paid_claim"), col("claim_amount")).otherwise(lit(0.0))
).withColumn(
    "denied_amount",
    when(col("is_denied_claim"), col("claim_amount")).otherwise(lit(0.0))
).withColumn(
    "pending_amount",
    when(col("is_pending_claim"), col("claim_amount")).otherwise(lit(0.0))
).withColumn(
    "adjusted_amount",
    when(col("is_adjusted_claim"), col("claim_amount")).otherwise(lit(0.0))
).withColumn(
    "claim_line_key",
    concat(col("claim_id"), lit("_"), col("claim_line_id"))
).withColumn(
    "processing_delay_days",
    datediff(col("submission_date"), col("service_date"))
).withColumn(
    "is_high_value_claim",
    col("claim_amount") > 1000.0
).withColumn(
    "is_out_of_network",
    col("provider_network_status") == "OUT_OF_NETWORK"
)

# Add processing metadata
final_fact_df = final_fact_df.withColumn(
    "fact_processing_timestamp",
    current_timestamp()
).withColumn(
    "fact_pipeline_run_id",
    lit(pipeline_run_id)
)

print(f"Final fact table: {final_fact_df.count()} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. Data Quality Checks on Fact Table

# COMMAND ----------

# Perform data quality checks on the fact table
quality_results = {}

# Check for null surrogate keys
null_checks = ["member_sk", "provider_sk", "diagnosis_sk", "procedure_sk", "payer_sk"]
for key in null_checks:
    null_count = final_fact_df.filter(col(key).isNull()).count()
    quality_results[f"null_{key}"] = null_count
    print(f"Null {key}: {null_count}")

# Check for null date keys
null_date_keys = ["service_date_key", "submission_date_key"]
for key in null_date_keys:
    null_count = final_fact_df.filter(col(key).isNull()).count()
    quality_results[f"null_{key}"] = null_count
    print(f"Null {key}: {null_count}")

# Check for negative amounts
negative_amounts = final_fact_df.filter(col("claim_amount") < 0).count()
quality_results["negative_amounts"] = negative_amounts
print(f"Negative amounts: {negative_amounts}")

# Check for future dates
future_service_dates = final_fact_df.filter(col("service_date") > current_date()).count()
quality_results["future_service_dates"] = future_service_dates
print(f"Future service dates: {future_service_dates}")

# Check processing delays
invalid_delays = final_fact_df.filter(col("processing_delay_days") < 0).count()
quality_results["invalid_processing_delays"] = invalid_delays
print(f"Invalid processing delays: {invalid_delays}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6. Write Gold Fact Table

# COMMAND ----------

try:
    # Write Gold fact table with optimal partitioning
    (
        final_fact_df.write
        .format("delta")
        .mode("overwrite")
        .partitionBy("service_year", "service_quarter", "claim_status")
        .option("overwriteSchema", "true")
        .option("delta.autoOptimize.optimizeWrite", "true")
        .option("delta.autoOptimize.autoCompact", "true")
        .save(gold_fact_claims_path)
    )
    
    print(f"Successfully wrote {final_fact_df.count()} records to Gold fact claims table")
    
    # Optimize the table
    spark.sql(f"OPTIMIZE delta.`{gold_fact_claims_path}`")
    
    # Create Z-ordering for common query patterns
    spark.sql(f"ZORDER BY delta.`{gold_fact_claims_path}` (member_sk, provider_sk, service_date_key)")
    
except Exception as e:
    print(f"Error writing Gold fact table: {str(e)}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7. Summary Statistics

# COMMAND ----------

print("=" * 80)
print("GOLD FACT CLAIMS BUILD SUMMARY")
print("=" * 80)
print(f"Pipeline Run ID: {pipeline_run_id}")
print(f"Total Fact Records: {final_fact_df.count():,}")
print()

# Status distribution
status_dist = final_fact_df.groupBy("claim_status").count().orderBy(desc("count"))
print("Claim Status Distribution:")
status_dist.show()

# Financial summary
financial_summary = final_fact_df.agg(
    sum("claim_amount").alias("total_claim_amount"),
    sum("paid_amount").alias("total_paid_amount"),
    sum("denied_amount").alias("total_denied_amount"),
    avg("claim_amount").alias("avg_claim_amount"),
    max("claim_amount").alias("max_claim_amount"),
    min("claim_amount").alias("min_claim_amount")
)
print("\nFinancial Summary:")
financial_summary.show()

# Quality results
print("\nData Quality Results:")
for rule, result in quality_results.items():
    print(f"  {rule}: {result}")

print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8. Verification

# COMMAND ----------

# Verify the fact table was created correctly
try:
    # Read back the fact table
    fact_table_df = spark.read.format("delta").load(gold_fact_claims_path)
    
    print(f"Verification - Fact table records: {fact_table_df.count():,}")
    
    # Check partition pruning works
    sample_year = 2026
    sample_quarter = 1
    sample_status = "PAID"
    
    filtered_df = spark.read.format("delta").load(gold_fact_claims_path) \
        .filter(col("service_year") == sample_year) \
        .filter(col("service_quarter") == sample_quarter) \
        .filter(col("claim_status") == sample_status)
    
    print(f"Sample filter result ({sample_year} Q{sample_quarter} {sample_status}): {filtered_df.count()} records")
    
    # Show sample records
    print("Sample fact records:")
    fact_table_df.select(
        "claim_line_key", "member_sk", "provider_sk", "claim_amount",
        "claim_status", "service_year", "service_quarter", "member_age_group",
        "provider_specialty_category", "payer_type"
    ).show(5, truncate=False)
    
    # Test common analytical query
    print("\nSample analytical query - Claims by specialty:")
    fact_table_df.groupBy("provider_specialty_category").agg(
        count("*").alias("claim_count"),
        sum("claim_amount").alias("total_amount"),
        avg("claim_amount").alias("avg_amount")
    ).orderBy(desc("claim_count")).show()
    
    print("Fact table verification completed successfully!")
    
except Exception as e:
    print(f"Error verifying fact table: {str(e)}")
