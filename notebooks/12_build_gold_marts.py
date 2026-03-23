"""
Gold Business Marts Builder Notebook
Creates analytical marts for business users from Gold fact and dimensions.
"""

# Databricks notebook source
# MAGIC %md
# MAGIC ## Gold Business Marts Builder
# MAGIC 
# MAGIC This notebook:
# MAGIC 1. Reads Gold fact and dimension tables
# MAGIC 2. Creates business-focused analytical marts
# MAGIC 3. Builds provider performance mart
# MAGIC 4. Builds denial trends mart
# MAGIC 5. Builds member utilization mart
# MAGIC 6. Builds claim anomalies mart
# MAGIC 7. Optimizes for BI and reporting workloads

# COMMAND ----------

# Import required libraries
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import uuid
from datetime import datetime, timedelta

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
pipeline_run_id = f"MARTS_GOLD_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{str(uuid.uuid4())[:8]}"

# Source paths
gold_fact_claims_path = Config.get_full_path(Config.GOLD_FACT_CLAIMS_PATH)
gold_dim_member_path = Config.get_full_path(Config.GOLD_DIM_MEMBER_PATH)
gold_dim_provider_path = Config.get_full_path(Config.GOLD_DIM_PROVIDER_PATH)
gold_dim_diagnosis_path = Config.get_full_path(Config.GOLD_DIM_DIAGNOSIS_PATH)
gold_dim_procedure_path = Config.get_full_path(Config.GOLD_DIM_PROCEDURE_PATH)
gold_dim_payer_path = Config.get_full_path(Config.GOLD_DIM_PAYER_PATH)

# Target paths
gold_mart_provider_performance_path = Config.get_full_path(Config.GOLD_MART_PROVIDER_PERFORMANCE_PATH)
gold_mart_denial_trends_path = Config.get_full_path(Config.GOLD_MART_DENIAL_TRENDS_PATH)
gold_mart_member_utilization_path = Config.get_full_path(Config.GOLD_MART_MEMBER_UTILIZATION_PATH)
gold_mart_claim_anomalies_path = Config.get_full_path(Config.GOLD_MART_CLAIM_ANOMALIES_PATH)

print(f"Pipeline Run ID: {pipeline_run_id}")
print("Building Gold Marts:")
print(f"  Provider Performance: {gold_mart_provider_performance_path}")
print(f"  Denial Trends: {gold_mart_denial_trends_path}")
print(f"  Member Utilization: {gold_mart_member_utilization_path}")
print(f"  Claim Anomalies: {gold_mart_claim_anomalies_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Read Source Data

# COMMAND ----------

try:
    # Read Gold fact table
    fact_claims_df = spark.read.format("delta").load(gold_fact_claims_path)
    print(f"Fact claims: {fact_claims_df.count():,} records")
    
    # Read Gold dimensions
    member_df = spark.read.format("delta").load(gold_dim_member_path)
    provider_df = spark.read.format("delta").load(gold_dim_provider_path)
    diagnosis_df = spark.read.format("delta").load(gold_dim_diagnosis_path)
    procedure_df = spark.read.format("delta").load(gold_dim_procedure_path)
    payer_df = spark.read.format("delta").load(gold_dim_payer_path)
    
    print(f"Dimensions loaded:")
    print(f"  Member: {member_df.count():,}")
    print(f"  Provider: {provider_df.count():,}")
    print(f"  Diagnosis: {diagnosis_df.count():,}")
    print(f"  Procedure: {procedure_df.count():,}")
    print(f"  Payer: {payer_df.count():,}")
    
except Exception as e:
    print(f"Error reading source data: {str(e)}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Build Provider Performance Mart

# COMMAND ----------

def build_provider_performance_mart():
    """Build provider performance analytical mart."""
    
    print("Building Provider Performance Mart...")
    
    # Provider performance metrics
    provider_performance = fact_claims_df.groupBy(
        "provider_sk", "provider_id", "provider_specialty", "provider_specialty_category",
        "provider_network_status", "provider_region", "service_year", "service_quarter", "service_month"
    ).agg(
        count("*").alias("total_claims"),
        sum("claim_amount").alias("total_claim_amount"),
        avg("claim_amount").alias("avg_claim_amount"),
        max("claim_amount").alias("max_claim_amount"),
        min("claim_amount").alias("min_claim_amount"),
        sum("paid_amount").alias("total_paid_amount"),
        sum("denied_amount").alias("total_denied_amount"),
        sum("pending_amount").alias("total_pending_amount"),
        sum("adjusted_amount").alias("total_adjusted_amount"),
        count(when(col("is_paid_claim"), 1)).alias("paid_claims"),
        count(when(col("is_denied_claim"), 1)).alias("denied_claims"),
        count(when(col("is_pending_claim"), 1)).alias("pending_claims"),
        count(when(col("is_adjusted_claim"), 1)).alias("adjusted_claims"),
        count(when(col("is_out_of_network"), 1)).alias("out_of_network_claims"),
        count(when(col("is_high_value_claim"), 1)).alias("high_value_claims"),
        countDistinct("member_sk").alias("unique_members"),
        avg("processing_delay_days").alias("avg_processing_delay_days"),
        avg("member_age").alias("avg_member_age")
    ).withColumn(
        "denial_rate",
        col("denied_claims") / col("total_claims")
    ).withColumn(
        "paid_rate",
        col("paid_claims") / col("total_claims")
    ).withColumn(
        "pending_rate",
        col("pending_claims") / col("total_claims")
    ).withColumn(
        "adjusted_rate",
        col("adjusted_claims") / col("total_claims")
    ).withColumn(
        "out_of_network_rate",
        col("out_of_network_claims") / col("total_claims")
    ).withColumn(
        "high_value_rate",
        col("high_value_claims") / col("total_claims")
    ).withColumn(
        "payment_rate",
        col("total_paid_amount") / col("total_claim_amount")
    ).withColumn(
        "avg_claim_per_member",
        col("total_claims") / col("unique_members")
    ).withColumn(
        "mart_processing_timestamp",
        current_timestamp()
    ).withColumn(
        "mart_pipeline_run_id",
        lit(pipeline_run_id)
    )
    
    return provider_performance

# Build provider performance mart
provider_performance_df = build_provider_performance_mart()
print(f"Provider Performance Mart: {provider_performance_df.count():,} records")

# Show sample
provider_performance_df.select(
    "provider_id", "provider_specialty", "total_claims", "total_claim_amount",
    "denial_rate", "paid_rate", "unique_members", "service_year"
).show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Build Denial Trends Mart

# COMMAND ----------

def build_denial_trends_mart():
    """Build denial trends analytical mart."""
    
    print("Building Denial Trends Mart...")
    
    # Filter for denied claims
    denied_claims = fact_claims_df.filter(col("is_denied_claim") == True)
    
    # Denial trends by various dimensions
    denial_trends = denied_claims.groupBy(
        "service_year", "service_quarter", "service_month",
        "payer_type", "provider_specialty_category", "provider_region",
        "member_age_group", "member_gender", "member_state",
        "diagnosis_code", "procedure_code", "place_of_service"
    ).agg(
        count("*").alias("denied_claims_count"),
        sum("claim_amount").alias("denied_amount"),
        avg("claim_amount").alias("avg_denied_amount"),
        max("claim_amount").alias("max_denied_amount"),
        min("claim_amount").alias("min_denied_amount"),
        countDistinct("provider_sk").alias("unique_providers"),
        countDistinct("member_sk").alias("unique_members"),
        countDistinct("claim_id").alias("unique_claims_denied"),
        avg("processing_delay_days").alias("avg_processing_delay_days")
    ).withColumn(
        "denial_date",
        to_date(concat(col("service_year"), lit("-"), 
                      lpad(col("service_month"), 2, "0"), lit("-01")))
    ).withColumn(
        "mart_processing_timestamp",
        current_timestamp()
    ).withColumn(
        "mart_pipeline_run_id",
        lit(pipeline_run_id)
    )
    
    # Calculate overall denial rates by joining back to total claims
    total_claims_by_period = fact_claims_df.groupBy(
        "service_year", "service_quarter", "service_month",
        "payer_type", "provider_specialty_category", "provider_region",
        "member_age_group", "member_gender", "member_state",
        "diagnosis_code", "procedure_code", "place_of_service"
    ).agg(
        count("*").alias("total_claims_count"),
        sum("claim_amount").alias("total_amount")
    )
    
    # Join to calculate denial rates
    denial_trends_with_rates = denial_trends.join(
        total_claims_by_period,
        ["service_year", "service_quarter", "service_month",
         "payer_type", "provider_specialty_category", "provider_region",
         "member_age_group", "member_gender", "member_state",
         "diagnosis_code", "procedure_code", "place_of_service"],
        "left"
    ).withColumn(
        "denial_count_rate",
        col("denied_claims_count") / col("total_claims_count")
    ).withColumn(
        "denial_amount_rate",
        col("denied_amount") / col("total_amount")
    )
    
    return denial_trends_with_rates

# Build denial trends mart
denial_trends_df = build_denial_trends_mart()
print(f"Denial Trends Mart: {denial_trends_df.count():,} records")

# Show sample
denial_trends_df.select(
    "service_year", "service_quarter", "payer_type", "provider_specialty_category",
    "denied_claims_count", "denied_amount", "denial_count_rate", "denial_amount_rate"
).show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. Build Member Utilization Mart

# COMMAND ----------

def build_member_utilization_mart():
    """Build member utilization analytical mart."""
    
    print("Building Member Utilization Mart...")
    
    # Member utilization metrics
    member_utilization = fact_claims_df.groupBy(
        "member_sk", "member_id", "member_age", "member_age_group",
        "member_gender", "member_state", "service_year", "service_quarter", "service_month"
    ).agg(
        count("*").alias("total_claims"),
        sum("claim_amount").alias("total_claim_amount"),
        avg("claim_amount").alias("avg_claim_amount"),
        max("claim_amount").alias("max_claim_amount"),
        sum("paid_amount").alias("total_paid_amount"),
        sum("denied_amount").alias("total_denied_amount"),
        count(when(col("is_paid_claim"), 1)).alias("paid_claims"),
        count(when(col("is_denied_claim"), 1)).alias("denied_claims"),
        count(when(col("is_high_value_claim"), 1)).alias("high_value_claims"),
        countDistinct("provider_sk").alias("unique_providers"),
        countDistinct("provider_specialty_category")).alias("unique_specialties"),
        countDistinct("payer_sk").alias("unique_payers"),
        countDistinct("diagnosis_code").alias("unique_diagnoses"),
        countDistinct("procedure_code").alias("unique_procedures"),
        avg("processing_delay_days").alias("avg_processing_delay_days")
    ).withColumn(
        "denial_rate",
        col("denied_claims") / col("total_claims")
    ).withColumn(
        "high_value_rate",
        col("high_value_claims") / col("total_claims")
    ).withColumn(
        "payment_rate",
        col("total_paid_amount") / col("total_claim_amount")
    ).withColumn(
        "avg_claims_per_provider",
        col("total_claims") / col("unique_providers")
    ).withColumn(
        "utilization_date",
        to_date(concat(col("service_year"), lit("-"), 
                      lpad(col("service_month"), 2, "0"), lit("-01")))
    ).withColumn(
        "is_high_utilization",
        col("total_claims") > 12  # More than 1 claim per month on average
    ).withColumn(
        "is_high_cost_member",
        col("total_claim_amount") > 10000
    ).withColumn(
        "mart_processing_timestamp",
        current_timestamp()
    ).withColumn(
        "mart_pipeline_run_id",
        lit(pipeline_run_id)
    )
    
    return member_utilization

# Build member utilization mart
member_utilization_df = build_member_utilization_mart()
print(f"Member Utilization Mart: {member_utilization_df.count():,} records")

# Show sample
member_utilization_df.select(
    "member_id", "member_age_group", "total_claims", "total_claim_amount",
    "denial_rate", "unique_providers", "is_high_utilization", "service_year"
).show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6. Build Claim Anomalies Mart

# COMMAND ----------

def build_claim_anomalies_mart():
    """Build claim anomalies detection mart."""
    
    print("Building Claim Anomalies Mart...")
    
    # Initialize quality rules for anomaly detection
    config = Config()
    quality_rules = DataQualityRules(config)
    
    # Detect anomalies using the quality rules
    anomalies_df = quality_rules.detect_anomalies(
        fact_claims_df, 
        provider_df.filter(col("is_current") == True),
        member_df.filter(col("is_current") == True)
    )
    
    # Filter for claims with anomalies
    anomaly_claims = anomalies_df.filter(col("has_anomaly") == True)
    
    # Create anomaly mart with detailed analysis
    claim_anomalies = anomaly_claims.select(
        "claim_line_key", "claim_id", "claim_line_id",
        "member_sk", "member_id", "provider_sk", "provider_id",
        "claim_amount", "claim_status", "service_date", "submission_date",
        "member_age", "provider_specialty_category", "payer_type",
        "diagnosis_code", "procedure_code", "place_of_service",
        "duplicate_flag", "amount_outlier_flag", "high_denial_provider_flag",
        "frequent_procedure_flag", "anomaly_reasons"
    ).withColumn(
        "anomaly_score",
        # Calculate an anomaly risk score
        (when(col("duplicate_flag"), 30).otherwise(0) +
         when(col("amount_outlier_flag"), 25).otherwise(0) +
         when(col("high_denial_provider_flag"), 20).otherwise(0) +
         when(col("frequent_procedure_flag"), 15).otherwise(0) +
         when(col("claim_amount") > 5000, 10).otherwise(0))
    ).withColumn(
        "anomaly_risk_level",
        when(col("anomaly_score") >= 50, "HIGH")
        .when(col("anomaly_score") >= 25, "MEDIUM")
        .otherwise("LOW")
    ).withColumn(
        "requires_investigation",
        col("anomaly_risk_level") == "HIGH"
    ).withColumn(
        "anomaly_detection_date",
        current_date()
    ).withColumn(
        "mart_processing_timestamp",
        current_timestamp()
    ).withColumn(
        "mart_pipeline_run_id",
        lit(pipeline_run_id)
    )
    
    return claim_anomalies

# Build claim anomalies mart
claim_anomalies_df = build_claim_anomalies_mart()
print(f"Claim Anomalies Mart: {claim_anomalies_df.count():,} records")

# Show sample
claim_anomalies_df.select(
    "claim_line_key", "anomaly_score", "anomaly_risk_level", "anomaly_reasons",
    "requires_investigation", "duplicate_flag", "amount_outlier_flag"
).show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7. Write Gold Marts

# COMMAND ----------

def write_mart(mart_df, mart_path, mart_name, partition_cols=None):
    """Write a mart to Delta with optimal configuration."""
    try:
        writer = mart_df.write.format("delta").mode("overwrite")
        
        if partition_cols:
            writer = writer.partitionBy(*partition_cols)
        
        writer = writer.option("overwriteSchema", "true") \
                     .option("delta.autoOptimize.optimizeWrite", "true") \
                     .option("delta.autoOptimize.autoCompact", "true")
        
        writer.save(mart_path)
        
        # Optimize the table
        spark.sql(f"OPTIMIZE delta.`{mart_path}`")
        
        print(f"Successfully wrote {mart_df.count():,} records to {mart_name}")
        
    except Exception as e:
        print(f"Error writing {mart_name}: {str(e)}")
        raise

# Write all marts
write_mart(
    provider_performance_df, 
    gold_mart_provider_performance_path, 
    "Provider Performance Mart",
    ["service_year", "service_quarter", "provider_specialty_category"]
)

write_mart(
    denial_trends_df, 
    gold_mart_denial_trends_path, 
    "Denial Trends Mart",
    ["service_year", "service_quarter", "payer_type"]
)

write_mart(
    member_utilization_df, 
    gold_mart_member_utilization_path, 
    "Member Utilization Mart",
    ["service_year", "service_quarter", "member_age_group"]
)

write_mart(
    claim_anomalies_df, 
    gold_mart_claim_anomalies_path, 
    "Claim Anomalies Mart",
    ["anomaly_risk_level", "service_year"]
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8. Summary and Analytics

# COMMAND ----------

print("=" * 80)
print("GOLD BUSINESS MARTS BUILD SUMMARY")
print("=" * 80)
print(f"Pipeline Run ID: {pipeline_run_id}")
print()

marts_info = [
    ("Provider Performance", provider_performance_df, gold_mart_provider_performance_path),
    ("Denial Trends", denial_trends_df, gold_mart_denial_trends_path),
    ("Member Utilization", member_utilization_df, gold_mart_member_utilization_path),
    ("Claim Anomalies", claim_anomalies_df, gold_mart_claim_anomalies_path)
]

for mart_name, mart_df, mart_path in marts_info:
    print(f"{mart_name} Mart: {mart_df.count():,} records")

print()

# Provider Performance Insights
print("Provider Performance Insights:")
provider_performance_df.groupBy("provider_specialty_category").agg(
    count("*").alias("provider_count"),
    avg("denial_rate").alias("avg_denial_rate"),
    avg("total_claim_amount").alias("avg_total_amount")
).orderBy(desc("avg_total_amount")).show(5)

# Denial Trends Insights
print("\nDenial Trends Insights:")
denial_trends_df.groupBy("payer_type").agg(
    sum("denied_claims_count").alias("total_denied"),
    avg("denial_count_rate").alias("avg_denial_rate")
).orderBy(desc("total_denied")).show(5)

# Member Utilization Insights
print("\nMember Utilization Insights:")
member_utilization_df.groupBy("member_age_group").agg(
    count("*").alias("member_count"),
    avg("total_claim_amount").alias("avg_total_amount"),
    avg("denial_rate").alias("avg_denial_rate")
).orderBy(desc("avg_total_amount")).show(5)

# Anomaly Insights
print("\nClaim Anomaly Insights:")
claim_anomalies_df.groupBy("anomaly_risk_level").agg(
    count("*").alias("anomaly_count"),
    avg("claim_amount").alias("avg_claim_amount")
).show()

print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 9. Verification

# COMMAND ----------

# Verify each mart was created correctly
for mart_name, mart_df, mart_path in marts_info:
    try:
        # Read back the mart
        verify_df = spark.read.format("delta").load(mart_path)
        
        print(f"\n{mart_name} Mart Verification:")
        print(f"  Records: {verify_df.count():,}")
        
        # Test a sample query
        if mart_name == "Provider Performance":
            verify_df.filter(col("service_year") == 2026) \
                    .groupBy("provider_specialty_category") \
                    .agg(sum("total_claims").alias("total_claims")) \
                    .orderBy(desc("total_claims")).show(3)
        
        elif mart_name == "Denial Trends":
            verify_df.filter(col("service_year") == 2026) \
                    .groupBy("payer_type") \
                    .agg(sum("denied_claims_count").alias("total_denied")) \
                    .orderBy(desc("total_denied")).show(3)
        
        elif mart_name == "Member Utilization":
            verify_df.filter(col("service_year") == 2026) \
                    .groupBy("member_age_group") \
                    .agg(avg("total_claim_amount").alias("avg_amount")) \
                    .orderBy(desc("avg_amount")).show(3)
        
        elif mart_name == "Claim Anomalies":
            verify_df.groupBy("anomaly_risk_level") \
                    .agg(count("*").alias("anomaly_count")) \
                    .orderBy(desc("anomaly_count")).show()
        
        print(f"  {mart_name} mart verification completed successfully!")
        
    except Exception as e:
        print(f"Error verifying {mart_name} mart: {str(e)}")

print("\nAll Gold marts built and verified successfully!")
