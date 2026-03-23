"""
Gold Dimensions Builder Notebook
Creates and populates Gold layer dimension tables from Silver data.
"""

# Databricks notebook source
# MAGIC %md
# MAGIC ## Gold Dimensions Builder
# MAGIC 
# MAGIC This notebook:
# MAGIC 1. Reads Silver layer dimension data
# MAGIC 2. Creates surrogate keys and slowly changing dimensions
# MAGIC 3. Builds Gold dimension tables with business-ready attributes
# MAGIC 4. Handles SCD Type 2 for provider changes
# MAGIC 5. Creates date dimension for analytics
# MAGIC 6. Writes Gold dimensions with proper partitioning

# COMMAND ----------

# Import required libraries
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import uuid
from datetime import datetime, date

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
pipeline_run_id = f"DIMENSIONS_GOLD_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{str(uuid.uuid4())[:8]}"

# Source and target paths
silver_members_path = Config.get_full_path(Config.SILVER_MEMBERS_PATH)
silver_providers_path = Config.get_full_path(Config.SILVER_PROVIDERS_PATH)
silver_diagnosis_ref_path = Config.get_full_path(Config.SILVER_DIAGNOSIS_REF_PATH)
silver_procedure_ref_path = Config.get_full_path(Config.SILVER_PROCEDURE_REF_PATH)
silver_payer_ref_path = Config.get_full_path(Config.SILVER_PAYER_REF_PATH)

# Gold dimension paths
gold_dim_member_path = Config.get_full_path(Config.GOLD_DIM_MEMBER_PATH)
gold_dim_provider_path = Config.get_full_path(Config.GOLD_DIM_PROVIDER_PATH)
gold_dim_diagnosis_path = Config.get_full_path(Config.GOLD_DIM_DIAGNOSIS_PATH)
gold_dim_procedure_path = Config.get_full_path(Config.GOLD_DIM_PROCEDURE_PATH)
gold_dim_payer_path = Config.get_full_path(Config.GOLD_DIM_PAYER_PATH)
gold_dim_date_path = Config.get_full_path(Config.GOLD_DIM_DATE_PATH)

print(f"Pipeline Run ID: {pipeline_run_id}")
print("Gold Dimensions to build:")
print(f"  Member: {gold_dim_member_path}")
print(f"  Provider: {gold_dim_provider_path}")
print(f"  Diagnosis: {gold_dim_diagnosis_path}")
print(f"  Procedure: {gold_dim_procedure_path}")
print(f"  Payer: {gold_dim_payer_path}")
print(f"  Date: {gold_dim_date_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Build Gold Member Dimension

# COMMAND ----------

try:
    # Read Silver members data
    silver_members_df = spark.read.format("delta").load(silver_members_path)
    
    print(f"Silver members: {silver_members_df.count()} records")
    
    # Create Gold Member dimension with surrogate key
    gold_member_df = silver_members_df.select(
        "member_id",
        "member_first_name",
        "member_last_name",
        "dob",
        "gender",
        "city",
        "state",
        "zip_code",
        "plan_id",
        "eligibility_start_date",
        "eligibility_end_date"
    ).withColumn(
        "member_sk", 
        monotonically_increasing_id()
    ).withColumn(
        "member_full_name",
        concat(col("member_first_name"), lit(" "), col("member_last_name"))
    ).withColumn(
        "member_age",
        floor(months_between(current_date(), col("dob")) / 12)
    ).withColumn(
        "member_age_group",
        when(col("member_age") < 18, "Minor")
        .when((col("member_age") >= 18) & (col("member_age") < 35), "Adult 18-34")
        .when((col("member_age") >= 35) & (col("member_age") < 50), "Adult 35-49")
        .when((col("member_age") >= 50) & (col("member_age") < 65), "Adult 50-64")
        .otherwise("Senior 65+")
    ).withColumn(
        "is_eligible",
        (col("eligibility_start_date") <= current_date()) & 
        (col("eligibility_end_date").isNull() | (col("eligibility_end_date") >= current_date()))
    ).withColumn(
        "effective_start_date",
        coalesce(col("eligibility_start_date"), col("dob"))
    ).withColumn(
        "effective_end_date",
        coalesce(col("eligibility_end_date"), lit(date(9999, 12, 31)))
    ).withColumn(
        "gold_processing_timestamp",
        current_timestamp()
    ).withColumn(
        "gold_pipeline_run_id",
        lit(pipeline_run_id)
    )
    
    # Write Gold Member dimension
    (
        gold_member_df.write
        .format("delta")
        .mode("overwrite")
        .partitionBy("gender", "state")
        .option("overwriteSchema", "true")
        .option("delta.autoOptimize.optimizeWrite", "true")
        .save(gold_dim_member_path)
    )
    
    print(f"Gold Member dimension: {gold_member_df.count()} records")
    
    # Show sample
    gold_member_df.select(
        "member_sk", "member_id", "member_full_name", "member_age", 
        "member_age_group", "is_eligible", "state"
    ).show(5, truncate=False)
    
except Exception as e:
    print(f"Error building Gold Member dimension: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Build Gold Provider Dimension (SCD Type 2)

# COMMAND ----------

try:
    # Read Silver providers data
    silver_providers_df = spark.read.format("delta").load(silver_providers_path)
    
    print(f"Silver providers: {silver_providers_df.count()} records")
    
    # Check if Gold provider dimension exists
    try:
        existing_gold_providers = spark.read.format("delta").load(gold_dim_provider_path)
        print(f"Existing Gold providers: {existing_gold_providers.count()} records")
        gold_exists = True
    except:
        print("No existing Gold provider dimension found")
        gold_exists = False
    
    if gold_exists:
        # Implement SCD Type 2 logic
        # Find providers with changes in key attributes
        latest_silver_providers = silver_providers_df.withColumn(
            "silver_row_hash",
            md5(concat_ws("|", 
                col("provider_name"), col("specialty"), col("facility_name"),
                col("city"), col("state"), col("zip_code"), col("network_status")
            ))
        )
        
        # Get current active records from Gold
        current_gold_providers = existing_gold_providers.filter(col("is_current") == True)
        
        # Compare and find changes
        provider_changes = latest_silver_providers.join(
            current_gold_providers,
            "provider_id",
            "inner"
        ).filter(
            col("silver_row_hash") != col("gold_row_hash")
        )
        
        print(f"Providers with changes: {provider_changes.count()}")
        
        if provider_changes.count() > 0:
            # Expire old records
            old_records_to_expire = current_gold_providers.join(
                provider_changes.select("provider_id"),
                "provider_id",
                "inner"
            ).withColumn("is_current", lit(False))
             .withColumn("effective_end_date", current_timestamp())
            
            # Create new records
            new_provider_records = provider_changes.select(
                col("provider_id"),
                col("provider_name"),
                col("specialty"),
                col("facility_name"),
                col("city"),
                col("state"),
                col("zip_code"),
                col("network_status"),
                col("effective_date"),
                col("termination_date"),
                lit(True).alias("is_current"),
                current_timestamp().alias("effective_start_date"),
                lit(date(9999, 12, 31)).alias("effective_end_date"),
                col("silver_row_hash").alias("gold_row_hash"),
                current_timestamp().alias("gold_processing_timestamp"),
                lit(pipeline_run_id).alias("gold_pipeline_run_id")
            )
            
            # Unchanged records
            unchanged_records = current_gold_providers.join(
                provider_changes.select("provider_id"),
                "provider_id",
                "left_anti"
            )
            
            # Combine all records
            final_gold_providers = old_records_to_expire.unionByName(unchanged_records).unionByName(new_provider_records)
        else:
            # No changes, use existing records
            final_gold_providers = existing_gold_providers
    else:
        # First load - create initial dimension
        final_gold_providers = silver_providers_df.select(
            "provider_id",
            "provider_name",
            "specialty",
            "facility_name",
            "city",
            "state",
            "zip_code",
            "network_status",
            "effective_date",
            "termination_date"
        ).withColumn(
            "provider_sk",
            monotonically_increasing_id()
        ).withColumn(
            "is_current",
            lit(True)
        ).withColumn(
            "effective_start_date",
            coalesce(col("effective_date"), current_date())
        ).withColumn(
            "effective_end_date",
            coalesce(col("termination_date"), lit(date(9999, 12, 31)))
        ).withColumn(
            "gold_row_hash",
            md5(concat_ws("|", 
                col("provider_name"), col("specialty"), col("facility_name"),
                col("city"), col("state"), col("zip_code"), col("network_status")
            ))
        ).withColumn(
            "gold_processing_timestamp",
            current_timestamp()
        ).withColumn(
            "gold_pipeline_run_id",
            lit(pipeline_run_id)
        )
    
    # Add derived attributes
    final_gold_providers = final_gold_providers.withColumn(
        "provider_region",
        when(col("state").isin("CA", "OR", "WA", "NV", "AZ", "AK", "HI"), "West")
        .when(col("state").isin("MT", "ID", "WY", "UT", "CO", "NM", "AZ"), "Mountain")
        .when(col("state").isin("ND", "SD", "NE", "KS", "OK", "TX", "MN", "IA", "MO", "AR", "LA"), "Central")
        .when(col("state").isin("WI", "MI", "IL", "IN", "KY", "TN", "MS", "AL", "OH", "WV", "PA", "NY", "VT", "ME", "NH", "MA", "RI", "CT", "NJ", "DE", "MD", "DC"), "East")
        .otherwise("Other")
    ).withColumn(
        "specialty_category",
        when(col("specialty").rlike("(?i)family|internal|general"), "Primary Care")
        .when(col("specialty").rlike("(?i)cardio|heart"), "Cardiology")
        .when(col("specialty").rlike("(?i)ortho|bone|joint"), "Orthopedics")
        .when(col("specialty").rlike("(?i)pedi|child"), "Pediatrics")
        .when(col("specialty").rlike("(?i)surg|operation"), "Surgery")
        .otherwise("Other")
    )
    
    # Write Gold Provider dimension
    (
        final_gold_providers.write
        .format("delta")
        .mode("overwrite")
        .partitionBy("specialty_category", "provider_region")
        .option("overwriteSchema", "true")
        .option("delta.autoOptimize.optimizeWrite", "true")
        .save(gold_dim_provider_path)
    )
    
    print(f"Gold Provider dimension: {final_gold_providers.count()} records")
    
    # Show sample
    final_gold_providers.select(
        "provider_sk", "provider_id", "provider_name", "specialty", 
        "specialty_category", "network_status", "is_current"
    ).show(5, truncate=False)
    
except Exception as e:
    print(f"Error building Gold Provider dimension: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Build Reference Dimensions (Diagnosis, Procedure, Payer)

# COMMAND ----------

def build_reference_dimension(silver_path, gold_path, table_name, key_col, desc_col):
    """Generic function to build reference dimensions."""
    try:
        # Read Silver data
        silver_df = spark.read.format("delta").load(silver_path)
        print(f"Silver {table_name}: {silver_df.count()} records")
        
        # Create Gold dimension
        gold_df = silver_df.select(
            key_col,
            desc_col,
            *[col for col in silver_df.columns if col not in [key_col, desc_col]]
        ).withColumn(
            f"{table_name}_sk",
            monotonically_increasing_id()
        ).withColumn(
            "gold_processing_timestamp",
            current_timestamp()
        ).withColumn(
            "gold_pipeline_run_id",
            lit(pipeline_run_id)
        )
        
        # Write Gold dimension
        (
            gold_df.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .option("delta.autoOptimize.optimizeWrite", "true")
            .save(gold_path)
        )
        
        print(f"Gold {table_name} dimension: {gold_df.count()} records")
        gold_df.show(3, truncate=False)
        
        return gold_df
        
    except Exception as e:
        print(f"Error building Gold {table_name} dimension: {str(e)}")
        return None

# Build Diagnosis dimension
gold_diagnosis_df = build_reference_dimension(
    silver_diagnosis_ref_path, gold_dim_diagnosis_path, 
    "diagnosis", "diagnosis_code", "diagnosis_desc"
)

# Build Procedure dimension
gold_procedure_df = build_reference_dimension(
    silver_procedure_ref_path, gold_dim_procedure_path, 
    "procedure", "procedure_code", "procedure_desc"
)

# Build Payer dimension
gold_payer_df = build_reference_dimension(
    silver_payer_ref_path, gold_dim_payer_path, 
    "payer", "payer_id", "payer_name"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. Build Date Dimension

# COMMAND ----------

def generate_date_dimension(start_date="2020-01-01", end_date="2030-12-31"):
    """Generate a comprehensive date dimension."""
    
    start = datetime.strptime(start_date, "%Y-%m-%d").date()
    end = datetime.strptime(end_date, "%Y-%m-%d").date()
    
    date_list = []
    current_date = start
    
    while current_date <= end:
        date_list.append({
            "date_key": int(current_date.strftime("%Y%m%d")),
            "date": current_date,
            "year": current_date.year,
            "quarter": (current_date.month - 1) // 3 + 1,
            "month": current_date.month,
            "month_name": current_date.strftime("%B"),
            "day": current_date.day,
            "day_of_week": current_date.weekday() + 1,
            "day_name": current_date.strftime("%A"),
            "is_weekend": current_date.weekday() >= 5,
            "is_holiday": False,  # Would need holiday calendar
            "week_of_year": current_date.isocalendar()[1],
            "fiscal_year": current_date.year if current_date.month >= 10 else current_date.year + 1,
            "fiscal_quarter": ((current_date.month + 2) % 12) // 3 + 1,
            "fiscal_month": ((current_date.month + 2) % 12) + 1,
            "decade": (current_date.year // 10) * 10,
            "century": (current_date.year // 100) + 1,
            "is_leap_year": (current_date.year % 4 == 0 and current_date.year % 100 != 0) or (current_date.year % 400 == 0),
            "days_in_month": 31 if current_date.month in [1,3,5,7,8,10,12] else 30 if current_date.month != 2 else 29 if (current_date.year % 4 == 0 and current_date.year % 100 != 0) or (current_date.year % 400 == 0) else 28,
            "days_remaining_in_month": (31 if current_date.month in [1,3,5,7,8,10,12] else 30 if current_date.month != 2 else 29 if (current_date.year % 4 == 0 and current_date.year % 100 != 0) or (current_date.year % 400 == 0) else 28) - current_date.day,
            "date_format_iso": current_date.strftime("%Y-%m-%d"),
            "date_format_us": current_date.strftime("%m/%d/%Y"),
            "date_format_eu": current_date.strftime("%d/%m/%Y"),
            "season": "Winter" if current_date.month in [12,1,2] else "Spring" if current_date.month in [3,4,5] else "Summer" if current_date.month in [6,7,8] else "Fall"
        })
        current_date += timedelta(days=1)
    
    return date_list

# Generate date dimension
date_data = generate_date_dimension()
date_dim_df = spark.createDataFrame(date_data)

# Add processing metadata
date_dim_df = date_dim_df.withColumn(
    "gold_processing_timestamp",
    current_timestamp()
).withColumn(
    "gold_pipeline_run_id",
    lit(pipeline_run_id)
)

# Write Date dimension
(
    date_dim_df.write
    .format("delta")
    .mode("overwrite")
    .partitionBy("year", "quarter")
    .option("overwriteSchema", "true")
    .option("delta.autoOptimize.optimizeWrite", "true")
    .save(gold_dim_date_path)
)

print(f"Date dimension: {date_dim_df.count()} records")
date_dim_df.select("date_key", "date", "year", "quarter", "month", "day_name", "season").show(10, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6. Summary

# COMMAND ----------

print("=" * 80)
print("GOLD DIMENSIONS BUILD SUMMARY")
print("=" * 80)
print(f"Pipeline Run ID: {pipeline_run_id}")
print()

# Get record counts for all dimensions
dimensions_info = [
    ("Member", gold_dim_member_path),
    ("Provider", gold_dim_provider_path),
    ("Diagnosis", gold_dim_diagnosis_path),
    ("Procedure", gold_dim_procedure_path),
    ("Payer", gold_dim_payer_path),
    ("Date", gold_dim_date_path)
]

for dim_name, dim_path in dimensions_info:
    try:
        df = spark.read.format("delta").load(dim_path)
        count = df.count()
        print(f"{dim_name} Dimension: {count:,} records")
    except Exception as e:
        print(f"{dim_name} Dimension: Error - {str(e)}")

print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7. Verification

# COMMAND ----------

# Verify each dimension was created correctly
for dim_name, dim_path in dimensions_info:
    try:
        df = spark.read.format("delta").load(dim_path)
        print(f"\n{dim_name} Dimension Verification:")
        print(f"  Records: {df.count():,}")
        
        if dim_name == "Member":
            df.select("member_sk", "member_id", "member_age_group", "is_eligible").show(3)
        elif dim_name == "Provider":
            df.select("provider_sk", "provider_id", "specialty_category", "is_current").show(3)
        elif dim_name == "Date":
            df.select("date_key", "date", "year", "quarter", "season").show(3)
        else:
            df.show(3)
            
    except Exception as e:
        print(f"Error verifying {dim_name} dimension: {str(e)}")
