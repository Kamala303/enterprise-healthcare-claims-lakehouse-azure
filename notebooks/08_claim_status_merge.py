"""
Claim Status Merge Notebook
Processes claim status events from streaming and batch sources to update current claim status.
"""

# Databricks notebook source
# MAGIC %md
# MAGIC ## Claim Status Merge (CDC Processing)
# MAGIC 
# MAGIC This notebook:
# MAGIC 1. Reads claim status events from Bronze streaming table
# MAGIC 2. Processes batch status updates if needed
# MAGIC 3. Merges status changes into Silver current claims table
# MAGIC 4. Maintains history of status changes
# MAGIC 5. Handles late-arriving events and out-of-order processing
# MAGIC 6. Records audit trail of all status changes

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
pipeline_run_id = f"CLAIM_STATUS_MERGE_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{str(uuid.uuid4())[:8]}"

# Source and target paths
bronze_claim_status_events_path = Config.get_full_path(Config.BRONZE_CLAIM_STATUS_EVENTS_PATH)
silver_claims_current_path = Config.get_full_path(Config.SILVER_CLAIMS_CURRENT_PATH)
silver_claims_history_path = Config.get_full_path(Config.SILVER_CLAIMS_HISTORY_PATH)
silver_claim_events_path = Config.get_full_path(Config.SILVER_CLAIMS_CURRENT_PATH.replace("claims_current", "claim_events"))

print(f"Pipeline Run ID: {pipeline_run_id}")
print(f"Source Events: {bronze_claim_status_events_path}")
print(f"Target Current: {silver_claims_current_path}")
print(f"Target History: {silver_claims_history_path}")
print(f"Target Events: {silver_claim_events_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Read Claim Status Events

# COMMAND ----------

try:
    # Read claim status events from Bronze
    status_events_df = spark.read.format("delta").load(bronze_claim_status_events_path)
    
    # Filter for unprocessed events (those not yet merged)
    # We'll use a watermark to process events in order
    watermark_delay = Config.STREAMING_WATERMARK_DELAY
    
    # Get events that haven't been processed yet
    # For this demo, we'll process all events with a timestamp filter
    latest_processed_time = datetime.now() - timedelta(hours=24)  # Process last 24 hours
    
    events_to_process = status_events_df.filter(
        col("event_timestamp") > lit(latest_processed_time)
    ).orderBy("event_timestamp")
    
    print(f"Found {events_to_process.count()} events to process")
    
    # Show sample events
    events_to_process.select(
        "event_id", "claim_id", "claim_line_id", "event_type",
        "old_status", "new_status", "denial_reason", "event_timestamp"
    ).show(10, truncate=False)
    
except Exception as e:
    print(f"Error reading claim status events: {str(e)}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Read Current Claims for Merge

# COMMAND ----------

try:
    # Read current Silver claims
    current_claims_df = spark.read.format("delta").load(silver_claims_current_path)
    
    print(f"Current claims records: {current_claims_df.count()}")
    
    # Show sample current claims
    current_claims_df.select(
        "claim_id", "claim_line_id", "member_id", "provider_id",
        "claim_amount", "claim_status", "service_date", "silver_processing_timestamp"
    ).show(5, truncate=False)
    
except Exception as e:
    print(f"Error reading current claims: {str(e)}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Process Status Events - Get Latest Status per Claim

# COMMAND ----------

# Get the latest status for each claim line from the events
# This handles multiple events for the same claim line
latest_status_events = events_to_process.withColumn(
    "event_rank",
    row_number().over(
        Window.partitionBy("claim_id", "claim_line_id")
        .orderBy(desc("event_timestamp"))
    )
).filter(col("event_rank") == 1).drop("event_rank")

print(f"Latest status events: {latest_status_events.count()}")

# Show latest events
latest_status_events.select(
    "claim_id", "claim_line_id", "old_status", "new_status", 
    "denial_reason", "event_timestamp"
).show(10, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. Identify Claims to Update

# COMMAND ----------

# Find claims that need status updates
claims_to_update = latest_status_events.join(
    current_claims_df,
    ["claim_id", "claim_line_id"],
    "inner"
).filter(
    col("current_claims_df.claim_status") != col("latest_status_events.new_status")
)

print(f"Claims requiring status updates: {claims_to_update.count()}")

# Show claims that will be updated
claims_to_update.select(
    "claim_id", "claim_line_id",
    col("current_claims_df.claim_status").alias("current_status"),
    col("latest_status_events.new_status").alias("new_status"),
    col("latest_status_events.denial_reason"),
    col("latest_status_events.event_timestamp")
).show(10, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6. Move Old Versions to History

# COMMAND ----------

if claims_to_update.count() > 0:
    # Get the current versions that need to be moved to history
    claims_for_history = current_claims_df.join(
        claims_to_update.select("claim_id", "claim_line_id"),
        ["claim_id", "claim_line_id"],
        "inner"
    ).withColumn("history_effective_end_date", current_timestamp()) \
     .withColumn("history_reason", lit("STATUS_UPDATE")) \
     .withColumn("history_event_id", col("latest_status_events.event_id"))
    
    # Write to history table
    (
        claims_for_history.write
        .format("delta")
        .mode("append")
        .partitionBy("claim_status")
        .save(silver_claims_history_path)
    )
    
    print(f"Moved {claims_for_history.count()} records to history")
else:
    print("No claims to move to history")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7. Update Current Claims with New Status

# COMMAND ----------

if claims_to_update.count() > 0:
    # Create updated claims records
    updated_claims = current_claims_df.join(
        claims_to_update.select("claim_id", "claim_line_id"),
        ["claim_id", "claim_line_id"],
        "left_anti"
    ).union(
        # Update the status for claims that changed
        current_claims_df.join(
            latest_status_events.select(
                "claim_id", "claim_line_id", 
                col("new_status").alias("updated_status"),
                col("denial_reason").alias("updated_denial_reason"),
                col("event_timestamp").alias("status_update_timestamp")
            ),
            ["claim_id", "claim_line_id"],
            "inner"
        ).withColumn("claim_status", col("updated_status"))
         .withColumn("denial_reason", col("updated_denial_reason"))
         .withColumn("status_update_timestamp", col("status_update_timestamp"))
         .drop("updated_status", "updated_denial_reason")
    )
    
    # Update processing timestamp
    updated_claims = updated_claims.withColumn("silver_processing_timestamp", current_timestamp())
    updated_claims = updated_claims.withColumn("silver_pipeline_run_id", lit(pipeline_run_id))
    
    # Overwrite current table with updated records
    (
        updated_claims.write
        .format("delta")
        .mode("overwrite")
        .partitionBy("claim_status", "submission_date")
        .option("overwriteSchema", "true")
        .option("delta.autoOptimize.optimizeWrite", "true")
        .save(silver_claims_current_path)
    )
    
    print(f"Updated current claims table with {updated_claims.count()} records")
else:
    print("No status updates to apply")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8. Record Claim Events for Audit Trail

# COMMAND ----------

if claims_to_update.count() > 0:
    # Create claim events audit record
    claim_events_df = latest_status_events.join(
        claims_to_update.select("claim_id", "claim_line_id"),
        ["claim_id", "claim_line_id"],
        "inner"
    ).select(
        col("event_id"),
        col("claim_id"),
        col("claim_line_id"),
        col("event_type"),
        col("old_status"),
        col("new_status"),
        col("denial_reason"),
        col("event_timestamp"),
        col("source_system"),
        lit(pipeline_run_id).alias("merge_pipeline_run_id"),
        current_timestamp().alias("merge_timestamp")
    )
    
    # Write to claim events table
    (
        claim_events_df.write
        .format("delta")
        .mode("append")
        .partitionBy("event_type")
        .save(silver_claim_events_path)
    )
    
    print(f"Recorded {claim_events_df.count()} claim events for audit trail")
else:
    print("No claim events to record")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 9. Create Status Change Summary

# COMMAND ----------

if claims_to_update.count() > 0:
    # Create summary of status changes
    status_change_summary = latest_status_events.groupBy(
        "old_status", "new_status", "denial_reason"
    ).agg(
        count("*").alias("change_count"),
        collect_list("claim_id").alias("affected_claims")
    ).orderBy(desc("change_count"))
    
    print("Status Change Summary:")
    status_change_summary.show(truncate=False)
    
    # Overall statistics
    total_changes = claims_to_update.count()
    unique_claims = claims_to_update.select("claim_id").distinct().count()
    
    print(f"\nStatus Change Statistics:")
    print(f"  Total status changes: {total_changes}")
    print(f"  Unique claims affected: {unique_claims}")
    
    # Changes by status
    changes_by_status = latest_status_events.groupBy("new_status").count().orderBy(desc("count"))
    print("\nChanges by New Status:")
    changes_by_status.show()
else:
    print("No status changes occurred")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 10. Summary

# COMMAND ----------

print("=" * 80)
print("CLAIM STATUS MERGE SUMMARY")
print("=" * 80)
print(f"Pipeline Run ID: {pipeline_run_id}")
print(f"Events Processed: {events_to_process.count()}")
print(f"Latest Status Events: {latest_status_events.count()}")
print(f"Claims Updated: {claims_to_update.count()}")
if claims_to_update.count() > 0:
    print(f"Records Moved to History: {claims_for_history.count()}")
    print(f"Claim Events Recorded: {claim_events_df.count()}")
print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 11. Verification

# COMMAND ----------

# Verify the updates were applied correctly
try:
    # Read updated current claims
    updated_current_df = spark.read.format("delta").load(silver_claims_current_path)
    
    print(f"Updated current claims: {updated_current_df.count()} records")
    
    # Check status distribution
    print("Current claim status distribution:")
    updated_current_df.groupBy("claim_status").count().orderBy(desc("count")).show()
    
    # Verify specific updates (show claims that were updated)
    if claims_to_update.count() > 0:
        updated_claims_sample = updated_current_df.join(
            claims_to_update.select("claim_id", "claim_line_id"),
            ["claim_id", "claim_line_id"],
            "inner"
        )
        
        print("Sample of updated claims:")
        updated_claims_sample.select(
            "claim_id", "claim_line_id", "member_id", "provider_id",
            "claim_status", "denial_reason", "status_update_timestamp"
        ).show(10, truncate=False)
    
    # Verify history table
    if claims_to_update.count() > 0:
        history_df = spark.read.format("delta").load(silver_claims_history_path)
        recent_history = history_df.filter(col("history_effective_end_date") > current_timestamp() - expr("INTERVAL 1 HOUR"))
        
        print(f"Recent history records: {recent_history.count()}")
        if recent_history.count() > 0:
            recent_history.select(
                "claim_id", "claim_line_id", "claim_status", 
                "history_effective_end_date", "history_reason"
            ).show(5, truncate=False)
    
except Exception as e:
    print(f"Error verifying updates: {str(e)}")
