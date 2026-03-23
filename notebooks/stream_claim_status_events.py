"""
Streaming Claim Status Events Processor
Processes real-time claim adjudication events from Azure Event Hubs.
"""

# Databricks notebook source
# MAGIC %md
# MAGIC ## Streaming Claim Status Events Processor
# MAGIC 
# MAGIC This notebook:
# MAGIC 1. Reads claim status events from Azure Event Hubs
# MAGIC 2. Processes events with Structured Streaming
# MAGIC 3. Validates and transforms event data
# MAGIC 4. Writes to Bronze streaming table with checkpointing
# MAGIC 5. Handles late-arriving events and watermarks
# MAGIC 6. Provides exactly-once processing guarantees

# COMMAND ----------

# Import required libraries
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.streaming import StreamingQueryException
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
pipeline_run_id = f"STREAMING_CLAIM_STATUS_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{str(uuid.uuid4())[:8]}"

# Event Hubs configuration
eventhub_connection_string = Config.EVENT_HUB_CONNECTION_STRING
eventhub_name = Config.EVENT_HUB_NAME
eventhub_namespace = Config.EVENT_HUB_NAMESPACE

# Target paths
bronze_claim_status_events_path = Config.get_full_path(Config.BRONZE_CLAIM_STATUS_EVENTS_PATH)
checkpoint_location = Config.STREAMING_CHECKPOINT_LOCATION

print(f"Pipeline Run ID: {pipeline_run_id}")
print(f"Event Hubs Namespace: {eventhub_namespace}")
print(f"Event Hubs Name: {eventhub_name}")
print(f"Target Path: {bronze_claim_status_events_path}")
print(f"Checkpoint Location: {checkpoint_location}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Event Hubs Connection Configuration

# COMMAND ----------

# Event Hubs connection configuration
eventhub_config = {
    "eventhubs.connectionString": eventhub_connection_string,
    "eventhubs.consumerGroup": "databricks-consumer",
    "eventhubs.maxEventsPerTrigger": 1000,
    "eventhubs.startingPosition": "latest",
    "eventhubs.startTime": datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%fZ')
}

print("Event Hubs Configuration:")
for key, value in eventhub_config.items():
    if "connectionString" not in key:  # Don't print sensitive connection strings
        print(f"  {key}: {value}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Define Event Schema

# COMMAND ----------

# Define the schema for claim status events
claim_status_event_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("claim_id", StringType(), True),
    StructField("claim_line_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("old_status", StringType(), True),
    StructField("new_status", StringType(), True),
    StructField("denial_reason", StringType(), True),
    StructField("event_timestamp", TimestampType(), True),
    StructField("source_system", StringType(), True),
    StructField("additional_data", StringType(), True)  # For future extensibility
])

print("Claim Status Event Schema:")
claim_status_event_schema.printTreeString()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Create Streaming DataFrame

# COMMAND ----------

try:
    # Create streaming DataFrame from Event Hubs
    streaming_events_df = (
        spark.readStream
        .format("eventhubs")
        .options(**eventhub_config)
        .load()
    )
    
    print("Successfully created streaming DataFrame from Event Hubs")
    
    # Parse the Event Hubs message body (JSON)
    parsed_events_df = streaming_events_df.select(
        from_json(col("body").cast("string"), claim_status_event_schema).alias("event_data"),
        col("enqueuedTime").alias("eventhub_enqueued_time"),
        col("partitionId"),
        col("offset"),
        col("sequenceNumber")
    ).select(
        "event_data.*",
        "eventhub_enqueued_time",
        "partitionId",
        "offset",
        "sequenceNumber"
    )
    
    # Add Event Hubs metadata
    parsed_events_df = parsed_events_df.withColumn(
        "eventhub_ingestion_timestamp",
        current_timestamp()
    ).withColumn(
        "streaming_pipeline_run_id",
        lit(pipeline_run_id)
    )
    
    print("Event parsing schema configured")
    parsed_events_df.printSchema()
    
except Exception as e:
    print(f"Error creating streaming DataFrame: {str(e)}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. Event Validation and Transformation

# COMMAND ----------

def validate_and_transform_events(events_df):
    """Validate and transform streaming events."""
    
    # Add validation logic
    validated_events = events_df.filter(
        # Required fields validation
        col("event_id").isNotNull() &
        col("claim_id").isNotNull() &
        col("claim_line_id").isNotNull() &
        col("event_type").isNotNull() &
        col("new_status").isNotNull() &
        col("event_timestamp").isNotNull()
    ).filter(
        # Event type validation
        col("event_type") == "STATUS_UPDATE"
    ).filter(
        # Status validation
        col("new_status").isin("PENDING", "PAID", "DENIED", "ADJUSTED")
    ).filter(
        # Timestamp validation (not too far in future)
        col("event_timestamp") <= current_timestamp() + expr("INTERVAL 1 HOUR")
    ).filter(
        # Event ID uniqueness (within the stream)
        # Note: This is basic validation, true deduplication happens downstream
        col("event_id").isNotNull()
    )
    
    # Add derived columns
    transformed_events = validated_events.withColumn(
        "processing_delay_seconds",
        unix_timestamp(current_timestamp()) - unix_timestamp(col("event_timestamp"))
    ).withColumn(
        "is_late_event",
        col("processing_delay_seconds") > 300  # Events arriving more than 5 minutes late
    ).withColumn(
        "event_date",
        to_date(col("event_timestamp"))
    ).withColumn(
        "event_hour",
        hour(col("event_timestamp"))
    ).withColumn(
        "record_hash",
        md5(concat_ws("|", 
            col("event_id"), col("claim_id"), col("claim_line_id"),
            col("event_type"), col("new_status"), col("event_timestamp")
        ))
    )
    
    return transformed_events

# Apply validation and transformation
transformed_events_df = validate_and_transform_events(parsed_events_df)

print("Event validation and transformation configured")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6. Watermarking and Deduplication

# COMMAND ----------

# Apply watermarking to handle late-arriving events
watermarked_events_df = transformed_events_df.withWatermark(
    "event_timestamp", 
    Config.STREAMING_WATERMARK_DELAY
)

# Deduplicate events within the watermark window
deduplicated_events_df = watermarked_events_df.dropDuplicates(
    ["record_hash"]
)

print(f"Watermark configured with delay: {Config.STREAMING_WATERMARK_DELAY}")
print("Deduplication configured on record_hash")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7. Stream Processing Metrics

# COMMAND ----------

# Create a function to monitor stream metrics
def monitor_stream_progress(batch_df, batch_id):
    """Monitor and log stream processing progress."""
    
    try:
        # Count events in this batch
        event_count = batch_df.count()
        
        # Calculate metrics
        late_events = batch_df.filter(col("is_late_event") == True).count()
        avg_delay = batch_df.agg(avg("processing_delay_seconds")).collect()[0][0]
        
        # Log metrics
        print(f"Batch {batch_id}:")
        print(f"  Events processed: {event_count}")
        print(f"  Late events: {late_events}")
        print(f"  Average processing delay: {avg_delay:.2f} seconds")
        
        # Write metrics to audit table (optional)
        audit_df = spark.createDataFrame([{
            "batch_id": batch_id,
            "pipeline_run_id": pipeline_run_id,
            "event_count": event_count,
            "late_events": late_events,
            "avg_processing_delay": avg_delay,
            "batch_timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            "stream_name": "claim_status_events"
        }])
        
        # Here you could write to an audit table if needed
        # audit_df.write.format("delta").mode("append").save(audit_path)
        
    except Exception as e:
        print(f"Error in stream monitoring: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8. Write Stream to Bronze Delta Table

# COMMAND ----------

def write_stream_to_bronze():
    """Write the streaming data to Bronze Delta table."""
    
    try:
        # Define the streaming write
        query = (
            deduplicated_events_df
            .writeStream
            .format("delta")
            .outputMode(Config.STREAMING_OUTPUT_MODE)
            .partitionBy("event_date", "event_hour")
            .option("checkpointLocation", checkpoint_location)
            .option("mergeSchema", "true")
            .option("delta.autoOptimize.optimizeWrite", "true")
            .foreachBatch(monitor_stream_progress)
            .trigger(processingTime="30 seconds")  # Process every 30 seconds
            .start(bronze_claim_status_events_path)
        )
        
        print("Streaming query started successfully")
        print(f"Query ID: {query.id}")
        print(f"Status: {query.status}")
        
        return query
        
    except Exception as e:
        print(f"Error starting streaming query: {str(e)}")
        raise

# COMMAND ----------

# MAGIC %md
# MAGIC ### 9. Stream Monitoring and Management

# COMMAND ----------

def monitor_streaming_query(query, timeout_minutes=60):
    """Monitor the streaming query for a specified time."""
    
    print(f"Monitoring streaming query for {timeout_minutes} minutes...")
    print("Press Ctrl+C to stop monitoring early")
    
    import time
    start_time = time.time()
    
    try:
        while query.isActive and (time.time() - start_time) < (timeout_minutes * 60):
            # Get current status
            status = query.status
            
            # Print progress
            print(f"Query Status: {status['message']}")
            print(f"Input Rows: {status.get('inputRowsPerSecond', {}).get('value', 0):.2f} rows/sec")
            print(f"Processing Rate: {status.get('processingTimeMsPerBatch', {}).get('value', 0):.2f} ms/batch")
            
            # Check for errors
            if status.get('isDataAvailable', False):
                print("Data available for processing")
            
            # Wait before next check
            time.sleep(30)
            
    except KeyboardInterrupt:
        print("Monitoring stopped by user")
    except Exception as e:
        print(f"Error during monitoring: {str(e)}")
    
    finally:
        print(f"Final query status: {query.status}")
        
        # Stop the query if it's still active
        if query.isActive:
            query.stop()
            print("Streaming query stopped")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 10. Start the Streaming Process

# COMMAND ----------

print("=" * 80)
print("STARTING CLAIM STATUS EVENT STREAMING")
print("=" * 80)
print(f"Pipeline Run ID: {pipeline_run_id}")
print(f"Event Hubs: {eventhub_namespace}/{eventhub_name}")
print(f"Target: {bronze_claim_status_events_path}")
print(f"Checkpoint: {checkpoint_location}")
print(f"Watermark Delay: {Config.STREAMING_WATERMARK_DELAY}")
print("=" * 80)

# Start the streaming process
try:
    streaming_query = write_stream_to_bronze()
    
    # Monitor the stream (this will run for the specified timeout)
    # For production, you might want to run this indefinitely
    monitor_streaming_query(streaming_query, timeout_minutes=5)  # 5 minutes for demo
    
except Exception as e:
    print(f"Error in streaming process: {str(e)}")
    if 'streaming_query' in locals() and streaming_query.isActive:
        streaming_query.stop()
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ### 11. Stream Statistics and Verification

# COMMAND ----------

# After streaming stops, verify the data was written
try:
    # Read the Bronze table to verify
    bronze_events_df = spark.read.format("delta").load(bronze_claim_status_events_path)
    
    print(f"Bronze Events Table: {bronze_events_df.count()} records")
    
    # Show sample records
    bronze_events_df.select(
        "event_id", "claim_id", "claim_line_id", "event_type",
        "old_status", "new_status", "event_timestamp", "event_date", "event_hour"
    ).show(10, truncate=False)
    
    # Show statistics
    print("\nEvent Statistics:")
    bronze_events_df.groupBy("new_status").count().orderBy(desc("count")).show()
    
    bronze_events_df.groupBy("event_date", "event_hour").count().orderBy("event_date", "event_hour").show(10)
    
    # Late events
    late_events_count = bronze_events_df.filter(col("is_late_event") == True).count()
    print(f"\nLate events: {late_events_count}")
    
    # Processing delay statistics
    bronze_events_df.select(
        avg("processing_delay_seconds").alias("avg_delay"),
        max("processing_delay_seconds").alias("max_delay"),
        min("processing_delay_seconds").alias("min_delay")
    ).show()
    
except Exception as e:
    print(f"Error verifying streaming output: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 12. Cleanup and Summary

# COMMAND ----------

print("=" * 80)
print("STREAMING PROCESS SUMMARY")
print("=" * 80)
print(f"Pipeline Run ID: {pipeline_run_id}")
print(f"Event Hubs Source: {eventhub_namespace}/{eventhub_name}")
print(f"Bronze Target: {bronze_claim_status_events_path}")
print(f"Checkpoint Location: {checkpoint_location}")

# Check if streaming query is still active
if 'streaming_query' in locals():
    if streaming_query.isActive:
        print("Query Status: ACTIVE")
        print("To stop the query manually, use: streaming_query.stop()")
    else:
        print("Query Status: STOPPED")
        print(f"Final Status: {streaming_query.status}")

print("\nStreaming Configuration:")
print(f"  Watermark Delay: {Config.STREAMING_WATERMARK_DELAY}")
print(f"  Output Mode: {Config.STREAMING_OUTPUT_MODE}")
print(f"  Processing Interval: 30 seconds")
print(f"  Partitioning: event_date, event_hour")

print("\nNext Steps:")
print("1. The claim status events are now available in Bronze layer")
print("2. Run the claim status merge notebook to update Silver claims")
print("3. Monitor the stream for performance and errors")
print("4. Set up alerts for stream failures or high latency")

print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 13. Stream Management Commands
# MAGIC 
# MAGIC Here are some useful commands for managing the streaming query:

# COMMAND ----------

# MAGIC %python
# # Get all active streaming queries
# active_queries = spark.streams.active
# for q in active_queries:
#     print(f"Query ID: {q.id}, Name: {q.name}, Status: {q.status}")

# COMMAND ----------

# MAGIC %python
# # Stop a specific streaming query
# # streaming_query.stop()

# COMMAND ----------

# MAGIC %python
# # Get detailed status of a streaming query
# # streaming_query.status

# COMMAND ----------

# MAGIC %python
# # Get recent progress of a streaming query
# # streaming_query.recentProgress

# COMMAND ----------

# MAGIC %python
# # Reset checkpoint location (if needed)
# # dbutils.fs.rm(checkpoint_location, recurse=True)

# COMMAND ----------

print("Streaming claim status events processor setup complete!")
print("The stream is ready to process real-time claim adjudication events.")
