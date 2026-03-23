"""
Utility functions for creating and managing Spark sessions.
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.conf import SparkConf
from typing import Optional
import os

class SparkSessionManager:
    """Manages Spark session creation and configuration."""
    
    @staticmethod
    def create_spark_session(
        app_name: str = "HealthcareLakehouse",
        enable_delta: bool = True,
        enable_hive: bool = True,
        azure_storage_account: Optional[str] = None,
        azure_storage_key: Optional[str] = None
    ) -> SparkSession:
        """
        Create a configured Spark session for healthcare data processing.
        
        Args:
            app_name: Name of the Spark application
            enable_delta: Whether to enable Delta Lake support
            enable_hive: Whether to enable Hive support
            azure_storage_account: Azure storage account name
            azure_storage_key: Azure storage account key
            
        Returns:
            Configured SparkSession
        """
        
        # Create Spark configuration
        conf = SparkConf()
        
        # Basic Spark configuration
        conf.set("spark.app.name", app_name)
        conf.set("spark.sql.adaptive.enabled", "true")
        conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
        conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128MB")
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        
        # Delta Lake configuration
        if enable_delta:
            conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            conf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
            conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
        
        # Hive configuration
        if enable_hive:
            conf.set("spark.sql.warehouse.dir", "/user/hive/warehouse")
            conf.set("spark.sql.catalogImplementation", "hive")
        
        # Azure Storage configuration
        if azure_storage_account and azure_storage_key:
            conf.set(f"fs.azure.account.key.{azure_storage_account}.dfs.core.windows.net", azure_storage_key)
            conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "true")
        
        # Event Hubs configuration for streaming
        conf.set("spark.eventhubs.connectionString", os.getenv("EVENT_HUB_CONNECTION_STRING", ""))
        
        # Performance tuning for healthcare data
        conf.set("spark.sql.shuffle.partitions", "200")
        conf.set("spark.sql.autoBroadcastJoinThreshold", "10MB")
        conf.set("spark.sql.broadcastTimeout", "300")
        
        # Create Spark session
        builder = SparkSession.builder.config(conf=conf)
        
        spark = builder.getOrCreate()
        
        # Set log level to WARN to reduce verbosity
        spark.sparkContext.setLogLevel("WARN")
        
        return spark
    
    @staticmethod
    def get_healthcare_schema(table_type: str) -> StructType:
        """
        Get the schema for healthcare data tables.
        
        Args:
            table_type: Type of table (claims, members, providers, etc.)
            
        Returns:
            StructType schema definition
        """
        
        schemas = {
            "claims": StructType([
                StructField("claim_id", StringType(), True),
                StructField("claim_line_id", StringType(), True),
                StructField("member_id", StringType(), True),
                StructField("provider_id", StringType(), True),
                StructField("diagnosis_code", StringType(), True),
                StructField("procedure_code", StringType(), True),
                StructField("payer_id", StringType(), True),
                StructField("claim_amount", DoubleType(), True),
                StructField("service_date", DateType(), True),
                StructField("claim_status", StringType(), True),
                StructField("place_of_service", StringType(), True),
                StructField("submission_date", DateType(), True)
            ]),
            
            "members": StructType([
                StructField("member_id", StringType(), True),
                StructField("member_first_name", StringType(), True),
                StructField("member_last_name", StringType(), True),
                StructField("dob", DateType(), True),
                StructField("gender", StringType(), True),
                StructField("city", StringType(), True),
                StructField("state", StringType(), True),
                StructField("zip_code", StringType(), True),
                StructField("plan_id", StringType(), True),
                StructField("eligibility_start_date", DateType(), True),
                StructField("eligibility_end_date", DateType(), True)
            ]),
            
            "providers": StructType([
                StructField("provider_id", StringType(), True),
                StructField("provider_name", StringType(), True),
                StructField("specialty", StringType(), True),
                StructField("facility_name", StringType(), True),
                StructField("city", StringType(), True),
                StructField("state", StringType(), True),
                StructField("zip_code", StringType(), True),
                StructField("network_status", StringType(), True),
                StructField("effective_date", DateType(), True),
                StructField("termination_date", DateType(), True)
            ]),
            
            "diagnosis_ref": StructType([
                StructField("diagnosis_code", StringType(), True),
                StructField("diagnosis_desc", StringType(), True),
                StructField("diagnosis_category", StringType(), True)
            ]),
            
            "procedure_ref": StructType([
                StructField("procedure_code", StringType(), True),
                StructField("procedure_desc", StringType(), True),
                StructField("procedure_category", StringType(), True)
            ]),
            
            "payer_ref": StructType([
                StructField("payer_id", StringType(), True),
                StructField("payer_name", StringType(), True),
                StructField("payer_type", StringType(), True)
            ]),
            
            "claim_status_events": StructType([
                StructField("event_id", StringType(), True),
                StructField("claim_id", StringType(), True),
                StructField("claim_line_id", StringType(), True),
                StructField("event_type", StringType(), True),
                StructField("old_status", StringType(), True),
                StructField("new_status", StringType(), True),
                StructField("denial_reason", StringType(), True),
                StructField("event_timestamp", TimestampType(), True),
                StructField("source_system", StringType(), True)
            ])
        }
        
        return schemas.get(table_type, StructType())
    
    @staticmethod
    def add_metadata_columns(df, source_file_name: str = None, pipeline_run_id: str = None):
        """
        Add metadata columns to a DataFrame.
        
        Args:
            df: Input DataFrame
            source_file_name: Name of the source file
            pipeline_run_id: Pipeline run identifier
            
        Returns:
            DataFrame with metadata columns added
        """
        from pyspark.sql.functions import current_timestamp, lit
        
        # Add ingestion timestamp
        df = df.withColumn("ingestion_timestamp", current_timestamp())
        
        # Add source file name if provided
        if source_file_name:
            df = df.withColumn("source_file_name", lit(source_file_name))
        
        # Add pipeline run ID if provided
        if pipeline_run_id:
            df = df.withColumn("pipeline_run_id", lit(pipeline_run_id))
        
        return df
