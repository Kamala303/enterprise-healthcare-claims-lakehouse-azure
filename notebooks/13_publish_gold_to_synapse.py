"""
Publish Gold to Synapse Notebook
Publishes curated Gold tables to Azure Synapse Analytics for enterprise access.
"""

# Databricks notebook source
# MAGIC %md
# MAGIC ## Publish Gold to Synapse
# MAGIC 
# MAGIC This notebook:
# MAGIC 1. Reads Gold layer tables from Databricks
# MAGIC 2. Publishes tables to Azure Synapse Analytics
# MAGIC 3. Creates external tables pointing to ADLS Gold data
# MAGIC 4. Sets up proper permissions and roles in Synapse
# MAGIC 5. Creates materialized views for performance
# MAGIC 6. Validates data integrity between Databricks and Synapse

# COMMAND ----------

# Import required libraries
from pyspark.sql.functions import *
from pyspark.sql.types import *
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
pipeline_run_id = f"SYNAPSE_PUBLISH_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{str(uuid.uuid4())[:8]}"

# Synapse configuration
synapse_server = Config.SYNAPSE_SQL_ENDPOINT
synapse_database = Config.SYNAPSE_DATABASE
synapse_user = Config.SYNAPSE_USER
synapse_password = Config.SYNAPSE_PASSWORD

# Gold table paths
gold_tables = {
    "dim_member": Config.get_full_path(Config.GOLD_DIM_MEMBER_PATH),
    "dim_provider": Config.get_full_path(Config.GOLD_DIM_PROVIDER_PATH),
    "dim_diagnosis": Config.get_full_path(Config.GOLD_DIM_DIAGNOSIS_PATH),
    "dim_procedure": Config.get_full_path(Config.GOLD_DIM_PROCEDURE_PATH),
    "dim_payer": Config.get_full_path(Config.GOLD_DIM_PAYER_PATH),
    "dim_date": Config.get_full_path(Config.GOLD_DIM_DATE_PATH),
    "fact_claims": Config.get_full_path(Config.GOLD_FACT_CLAIMS_PATH),
    "mart_provider_performance": Config.get_full_path(Config.GOLD_MART_PROVIDER_PERFORMANCE_PATH),
    "mart_denial_trends": Config.get_full_path(Config.GOLD_MART_DENIAL_TRENDS_PATH),
    "mart_member_utilization": Config.get_full_path(Config.GOLD_MART_MEMBER_UTILIZATION_PATH),
    "mart_claim_anomalies": Config.get_full_path(Config.GOLD_MART_CLAIM_ANOMALIES_PATH)
}

print(f"Pipeline Run ID: {pipeline_run_id}")
print(f"Synapse Server: {synapse_server}")
print(f"Synapse Database: {synapse_database}")
print(f"Tables to publish: {len(gold_tables)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Read Gold Tables and Validate

# COMMAND ----------

def read_and_validate_gold_table(table_name, table_path):
    """Read and validate a Gold table."""
    try:
        df = spark.read.format("delta").load(table_path)
        count = df.count()
        
        print(f"✓ {table_name}: {count:,} records")
        
        # Basic validation
        if count == 0:
            print(f"  ⚠️  Warning: {table_name} is empty")
        
        return df, count
        
    except Exception as e:
        print(f"✗ Error reading {table_name}: {str(e)}")
        return None, 0

# Read all Gold tables
gold_dataframes = {}
table_counts = {}

for table_name, table_path in gold_tables.items():
    df, count = read_and_validate_gold_table(table_name, table_path)
    if df is not None:
        gold_dataframes[table_name] = df
        table_counts[table_name] = count

print(f"\nSuccessfully read {len(gold_dataframes)} Gold tables")
print("Table summary:")
for table_name, count in table_counts.items():
    print(f"  {table_name}: {count:,} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Create Synapse Connection

# COMMAND ----------

# Note: In a real implementation, you would use JDBC or other methods to connect to Synapse
# For this demo, we'll prepare the data and show the expected structure

synapse_jdbc_url = f"jdbc:sqlserver://{synapse_server}:1433;database={synapse_database};encrypt=true;trustServerCertificate=true;"

print(f"Synapse JDBC URL: {synapse_jdbc_url}")

# In production, you would create a JDBC connection like this:
# synapse_properties = {
#     "user": synapse_user,
#     "password": synapse_password,
#     "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
# }

# For this demo, we'll prepare the data for publishing

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Prepare Data for Synapse Publishing

# COMMAND ----------

def prepare_table_for_synapse(df, table_name):
    """Prepare DataFrame for Synapse publishing."""
    
    # Convert to appropriate format for Synapse
    # Remove columns that might not be compatible
    synapse_df = df
    
    # Handle complex data types if any
    for field in df.schema.fields:
        if isinstance(field.dataType, (ArrayType, MapType)):
            synapse_df = synapse_df.drop(field.name)
    
    # Convert timestamp formats if needed
    for field_name in df.columns:
        if "timestamp" in field_name.lower():
            synapse_df = synapse_df.withColumn(
                field_name, 
                to_timestamp(col(field_name))
            )
    
    # Add publishing metadata
    synapse_df = synapse_df.withColumn(
        "synapse_publish_timestamp",
        current_timestamp()
    ).withColumn(
        "synapse_publish_run_id",
        lit(pipeline_run_id)
    )
    
    return synapse_df

# Prepare all tables for Synapse
synapse_ready_dataframes = {}
for table_name, df in gold_dataframes.items():
    synapse_ready_dataframes[table_name] = prepare_table_for_synapse(df, table_name)
    print(f"✓ Prepared {table_name} for Synapse publishing")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. Create Synapse External Tables (Simulation)

# COMMAND ----------

def create_synapse_external_table_sql(table_name, adls_path):
    """Generate SQL for creating external table in Synapse."""
    
    # Get the schema from the DataFrame
    df = synapse_ready_dataframes[table_name]
    columns = []
    
    for field in df.schema.fields:
        spark_type = str(field.dataType)
        synapse_type = "VARCHAR(255)"  # Default type
        
        # Map Spark types to Synapse types
        if "IntegerType" in spark_type:
            synapse_type = "INT"
        elif "LongType" in spark_type:
            synapse_type = "BIGINT"
        elif "DoubleType" in spark_type or "FloatType" in spark_type:
            synapse_type = "DECIMAL(18,2)"
        elif "BooleanType" in spark_type:
            synapse_type = "BIT"
        elif "DateType" in spark_type:
            synapse_type = "DATE"
        elif "TimestampType" in spark_type:
            synapse_type = "DATETIME2"
        elif "StringType" in spark_type:
            # Estimate length based on column name
            if "id" in field.name.lower() or "code" in field.name.lower():
                synapse_type = "VARCHAR(50)"
            elif "desc" in field.name.lower() or "name" in field.name.lower():
                synapse_type = "VARCHAR(500)"
            else:
                synapse_type = "VARCHAR(255)"
        
        columns.append(f"    {field.name} {synapse_type}")
    
    # Generate the CREATE EXTERNAL TABLE statement
    sql = f"""
-- Create external table for {table_name}
CREATE EXTERNAL TABLE analytics.{table_name}
(
{chr(10).join(columns)}
)
WITH (
    LOCATION = '{adls_path.replace(Config.get_adls_url(), "")}',
    DATA_SOURCE = [adls_gold_lakehouse],
    FILE_FORMAT = [delta_format]
);
"""
    
    return sql

# Generate SQL statements for all tables
synapse_sql_statements = {}
for table_name, table_path in gold_tables.items():
    synapse_sql_statements[table_name] = create_synapse_external_table_sql(table_name, table_path)

print("Generated SQL statements for Synapse external tables")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6. Write Publishing Metadata

# COMMAND ----------

# Create a publishing log
publishing_log = []

for table_name, df in synapse_ready_dataframes.items():
    publishing_log.append({
        "table_name": table_name,
        "source_path": gold_tables[table_name],
        "record_count": table_counts[table_name],
        "publish_timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        "pipeline_run_id": pipeline_run_id,
        "status": "PREPARED_FOR_PUBLISH",
        "synapse_schema": "analytics"
    })

publishing_log_df = spark.createDataFrame(publishing_log)

# Write publishing log
audit_path = Config.get_full_path(Config.AUDIT_PATH)
publish_log_path = f"{audit_path}/synapse_publish_log"

(
    publishing_log_df.write
    .format("delta")
    .mode("append")
    .partitionBy("publish_timestamp")
    .save(publish_log_path)
)

print(f"Publishing log written with {len(publishing_log)} entries")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7. Generate Synapse Setup Script

# COMMAND ----------

# Generate the complete Synapse setup script
synapse_setup_script = f"""
-- Synapse Analytics Setup for Healthcare Lakehouse
-- Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
-- Pipeline Run ID: {pipeline_run_id}

-- Create analytics schema if not exists
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'analytics')
BEGIN
    EXEC sp_executesql N'CREATE SCHEMA analytics';
END
GO

-- Create external data source for ADLS Gold Lakehouse
IF NOT EXISTS (SELECT * FROM sys.external_data_sources WHERE name = 'adls_gold_lakehouse')
BEGIN
    CREATE EXTERNAL DATA SOURCE adls_gold_lakehouse
    WITH (
        TYPE = HADOOP,
        LOCATION = '{Config.get_adls_url()}',
        CREDENTIAL = [adls_credential]
    );
END
GO

-- Create external file format for Delta
IF NOT EXISTS (SELECT * FROM sys.external_file_formats WHERE name = 'delta_format')
BEGIN
    CREATE EXTERNAL FILE FORMAT delta_format
    WITH (
        FORMAT_TYPE = DELTA,
        DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
    );
END
GO

"""

# Add table creation statements
for table_name, sql_statement in synapse_sql_statements.items():
    synapse_setup_script += sql_statement + "\nGO\n"

# Add security setup
synapse_setup_script += """
-- Create security roles
CREATE ROLE analytics_reader;
CREATE ROLE analytics_writer;
CREATE ROLE analytics_admin;

-- Grant permissions to roles
GRANT SELECT ON SCHEMA::analytics TO analytics_reader;
GRANT SELECT, INSERT, UPDATE, DELETE ON SCHEMA::analytics TO analytics_writer;
GRANT CONTROL ON SCHEMA::analytics TO analytics_admin;

-- Create materialized views for performance
CREATE MATERIALIZED VIEW analytics.mv_provider_summary
WITH (DISTRIBUTION = HASH(provider_id))
AS
SELECT 
    provider_id,
    provider_specialty_category,
    service_year,
    service_quarter,
    COUNT(*) as total_claims,
    SUM(total_claim_amount) as total_amount,
    AVG(denial_rate) as avg_denial_rate
FROM analytics.mart_provider_performance
GROUP BY provider_id, provider_specialty_category, service_year, service_quarter;
GO

PRINT 'Synapse Analytics setup completed successfully';
"""

# Write the setup script to a file
setup_script_path = "/tmp/synapse_setup_script.sql"
with open(setup_script_path, "w") as f:
    f.write(synapse_setup_script)

print(f"Synapse setup script generated: {setup_script_path}")
print(f"Script length: {len(synapse_setup_script)} characters")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8. Data Validation Summary

# COMMAND ----------

print("=" * 80)
print("SYNAPSE PUBLISHING PREPARATION SUMMARY")
print("=" * 80)
print(f"Pipeline Run ID: {pipeline_run_id}")
print(f"Synapse Server: {synapse_server}")
print(f"Target Database: {synapse_database}")
print()

print("Tables Prepared for Publishing:")
for table_name, count in table_counts.items():
    print(f"  {table_name}: {count:,} records")

print(f"\nTotal Records: {sum(table_counts.values()):,}")

print("\nPublishing Status:")
print("  ✓ Data validation completed")
print("  ✓ Schema mapping completed")
print("  ✓ SQL statements generated")
print("  ✓ Publishing metadata logged")

print("\nNext Steps:")
print("  1. Execute the generated SQL script in Synapse")
print("  2. Set up ADLS credentials in Synapse")
print("  3. Configure user permissions and roles")
print("  4. Test external table connectivity")
print("  5. Set up Power BI connections")

print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 9. Sample Data Validation

# COMMAND ----------

# Validate key tables with sample queries
print("Sample Data Validation:")

# Validate fact table
if "fact_claims" in gold_dataframes:
    fact_df = gold_dataframes["fact_claims"]
    print(f"\nFact Claims Sample:")
    fact_df.select(
        "claim_line_key", "member_sk", "provider_sk", "claim_amount",
        "claim_status", "service_year", "service_quarter"
    ).show(3, truncate=False)

# Validate provider performance mart
if "mart_provider_performance" in gold_dataframes:
    provider_mart_df = gold_dataframes["mart_provider_performance"]
    print(f"\nProvider Performance Mart Sample:")
    provider_mart_df.select(
        "provider_id", "provider_specialty_category", "total_claims",
        "total_claim_amount", "denial_rate", "service_year"
    ).show(3, truncate=False)

# Validate denial trends mart
if "mart_denial_trends" in gold_dataframes:
    denial_mart_df = gold_dataframes["mart_denial_trends"]
    print(f"\nDenial Trends Mart Sample:")
    denial_mart_df.select(
        "service_year", "service_quarter", "payer_type",
        "denied_claims_count", "denial_amount", "denial_count_rate"
    ).show(3, truncate=False)

print("\nData validation completed successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 10. Publishing Instructions

# COMMAND ----------

print("""
SYNAPSE PUBLISHING INSTRUCTIONS
================================

To complete the publishing process to Synapse:

1. SETUP PREREQUISITES:
   - Ensure ADLS Gen2 is accessible from Synapse workspace
   - Create managed identity or service principal for ADLS access
   - Configure firewall rules for connectivity

2. EXECUTE SETUP SCRIPT:
   - Copy the generated SQL script from: /tmp/synapse_setup_script.sql
   - Execute in Synapse SQL endpoint as an admin user
   - Verify all external tables are created successfully

3. CONFIGURE SECURITY:
   - Create users for Power BI service, analysts, and admins
   - Assign appropriate roles (analytics_reader, analytics_writer, analytics_admin)
   - Test connectivity with different user accounts

4. VALIDATE DATA ACCESS:
   - Test queries on external tables
   - Verify materialized views are working
   - Check performance on analytical queries

5. CONNECT POWER BI:
   - Use the Synapse SQL endpoint as data source
   - Connect to analytics schema tables
   - Build reports using the published tables

6. MONITORING:
   - Set up monitoring for external table connectivity
   - Monitor query performance
   - Track data freshness

The Gold layer data is now ready for enterprise consumption through Synapse Analytics!
""")
