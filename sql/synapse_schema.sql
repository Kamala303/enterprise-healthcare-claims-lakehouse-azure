-- Synapse Analytics Schema Setup
-- Creates the analytics schema and publishes Gold tables to Synapse

-- Create analytics schema
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'analytics')
BEGIN
    EXEC sp_executesql N'CREATE SCHEMA analytics';
END
GO

-- Create external data sources pointing to ADLS Gold layer
-- Note: These would be created by the Databricks publish process
-- This script shows the expected structure in Synapse

-- Create external tables for Gold dimensions
CREATE EXTERNAL TABLE analytics.dim_member
WITH (
    LOCATION = '/gold/dim_member',
    DATA_SOURCE = [adls_gold_lakehouse],
    FILE_FORMAT = [delta_format]
)
AS SELECT * FROM [gold_lakehouse].[dim_member];
GO

CREATE EXTERNAL TABLE analytics.dim_provider
WITH (
    LOCATION = '/gold/dim_provider',
    DATA_SOURCE = [adls_gold_lakehouse],
    FILE_FORMAT = [delta_format]
)
AS SELECT * FROM [gold_lakehouse].[dim_provider];
GO

CREATE EXTERNAL TABLE analytics.dim_diagnosis
WITH (
    LOCATION = '/gold/dim_diagnosis',
    DATA_SOURCE = [adls_gold_lakehouse],
    FILE_FORMAT = [delta_format]
)
AS SELECT * FROM [gold_lakehouse].[dim_diagnosis];
GO

CREATE EXTERNAL TABLE analytics.dim_procedure
WITH (
    LOCATION = '/gold/dim_procedure',
    DATA_SOURCE = [adls_gold_lakehouse],
    FILE_FORMAT = [delta_format]
)
AS SELECT * FROM [gold_lakehouse].[dim_procedure];
GO

CREATE EXTERNAL TABLE analytics.dim_payer
WITH (
    LOCATION = '/gold/dim_payer',
    DATA_SOURCE = [adls_gold_lakehouse],
    FILE_FORMAT = [delta_format]
)
AS SELECT * FROM [gold_lakehouse].[dim_payer];
GO

CREATE EXTERNAL TABLE analytics.dim_date
WITH (
    LOCATION = '/gold/dim_date',
    DATA_SOURCE = [adls_gold_lakehouse],
    FILE_FORMAT = [delta_format]
)
AS SELECT * FROM [gold_lakehouse].[dim_date];
GO

-- Create external table for Gold fact
CREATE EXTERNAL TABLE analytics.fct_claims
WITH (
    LOCATION = '/gold/fact_claims',
    DATA_SOURCE = [adls_gold_lakehouse],
    FILE_FORMAT = [delta_format]
)
AS SELECT * FROM [gold_lakehouse].[fact_claims];
GO

-- Create external tables for Gold marts
CREATE EXTERNAL TABLE analytics.mart_provider_performance
WITH (
    LOCATION = '/gold/mart_provider_performance',
    DATA_SOURCE = [adls_gold_lakehouse],
    FILE_FORMAT = [delta_format]
)
AS SELECT * FROM [gold_lakehouse].[mart_provider_performance];
GO

CREATE EXTERNAL TABLE analytics.mart_denial_trends
WITH (
    LOCATION = '/gold/mart_denial_trends',
    DATA_SOURCE = [adls_gold_lakehouse],
    FILE_FORMAT = [delta_format]
)
AS SELECT * FROM [gold_lakehouse].[mart_denial_trends];
GO

CREATE EXTERNAL TABLE analytics.mart_member_utilization
WITH (
    LOCATION = '/gold/mart_member_utilization',
    DATA_SOURCE = [adls_gold_lakehouse],
    FILE_FORMAT = [delta_format]
)
AS SELECT * FROM [gold_lakehouse].[mart_member_utilization];
GO

CREATE EXTERNAL TABLE analytics.mart_claim_anomalies
WITH (
    LOCATION = '/gold/mart_claim_anomalies',
    DATA_SOURCE = [adls_gold_lakehouse],
    FILE_FORMAT = [delta_format]
)
AS SELECT * FROM [gold_lakehouse].[mart_claim_anomalies];
GO

-- Create materialized views for common analytical patterns
CREATE MATERIALIZED VIEW analytics.mv_provider_monthly_performance
WITH (DISTRIBUTION = HASH(provider_id))
AS
SELECT 
    provider_id,
    provider_specialty_category,
    service_year,
    service_month,
    COUNT(*) as total_claims,
    SUM(total_claim_amount) as total_amount,
    AVG(total_claim_amount) as avg_amount,
    SUM(total_denied_amount) as total_denied,
    AVG(denial_rate) as avg_denial_rate,
    COUNT(DISTINCT member_sk) as unique_members
FROM analytics.mart_provider_performance
GROUP BY 
    provider_id,
    provider_specialty_category,
    service_year,
    service_month;
GO

CREATE MATERIALIZED VIEW analytics.mv_denial_monthly_trends
WITH (DISTRIBUTION = HASH(payer_type))
AS
SELECT 
    service_year,
    service_month,
    payer_type,
    provider_specialty_category,
    SUM(denied_claims_count) as total_denied_claims,
    SUM(denied_amount) as total_denied_amount,
    AVG(denial_count_rate) as avg_denial_rate,
    COUNT(DISTINCT provider_id) as affected_providers
FROM analytics.mart_denial_trends
GROUP BY 
    service_year,
    service_month,
    payer_type,
    provider_specialty_category;
GO

-- Create stored procedures for common analytical queries
CREATE PROCEDURE analytics.sp_provider_performance_summary
    @provider_id VARCHAR(50),
    @year INT,
    @quarter INT
AS
BEGIN
    SELECT 
        provider_id,
        provider_name,
        provider_specialty_category,
        service_year,
        service_quarter,
        total_claims,
        total_claim_amount,
        avg_claim_amount,
        denial_rate,
        paid_rate,
        unique_members,
        specialty_rank_quarter,
        volume_category,
        denial_performance_category
    FROM analytics.mart_provider_performance
    WHERE provider_id = @provider_id
        AND service_year = @year
        AND service_quarter = @quarter;
END
GO

CREATE PROCEDURE analytics.sp_denial_analysis_by_payer
    @payer_type VARCHAR(50),
    @start_date DATE,
    @end_date DATE
AS
BEGIN
    SELECT 
        service_year,
        service_quarter,
        payer_type,
        provider_specialty_category,
        denied_claims_count,
        denied_amount,
        denial_count_rate,
        denial_amount_rate,
        qoq_denial_count_growth,
        denial_trend_category
    FROM analytics.mart_denial_trends
    WHERE payer_type = @payer_type
        AND denial_date BETWEEN @start_date AND @end_date
    ORDER BY service_year, service_quarter;
END
GO

-- Create security roles
CREATE ROLE analytics_reader;
CREATE ROLE analytics_writer;
CREATE ROLE analytics_admin;

-- Grant permissions
GRANT SELECT ON SCHEMA::analytics TO analytics_reader;
GRANT SELECT, INSERT, UPDATE, DELETE ON SCHEMA::analytics TO analytics_writer;
GRANT CONTROL ON SCHEMA::analytics TO analytics_admin;

-- Create users for different roles (example)
-- CREATE USER [powerbi_service] FOR LOGIN [powerbi_service];
-- ALTER ROLE analytics_reader ADD MEMBER [powerbi_service];

-- CREATE USER [analyst_user] FOR LOGIN [analyst_user];
-- ALTER ROLE analytics_writer ADD MEMBER [analyst_user];

-- CREATE USER [admin_user] FOR LOGIN [admin_user];
-- ALTER ROLE analytics_admin ADD MEMBER [admin_user];

PRINT 'Synapse Analytics schema setup completed successfully';
