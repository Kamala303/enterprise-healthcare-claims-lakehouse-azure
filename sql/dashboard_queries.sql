-- Power BI Dashboard SQL Queries
-- These queries are optimized for Power BI performance and can be used directly
-- in Power BI Desktop or through the Synapse SQL endpoint.

-- =====================================================
-- EXECUTIVE OVERVIEW DASHBOARD QUERIES
-- =====================================================

-- Executive KPI Summary
SELECT 
    COUNT(DISTINCT claim_id) as total_claims,
    COUNT(DISTINCT claim_id WHERE claim_status = 'PENDING') as pending_claims,
    SUM(paid_amount) as total_paid_amount,
    AVG(claim_amount) as avg_claim_amount,
    COUNT(DISTINCT provider_id) as active_providers,
    COUNT(DISTINCT member_sk) as active_members,
    SUM(CASE WHEN claim_status = 'DENIED' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as denial_rate,
    AVG(processing_delay_days) as avg_processing_delay,
    SUM(CASE WHEN claim_amount > 1000 THEN 1 ELSE 0 END) as high_value_claims
FROM analytics.fct_claims 
WHERE service_date >= DATEADD(month, -12, GETDATE());

-- Monthly Claim Trends
SELECT 
    DATEFROMPARTS(service_year, service_month, 1) as claim_month,
    COUNT(DISTINCT claim_id) as total_claims,
    SUM(claim_amount) as total_amount,
    SUM(paid_amount) as paid_amount,
    SUM(CASE WHEN claim_status = 'DENIED' THEN 1 ELSE 0 END) as denied_claims,
    SUM(CASE WHEN claim_status = 'PENDING' THEN 1 ELSE 0 END) as pending_claims
FROM analytics.fct_claims 
WHERE service_date >= DATEADD(month, -24, GETDATE())
GROUP BY service_year, service_month
ORDER BY service_year, service_month;

-- Top Providers by Claim Amount
SELECT TOP 10
    provider_id,
    provider_specialty,
    provider_specialty_category,
    COUNT(DISTINCT claim_id) as claim_count,
    SUM(claim_amount) as total_amount,
    AVG(claim_amount) as avg_amount,
    SUM(CASE WHEN claim_status = 'DENIED' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as denial_rate
FROM analytics.mart_provider_performance 
WHERE service_year = YEAR(GETDATE()) 
    AND service_month = MONTH(GETDATE())
GROUP BY provider_id, provider_specialty, provider_specialty_category
ORDER BY total_amount DESC;

-- Claims by Payer Type
SELECT 
    payer_type,
    COUNT(DISTINCT claim_id) as claim_count,
    SUM(claim_amount) as total_amount,
    AVG(claim_amount) as avg_amount,
    SUM(CASE WHEN claim_status = 'DENIED' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as denial_rate
FROM analytics.fct_claims 
WHERE service_date >= DATEADD(month, -12, GETDATE())
GROUP BY payer_type
ORDER BY claim_count DESC;

-- Geographic Distribution
SELECT 
    member_state,
    COUNT(DISTINCT claim_id) as claim_count,
    SUM(claim_amount) as total_amount,
    COUNT(DISTINCT member_sk) as unique_members,
    COUNT(DISTINCT provider_id) as unique_providers
FROM analytics.fct_claims 
WHERE service_date >= DATEADD(month, -12, GETDATE())
GROUP BY member_state
ORDER BY claim_count DESC;

-- =====================================================
-- PROVIDER PERFORMANCE DASHBOARD QUERIES
-- =====================================================

-- Provider Search and Performance Summary
SELECT 
    provider_id,
    provider_name,
    provider_specialty,
    provider_specialty_category,
    provider_network_status,
    provider_region,
    SUM(total_claims) as total_claims,
    SUM(total_claim_amount) as total_amount,
    AVG(avg_claim_amount) as avg_claim_amount,
    AVG(denial_rate) as denial_rate,
    AVG(payment_rate) as payment_rate,
    SUM(unique_members) as unique_members,
    AVG(avg_processing_delay_days) as avg_processing_delay
FROM analytics.mart_provider_performance 
WHERE service_year = YEAR(GETDATE()) 
    AND service_month = MONTH(GETDATE())
GROUP BY provider_id, provider_name, provider_specialty, provider_specialty_category, 
         provider_network_status, provider_region
ORDER BY total_amount DESC;

-- Provider Performance Trends
SELECT 
    provider_id,
    DATEFROMPARTS(service_year, service_month, 1) as performance_month,
    total_claims,
    total_claim_amount,
    denial_rate,
    payment_rate,
    unique_members
FROM analytics.mart_provider_performance 
WHERE provider_id = @ProviderID  -- Parameter for selected provider
    AND service_date >= DATEADD(month, -12, GETDATE())
ORDER BY service_year, service_month;

-- Specialty Comparison
SELECT 
    provider_specialty_category,
    COUNT(DISTINCT provider_id) as provider_count,
    SUM(total_claims) as total_claims,
    SUM(total_claim_amount) as total_amount,
    AVG(denial_rate) as avg_denial_rate,
    AVG(payment_rate) as avg_payment_rate
FROM analytics.mart_provider_performance 
WHERE service_year = YEAR(GETDATE()) 
    AND service_month = MONTH(GETDATE())
GROUP BY provider_specialty_category
ORDER BY total_amount DESC;

-- Provider Quality Metrics
SELECT 
    provider_id,
    denial_rate,
    payment_rate,
    avg_processing_delay_days,
    out_of_network_rate,
    high_value_rate
FROM analytics.mart_provider_performance 
WHERE service_year = YEAR(GETDATE()) 
    AND service_month = MONTH(GETDATE())
    AND provider_id IN (
        SELECT TOP 20 provider_id 
        FROM analytics.mart_provider_performance 
        WHERE service_year = YEAR(GETDATE()) 
            AND service_month = MONTH(GETDATE())
        ORDER BY total_claim_amount DESC
    );

-- Top Providers Ranking Table
SELECT TOP 20
    provider_id,
    provider_name,
    provider_specialty_category,
    total_claims,
    total_claim_amount,
    denial_rate,
    payment_rate,
    unique_members,
    specialty_rank_quarter,
    volume_category,
    denial_performance_category
FROM analytics.mart_provider_performance 
WHERE service_year = YEAR(GETDATE()) 
    AND service_quarter = DATEPART(quarter, GETDATE())
ORDER BY total_claim_amount DESC;

-- =====================================================
-- DENIAL ANALYSIS DASHBOARD QUERIES
-- =====================================================

-- Denial Summary Metrics
SELECT 
    SUM(denied_claims_count) as total_denied_claims,
    SUM(denied_amount) as total_denied_amount,
    AVG(denial_count_rate) as avg_denial_rate,
    AVG(avg_denied_amount) as avg_denial_amount,
    COUNT(DISTINCT provider_id) as affected_providers,
    COUNT(DISTINCT member_id) as affected_members
FROM analytics.mart_denial_trends 
WHERE service_year = YEAR(GETDATE());

-- Denial Trends Over Time
SELECT 
    DATEFROMPARTS(service_year, service_month, 1) as denial_month,
    SUM(denied_claims_count) as denied_claims,
    SUM(denied_amount) as denied_amount,
    AVG(denial_count_rate) as denial_rate,
    COUNT(DISTINCT provider_id) as unique_providers
FROM analytics.mart_denial_trends 
WHERE service_date >= DATEADD(month, -24, GETDATE())
GROUP BY service_year, service_month
ORDER BY service_year, service_month;

-- Denial Reasons Breakdown
SELECT 
    denial_reason,
    COUNT(*) as denial_count,
    SUM(denied_amount) as total_denied_amount,
    AVG(denied_amount) as avg_denied_amount,
    COUNT(DISTINCT provider_id) as affected_providers
FROM analytics.fct_claims 
WHERE claim_status = 'DENIED' 
    AND denial_reason IS NOT NULL
    AND service_date >= DATEADD(month, -12, GETDATE())
GROUP BY denial_reason
ORDER BY denial_count DESC;

-- Denial by Payer Type
SELECT 
    payer_type,
    SUM(denied_claims_count) as denied_claims,
    SUM(denied_amount) as denied_amount,
    AVG(denial_count_rate) as denial_rate,
    COUNT(DISTINCT provider_id) as unique_providers
FROM analytics.mart_denial_trends 
WHERE service_year = YEAR(GETDATE())
GROUP BY payer_type
ORDER BY denied_amount DESC;

-- Provider Denial Heat Map
SELECT 
    provider_specialty_category,
    denial_reason,
    COUNT(*) as denial_count,
    SUM(denied_amount) as total_denied_amount
FROM analytics.fct_claims 
WHERE claim_status = 'DENIED' 
    AND service_date >= DATEADD(month, -12, GETDATE())
GROUP BY provider_specialty_category, denial_reason
ORDER BY denial_count DESC;

-- High-Risk Providers (High Denial Rates)
SELECT 
    provider_id,
    provider_specialty_category,
    total_claims,
    denied_claims,
    denial_rate,
    total_denied_amount
FROM (
    SELECT 
        provider_id,
        provider_specialty_category,
        COUNT(*) as total_claims,
        SUM(CASE WHEN claim_status = 'DENIED' THEN 1 ELSE 0 END) as denied_claims,
        SUM(CASE WHEN claim_status = 'DENIED' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as denial_rate,
        SUM(CASE WHEN claim_status = 'DENIED' THEN claim_amount ELSE 0 END) as total_denied_amount
    FROM analytics.fct_claims 
    WHERE service_date >= DATEADD(month, -6, GETDATE())
    GROUP BY provider_id, provider_specialty_category
) as provider_denials
WHERE denial_rate > 15.0  -- High denial rate threshold
ORDER BY denial_rate DESC;

-- =====================================================
-- ANOMALY DETECTION DASHBOARD QUERIES
-- =====================================================

-- Anomaly Summary Metrics
SELECT 
    COUNT(*) as total_anomalies,
    SUM(CASE WHEN anomaly_risk_level = 'HIGH' THEN 1 ELSE 0 END) as high_risk_anomalies,
    SUM(CASE WHEN anomaly_risk_level = 'MEDIUM' THEN 1 ELSE 0 END) as medium_risk_anomalies,
    SUM(CASE WHEN anomaly_risk_level = 'LOW' THEN 1 ELSE 0 END) as low_risk_anomalies,
    SUM(CASE WHEN requires_investigation = 1 THEN 1 ELSE 0 END) as requires_investigation,
    AVG(claim_amount) as avg_anomaly_amount,
    MAX(claim_amount) as max_anomaly_amount
FROM analytics.mart_claim_anomalies;

-- Anomaly Trends
SELECT 
    DATEFROMPARTS(service_year, service_month, 1) as anomaly_month,
    COUNT(*) as total_anomalies,
    SUM(CASE WHEN anomaly_risk_level = 'HIGH' THEN 1 ELSE 0 END) as high_risk_count,
    SUM(CASE WHEN requires_investigation = 1 THEN 1 ELSE 0 END) as investigation_count
FROM analytics.mart_claim_anomalies 
WHERE service_date >= DATEADD(month, -12, GETDATE())
GROUP BY service_year, service_month
ORDER BY service_year, service_month;

-- Anomaly Types Distribution
SELECT 
    SUM(CASE WHEN duplicate_flag = 1 THEN 1 ELSE 0 END) as duplicate_claims,
    SUM(CASE WHEN amount_outlier_flag = 1 THEN 1 ELSE 0 END) as amount_outliers,
    SUM(CASE WHEN high_denial_provider_flag = 1 THEN 1 ELSE 0 END) as high_denial_provider,
    SUM(CASE WHEN frequent_procedure_flag = 1 THEN 1 ELSE 0 END) as frequent_procedures
FROM analytics.mart_claim_anomalies;

-- Risk Level Breakdown
SELECT 
    anomaly_risk_level,
    COUNT(*) as anomaly_count,
    SUM(claim_amount) as total_amount,
    AVG(claim_amount) as avg_amount,
    SUM(CASE WHEN requires_investigation = 1 THEN 1 ELSE 0 END) as investigation_count
FROM analytics.mart_claim_anomalies 
GROUP BY anomaly_risk_level
ORDER BY anomaly_count DESC;

-- Investigation Queue (High Priority)
SELECT TOP 50
    claim_line_key,
    claim_id,
    anomaly_score,
    anomaly_risk_level,
    anomaly_reasons,
    claim_amount,
    provider_specialty_category,
    payer_type,
    service_date,
    requires_investigation
FROM analytics.mart_claim_anomalies 
WHERE requires_investigation = 1
    AND anomaly_risk_level = 'HIGH'
ORDER BY anomaly_score DESC, service_date DESC;

-- Anomaly Geographic Hotspots
SELECT 
    member_state,
    COUNT(*) as anomaly_count,
    SUM(claim_amount) as total_anomaly_amount,
    AVG(anomaly_score) as avg_risk_score
FROM analytics.mart_claim_anomalies 
WHERE service_date >= DATEADD(month, -6, GETDATE())
GROUP BY member_state
HAVING COUNT(*) > 10  -- Minimum threshold for hotspots
ORDER BY anomaly_count DESC;

-- =====================================================
-- MEMBER UTILIZATION DASHBOARD QUERIES
-- =====================================================

-- Member Utilization Summary
SELECT 
    COUNT(DISTINCT member_sk) as active_members,
    COUNT(*) as total_claims,
    SUM(claim_amount) as total_amount,
    AVG(claim_amount) as avg_claim_amount,
    COUNT(*) * 1.0 / COUNT(DISTINCT member_sk) as claims_per_member,
    SUM(CASE WHEN claim_amount > 10000 THEN 1 ELSE 0 END) as high_cost_members,
    AVG(processing_delay_days) as avg_processing_delay
FROM analytics.fct_claims 
WHERE service_date >= DATEADD(month, -12, GETDATE());

-- Member Utilization Trends
SELECT 
    DATEFROMPARTS(service_year, service_month, 1) as utilization_month,
    COUNT(DISTINCT member_sk) as active_members,
    COUNT(*) as total_claims,
    SUM(claim_amount) as total_amount,
    AVG(claim_amount) as avg_amount,
    COUNT(*) * 1.0 / COUNT(DISTINCT member_sk) as claims_per_member
FROM analytics.fct_claims 
WHERE service_date >= DATEADD(month, -24, GETDATE())
GROUP BY service_year, service_month
ORDER BY service_year, service_month;

-- Age Group Utilization
SELECT 
    member_age_group,
    COUNT(DISTINCT member_sk) as unique_members,
    COUNT(*) as total_claims,
    SUM(claim_amount) as total_amount,
    AVG(claim_amount) as avg_amount,
    COUNT(*) * 1.0 / COUNT(DISTINCT member_sk) as claims_per_member
FROM analytics.fct_claims 
WHERE service_date >= DATEADD(month, -12, GETDATE())
GROUP BY member_age_group
ORDER BY total_amount DESC;

-- Gender Distribution
SELECT 
    member_gender,
    COUNT(DISTINCT member_sk) as unique_members,
    COUNT(*) as total_claims,
    SUM(claim_amount) as total_amount,
    AVG(claim_amount) as avg_amount
FROM analytics.fct_claims 
WHERE service_date >= DATEADD(month, -12, GETDATE())
    AND member_gender IS NOT NULL
GROUP BY member_gender
ORDER BY total_amount DESC;

-- High-Cost Members
SELECT TOP 50
    member_id,
    member_age_group,
    member_gender,
    member_state,
    total_claims,
    total_claim_amount,
    avg_claim_amount,
    denial_rate,
    unique_providers,
    is_high_utilization,
    is_high_cost_member
FROM analytics.mart_member_utilization 
WHERE service_year = YEAR(GETDATE())
    AND service_month = MONTH(GETDATE())
ORDER BY total_claim_amount DESC;

-- Service Pattern Analysis
SELECT 
    provider_specialty_category,
    member_age_group,
    COUNT(*) as claim_count,
    SUM(claim_amount) as total_amount,
    AVG(claim_amount) as avg_amount
FROM analytics.fct_claims 
WHERE service_date >= DATEADD(month, -12, GETDATE())
GROUP BY provider_specialty_category, member_age_group
ORDER BY claim_count DESC;

-- =====================================================
-- SYSTEM HEALTH MONITORING QUERIES
-- =====================================================

-- Data Freshness Indicators
SELECT 
    'Claims Fact' as table_name,
    MAX(fact_processing_timestamp) as last_update,
    DATEDIFF(hour, MAX(fact_processing_timestamp), GETDATE()) as hours_since_update
FROM analytics.fct_claims

UNION ALL

SELECT 
    'Provider Performance Mart' as table_name,
    MAX(mart_processing_timestamp) as last_update,
    DATEDIFF(hour, MAX(mart_processing_timestamp), GETDATE()) as hours_since_update
FROM analytics.mart_provider_performance

UNION ALL

SELECT 
    'Denial Trends Mart' as table_name,
    MAX(mart_processing_timestamp) as last_update,
    DATEDIFF(hour, MAX(mart_processing_timestamp), GETDATE()) as hours_since_update
FROM analytics.mart_denial_trends;

-- Pipeline Health Status
SELECT 
    pipeline_name,
    COUNT(*) as total_runs,
    SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) as successful_runs,
    SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) as failed_runs,
    MAX(start_time) as last_run_time,
    AVG(DATEDIFF(minute, start_time, end_time)) as avg_duration_minutes
FROM audit.pipeline_runs 
WHERE start_time >= DATEADD(day, -7, GETDATE())
GROUP BY pipeline_name
ORDER BY failed_runs DESC;

-- Data Quality Results
SELECT 
    table_name,
    rule_name,
    AVG(pass_rate) as avg_pass_rate,
    SUM(failed_count) as total_failures,
    MAX(run_timestamp) as last_check_time
FROM analytics.data_quality_results 
WHERE run_timestamp >= DATEADD(day, -7, GETDATE())
GROUP BY table_name, rule_name
ORDER BY avg_pass_rate ASC;

-- =====================================================
-- PERFORMANCE OPTIMIZATION VIEWS
-- =====================================================

-- Create optimized views for Power BI (run these in Synapse)
-- Note: These views should be created in the analytics schema

-- Executive Summary View
CREATE OR ALTER VIEW analytics.vw_executive_summary AS
SELECT 
    DATEFROMPARTS(service_year, service_month, 1) as report_month,
    COUNT(DISTINCT claim_id) as total_claims,
    SUM(claim_amount) as total_amount,
    SUM(paid_amount) as paid_amount,
    SUM(CASE WHEN claim_status = 'DENIED' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as denial_rate,
    COUNT(DISTINCT provider_id) as active_providers,
    COUNT(DISTINCT member_sk) as active_members,
    AVG(processing_delay_days) as avg_processing_delay
FROM analytics.fct_claims 
GROUP BY service_year, service_month;

-- Provider Performance View
CREATE OR ALTER VIEW analytics.vw_provider_performance_monthly AS
SELECT 
    provider_id,
    provider_name,
    provider_specialty_category,
    DATEFROMPARTS(service_year, service_month, 1) as performance_month,
    total_claims,
    total_claim_amount,
    denial_rate,
    payment_rate,
    unique_members
FROM analytics.mart_provider_performance;

-- Denial Analysis View
CREATE OR ALTER VIEW analytics.vw_denial_analysis_monthly AS
SELECT 
    DATEFROMPARTS(service_year, service_month, 1) as denial_month,
    payer_type,
    provider_specialty_category,
    denied_claims_count,
    denied_amount,
    denial_count_rate
FROM analytics.mart_denial_trends;
