-- Denial Trends Mart
-- Business-focused analytical table for claim denial analysis

{{ 
  config(
    materialized='table',
    schema='marts'
  ) 
}}

with denial_trends as (

    select * from {{ source('gold_lakehouse', 'mart_denial_trends') }}

),

time_periods as (

    select * from {{ source('gold_lakehouse', 'dim_date') }}

),

enriched_denials as (

    select
        dt.service_year,
        dt.service_quarter,
        dt.service_month,
        dt.denial_date,
        
        -- Denial metrics
        dt.denied_claims_count,
        dt.denied_amount,
        dt.avg_denied_amount,
        dt.max_denied_amount,
        dt.min_denied_amount,
        
        -- Rates
        dt.denial_count_rate,
        dt.denial_amount_rate,
        
        -- Provider metrics
        dt.unique_providers,
        dt.unique_members,
        dt.unique_claims_denied,
        
        -- Operational metrics
        dt.avg_processing_delay_days,
        
        -- Dimension attributes
        dt.payer_type,
        dt.provider_specialty_category,
        dt.provider_region,
        dt.member_age_group,
        dt.member_gender,
        dt.member_state,
        dt.diagnosis_code,
        dt.procedure_code,
        dt.place_of_service,
        
        -- Time period details
        tp.quarter_name,
        tp.month_name,
        tp.season,
        tp.is_weekend,
        tp.fiscal_year,
        tp.fiscal_quarter,
        
        -- Metadata
        dt.mart_processing_timestamp,
        dt.mart_pipeline_run_id

    from denial_trends dt
    left join time_periods tp 
        on dt.service_year = tp.year 
        and dt.service_month = tp.month

),

denial_analysis as (

    select
        *,
        -- Trend analysis
        lag(denied_claims_count) over (
            partition by payer_type, provider_specialty_category
            order by service_year, service_quarter
        ) as prev_quarter_denials,
        lag(denied_amount) over (
            partition by payer_type, provider_specialty_category
            order by service_year, service_quarter
        ) as prev_quarter_denied_amount,
        
        -- Quarter over quarter growth
        case 
            when lag(denied_claims_count) over (
                partition by payer_type, provider_specialty_category
                order by service_year, service_quarter
            ) > 0 
            then (denied_claims_count - lag(denied_claims_count) over (
                partition by payer_type, provider_specialty_category
                order by service_year, service_quarter
            )) / lag(denied_claims_count) over (
                partition by payer_type, provider_specialty_category
                order by service_year, service_quarter
            )
            else null
        end as qoq_denial_count_growth,
        
        case 
            when lag(denied_amount) over (
                partition by payer_type, provider_specialty_category
                order by service_year, service_quarter
            ) > 0 
            then (denied_amount - lag(denied_amount) over (
                partition by payer_type, provider_specialty_category
                order by service_year, service_quarter
            )) / lag(denied_amount) over (
                partition by payer_type, provider_specialty_category
                order by service_year, service_quarter
            )
            else null
        end as qoq_denial_amount_growth
        
    from enriched_denials

),

denial_categories as (

    select
        *,
        case 
            when denied_claims_count >= 100 then 'High Volume Denials'
            when denied_claims_count >= 50 then 'Medium Volume Denials'
            else 'Low Volume Denials'
        end as denial_volume_category,
        case 
            when denial_count_rate >= 0.20 then 'High Denial Rate'
            when denial_count_rate >= 0.10 then 'Medium Denial Rate'
            else 'Low Denial Rate'
        end as denial_rate_category,
        case 
            when avg_denied_amount >= 1000 then 'High Value Denials'
            when avg_denied_amount >= 500 then 'Medium Value Denials'
            else 'Low Value Denials'
        end as denial_value_category,
        case 
            when qoq_denial_count_growth >= 0.10 then 'Growing Fast'
            when qoq_denial_count_growth >= 0.05 then 'Growing Moderate'
            when qoq_denial_count_growth <= -0.10 then 'Declining Fast'
            when qoq_denial_count_growth <= -0.05 then 'Declining Moderate'
            else 'Stable'
        end as denial_trend_category
    from denial_analysis

)

select * from denial_categories
