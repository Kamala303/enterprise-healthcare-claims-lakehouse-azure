-- Provider Performance Mart
-- Business-focused analytical table for provider performance metrics

{{ 
  config(
    materialized='table',
    schema='marts'
  ) 
}}

with provider_performance as (

    select * from {{ source('gold_lakehouse', 'mart_provider_performance') }}

),

provider_details as (

    select * from {{ ref('dim_provider') }}

),

member_details as (

    select * from {{ ref('dim_member') }}

),

enriched_performance as (

    select
        pp.provider_sk,
        pp.provider_id,
        pp.provider_specialty,
        pp.provider_specialty_category,
        pp.provider_network_status,
        pp.provider_region,
        pp.service_year,
        pp.service_quarter,
        pp.service_month,
        
        -- Volume metrics
        pp.total_claims,
        pp.unique_members,
        pp.avg_claim_per_member,
        
        -- Financial metrics
        pp.total_claim_amount,
        pp.avg_claim_amount,
        pp.max_claim_amount,
        pp.min_claim_amount,
        pp.total_paid_amount,
        pp.total_denied_amount,
        pp.total_pending_amount,
        pp.total_adjusted_amount,
        pp.payment_rate,
        
        -- Status metrics
        pp.paid_claims,
        pp.denied_claims,
        pp.pending_claims,
        pp.adjusted_claims,
        pp.denial_rate,
        pp.paid_rate,
        pp.pending_rate,
        pp.adjusted_rate,
        
        -- Quality metrics
        pp.out_of_network_claims,
        pp.high_value_claims,
        pp.out_of_network_rate,
        pp.high_value_rate,
        
        -- Operational metrics
        pp.avg_processing_delay_days,
        pp.avg_member_age,
        
        -- Provider details
        pd.provider_name,
        pd.facility_name,
        pd.city as provider_city,
        pd.state as provider_state,
        
        -- Metadata
        pp.mart_processing_timestamp,
        pp.mart_pipeline_run_id

    from provider_performance pp
    left join provider_details pd 
        on pp.provider_sk = pd.provider_sk 
        and pd.is_current = true

),

performance_ranking as (

    select
        *,
        row_number() over (
            partition by service_year, service_quarter, provider_specialty_category
            order by total_claim_amount desc
        ) as specialty_rank_quarter,
        row_number() over (
            partition by service_year, provider_specialty_category
            order by total_claim_amount desc
        ) as specialty_rank_year,
        row_number() over (
            partition by service_year, service_quarter, provider_region
            order by total_claim_amount desc
        ) as regional_rank_quarter,
        row_number() over (
            partition by service_year, provider_region
            order by total_claim_amount desc
        ) as regional_rank_year

    from enriched_performance

),

performance_categories as (

    select
        *,
        case 
            when total_claim_amount >= 1000000 then 'High Volume'
            when total_claim_amount >= 100000 then 'Medium Volume'
            else 'Low Volume'
        end as volume_category,
        case 
            when denial_rate <= 0.05 then 'Excellent'
            when denial_rate <= 0.10 then 'Good'
            when denial_rate <= 0.20 then 'Average'
            else 'Poor'
        end as denial_performance_category,
        case 
            when avg_claim_amount >= 500 then 'High Value'
            when avg_claim_amount >= 200 then 'Medium Value'
            else 'Low Value'
        end as value_category
    from performance_ranking

)

select * from performance_categories
