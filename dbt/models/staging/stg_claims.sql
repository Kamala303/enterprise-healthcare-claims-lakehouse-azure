-- Staging model for claims fact table
-- This model reads from the Gold fact_claims table and applies basic transformations

{{ 
  config(
    materialized='view',
    schema='staging'
  ) 
}}

with source_claims as (

    select * from {{ source('gold_lakehouse', 'fact_claims') }}

),

renamed_claims as (

    select
        claim_line_key,
        claim_id,
        claim_line_id,
        member_sk,
        provider_sk,
        diagnosis_sk,
        procedure_sk,
        payer_sk,
        service_date_key,
        submission_date_key,
        claim_amount,
        claim_status,
        place_of_service,
        diagnosis_code,
        procedure_code,
        payer_id,
        member_age,
        member_age_group,
        member_gender,
        member_state,
        provider_specialty,
        provider_specialty_category,
        provider_network_status,
        provider_region,
        payer_type,
        service_year,
        service_quarter,
        service_month,
        service_season,
        submission_year,
        submission_quarter,
        service_date,
        submission_date,
        is_paid_claim,
        is_denied_claim,
        is_pending_claim,
        is_adjusted_claim,
        paid_amount,
        denied_amount,
        pending_amount,
        adjusted_amount,
        processing_delay_days,
        is_high_value_claim,
        is_out_of_network,
        claim_line_key,
        processing_delay_days,
        is_high_value_claim,
        is_out_of_network,
        fact_processing_timestamp,
        fact_pipeline_run_id

    from source_claims

),

final as (

    select
        -- Business keys
        claim_line_key,
        claim_id,
        claim_line_id,
        
        -- Foreign keys
        member_sk,
        provider_sk,
        diagnosis_sk,
        procedure_sk,
        payer_sk,
        service_date_key,
        submission_date_key,
        
        -- Measures
        claim_amount,
        paid_amount,
        denied_amount,
        pending_amount,
        adjusted_amount,
        
        -- Flags
        is_paid_claim,
        is_denied_claim,
        is_pending_claim,
        is_adjusted_claim,
        is_high_value_claim,
        is_out_of_network,
        
        -- Attributes
        claim_status,
        place_of_service,
        diagnosis_code,
        procedure_code,
        payer_id,
        member_age,
        member_age_group,
        member_gender,
        member_state,
        provider_specialty,
        provider_specialty_category,
        provider_network_status,
        provider_region,
        payer_type,
        service_year,
        service_quarter,
        service_month,
        service_season,
        submission_year,
        submission_quarter,
        service_date,
        submission_date,
        processing_delay_days,
        
        -- Metadata
        fact_processing_timestamp,
        fact_pipeline_run_id

    from renamed_claims

)

select * from final
