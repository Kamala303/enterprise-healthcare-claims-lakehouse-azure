-- Provider dimension model
-- Conformed provider dimension with SCD Type 2 support

{{ 
  config(
    materialized='table',
    schema='dimensions'
  ) 
}}

with source_provider as (

    select * from {{ source('gold_lakehouse', 'dim_provider') }}

),

renamed_provider as (

    select
        provider_sk,
        provider_id,
        provider_name,
        specialty,
        facility_name,
        city,
        state,
        zip_code,
        network_status,
        effective_date,
        termination_date,
        is_current,
        effective_start_date,
        effective_end_date,
        gold_row_hash,
        provider_region,
        specialty_category,
        gold_processing_timestamp,
        gold_pipeline_run_id

    from source_provider

),

final as (

    select
        -- Surrogate key
        provider_sk,
        
        -- Business key
        provider_id,
        
        -- Provider attributes
        provider_name,
        specialty,
        facility_name,
        city,
        state,
        zip_code,
        network_status,
        effective_date,
        termination_date,
        
        -- SCD Type 2 attributes
        is_current,
        effective_start_date,
        effective_end_date,
        gold_row_hash,
        
        -- Derived attributes
        provider_region,
        specialty_category,
        
        -- Metadata
        gold_processing_timestamp,
        gold_pipeline_run_id

    from renamed_provider

)

select * from final
