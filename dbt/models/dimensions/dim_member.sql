-- Member dimension model
-- Conformed member dimension with SCD Type 2 support

{{ 
  config(
    materialized='table',
    schema='dimensions'
  ) 
}}

with source_member as (

    select * from {{ source('gold_lakehouse', 'dim_member') }}

),

renamed_member as (

    select
        member_sk,
        member_id,
        member_first_name,
        member_last_name,
        member_full_name,
        dob,
        gender,
        city,
        state,
        zip_code,
        plan_id,
        eligibility_start_date,
        eligibility_end_date,
        member_age,
        member_age_group,
        is_eligible,
        effective_start_date,
        effective_end_date,
        gold_processing_timestamp,
        gold_pipeline_run_id

    from source_member

),

final as (

    select
        -- Surrogate key
        member_sk,
        
        -- Business key
        member_id,
        
        -- Member attributes
        member_first_name,
        member_last_name,
        member_full_name,
        dob,
        gender,
        city,
        state,
        zip_code,
        plan_id,
        eligibility_start_date,
        eligibility_end_date,
        
        -- Derived attributes
        member_age,
        member_age_group,
        is_eligible,
        
        -- SCD Type 2 attributes
        effective_start_date,
        effective_end_date,
        
        -- Metadata
        gold_processing_timestamp,
        gold_pipeline_run_id

    from renamed_member

)

select * from final
