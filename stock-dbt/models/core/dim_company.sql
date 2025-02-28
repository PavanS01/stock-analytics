{{
    config(
        materialized='table'
    )
}}

select * from {{ ref('stg_company_info_data') }}
