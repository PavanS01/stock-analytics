{{
    config(
        materialized='incremental',
        unique_key='stock_id',  
        incremental_strategy='merge',
        partition_by={
            "field": "date",          
            "data_type": "date",      
            "granularity": "day"
        }
    )
}}

WITH historic_data AS (
    SELECT * FROM {{ ref('stg_historical_data') }}
),

daily_data AS (
    SELECT * FROM {{ ref('stg_daily_data') }}
),

combined_data AS (
    SELECT * FROM historic_data
    UNION ALL
    SELECT * FROM daily_data
)

SELECT * FROM combined_data

{% if is_incremental() %}
WHERE date > (SELECT MAX(date) FROM {{ this }})
{% endif %}