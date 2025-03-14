{{
    config(
        materialized='incremental',
        unique_key='yearly_id',
        incremental_strategy='merge'
    )
}}
WITH year_col AS (
    SELECT *, EXTRACT(YEAR FROM date) AS year FROM {{ ref('fact_stocks') }}
),
monthly_aggregate AS (
    SELECT
        symbol,
        year,
        {{ dbt_utils.generate_surrogate_key(['symbol', 'year']) }} AS yearly_id,
        MIN(open) AS month_open,               
        MAX(high) AS month_high,               
        MIN(low) AS month_low,                 
        MAX(close) AS month_close,            
        AVG(volume) AS avg_daily_volume,     
        SUM(volume) AS total_volume,           
        AVG(vwap) AS avg_vwap                 
    FROM year_col 
    {% if is_incremental() %}
    WHERE year >= (SELECT MAX(year) FROM {{ this }})
    {% endif %}         
    GROUP BY 1, 2, 3              
)

SELECT *
FROM monthly_aggregate