{{
    config(
        materialized='incremental',
        unique_key='monthly_key',
        incremental_strategy='merge'
    )
}}
WITH month_year AS (
    SELECT *, EXTRACT(YEAR FROM date) AS year, EXTRACT(MONTH FROM date) AS month
    FROM {{ ref('fact_stocks') }}
),
max_month_year AS (
    SELECT MAX(year) as max_year, MAX(month) as max_month FROM {{ this }}
),
monthly_aggregate AS (
    SELECT
        symbol,
        year,        
        month,
        {{ dbt_utils.generate_surrogate_key(['symbol', 'year', 'month']) }} AS monthly_key,     
        MIN(open) AS month_open,               
        MAX(high) AS month_high,               
        MIN(low) AS month_low,                 
        MAX(close) AS month_close,            
        AVG(volume) AS avg_daily_volume,     
        SUM(volume) AS total_volume,           
        AVG(vwap) AS avg_vwap                 
    FROM month_year, max_month_year          
    {% if is_incremental() %}
    WHERE year > max_year OR (year = max_year AND month > max_month)
    {% endif %}
    GROUP BY symbol, year, month              
)

SELECT *
FROM monthly_aggregate