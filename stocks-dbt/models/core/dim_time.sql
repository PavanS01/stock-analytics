{{ config(
    materialized='table',
    unique_key='date_id'
) }}

WITH date_range AS (
    SELECT 
        date AS date_id
    FROM 
        UNNEST(GENERATE_DATE_ARRAY(DATE('2019-01-01'), DATE('2030-12-31'), INTERVAL 1 DAY)) AS date
),

time_dimension AS (
    SELECT
        date_id AS full_date,
        EXTRACT(YEAR FROM date_id) AS year,
        EXTRACT(QUARTER FROM date_id) AS quarter,
        EXTRACT(MONTH FROM date_id) AS month,
        EXTRACT(DAY FROM date_id) AS day,
        EXTRACT(DAYOFWEEK FROM date_id) AS day_of_week, 
        EXTRACT(DAYOFYEAR FROM date_id) AS day_of_year,  
        FORMAT_DATE('%B', date_id) AS month_name,  
        FORMAT_DATE('%b', date_id) AS month_name_short,  
        FORMAT_DATE('%A', date_id) AS day_name,  
        FORMAT_DATE('%a', date_id) AS day_name_short,  
        
        -- Boolean flags
        CASE WHEN EXTRACT(DAYOFWEEK FROM date_id) IN (1, 7) THEN TRUE ELSE FALSE END AS is_weekend,
        CASE WHEN EXTRACT(DAYOFWEEK FROM date_id) NOT IN (1, 7) THEN TRUE ELSE FALSE END AS is_weekday,
        CASE WHEN EXTRACT(MONTH FROM date_id) = 1 AND EXTRACT(DAY FROM date_id) = 1 THEN TRUE ELSE FALSE END AS is_new_year,
        CASE WHEN EXTRACT(MONTH FROM date_id) = 12 AND EXTRACT(DAY FROM date_id) = 25 THEN TRUE ELSE FALSE END AS is_christmas,

        -- NASDAQ Holidays
        CASE 
            -- New Year's Day (or observed)
            WHEN (EXTRACT(MONTH FROM date_id) = 1 AND EXTRACT(DAY FROM date_id) = 1) THEN TRUE
            WHEN (EXTRACT(MONTH FROM date_id) = 1 AND EXTRACT(DAY FROM date_id) = 2 AND EXTRACT(DAYOFWEEK FROM date_id) = 2) THEN TRUE -- Observed on Monday

            -- Martin Luther King Jr. Day (3rd Monday in January)
            WHEN (EXTRACT(MONTH FROM date_id) = 1 AND EXTRACT(DAYOFWEEK FROM date_id) = 2 AND EXTRACT(DAY FROM date_id) BETWEEN 15 AND 21) THEN TRUE

            -- Presidents' Day (3rd Monday in February)
            WHEN (EXTRACT(MONTH FROM date_id) = 2 AND EXTRACT(DAYOFWEEK FROM date_id) = 2 AND EXTRACT(DAY FROM date_id) BETWEEN 15 AND 21) THEN TRUE

            -- Memorial Day (Last Monday in May)
            WHEN (EXTRACT(MONTH FROM date_id) = 5 AND EXTRACT(DAYOFWEEK FROM date_id) = 2 AND EXTRACT(DAY FROM date_id) BETWEEN 25 AND 31) THEN TRUE

            -- Juneteenth (June 19 or observed)
            WHEN (EXTRACT(MONTH FROM date_id) = 6 AND EXTRACT(DAY FROM date_id) = 19) THEN TRUE
            WHEN (EXTRACT(MONTH FROM date_id) = 6 AND EXTRACT(DAY FROM date_id) = 20 AND EXTRACT(DAYOFWEEK FROM date_id) = 2) THEN TRUE

            -- Independence Day (July 4 or observed)
            WHEN (EXTRACT(MONTH FROM date_id) = 7 AND EXTRACT(DAY FROM date_id) = 4) THEN TRUE
            WHEN (EXTRACT(MONTH FROM date_id) = 7 AND EXTRACT(DAY FROM date_id) = 5 AND EXTRACT(DAYOFWEEK FROM date_id) = 2) THEN TRUE

            -- Labor Day (1st Monday in September)
            WHEN (EXTRACT(MONTH FROM date_id) = 9 AND EXTRACT(DAYOFWEEK FROM date_id) = 2 AND EXTRACT(DAY FROM date_id) BETWEEN 1 AND 7) THEN TRUE

            -- Thanksgiving (4th Thursday in November)
            WHEN (EXTRACT(MONTH FROM date_id) = 11 AND EXTRACT(DAYOFWEEK FROM date_id) = 5 AND EXTRACT(DAY FROM date_id) BETWEEN 22 AND 28) THEN TRUE

            -- Christmas (or observed)
            WHEN (EXTRACT(MONTH FROM date_id) = 12 AND EXTRACT(DAY FROM date_id) = 25) THEN TRUE
            WHEN (EXTRACT(MONTH FROM date_id) = 12 AND EXTRACT(DAY FROM date_id) = 26 AND EXTRACT(DAYOFWEEK FROM date_id) = 2) THEN TRUE
            
            ELSE FALSE 
        END AS is_nasdaq_holiday

    FROM date_range
)

SELECT * FROM time_dimension