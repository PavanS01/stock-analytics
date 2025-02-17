{{
    config(
        materialized='table'
    )
}}

select * from {{ ref('stg_news_data') }}