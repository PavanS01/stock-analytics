with 

sources as (

    select *,
            row_number() over (partition by symbol, url order by date desc) as dr
    from {{ source('staging', 'news_data') }}

),

renamed as (

    select
        {{ dbt_utils.generate_surrogate_key(['symbol', 'url', 'date']) }} as news_id,
        symbol,
        title,
        url,
        time_published,
        summary,
        source,
        overall_sentiment_label,
        date

    from sources
    where dr=1

)

select * from renamed
