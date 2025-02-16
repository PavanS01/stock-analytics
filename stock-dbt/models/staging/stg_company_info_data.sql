{{
    config(
        materialized='view'
    )
}}

with 

source as (

    select *,
        row_number() over (partition by symbol, date order by date desc) as rn
    from {{ source('staging', 'company_info_data') }}

),

renamed as (

    select
        {{ db_utils.generate_surrogate_key(['symbol','date']) }} as company_id,
        symbol,
        date,
        assettype,
        name,
        description,
        exchange,
        currency,
        country,
        sector,
        industry,
        marketcapitalization,
        dividendpershare,
        analysttargetprice,
        {{analyst_rating_buy(analystratingstrongbuy, analystratingbuy)}} as analyst_rating_buy,
        analystratinghold,
        {{analyst_rating_sell(analystratingstrongsell, analystratingsell)}} as analyst_rating_sell

    from source
    where rn=1
)

select * from renamed
