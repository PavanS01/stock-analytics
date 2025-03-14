with 

source as (

    select * from {{ source('staging', 'historical_data') }}

),

renamed as (

    select
        {{ dbt_utils.generate_surrogate_key(['symbol', 'date']) }} as stock_id,
        symbol,
        date,
        open,
        high,
        low,
        close,
        adjclose,
        volume,
        unadjustedvolume,
        change,
        changepercent,
        vwap,
        label,
        changeovertime

    from source

),

deduplicated as (

    select
        *,
        row_number() over (partition by stock_id order by date desc) as rn
    from renamed

)

select
    stock_id,
    symbol,
    date,
    open,
    high,
    low,
    close,
    adjclose,
    volume,
    unadjustedvolume,
    change,
    changepercent,
    vwap,
    label,
    changeovertime
from deduplicated
where rn = 1