with 

source as (

    select * from {{ source('staging', 'daily_data') }}

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
        swap as vwap,
        label,
        changeovertime

    from source

)

select * from renamed
