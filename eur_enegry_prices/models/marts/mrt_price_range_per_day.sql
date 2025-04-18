with min_price as (
    select date,market, price_cent_kwh as min_price from {{ ref('mrt_lowest_price_per_day') }}
),

max_price as (
   select date,market, price_cent_kwh as max_price from {{ ref('mrt_highest_price_per_day') }} 
),

final as (
    select minp.date, minp.market,min_price, max_price from min_price minp join max_price maxp 
    on  minp.date = maxp.date and minp.market = maxp.market
)

select * from final