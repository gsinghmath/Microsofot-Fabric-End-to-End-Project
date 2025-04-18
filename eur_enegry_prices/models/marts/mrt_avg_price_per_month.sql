with stg as (
    select *
    from {{ ref('stg_epex_spot_prices') }}
),

with_month as (
    select
        *,
        month(date) as mon,
        year(date) as yr,
        LEFT(MONTHNAME(date), 3) AS month_name_short
    from stg
),

final as (
    select
        market,
        country,
        region,
        mon,
        month_name_short,
        yr,
        avg(price_cent_kwh) as avg_month_price
    from with_month
    group by market, country, region, mon, month_name_short, yr
)

select *
from final