with stg as (
    select
        market,
        date,
        start_time,
        end_time,
        price_cent_kwh
    from {{ ref('stg_epex_spot_prices') }}
),

with_rank as (
    select
        *,
        row_number() over (partition by date, market order by price_cent_kwh asc) as rn
    from stg
),

final as (
    select
        date,
        market,
        start_time,
        end_time,
        price_cent_kwh
    from with_rank
    where rn = 1
)

select *
from final