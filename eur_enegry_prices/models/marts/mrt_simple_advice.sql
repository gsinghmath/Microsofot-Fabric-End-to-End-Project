{{ find_date_moment("lowest") }}

, final as (
    select
        market,
        date,
        start_time::varchar as start_time,
        end_time::varchar as end_time,
        price_cent_kwh,
        case
            when price_cent_kwh < 0 then 'discharge'
            when rn < 10 then 'charge+grid'
            when rn < 18 then 'grid'
            when rn < 24 then 'battery'
            else 'battery+discharge'
        end as simple_advice
    from with_rank
)

select *
from final