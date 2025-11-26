WITH
numbers as (
    SELECT row_number() over() - 1 as n
    FROM SELECT 1
        FROM (SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1) a,
             (SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1) b,
             (SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1) c,
             (SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1) d,
             (SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1) e,
             (SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1) f
    ) x
    LIMIT 366
),
-- Filter reservations loaded up to today
filtered_rsrv_curr_month as (
    select
        spr.bkg_num,
        spr.load_datetime,
        spr.arrival_dt,
        spr.departure_dt,
        spr.reservation_status,
        spr.room_typ,
        spr.rm_rate,
        spr.rm_rate_wo_split,
        spr.hotel_id,
        spr.price_typ_grp,
        spr.num_of_room_booked,
        mh.hotel_code,
        mh.no_of_rooms
    from silver.protel_rsrvtns spr
    left join md.hotels mh
    on spr.hotel_id = mh.hotel_id
    where spr.load_datetime <= CURRENT_DATE
),
-- Get the latest version of each reservation
ranked_rsrv_curr_month as (
    select
        *,
        rank() over (partition by bkg_num order by load_datetime desc) as rnk
    from filtered_rsrv_curr_month
),

-- Adjust arrival and departure dates to fit within the current month
dated_resrv_curr_month as (
    select
        bkg_num,
        load_datetime,
        arrival_dt,
        departure_dt,
        reservation_status,
        room_typ,
        rm_rate,
        rm_rate_wo_split,
        hotel_id,
        price_typ_grp,
        num_of_room_booked,
        arrival_dt as arrival_dt_new,
        departure_dt as departure_dt_new,
        hotel_code,
        no_of_rooms
    from ranked_rsrv_curr_month
    where rnk = 1
      and departure_dt >= CURRENT_DATE
),

-- Explode reservations into one row per stay date
rns_for_rsrv_curr_month as (
    select
        drcm.hotel_id,
        drcm.hotel_code,
        drcm.no_of_rooms,
        drcm.reservation_status,
        drcm.bkg_num,
        drcm.arrival_dt_new,
        drcm.departure_dt_new,
        fr.source_rsrv_id,
        dss.src_sgmnt_id,
        dss.source,
        dss.segment,
        coalesce(
            cast(drcm.rm_rate as decimal(10,2)),
            cast(drcm.rm_rate_wo_split as decimal(10,2))
        ) as room_rate,
        dateadd(day, n.n, drcm.arrival_dt_new) as stay_date,
        num_of_room_booked
    from dated_resrv_curr_month drcm
    join numbers n
        on n.n < datediff(day, drcm.arrival_dt_new, drcm.departure_dt_new)
    left join gold.fact_reservation fr
        on fr.source_rsrv_id = drcm.bkg_num
    left join gold.dim_source_segment dss
        on dss.src_sgmnt_id = fr.src_sgmnt_id
    where
        drcm.reservation_status in (0,5)
        and drcm.room_typ not in (32,33)
        and price_typ_grp not in (
            19955, 19956, 23022, 8899, 17929, 23170, 23441, 23442, 23443, 23444, 23445, 23446,
            23447, 23448, 23449, 23450, 23451, 23455, 23456, 23508, 681, 121
        )
),

rob_curr_month as (
    select
        CURRENT_DATE as as_of_date,
        stay_date,
        hotel_id,
        hotel_code,
        no_of_rooms as inventory,
        source as source_nm,
        segment as segment_nm,
        reservation_status,
        SUM(num_of_room_booked * DATEDIFF(day, arrival_dt_new, departure_dt_new)) AS number_of_room_nights,
        sum(coalesce(num_of_room_booked * room_rate)) as room_revenue,
        sum(num_of_room_booked) as rob
    from rns_for_rsrv_curr_month
    group by stay_date, hotel_id, hotel_code, no_of_rooms, source, segment, reservation_status
)

-- Final output
select *
from rob_curr_month
where stay_date >= CURRENT_DATE order by stay_date;
