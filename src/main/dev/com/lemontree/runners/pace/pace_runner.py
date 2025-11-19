import time
from com.lemontree.runners.base.base_runner import BaseJobRunner
import redshift_connector
import pandas as pd
import sys
from datetime import datetime, timedelta
import boto3
import json

class PaceRunner(BaseJobRunner):
    def run_job(self, spark_session, glue_context) -> None:
        self.logger.info(f"[{PaceRunner.__name__}] Starting PaceRunner Job ...")

        runtype = self.args['runtype']
        secretID = self.args['secretID']

        sm_client = boto3.client("secretsmanager", region_name="ap-south-1")
        try:
            response = sm_client.get_secret_value(SecretId=secretID)
            secret_string = response['SecretString']
            secret = json.loads(secret_string)
        except Exception as e:
            print("Error getting secret - ", e, "\nExiting system")
            sys.exit()

        db_read = 'dwh'
        host = secret['host']
        user = secret['username']
        password = secret['password']

        yesterday = datetime.today() - timedelta(days=1)
        yes_date = yesterday.strftime('%Y-%m-%d')
        yes_year, yes_month, yes_day = yes_date.split('-')

        print(runtype)

        def process_data():
            pace_sql = f"""
                with mat_for_curr_month_summ as 
                (
                    select  
                        hotel_code,
                        src_sgmnt_id,
                        sum(coalesce(room_nights,0)) as mat_curr_month,
                        sum(coalesce(room_revenue,0)) as mat_curr_month_rev
                    from gold.fact_hotel_tags
                    where business_date between (DATE_TRUNC('month', CURRENT_DATE )) and (CURRENT_DATE - INTERVAL '1 day')
                    group by hotel_code, src_sgmnt_id
                ),
                mat_for_curr_month_summarized as 
                (
                    select t1.* , h.hotel_id as source_hotel_id
                    from mat_for_curr_month_summ t1
                    left join md.hotels h
                    on h.hotel_code = t1.hotel_code
                ),
                mat_for_full_curr_month_summ as 
                (
                    select  
                        hotel_code,
                        src_sgmnt_id,
                        sum(coalesce(room_nights,0)) * (DATE_PART('day', DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '1 month - 1 day')::int)
                            / (EXTRACT(DAY FROM CURRENT_DATE) - 1)::float as mat_full_curr_month_prorated,
                        sum(coalesce(room_revenue,0)) * (DATE_PART('day', DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '1 month - 1 day')::int)
                            / (EXTRACT(DAY FROM CURRENT_DATE) - 1)::float as mat_full_curr_month_prorated_rev
                    from gold.fact_hotel_tags
                    where business_date between (DATE_TRUNC('month', CURRENT_DATE )) and (CURRENT_DATE - INTERVAL '1 day')
                    group by hotel_code, src_sgmnt_id
                ),
                mat_for_full_curr_month_summarized as 
                (
                    select t1.* , h.hotel_id as source_hotel_id
                    from mat_for_full_curr_month_summ t1
                    left join md.hotels h
                    on h.hotel_code = t1.hotel_code
                ),
                mat_for_last_month_summ as 
                (
                    select  
                        hotel_code,
                        src_sgmnt_id,
                        sum(coalesce(room_nights,0)) as mat_last_month,
                        sum(coalesce(room_revenue,0)) as mat_last_month_rev
                    from gold.fact_hotel_tags
                    where business_date between (DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month')) and (CURRENT_DATE - INTERVAL '1 month' - INTERVAL '1 day')
                    group by hotel_code, src_sgmnt_id
                ),
                mat_for_last_month_summarized as 
                (
                    select t1.* , h.hotel_id as source_hotel_id
                    from mat_for_last_month_summ t1
                    left join md.hotels h
                    on h.hotel_code = t1.hotel_code
                ),
                mat_for_full_last_month_summ as 
                (
                    select  
                        hotel_code,
                        src_sgmnt_id,
                        sum(coalesce(room_nights,0)) as mat_for_full_last_month,
                        sum(coalesce(room_revenue,0)) as mat_for_full_last_month_rev
                    from gold.fact_hotel_tags
                    where business_date between (DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month')) and (DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '1 day')
                    group by hotel_code, src_sgmnt_id
                ),
                mat_for_full_last_month_summarized as 
                (
                    select t1.* , h.hotel_id as source_hotel_id
                    from mat_for_full_last_month_summ t1
                    left join md.hotels h
                    on h.hotel_code = t1.hotel_code
                ),
                mat_for_last_to_last_month_summ as 
                (
                    select  
                        hotel_code,
                        src_sgmnt_id,
                        sum(coalesce(room_nights,0)) as mat_last_to_last_month,
                        sum(coalesce(room_revenue,0)) as mat_last_to_last_month_rev
                    from gold.fact_hotel_tags
                    where business_date between (DATE_TRUNC('month', CURRENT_DATE - INTERVAL '2 month')) and (CURRENT_DATE - INTERVAL '2 month' - INTERVAL '1 day')
                    group by hotel_code, src_sgmnt_id
                ),
                mat_for_last_to_last_month_summarized as 
                (
                    select t1.* , h.hotel_id as source_hotel_id
                    from mat_for_last_to_last_month_summ t1
                    left join md.hotels h
                    on h.hotel_code = t1.hotel_code
                ),
                mat_for_full_last_to_last_month_summ as 
                (
                    select  
                        hotel_code,
                        src_sgmnt_id,
                        sum(coalesce(room_nights,0)) as mat_for_full_last_to_last_month,
                        sum(coalesce(room_revenue,0)) as mat_for_full_last_to_last_month_rev
                    from gold.fact_hotel_tags
                    where business_date between (DATE_TRUNC('month', CURRENT_DATE - INTERVAL '2 month')) and (DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month') - INTERVAL '1 day')
                    group by hotel_code, src_sgmnt_id
                ),
                mat_for_full_last_to_last_month_summarized as 
                (
                    select t1.* , h.hotel_id as source_hotel_id
                    from mat_for_full_last_to_last_month_summ t1
                    left join md.hotels h
                    on h.hotel_code = t1.hotel_code
                ),
                filtered_rsrv_curr_month as 
                (
                    select 
                    *
                    from silver.protel_rsrvtns
                    where load_datetime <= CURRENT_DATE
                ),
                ranked_rsrv_curr_month as 
                (
                    select 
                    *,
                    RANK() over (partition by bkg_num order by load_datetime desc) as rank
                    from filtered_rsrv_curr_month
                ),
                dated_resrv_curr_month as 
                (
                    select 
                    *,
                    CASE WHEN arrival_dt < CURRENT_DATE THEN CURRENT_DATE 
                    ELSE arrival_dt 
                    END AS arrival_dt_new ,
                    CASE WHEN departure_dt > (DATE_TRUNC('month', CURRENT_DATE + INTERVAL '1 month') ) THEN (DATE_TRUNC('month', CURRENT_DATE + INTERVAL '1 month') ) 
                    ELSE departure_dt 
                    END AS departure_dt_new
                    from ranked_rsrv_curr_month
                    where rank = 1
                    and departure_dt >=CURRENT_DATE
                    AND arrival_dt<= (DATE_TRUNC('month', CURRENT_DATE + INTERVAL '1 month') - INTERVAL '1 day')
                ),
                rns_for_rsrv_curr_month as 
                (
                    select 
                    hotel_id,
                    dated_resrv_curr_month.bkg_num as cte_b_num,
                    fact_reservation.source_rsrv_id as fr_b_num,
                    DATEDIFF(day,arrival_dt_new,departure_dt_new)*num_of_room_booked as room_nights,
                    fact_reservation.src_sgmnt_id,
                    coalesce(dated_resrv_curr_month.rm_rate, dated_resrv_curr_month.rm_rate_wo_split) as room_rate
                    from dated_resrv_curr_month
                    left join gold.fact_reservation on fact_reservation.source_rsrv_id = dated_resrv_curr_month.bkg_num
                    left join gold.dim_source_segment 
                    on dim_source_segment.src_sgmnt_id = fact_reservation.src_sgmnt_id
                    where 
                    reservation_status in (0,5)
                    and dated_resrv_curr_month.room_typ not in (32,33)
                    AND price_typ_grp not in (19955, 19956, 23022, 8899, 17929, 23170, 23441, 23442, 23443, 23444, 23445, 23446, 
                                                    23447, 23448, 23449, 23450, 23451, 23455, 23456, 23508,681,121)

                ),
                rob_curr_month as 
                (
                    select 
                        hotel_id, 
                        src_sgmnt_id, 
                        sum(room_nights) as rob_curr_month,
                        sum(room_rate * room_nights) as rob_curr_month_rev
                    from rns_for_rsrv_curr_month
                    group by hotel_id, src_sgmnt_id
                ),
                dated_next_month_resrv_curr_month as 
                (
                    select 
                    *,
                    CASE WHEN arrival_dt < DATE_TRUNC('month', CURRENT_DATE + INTERVAL '1 month') THEN DATE_TRUNC('month', CURRENT_DATE + INTERVAL '1 month') 
                    ELSE arrival_dt 
                    END AS arrival_dt_new ,
                    CASE WHEN departure_dt > (DATE_TRUNC('month', CURRENT_DATE + INTERVAL '2 month') ) THEN (DATE_TRUNC('month', CURRENT_DATE + INTERVAL '2 month') ) 
                    ELSE departure_dt 
                    END AS departure_dt_new
                    from ranked_rsrv_curr_month
                    where rank = 1
                    and departure_dt >= DATE_TRUNC('month', CURRENT_DATE + INTERVAL '1 month')
                    AND arrival_dt<= (DATE_TRUNC('month', CURRENT_DATE + INTERVAL '2 month') - INTERVAL '1 day')
                ),
                rns_next_month_for_rsrv_curr_month as 
                (
                    select 
                    hotel_id,
                    DATEDIFF(day,arrival_dt_new,departure_dt_new)*num_of_room_booked as room_nights,
                    fact_reservation.src_sgmnt_id,
                    coalesce(dated_next_month_resrv_curr_month.rm_rate, dated_next_month_resrv_curr_month.rm_rate_wo_split) as room_rate
                    from dated_next_month_resrv_curr_month
                    left join gold.fact_reservation on fact_reservation.source_rsrv_id = dated_next_month_resrv_curr_month.bkg_num
                    left join gold.dim_source_segment 
                    on dim_source_segment.src_sgmnt_id = fact_reservation.src_sgmnt_id
                    where 
                    reservation_status in (0,5)
                    and dated_next_month_resrv_curr_month.room_typ not in (32,33)
                    AND price_typ_grp not in (19955, 19956, 23022, 8899, 17929, 23170, 23441, 23442, 23443, 23444, 23445, 23446, 
                                                    23447, 23448, 23449, 23450, 23451, 23455, 23456, 23508,681,121)

                ),
                rob_next_month_curr_month as 
                (
                    select 
                    hotel_id, 
                    src_sgmnt_id,
                    sum(room_nights) as rob_next_month_curr_month,
                    sum(room_nights * room_rate) as rob_next_month_curr_month_rev
                    from rns_next_month_for_rsrv_curr_month
                    group by hotel_id, src_sgmnt_id
                ),
                dated_next_to_next_month_resrv_curr_month as 
                (
                    select 
                    *,
                    CASE WHEN arrival_dt < DATE_TRUNC('month', CURRENT_DATE + INTERVAL '2 month') THEN DATE_TRUNC('month', CURRENT_DATE + INTERVAL '2 month') 
                    ELSE arrival_dt 
                    END AS arrival_dt_new ,
                    CASE WHEN departure_dt > (DATE_TRUNC('month', CURRENT_DATE + INTERVAL '3 month') ) THEN (DATE_TRUNC('month', CURRENT_DATE + INTERVAL '3 month') ) 
                    ELSE departure_dt 
                    END AS departure_dt_new
                    from ranked_rsrv_curr_month
                    where rank = 1
                    and departure_dt >= DATE_TRUNC('month', CURRENT_DATE + INTERVAL '2 month')
                    AND arrival_dt<= (DATE_TRUNC('month', CURRENT_DATE + INTERVAL '3 month') - INTERVAL '1 day')
                ),
                rns_next_to_next_month_for_rsrv_curr_month as 
                (
                    select 
                    hotel_id,
                    DATEDIFF(day,arrival_dt_new,departure_dt_new)*num_of_room_booked as room_nights,
                    fact_reservation.src_sgmnt_id,
                    coalesce(dated_next_to_next_month_resrv_curr_month.rm_rate, dated_next_to_next_month_resrv_curr_month.rm_rate_wo_split) as room_rate
                    from dated_next_to_next_month_resrv_curr_month
                    left join gold.fact_reservation on fact_reservation.source_rsrv_id = dated_next_to_next_month_resrv_curr_month.bkg_num
                    left join gold.dim_source_segment 
                    on dim_source_segment.src_sgmnt_id = fact_reservation.src_sgmnt_id
                    where 
                    reservation_status in (0,5)
                    and dated_next_to_next_month_resrv_curr_month.room_typ not in (32,33)
                    AND price_typ_grp not in (19955, 19956, 23022, 8899, 17929, 23170, 23441, 23442, 23443, 23444, 23445, 23446, 
                                                    23447, 23448, 23449, 23450, 23451, 23455, 23456, 23508,681,121)

                ),
                rob_next_to_next_month_curr_month as 
                (
                    select 
                    hotel_id, 
                    src_sgmnt_id,
                    sum(room_nights) as rob_next_to_next_month_curr_month,
                    sum(room_nights * room_rate) as rob_next_to_next_month_curr_month_rev
                    from rns_next_to_next_month_for_rsrv_curr_month
                    group by hotel_id, src_sgmnt_id
                ),
                filtered_rsrv_last_month as 
                (
                    select 
                    *
                    from silver.protel_rsrvtns
                    where load_datetime <= (CURRENT_DATE - INTERVAL '1 month')
                ),
                ranked_rsrv_last_month as 
                (
                    select 
                    *,
                    RANK() over (partition by bkg_num order by load_datetime desc) as rank
                    from filtered_rsrv_last_month
                ),
                dated_resrv_last_month as 
                (
                    select 
                    *,
                    CASE WHEN arrival_dt < (CURRENT_DATE - INTERVAL '1 month') THEN (CURRENT_DATE - INTERVAL '1 month') 
                    ELSE arrival_dt 
                    END AS arrival_dt_new ,
                    CASE WHEN departure_dt > (DATE_TRUNC('month', CURRENT_DATE) ) THEN (DATE_TRUNC('month', CURRENT_DATE ) ) 
                    ELSE departure_dt 
                    END AS departure_dt_new
                    from ranked_rsrv_last_month
                    where rank = 1
                    and departure_dt >=(CURRENT_DATE - INTERVAL '1 month')
                    AND arrival_dt<= (DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '1 day')
                ),
                rns_for_rsrv_last_month as 
                (
                    select 
                    hotel_id,
                    DATEDIFF(day,arrival_dt_new,departure_dt_new)*num_of_room_booked as room_nights,
                    fact_reservation.src_sgmnt_id,
                    coalesce(dated_resrv_last_month.rm_rate, dated_resrv_last_month.rm_rate_wo_split) as room_rate
                    from dated_resrv_last_month
                    left join gold.fact_reservation on fact_reservation.source_rsrv_id = dated_resrv_last_month.bkg_num
                    left join gold.dim_source_segment 
                    on dim_source_segment.src_sgmnt_id = fact_reservation.src_sgmnt_id
                    where 
                    reservation_status in (0,5)
                    and dated_resrv_last_month.room_typ not in (32,33)
                    AND price_typ_grp not in (19955, 19956, 23022, 8899, 17929, 23170, 23441, 23442, 23443, 23444, 23445, 23446, 
                                                    23447, 23448, 23449, 23450, 23451, 23455, 23456, 23508,681,121)

                ),
                rob_last_month as 
                (
                    select 
                    hotel_id, 
                    src_sgmnt_id,
                    sum(room_nights) as rob_last_month,
                    sum(room_nights * room_rate) as rob_last_month_rev
                    from rns_for_rsrv_last_month
                    group by hotel_id, src_sgmnt_id
                ),
                dated_next_month_resrv_last_month as 
                (
                    select 
                    *,
                    CASE WHEN arrival_dt < DATE_TRUNC('month', CURRENT_DATE) THEN DATE_TRUNC('month', CURRENT_DATE)
                    ELSE arrival_dt 
                    END AS arrival_dt_new ,
                    CASE WHEN departure_dt > (DATE_TRUNC('month', CURRENT_DATE + INTERVAL '1 month') ) THEN (DATE_TRUNC('month', CURRENT_DATE + INTERVAL '1 month') )
                    ELSE departure_dt 
                    END AS departure_dt_new
                    from ranked_rsrv_last_month
                    where rank = 1
                    and departure_dt >= DATE_TRUNC('month', CURRENT_DATE)
                    AND arrival_dt<= (DATE_TRUNC('month', CURRENT_DATE + INTERVAL '1 month') - INTERVAL '1 day')
                ),
                rns_next_month_for_rsrv_last_month as 
                (
                    select 
                    hotel_id,
                    DATEDIFF(day,arrival_dt_new,departure_dt_new)*num_of_room_booked as room_nights,
                    fact_reservation.src_sgmnt_id,
                    coalesce(dated_next_month_resrv_last_month.rm_rate, dated_next_month_resrv_last_month.rm_rate_wo_split) as room_rate
                    from dated_next_month_resrv_last_month
                    left join gold.fact_reservation 
                    on fact_reservation.source_rsrv_id = dated_next_month_resrv_last_month.bkg_num
                    left join gold.dim_source_segment 
                    on dim_source_segment.src_sgmnt_id = fact_reservation.src_sgmnt_id
                    where 
                    reservation_status in (0,5)
                    and dated_next_month_resrv_last_month.room_typ not in (32,33)
                    AND price_typ_grp not in (19955, 19956, 23022, 8899, 17929, 23170, 23441, 23442, 23443, 23444, 23445, 23446, 
                                                    23447, 23448, 23449, 23450, 23451, 23455, 23456, 23508,681,121)

                ),
                rob_next_month_last_month as 
                (
                    select 
                        hotel_id, 
                        src_sgmnt_id, 
                        sum(room_nights) as rob_next_month_last_month, 
                        sum(room_nights * room_rate) as rob_next_month_last_month_rev
                    from rns_next_month_for_rsrv_last_month
                    group by hotel_id, src_sgmnt_id
                ),
                dated_next_to_next_month_resrv_last_month as 
                (
                    select 
                    *,
                    CASE WHEN arrival_dt < DATE_TRUNC('month', CURRENT_DATE + INTERVAL '1 month') THEN DATE_TRUNC('month', CURRENT_DATE + INTERVAL '1 month')
                    ELSE arrival_dt 
                    END AS arrival_dt_new ,
                    CASE WHEN departure_dt > (DATE_TRUNC('month', CURRENT_DATE + INTERVAL '2 month') ) THEN (DATE_TRUNC('month', CURRENT_DATE + INTERVAL '2 month') )
                    ELSE departure_dt 
                    END AS departure_dt_new
                    from ranked_rsrv_last_month
                    where rank = 1
                    and departure_dt >= DATE_TRUNC('month', CURRENT_DATE + INTERVAL '1 month')
                    AND arrival_dt<= (DATE_TRUNC('month', CURRENT_DATE + INTERVAL '2 month') - INTERVAL '1 day')
                ),
                rns_next_to_next_month_for_rsrv_last_month as 
                (
                    select 
                    hotel_id,
                    DATEDIFF(day,arrival_dt_new,departure_dt_new)*num_of_room_booked as room_nights,
                    fact_reservation.src_sgmnt_id,
                    coalesce(dated_next_to_next_month_resrv_last_month.rm_rate, dated_next_to_next_month_resrv_last_month.rm_rate_wo_split) as room_rate
                    from dated_next_to_next_month_resrv_last_month
                    left join gold.fact_reservation 
                    on fact_reservation.source_rsrv_id = dated_next_to_next_month_resrv_last_month.bkg_num
                    left join gold.dim_source_segment 
                    on dim_source_segment.src_sgmnt_id = fact_reservation.src_sgmnt_id
                    where 
                    reservation_status in (0,5)
                    and dated_next_to_next_month_resrv_last_month.room_typ not in (32,33)
                    AND price_typ_grp not in (19955, 19956, 23022, 8899, 17929, 23170, 23441, 23442, 23443, 23444, 23445, 23446, 
                                                    23447, 23448, 23449, 23450, 23451, 23455, 23456, 23508,681,121)

                ),
                rob_next_to_next_month_last_month as 
                (
                    select 
                        hotel_id, 
                        src_sgmnt_id, 
                        sum(room_nights) as rob_next_to_next_month_last_month, 
                        sum(room_nights * room_rate) as rob_next_to_next_month_last_month_rev
                    from rns_next_to_next_month_for_rsrv_last_month
                    group by hotel_id, src_sgmnt_id
                ),
                filtered_rsrv_last_to_last_month as 
                (
                    select 
                    *
                    from silver.protel_rsrvtns
                    where load_datetime <= (CURRENT_DATE - INTERVAL '2 month')
                ),
                ranked_rsrv_last_to_last_month as 
                (
                    select 
                    *,
                    RANK() over (partition by bkg_num order by load_datetime desc) as rank
                    from filtered_rsrv_last_to_last_month
                ),
                dated_resrv_last_to_last_month as 
                (
                    select 
                    *,
                    CASE WHEN arrival_dt < (CURRENT_DATE - INTERVAL '2 month') THEN (CURRENT_DATE - INTERVAL '2 month') 
                    ELSE arrival_dt 
                    END AS arrival_dt_new ,
                    CASE WHEN departure_dt > (DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month') ) THEN (DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month') ) 
                    ELSE departure_dt 
                    END AS departure_dt_new
                    from ranked_rsrv_last_to_last_month
                    where rank = 1
                    and departure_dt >=(CURRENT_DATE - INTERVAL '2 month')
                    AND arrival_dt<= (DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month') - INTERVAL '1 day')
                ),
                rns_for_rsrv_last_to_last_month as 
                (
                    select 
                    hotel_id,
                    DATEDIFF(day,arrival_dt_new,departure_dt_new)*num_of_room_booked as room_nights,
                    fact_reservation.src_sgmnt_id,
                    coalesce(dated_resrv_last_to_last_month.rm_rate, dated_resrv_last_to_last_month.rm_rate_wo_split) as room_rate
                    from dated_resrv_last_to_last_month
                    left join gold.fact_reservation 
                    on fact_reservation.source_rsrv_id = dated_resrv_last_to_last_month.bkg_num
                    left join gold.dim_source_segment 
                    on dim_source_segment.src_sgmnt_id = fact_reservation.src_sgmnt_id
                    where 
                    reservation_status in (0,5)
                    and dated_resrv_last_to_last_month.room_typ not in (32,33)
                    AND price_typ_grp not in (19955, 19956, 23022, 8899, 17929, 23170, 23441, 23442, 23443, 23444, 23445, 23446, 
                                                    23447, 23448, 23449, 23450, 23451, 23455, 23456, 23508,681,121)

                ),
                rob_last_to_last_month as 
                (
                    select 
                        hotel_id, 
                        src_sgmnt_id, 
                        sum(room_nights) as rob_last_to_last_month, 
                        sum(room_nights * room_rate) as rob_last_to_last_month_rev
                    from rns_for_rsrv_last_to_last_month
                    group by hotel_id, src_sgmnt_id
                ),
                dated_next_month_resrv_last_to_last_month as 
                (
                    select 
                    *,
                    CASE WHEN arrival_dt < DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month') THEN DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month')
                    ELSE arrival_dt 
                    END AS arrival_dt_new ,
                    CASE WHEN departure_dt > (DATE_TRUNC('month', CURRENT_DATE) ) THEN (DATE_TRUNC('month', CURRENT_DATE) )
                    ELSE departure_dt 
                    END AS departure_dt_new
                    from ranked_rsrv_last_to_last_month
                    where rank = 1
                    and departure_dt >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month')
                    AND arrival_dt<= (DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '1 day')
                ),
                rns_next_month_for_rsrv_last_to_last_month as 
                (
                    select 
                    hotel_id,
                    DATEDIFF(day,arrival_dt_new,departure_dt_new)*num_of_room_booked as room_nights,
                    fact_reservation.src_sgmnt_id,
                    coalesce(dated_next_month_resrv_last_to_last_month.rm_rate, dated_next_month_resrv_last_to_last_month.rm_rate_wo_split) as room_rate
                    from dated_next_month_resrv_last_to_last_month
                    left join gold.fact_reservation 
                    on fact_reservation.source_rsrv_id = dated_next_month_resrv_last_to_last_month.bkg_num
                    left join gold.dim_source_segment 
                    on dim_source_segment.src_sgmnt_id = fact_reservation.src_sgmnt_id
                    where 
                    reservation_status in (0,5)
                    and dated_next_month_resrv_last_to_last_month.room_typ not in (32,33)
                    AND price_typ_grp not in (19955, 19956, 23022, 8899, 17929, 23170, 23441, 23442, 23443, 23444, 23445, 23446, 
                                                    23447, 23448, 23449, 23450, 23451, 23455, 23456, 23508,681,121)

                ),
                rob_next_month_last_to_last_month as 
                (
                    select hotel_id, src_sgmnt_id, sum(room_nights) as rob_next_month_last_to_last_month, sum(room_nights * room_rate) as rob_next_month_last_to_last_month_rev
                    from rns_next_month_for_rsrv_last_to_last_month
                    group by hotel_id, src_sgmnt_id
                ),
                dated_next_to_next_month_resrv_last_to_last_month as 
                (
                    select 
                    *,
                    CASE WHEN arrival_dt < DATE_TRUNC('month', CURRENT_DATE) THEN DATE_TRUNC('month', CURRENT_DATE )
                    ELSE arrival_dt 
                    END AS arrival_dt_new ,
                    CASE WHEN departure_dt > (DATE_TRUNC('month', CURRENT_DATE + INTERVAL '1 month') ) THEN (DATE_TRUNC('month', CURRENT_DATE + INTERVAL '1 month') )
                    ELSE departure_dt 
                    END AS departure_dt_new
                    from ranked_rsrv_last_to_last_month
                    where rank = 1
                    and departure_dt >= DATE_TRUNC('month', CURRENT_DATE )
                    AND arrival_dt<= (DATE_TRUNC('month', CURRENT_DATE + INTERVAL '1 month') - INTERVAL '1 day')
                ),
                rns_next_to_next_month_for_rsrv_last_to_last_month as 
                (
                    select 
                    hotel_id,
                    DATEDIFF(day,arrival_dt_new,departure_dt_new)*num_of_room_booked as room_nights,
                    fact_reservation.src_sgmnt_id,
                    coalesce(dated_next_to_next_month_resrv_last_to_last_month.rm_rate, dated_next_to_next_month_resrv_last_to_last_month.rm_rate_wo_split) as room_rate
                    from dated_next_to_next_month_resrv_last_to_last_month
                    left join gold.fact_reservation 
                    on fact_reservation.source_rsrv_id = dated_next_to_next_month_resrv_last_to_last_month.bkg_num
                    left join gold.dim_source_segment 
                    on dim_source_segment.src_sgmnt_id = fact_reservation.src_sgmnt_id
                    where 
                    reservation_status in (0,5)
                    and dated_next_to_next_month_resrv_last_to_last_month.room_typ not in (32,33)
                    AND price_typ_grp not in (19955, 19956, 23022, 8899, 17929, 23170, 23441, 23442, 23443, 23444, 23445, 23446, 
                                                    23447, 23448, 23449, 23450, 23451, 23455, 23456, 23508,681,121)

                ),
                rob_next_to_next_month_last_to_last_month as 
                (
                    select hotel_id, src_sgmnt_id, sum(room_nights) as rob_next_to_next_month_last_to_last_month, sum(room_nights * room_rate) as rob_next_to_next_month_last_to_last_month_rev
                    from rns_next_to_next_month_for_rsrv_last_to_last_month
                    group by hotel_id, src_sgmnt_id
                ),
                filtered_rsrv_last_to_last_to_last_month as 
                (
                    select 
                    *
                    from silver.protel_rsrvtns
                    where load_datetime <= (CURRENT_DATE - INTERVAL '3 month')
                ),
                ranked_rsrv_last_to_last_to_last_month as 
                (
                    select 
                    *,
                    RANK() over (partition by bkg_num order by load_datetime desc) as rank
                    from filtered_rsrv_last_to_last_to_last_month
                ),
                dated_next_to_next_month_resrv_last_to_last_to_last_month as 
                (
                    select 
                    *,
                    CASE WHEN arrival_dt < DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month') THEN DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month')
                    ELSE arrival_dt 
                    END AS arrival_dt_new ,
                    CASE WHEN departure_dt > (DATE_TRUNC('month', CURRENT_DATE) ) THEN (DATE_TRUNC('month', CURRENT_DATE) )
                    ELSE departure_dt 
                    END AS departure_dt_new
                    from ranked_rsrv_last_to_last_to_last_month
                    where rank = 1
                    and departure_dt >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month')
                    AND arrival_dt<= (DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '1 day')
                ),
                rns_next_to_next_month_for_rsrv_last_to_last_to_last_month as 
                (
                    select 
                    hotel_id,
                    DATEDIFF(day,arrival_dt_new,departure_dt_new)*num_of_room_booked as room_nights,
                    fact_reservation.src_sgmnt_id,
                    coalesce(dated_next_to_next_month_resrv_last_to_last_to_last_month.rm_rate, dated_next_to_next_month_resrv_last_to_last_to_last_month.rm_rate_wo_split) as room_rate
                    from dated_next_to_next_month_resrv_last_to_last_to_last_month
                    left join gold.fact_reservation 
                    on fact_reservation.source_rsrv_id = dated_next_to_next_month_resrv_last_to_last_to_last_month.bkg_num
                    left join gold.dim_source_segment 
                    on dim_source_segment.src_sgmnt_id = fact_reservation.src_sgmnt_id
                    where 
                    reservation_status in (0,5)
                    and dated_next_to_next_month_resrv_last_to_last_to_last_month.room_typ not in (32,33)
                    AND price_typ_grp not in (19955, 19956, 23022, 8899, 17929, 23170, 23441, 23442, 23443, 23444, 23445, 23446, 
                                                    23447, 23448, 23449, 23450, 23451, 23455, 23456, 23508,681,121)

                ),
                rob_next_to_next_month_last_to_last_to_last_month as 
                (
                    select hotel_id, src_sgmnt_id, sum(room_nights) as rob_next_to_next_month_last_to_last_to_last_month, sum(room_nights * room_rate) as rob_next_to_next_month_last_to_last_to_last_month_rev
                    from rns_next_to_next_month_for_rsrv_last_to_last_to_last_month
                    group by hotel_id, src_sgmnt_id
                ),
                master_hotel_list as 
                (
                    select * from 
                    (
                        select 
                            hotels.hotel_id,
                            hotels.hotel_code,
                            hotels.no_of_rooms,
                            hotels.owned_vs_managed
                        from md.hotels
                        where hotel_code not like '%ZZZ%'  --- to remove inactive hotels
                    ) hot
                    cross join
                    (
                        select 
                            dim_source_segment.src_sgmnt_id,
                            dim_source_segment.segment,
                            dim_source_segment.source
                        from gold.dim_source_segment
                    ) seg
                ),
                pace_curr_month as 
                (
                    select 
                        mhl.*,
                        COALESCE(rcm.rob_curr_month, 0) AS rob_curr_month,
                        COALESCE(rcm.rob_curr_month_rev, 0) AS rob_curr_month_rev,
                        COALESCE(mfcm.mat_curr_month, 0) AS mat_curr_month,
                        COALESCE(mfcm.mat_curr_month_rev, 0) AS mat_curr_month_rev,
                        COALESCE(rlm.rob_last_month, 0) AS rob_last_month,
                        COALESCE(rlm.rob_last_month_rev, 0) AS rob_last_month_rev,
                        COALESCE(mflm.mat_last_month, 0) AS mat_last_month,
                        COALESCE(mflm.mat_last_month_rev, 0) AS mat_last_month_rev,
                        COALESCE(rllm.rob_last_to_last_month, 0) AS rob_last_to_last_month,
                        COALESCE(rllm.rob_last_to_last_month_rev, 0) AS rob_last_to_last_month_rev,
                        COALESCE(mfllm.mat_last_to_last_month, 0) AS mat_last_to_last_month,
                        COALESCE(mfllm.mat_last_to_last_month_rev, 0) AS mat_last_to_last_month_rev,
                        COALESCE(mffcm.mat_full_curr_month_prorated, 0) AS mat_full_curr_month_prorated,
                        COALESCE(mffcm.mat_full_curr_month_prorated_rev, 0) AS mat_full_curr_month_prorated_rev,
                        COALESCE(mfflm.mat_for_full_last_month, 0) AS mat_for_full_last_month,
                        COALESCE(mfflm.mat_for_full_last_month_rev, 0) AS mat_for_full_last_month_rev,
                        COALESCE(mffllm.mat_for_full_last_to_last_month, 0) AS mat_for_full_last_to_last_month,
                        COALESCE(mffllm.mat_for_full_last_to_last_month_rev, 0) AS mat_for_full_last_to_last_month_rev,
                        COALESCE(rncm.rob_next_month_curr_month, 0) AS rob_next_month_curr_month,
                        COALESCE(rncm.rob_next_month_curr_month_rev, 0) AS rob_next_month_curr_month_rev,
                        COALESCE(rnmlm.rob_next_month_last_month, 0) AS rob_next_month_last_month,
                        COALESCE(rnmlm.rob_next_month_last_month_rev, 0) AS rob_next_month_last_month_rev,
                        COALESCE(rnmltlm.rob_next_month_last_to_last_month, 0) AS rob_next_month_last_to_last_month,
                        COALESCE(rnmltlm.rob_next_month_last_to_last_month_rev, 0) AS rob_next_month_last_to_last_month_rev,
                        COALESCE(rntnmcm.rob_next_to_next_month_curr_month, 0) AS rob_next_to_next_month_curr_month,
                        COALESCE(rntnmcm.rob_next_to_next_month_curr_month_rev, 0) AS rob_next_to_next_month_curr_month_rev,
                        COALESCE(rntnmlm.rob_next_to_next_month_last_month, 0) AS rob_next_to_next_month_last_month,
                        COALESCE(rntnmlm.rob_next_to_next_month_last_month_rev, 0) AS rob_next_to_next_month_last_month_rev,
                        COALESCE(rntnmltlm.rob_next_to_next_month_last_to_last_month, 0) AS rob_next_to_next_month_last_to_last_month,
                        COALESCE(rntnmltlm.rob_next_to_next_month_last_to_last_month_rev, 0) AS rob_next_to_next_month_last_to_last_month_rev,
                        COALESCE(rntnmltltlm.rob_next_to_next_month_last_to_last_to_last_month, 0) AS rob_next_to_next_month_last_to_last_to_last_month,
                        COALESCE(rntnmltltlm.rob_next_to_next_month_last_to_last_to_last_month_rev, 0) AS rob_next_to_next_month_last_to_last_to_last_month_rev
                    from master_hotel_list mhl
                    left join rob_curr_month rcm 
                        on rcm.hotel_id = mhl.hotel_id 
                    and rcm.src_sgmnt_id = mhl.src_sgmnt_id
                    left join mat_for_curr_month_summarized mfcm 
                        on mfcm.source_hotel_id = mhl.hotel_id 
                    and mfcm.src_sgmnt_id = mhl.src_sgmnt_id
                    left join rob_last_month rlm 
                        on rlm.hotel_id = mhl.hotel_id 
                    and rlm.src_sgmnt_id = mhl.src_sgmnt_id
                    left join mat_for_last_month_summarized mflm 
                        on mflm.source_hotel_id = mhl.hotel_id 
                    and mflm.src_sgmnt_id = mhl.src_sgmnt_id
                    left join rob_last_to_last_month rllm 
                        on rllm.hotel_id = mhl.hotel_id 
                    and rllm.src_sgmnt_id = mhl.src_sgmnt_id
                    left join mat_for_last_to_last_month_summarized mfllm 
                        on mfllm.source_hotel_id = mhl.hotel_id 
                    and mfllm.src_sgmnt_id = mhl.src_sgmnt_id
                    left join mat_for_full_curr_month_summarized mffcm 
                        on mffcm.source_hotel_id = mhl.hotel_id 
                    and mffcm.src_sgmnt_id = mhl.src_sgmnt_id
                    left join mat_for_full_last_month_summarized mfflm 
                        on mfflm.source_hotel_id = mhl.hotel_id 
                    and mfflm.src_sgmnt_id = mhl.src_sgmnt_id
                    left join mat_for_full_last_to_last_month_summarized mffllm 
                        on mffllm.source_hotel_id = mhl.hotel_id 
                    and mffllm.src_sgmnt_id = mhl.src_sgmnt_id
                    left join rob_next_month_curr_month rncm 
                        on rncm.hotel_id = mhl.hotel_id 
                    and rncm.src_sgmnt_id = mhl.src_sgmnt_id
                    left join rob_next_month_last_month rnmlm 
                        on rnmlm.hotel_id = mhl.hotel_id 
                    and rnmlm.src_sgmnt_id = mhl.src_sgmnt_id
                    left join rob_next_month_last_to_last_month rnmltlm 
                        on rnmltlm.hotel_id = mhl.hotel_id 
                    and rnmltlm.src_sgmnt_id = mhl.src_sgmnt_id
                    left join rob_next_to_next_month_curr_month rntnmcm
                        on rntnmcm.hotel_id = mhl.hotel_id 
                    and rntnmcm.src_sgmnt_id = mhl.src_sgmnt_id
                    left join rob_next_to_next_month_last_month rntnmlm
                        on rntnmlm.hotel_id = mhl.hotel_id 
                    and rntnmlm.src_sgmnt_id = mhl.src_sgmnt_id
                    left join rob_next_to_next_month_last_to_last_month rntnmltlm
                        on rntnmltlm.hotel_id = mhl.hotel_id 
                    and rntnmltlm.src_sgmnt_id = mhl.src_sgmnt_id
                    left join rob_next_to_next_month_last_to_last_to_last_month rntnmltltlm
                        on rntnmltltlm.hotel_id = mhl.hotel_id 
                    and rntnmltltlm.src_sgmnt_id = mhl.src_sgmnt_id
                )
                select 
                    p.hotel_code as "Hotel_code",
                    p.no_of_rooms as "Keys",
                    p.owned_vs_managed as "OMF",
                    p.segment as "Segment",
                    p.source as "Source",
                    p.rob_last_to_last_month + p.mat_last_to_last_month as "m_RNs_m-2_rob",
                    p.mat_for_full_last_to_last_month as "m_RNs_m-2_actual",
                    p.rob_last_month + p.mat_last_month as "m_RNs_m-1_rob",
                    p.mat_for_full_last_month as "RNs_m-1_actual",
                    p.rob_curr_month + p.mat_curr_month as "m_RNs_m_rob",
                    p.rob_last_to_last_month_rev + p.mat_last_to_last_month_rev as "m_Rev_m-2_rob",
                    p.mat_for_full_last_to_last_month_rev as "m_Rev_m-2_actual",
                    p.rob_last_month_rev + p.mat_last_month_rev as "m_Rev_m-1_rob",
                    p.mat_for_full_last_month_rev as "Rev_m-1_actual",
                    p.rob_curr_month_rev + p.mat_curr_month_rev as "m_Rev_m_rob",

                    p.rob_next_month_last_to_last_month as "m+1_RNs_m-1_ROB",
                    -- p.mat_for_full_last_month as "m+1_RNs_m-1_actuals",
                    p.rob_next_month_last_month as "m+1_RNs_m_ROB",
                    p.mat_full_curr_month_prorated as "RNs_m_Prorated_Actual",
                    p.rob_next_month_curr_month as "m+1_RNs_m+1_ROB",
                    p.rob_next_month_last_to_last_month_rev as "m+1_Rev_m-1_ROB",
                    -- p.mat_for_full_last_month_rev as "m+1_Rev_m-1_actuals",
                    p.rob_next_month_last_month_rev as "m+1_Rev_m_ROB",
                    p.mat_full_curr_month_prorated_rev as "Rev_m_Prorated_Actual",
                    p.rob_next_month_curr_month_rev as "m+1_Rev_m+1_ROB",

                    p.rob_next_to_next_month_last_to_last_to_last_month as "m+2_RNs_m-1_ROB",
                    -- p.mat_for_full_last_month as "m+2_RNs_m-1_actuals",
                    p.rob_next_to_next_month_last_to_last_month as "m+2_RNs_m_ROB",
                    -- p.mat_full_curr_month_prorated as "m+2_RNs_m_Prorated_Actual",
                    p.rob_next_to_next_month_curr_month as "m+2_RNs_m+2_ROB",
                    p.rob_next_to_next_month_last_to_last_to_last_month_rev as "m+2_Rev_m-1_ROB",
                    -- p.mat_for_full_last_month_rev as "m+2_Rev_m-1_actuals",
                    p.rob_next_to_next_month_last_to_last_month_rev as "m+2_Rev_m_ROB",
                    -- p.mat_full_curr_month_prorated_rev as "m+2_Rev_m_Prorated_Actual",
                    p.rob_next_to_next_month_curr_month_rev as "m+2_Rev_m+2_ROB",

                    CURRENT_DATE - INTERVAL '1 day' as "as_of_date"

                from pace_curr_month p
        """

            print(pace_sql)

            conn = redshift_connector.connect(
                host=host,
                database=db_read,
                user=user,
                password=password
            )
            df = pd.read_sql(pace_sql, conn)

            # decode column names (this is the missing piece)
            df.columns = [col.decode('utf-8') if isinstance(col, bytes) else col for col in df.columns]
            # decode values
            df = df.applymap(lambda x: x.decode('utf-8') if isinstance(x, bytes) else x)
            print("length of df is - ", len(df), df)

            conn.close()
            print(df.columns)
            print()
            df['omf'] = df['omf'].fillna('NA')

            remove_cols = ['hotel_code', 'keys', 'omf', 'source', 'segment', 'as_of_date']
            cols = [c for c in df.columns if c not in remove_cols]

            agg_df = (
                df.groupby(['hotel_code', 'keys', 'omf', 'source', 'segment', 'as_of_date'])[cols]
                .first()
                .reset_index()
            )

            agg_df['metrics'] = agg_df[cols].astype(str).apply(lambda x: x.to_dict(), axis=1)

            exploded_records = []

            for _, row in agg_df.iterrows():
                for k, v in row['metrics'].items():
                    exploded_records.append({
                        'hotel_code': row['hotel_code'],
                        'keys': row['keys'],
                        'omf': row['omf'],
                        'source': row['source'],
                        'segment': row['segment'],
                        'as_of_date': row['as_of_date'],
                        'metric_key': k,
                        'metric_value': v
                    })

            exploded_df = pd.DataFrame(exploded_records)

            print(exploded_df.head(10))

            return exploded_df

        def write_full_to_redshift(df):
            print("Writing full data to Redshift...")
            df.to_csv(f"s3://tfs-lemontree-de/dashboard/pace/{yes_year}/{yes_month}/{yes_day}/pace_full.csv", index=False)

            print("Full data written to Redshift successfully.")

        def write_incremental_to_redshift(df):
            print("Writing incremental data to Redshift...")
            df.to_csv(f"s3://tfs-lemontree-de/redshift_data/pace/{yes_year}/{yes_month}/{yes_day}/pace.csv", index=False)

            print("Incremental data written to Redshift successfully.")

        def send_email(mail_body):
            ses = boto3.client('ses', region_name='ap-south-1')
            sender = "ltdt@lemontreehotels.com"
            recipient = [e.strip() for e in self.config.get('notify_list').split(",")]
            subject = f"Pace Data Pipeline {runtype.capitalize()} Run Status"
            response = ses.send_email(
                Source=sender,
                Destination={
                    'ToAddresses': recipient
                },
                Message={
                    'Subject': {
                        'Data': subject
                    },
                    'Body': {
                        'Text': {
                            'Data': mail_body
                        }
                    }
                }
            )

            print("Email sent! Message ID:", response['MessageId'])

        try:
            df = process_data()
            path = f's3://tfs-lemontree-de/redshift_data/pace/{yes_year}/{yes_month}/{yes_day}/pace.csv'
            df.to_csv(path, index=False)

            glue = boto3.client('glue')
            crawler_name = "tfs_pace_crawler"
            # Start crawler
            glue.start_crawler(Name=crawler_name)
            print(f"Started crawler: {crawler_name}")
            while True:
                state = glue.get_crawler(Name=crawler_name)['Crawler']['State']
                if state != 'RUNNING':
                    print(f"Crawler {crawler_name} finished.")
                    break
                print(f"Crawler {crawler_name} still running...")
                time.sleep(10)

            mail_body = f"Hi Team,\n\nThe {runtype} run of the pace data pipeline completed successfully.\n Saved at path: {path}\nWarm Regards\n\nAutomated Script"

        except Exception as e:
            print("An error occurred:", str(e))
            mail_body = f"Hi Team,\n\nAn error occurred during the {runtype} run of the pace data pipeline:\n\n{str(e)}.\n\nWarm Regards\n\nAutomated Script"

        send_email(mail_body)