from com.lemontree.runners.base.base_runner import BaseJobRunner
from com.lemontree.utils.utils_redshift import read_from_redshift
from com.lemontree.constants.redshift_tables import GOLD_FACT_RESERVATIONS, MD_HOTELS, SILVER_PROTEL_RESERVATIONS, GOLD_DIM_SOURCE_SEGMENT
from com.lemontree.constants.constants import PRICE_GROUP_TYPES,ROOM_TYPES
from datetime import date, timedelta

class RobBulk(BaseJobRunner):
    def run_job(self, spark_session, glue_context) -> None:
        self.logger.info(f"[{RobBulk.__name__}] Starting Local Job ...")

        destination_bucket = self.config.get("destination_bucket")
        output_path = self.config.get("output_path")
        final_output_path = f"{destination_bucket}{output_path}"
        self.logger.info(f"final_output_path : {final_output_path}")

        # -----------------------------------------------------------
        # Step 0: Set dynamic date parameters and reading data
        # -----------------------------------------------------------

        fact_reservation_df = read_from_redshift(glue_context, table_name=GOLD_FACT_RESERVATIONS, query=None)
        md_hotels_df = read_from_redshift(glue_context, table_name=MD_HOTELS, query=None)
        protel_reservation_df = read_from_redshift(glue_context, table_name=SILVER_PROTEL_RESERVATIONS, query=None)
        source_segment_df = read_from_redshift(glue_context, table_name=GOLD_DIM_SOURCE_SEGMENT, query=None)

        start_date = date.today()
        self.logger.info(f"Start Date: {start_date}")

        # call the method to calculate rob
        final_result = calculate_future_rob_backdated_bulk(self, fact_reservation_df, md_hotels_df, protel_reservation_df, source_segment_df)
        final_result.repartition(self.config.get("partitions")).write.partitionBy('as_of_date','hotel_id').\
            mode("overwrite").option("header", True). \
            option("delimiter", ",").\
            csv(final_output_path)


def calculate_future_rob_backdated_bulk(self, fact_reservation_df, md_hotels_df, protel_reservation_df, source_segment_df) -> BaseJobRunner.DataFrame:
    # ----------------------------
    # Step 1: Filter reservations loaded up to today
    # ----------------------------

    F = self.F
    W = self.W

    protel_reservation_df = protel_reservation_df.withColumn("load_datetime", F.to_timestamp("load_datetime"))
    min_load_dt = protel_reservation_df.agg(
        F.min("load_datetime").alias("min_load_datetime")
    ).collect()[0]["min_load_datetime"]

    min_date = min_load_dt.date()
    today = date.today()
    end_date = today - timedelta(days=1)  # current_date - 1
    # end_date = min_date + timedelta(days=4)
    print("Running calculations from:", min_date, "to:", end_date)

    # Store results for all dates
    all_results_df = None
    current = min_date
    while current <= end_date:
        print(f"Processing date: {current}")
        filtered_rsrv_curr_month = (
            protel_reservation_df.join(md_hotels_df, "hotel_id", "left")
            .filter(F.col("load_datetime") <= F.lit(current))
            .select(
                "bkg_num",
                "load_datetime",
                "arrival_dt",
                "departure_dt",
                "reservation_status",
                "room_typ",
                "rm_rate",
                "rm_rate_wo_split",
                "hotel_id",
                "price_typ_grp",
                "num_of_room_booked",
                "hotel_code",
                "no_of_rooms"
            )
        )
        # ----------------------------
        # Step 2: Rank reservations to get the latest per booking
        # ----------------------------
        window_spec = W.partitionBy("bkg_num").orderBy(F.col("load_datetime").desc())
        ranked_rsrv_curr_month = filtered_rsrv_curr_month.withColumn("rnk",F.rank().over(window_spec))

        # ----------------------------
        # Step 3: Keep only latest reservations and adjust dates
        # ----------------------------
        dated_rsrv_curr_month = ranked_rsrv_curr_month.filter((F.col("rnk")==1) & (F.col("departure_dt") >= F.lit(current)) \
           & (F.col("departure_dt") > F.col("arrival_dt"))). withColumn("arrival_dt_new",F.col("arrival_dt")).withColumn("departure_dt_new", F.col("departure_dt"))

        # ----------------------------
        # Step 4: Explode reservations into one row per stay date
        # ----------------------------
        dated_rsrv_curr_month = dated_rsrv_curr_month
        rns_for_rsrv_curr_month = dated_rsrv_curr_month.alias("drcm"). \
            withColumn("stay_date",F.explode(F.sequence(F.col("arrival_dt_new").cast("date"),
                                                        F.date_sub(F.col("departure_dt_new").cast("date"), 1),
                                                        F.expr("interval 1 day")))) \
            .join(fact_reservation_df,dated_rsrv_curr_month["bkg_num"] == fact_reservation_df["source_rsrv_id"],"left")\
            .join(source_segment_df,fact_reservation_df["src_sgmnt_id"] == source_segment_df["src_sgmnt_id"],"left")\
            .withColumn("room_rate",F.when(F.col("rm_rate").isNotNull(), F.col("rm_rate").\
                                           cast("decimal(10,2)")).otherwise(F.col("drcm.rm_rate_wo_split").cast("decimal(10,2)")))\
            .filter(
                (F.col("reservation_status").isin(0, 5)) &
                (~F.col("drcm.room_typ").isin(ROOM_TYPES)) &
                (~F.col("price_typ_grp").isin(PRICE_GROUP_TYPES))
            )\
            .select(
                "drcm.hotel_id",
                "drcm.hotel_code",
                "drcm.no_of_rooms",
                "reservation_status",
                "bkg_num",
                "arrival_dt_new",
                "departure_dt_new",
                "source_rsrv_id",
                fact_reservation_df["src_sgmnt_id"].alias("src_sgmnt_id"),
                "room_rate",
                "stay_date",
                "num_of_room_booked",
                source_segment_df["source"],
                source_segment_df["segment"]
            )

        # ----------------------------
        # Step 5: Aggregate to get rob_curr_month
        # ----------------------------
        rob_curr_month = (
            rns_for_rsrv_curr_month
            .groupBy("stay_date","hotel_id","hotel_code","no_of_rooms","source","segment","reservation_status")
            .agg(
                F.sum("num_of_room_booked").alias("rob"),
                F.sum(F.col("num_of_room_booked") * F.col("room_rate")).alias("room_revenue"),
                F.sum(F.col("num_of_room_booked") * F.datediff(F.col("departure_dt_new"),F.col("arrival_dt_new"))).alias("number_of_room_nights")
            )
            .withColumn("as_of_date", F.lit(current))
            .withColumnRenamed("no_of_rooms", "inventory")
            .withColumnRenamed("source", "source_nm")
            .withColumnRenamed("segment", "segment_nm")
        )

        # Append into a cumulative DataFrame
        if all_results_df is None:
            all_results_df = rob_curr_month
        else:
            all_results_df = all_results_df.unionByName(rob_curr_month)

        # Move to next date
        current += timedelta(days=1)

    final_results = all_results_df.select(
        "as_of_date",
        "stay_date",
        "hotel_id",
        "hotel_code",
        "inventory",
        "source_nm",
        "segment_nm",
        "reservation_status",
        "number_of_room_nights",
        "room_revenue",
        "rob",
    )

    return final_results
