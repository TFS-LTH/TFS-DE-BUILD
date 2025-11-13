from com.lemontree.runners.base.base_runner import BaseJobRunner
from com.lemontree.utils.utils_redshift import read_from_redshift
from com.lemontree.constants.redshift_tables import GOLD_FACT_RESERVATIONS, MD_HOTELS, SILVER_PROTEL_RESERVATIONS, GOLD_DIM_SOURCE_SEGMENT
from com.lemontree.utils.utils_helper_methods import calculate_week_number_dynamic_year
from pyspark.sql.types import IntegerType, DateType

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from datetime import date, timedelta
from pyspark.sql.window import Window

class FutureRob(BaseJobRunner):
    def run_job(self, spark_session, glue_context) -> None:
        self.logger.info(f"[{FutureRob.__name__}] Starting Local Job ...")

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
        # end_date = date(start_date.year, 12, 31)
        filter_from_date = start_date + timedelta(days=1)

        self.logger.info(f"Start Date: {start_date}")
        # self.logger.info(f"End Date: {end_date}")
        self.logger.info(f"Filter From Date: {filter_from_date}")

        numbers_df = spark_session.range(0,366).withColumnRenamed("id","n")
        # call the method to calculate rob
        final_result = calculate_future_rob(fact_reservation_df, md_hotels_df, protel_reservation_df, source_segment_df,numbers_df, start_date, filter_from_date)
        final_result.to_csv("final_result.csv")
        # final_result.repartition(1).write.mode("overwrite").option("header", True).option("delimiter", ",").csv(final_output_path)


def calculate_future_rob(fact_reservation_df, md_hotels_df, protel_reservation_df, source_segment_df,numbers_df, start_date, filter_from_date) -> DataFrame:

    # ----------------------------
    # Step 2: Filter reservations loaded up to today
    # ----------------------------
    filtered_rsrv_curr_month = (
        protel_reservation_df
        .filter(F.col("load_datetime") <= F.current_date())
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
            "num_of_room_booked"
        )
    )

    # ----------------------------
    # Step 3: Rank reservations to get the latest per booking
    # ----------------------------
    window_spec = Window.partitionBy("bkg_num").orderBy(F.col("load_datetime").desc())
    ranked_rsrv_curr_month = filtered_rsrv_curr_month.withColumn("rnk",F.rank().over(window_spec))

    # ----------------------------
    # Step 4: Keep only latest reservations and adjust dates
    # ----------------------------
    dated_rsrv_curr_month = ranked_rsrv_curr_month.filter((F.col("rnk")==1) & (F.col("departure_dt") >= F.current_date())).\
    withColumn("arrival_dt_new",F.column("arrival_dt")).withColumn("departure_dt_new", F.column("departure_dt"))

    # ----------------------------
    # Step 5: Explode reservations into one row per stay date
    # ----------------------------
    # Calculate number of stay days
    dated_rsrv_curr_month = dated_rsrv_curr_month.withColumn(
        "stay_days",
        F.datediff(F.col("departure_dt_new"), F.col("arrival_dt_new"))
    )

    # Cross join with numbers and filter to stay_days
    rns_for_rsrv_curr_month = (
        dated_rsrv_curr_month.alias("drcm")
        .crossJoin(numbers_df.alias("n"))
        .filter(F.col("n.n") < F.col("drcm.stay_days"))
        # Optional joins
        .join(fact_reservation_df.alias("fr"),
            F.col("fr.source_rsrv_id") == F.col("drcm.bkg_num"),
            "left")
        .join(source_segment_df.alias("dss"),
            F.col("dss.src_sgmnt_id") == F.col("fr.src_sgmnt_id"),
            "left")
        .withColumn(
            "room_rate",
            F.coalesce(F.col("rm_rate").cast("decimal(10,2)"),
                    F.col("rm_rate_wo_split").cast("decimal(10,2)"))
        )
        .withColumn("stay_date", F.expr("date_add(arrival_dt_new, n)"))
        .filter(
            (F.col("reservation_status").isin(0,5)) &
            (~F.col("room_typ").isin(32,33)) &
            (~F.col("price_typ_grp").isin(
                19955,19956,23022,8899,17929,23170,23441,23442,23443,23444,
                23445,23446,23447,23448,23449,23450,23451,23455,23456,23508,681,121
            ))
        )
        .select(
            "hotel_id",
            "reservation_status",
            "bkg_num",
            "arrival_dt_new",
            "departure_dt_new",
            "source_rsrv_id",
            "src_sgmnt_id",
            "room_rate",
            "stay_date",
            "num_of_room_booked"
        )
    )

    # ----------------------------
    # Step 6: Aggregate to get rob_curr_month
    # ----------------------------
    rob_curr_month = (
        rns_for_rsrv_curr_month
        .groupBy("hotel_id","stay_date")
        .agg(
            F.sum("num_of_room_booked").alias("rob_curr_month"),
            F.sum(F.col("num_of_room_booked") * F.col("room_rate")).alias("room_revenue"),
            F.sum(F.col("num_of_room_booked") * F.datediff(F.col("departure_dt_new"),F.col("arrival_dt_new"))).alias("number_of_room_nights")
        )
    ) 

    # ----------------------------
    # Step 7: Filter final output for hotel_id = 61 and future dates
    # ----------------------------
    final_result = (
        rob_curr_month
        .filter((F.col("hotel_id")==61) & (F.col("stay_date") >= F.current_date()))
    )

    return final_result
