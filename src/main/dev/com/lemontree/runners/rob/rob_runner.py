# from com.lemontree.runners.base.base_runner import BaseJobRunner
# from com.lemontree.utils.utils_redshift import read_from_redshift
# from com.lemontree.constants.redshift_tables import GOLD_FACT_RESERVATIONS, MD_HOTELS
# from com.lemontree.utils.utils_helper_methods import calculate_week_number_dynamic_year
# from datetime import date, timedelta
# from pyspark.sql.types import IntegerType, DateType
# from pyspark.sql.functions import *
# from pyspark.sql import DataFrame
#
# class Rob(BaseJobRunner):
#     def run_job(self, spark_session, glue_context) -> None:
#         self.logger.info(f"[{Rob.__name__}] Starting Local Job ...")
#
#         destination_bucket = self.config.get("destination_bucket")
#         output_path = self.config.get("output_path")
#         final_output_path = f"{destination_bucket}{output_path}"
#         self.logger.info(f"final_output_path : {final_output_path}")
#
#         # -----------------------------------------------------------
#         # Step 0: Set dynamic date parameters and reading data
#         # -----------------------------------------------------------
#
#         fact_reservation_df = read_from_redshift(glue_context, table_name=GOLD_FACT_RESERVATIONS, query=None)
#         md_hotels_df = read_from_redshift(glue_context, table_name=MD_HOTELS, query=None)
#
#         start_date = date.today()
#         end_date = date(start_date.year, 12, 31)
#         filter_from_date = start_date + timedelta(days=1)
#
#         self.logger.info(f"Start Date: {start_date}")
#         self.logger.info(f"End Date: {end_date}")
#         self.logger.info(f"Filter From Date: {filter_from_date}")
#
#         # call the method to calculate rob
#         final_result = calculate_rob(fact_reservation_df, md_hotels_df, start_date, end_date, filter_from_date)
#         final_result.repartition(1).write.mode("overwrite").option("header", True).option("delimiter", ",").csv(final_output_path)
#
#
# def calculate_rob(fact_reservation_df, md_hotels_df, start_date, end_date, filter_from_date) -> DataFrame:
#
#     # --------------------------------------
#     # Step 1: Filter fact_rsrv DataFrame
#     # --------------------------------------
#     filtered_reservations = fact_reservation_df. \
#         select(
#         "hotel_id",
#         "sk_bkg_id",
#         "src_sys_bkg_id",
#         "room_bkd_cnt",
#         when(col("rsrv_status") != "confirmed", "tentative").otherwise("confirmed").alias("rsrv_status"),
#         "rsrv_frm_dt",
#         "rsrv_to_dt",
#         "room_rt"
#     ). \
#         filter(
#         (col("rsrv_to_dt") >= lit(filter_from_date)) &
#         (col("rsrv_frm_dt") <= lit(end_date)) &
#         (col("sourcefile") == "buch") &
#         (~col("room_typ").isin(32, 33)) &
#         (~col("prc_typ_grp").isin(
#             19955, 19956, 23022, 8899, 17929, 23170, 23441, 23442, 23443, 23444,
#             23445, 23446, 23447, 23448, 23449, 23450, 23451, 23455, 23456, 23508,
#             681, 121
#         ))
#     ).withColumn(
#         "room_rvnu", (col("room_bkd_cnt").cast(IntegerType()) * col("room_rt").cast(IntegerType())) * abs(datediff(col("rsrv_to_dt"), col("rsrv_frm_dt")))
#     ).drop("room_rt")
#
#     # ------------------------------------------------------------------------
#     # Step 2: Join with md_hotels DataFrame to get hotel_cd & inventories
#     # -------------------------------------------------------------------------
#     joined_reservations = filtered_reservations.join(md_hotels_df, on="hotel_id", how="left")
#
#     # --------------------------------------
#     # Step 3: Adjust reservation dates to stay within the date window
#     # --------------------------------------
#     adjusted_reservations = joined_reservations.withColumn(
#         "rsrv_frm_dt_new",
#         when(col("rsrv_frm_dt") < lit(start_date), lit(start_date))
#         .otherwise(col("rsrv_frm_dt"))
#     ).withColumn(
#         "rsrv_to_dt_new",
#         when(col("rsrv_to_dt") > lit(end_date), lit(end_date))
#         .otherwise(col("rsrv_to_dt"))
#     )
#
#
#     # --------------------------------------
#     # Step 4: Generate one row per night of stay
#     # --------------------------------------
#     exploded_reservations = adjusted_reservations.withColumn(
#         "stay_night",
#         explode(
#             sequence(col("rsrv_frm_dt_new").cast(DateType()), col("rsrv_to_dt_new").cast(DateType()) - expr("INTERVAL 1 day"))
#         )
#     )
#
#     # --------------------------------------
#     # Step 5: Aggregate functions
#     # --------------------------------------
#
#     aggregated_result = exploded_reservations. \
#         groupBy(
#         col("hotel_id"), col("hotel_code"), col("stay_night"), col("no_of_rooms"), col("rsrv_status")
#     ).agg(
#         sum("room_bkd_cnt").alias("rob"),
#         sum("room_rvnu").alias("gross_room_revenue")
#     )
#
#     # --------------------------------------
#     # Step 6: Preparing the final format
#     # --------------------------------------
#
#     week_number_udf = udf(calculate_week_number_dynamic_year, IntegerType())
#
#     final_result = aggregated_result. \
#         withColumn("DOW", date_format(col("stay_night"), "EEEE")). \
#         withColumn(
#         "Occ%",
#         when(col("no_of_rooms") != 0, round((col("rob") / col("no_of_rooms")) * 100, 2)).otherwise(0)
#     ). \
#         withColumn(
#         "ARR", round(col("gross_room_revenue") / col("rob"), 2)
#     ). \
#         withColumn("FY week number", week_number_udf(col("stay_night"))). \
#     select(
#         col("hotel_id"),
#         col("hotel_code"),
#         col("FY week number"),
#         col("stay_night").alias("Day of Stay"),
#         col("rsrv_status"),
#         col("DOW"),
#         col("no_of_rooms").alias("Total_inventory"),
#         col("Occ%"),
#         col("rob"),
#         col("ARR")
#     ). \
#         orderBy("stay_night")
#
#     return final_result
