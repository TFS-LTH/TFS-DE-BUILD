from com.lemontree.runners.base.base_runner import BaseJobRunner
from com.lemontree.utils.utils_redshift import read_from_redshift
from com.lemontree.constants.redshift_tables import GOLD_FACT_RESERVATIONS, MD_HOTELS, SILVER_PROTEL_RESERVATIONS, GOLD_DIM_SOURCE_SEGMENT
from datetime import date, timedelta, datetime
from com.lemontree.runners.rob.rob_base import calculate_rob
from com.lemontree.utils.utils_helper_methods import run_crawler

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
        self.logger.info(f"Today's Date: {start_date}")

        protel_reservation_df = protel_reservation_df.withColumn("load_datetime", self.F.to_timestamp("load_datetime"))
        min_load_dt = protel_reservation_df.agg(
            self.F.min("load_datetime").alias("min_load_datetime")
        ).collect()[0]["min_load_datetime"]

        if self.args.get("start_date").strip() == "":
            min_date = min_load_dt.date()
        else:
            min_date = datetime.strptime(self.args.get("start_date"), "%Y-%m-%d")

        if self.args.get("end_date").strip() == "":
            today = date.today()
            end_date = today - timedelta(days=1)  # current_date - 1
        else:
            end_date = datetime.strptime(self.args.get("end_date"), "%Y-%m-%d")

        # end_date = min_date + timedelta(days=4)
        print("Running calculations from:", min_date, "to:", end_date)

        current = min_date
        while current <= end_date:
            print(f"Processing date: {current}")
            # call the method to calculate rob
            rob = calculate_rob(self, fact_reservation_df, md_hotels_df, protel_reservation_df, source_segment_df, current)

            rob \
                .repartition(self.config.get("partitions")) \
                .write \
                .partitionBy("as_of_date") \
                .mode("append") \
                .parquet(final_output_path)

            # Move to next date
            current += timedelta(days=1)

        run_crawler(self.config.get("crawler_name"))
