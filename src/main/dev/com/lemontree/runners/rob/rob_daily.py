from com.lemontree.runners.base.base_runner import BaseJobRunner
from com.lemontree.utils.utils_redshift import read_from_redshift
from com.lemontree.constants.redshift_tables import GOLD_FACT_RESERVATIONS, MD_HOTELS, SILVER_PROTEL_RESERVATIONS, GOLD_DIM_SOURCE_SEGMENT
from com.lemontree.constants.constants import PRICE_GROUP_TYPES,ROOM_TYPES, RESERVATION_STATUS, RESERVATION_STATUS_MAPPING
from datetime import date
from com.lemontree.utils.utils_helper_methods import run_crawler
from rob_base import calculate_rob

class RobDaily(BaseJobRunner):
    def run_job(self, spark_session, glue_context) -> None:
        self.logger.info(f"[{RobDaily.__name__}] Starting Local Job ...")

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

        # call the method to calculate rob
        rob = calculate_rob(self, fact_reservation_df, md_hotels_df, protel_reservation_df, source_segment_df, start_date)
        final_result = rob.select(
            "as_of_date",
            "stay_date",
            "hotel_id",
            "hotel_code",
            "inventory",
            "source_nm",
            "segment_nm",
            "reservation_status",
            "room_revenue",
            "rob",
        )

        final_result.repartition(self.config.get("partitions")).write.partitionBy('as_of_date').\
            mode("append").parquet(final_output_path)

        run_crawler(self.config.get("crawler_name"))
