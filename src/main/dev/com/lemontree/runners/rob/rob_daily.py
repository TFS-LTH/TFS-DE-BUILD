from datetime import date

from com.lemontree.constants.redshift_tables import (
    GOLD_DIM_SOURCE_SEGMENT,
    GOLD_FACT_RESERVATIONS,
    MD_HOTELS,
    SILVER_PROTEL_RESERVATIONS,
)
from com.lemontree.runners.base.base_runner import BaseJobRunner
from com.lemontree.runners.rob.rob_base import calculate_rob
from com.lemontree.utils.utils_helper_methods import run_crawler
from com.lemontree.utils.utils_redshift import read_from_redshift


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
        rob = calculate_rob(
            self,
            fact_reservation_df,
            md_hotels_df,
            protel_reservation_df,
            source_segment_df,
            start_date,
        )

        rob.repartition(self.config.get("partitions")).write.partitionBy("as_of_date").mode("append").parquet(
            final_output_path
        )

        run_crawler(self.config.get("crawler_name"))
