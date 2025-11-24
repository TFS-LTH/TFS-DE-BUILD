from com.lemontree.runners.base.base_runner import BaseJobRunner
from com.lemontree.runners.mat.rob_materialized import calculate_mat
from com.lemontree.utils.utils_redshift import read_from_redshift
from com.lemontree.constants.redshift_tables import GOLD_FACT_HOTEL_TAGS, GOLD_DIM_SOURCE_SEGMENT, MD_HOTELS
from datetime import date, timedelta, datetime
from com.lemontree.utils.utils_helper_methods import run_crawler

class RobMaterializeBulk(BaseJobRunner):

    def run_job(self, spark_session, glue_context) -> None:
        self.logger.info(f"[{RobMaterializeBulk.__name__}] Starting Bulk MAT Job ...")

        # -----------------------------------------------------------
        # Step 0: Set destination and output path
        # -----------------------------------------------------------
        destination_bucket = self.config.get("destination_bucket")
        output_path = self.config.get("output_path")
        final_output_path = f"{destination_bucket}{output_path}"
        self.logger.info(f"final_output_path : {final_output_path}")

        # -----------------------------------------------------------
        # Step 1: Read source data from Redshift
        # -----------------------------------------------------------
        fact_hotel_tags_df = read_from_redshift(glue_context, table_name=GOLD_FACT_HOTEL_TAGS, query=None)
        md_hotels_df = read_from_redshift(glue_context, table_name=MD_HOTELS, query=None)
        dim_source_segment_df = read_from_redshift(glue_context, table_name=GOLD_DIM_SOURCE_SEGMENT, query=None)

        # -----------------------------------------------------------
        # Step 2: Determine date range for bulk processing
        # -----------------------------------------------------------
        if self.args.get("start_date").strip() == "":
            min_date = fact_hotel_tags_df.select(self.F.min("business_date")).first()[0]
        else:
            min_date = datetime.strptime(self.args.get("start_date"), "%Y-%m-%d")

        if self.args.get("end_date").strip() == "":
            max_date =  date.today() - timedelta(days=2)
        else:
            max_date = datetime.strptime(self.args.get("end_date"), "%Y-%m-%d")

        self.logger.info(f"Processing MAT from {min_date} to {max_date}")

        # -----------------------------------------------------------
        # Step 3: Loop over dates to calculate MAT
        # -----------------------------------------------------------
        current_date = min_date
        while current_date <= max_date:
            self.logger.info(f"Processing date: {current_date}")
            daily_result = calculate_mat(self,
                fact_hotel_tags_df,
                md_hotels_df,
                dim_source_segment_df,
                current_date
            )

            daily_result.repartition(self.config.get("partitions")).write.partitionBy("as_of_date") \
                .mode("append").parquet(final_output_path)

            current_date += timedelta(days=1)

        run_crawler(self.config.get("crawler_name"))

        self.logger.info(f"[{RobMaterializeBulk.__name__}] Bulk MAT Job Completed Successfully.")



