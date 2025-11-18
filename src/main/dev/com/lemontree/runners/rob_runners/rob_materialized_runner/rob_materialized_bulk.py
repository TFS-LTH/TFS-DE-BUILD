from com.lemontree.runners.base.base_runner import BaseJobRunner
from com.lemontree.utils.utils_redshift import read_from_redshift
from com.lemontree.constants.redshift_tables import GOLD_FACT_HOTEL_TAGS, GOLD_DIM_SOURCE_SEGMENT, MD_HOTELS
from datetime import date, timedelta

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
        min_date = fact_hotel_tags_df.select(self.F.min("business_date")).first()[0]
        max_date =  date.today() - timedelta(days=2)
        self.logger.info(f"Processing MAT from {min_date} to {max_date}")

        # -----------------------------------------------------------
        # Step 3: Loop over dates to calculate MAT
        # -----------------------------------------------------------
        current_date = min_date
        while current_date <= max_date:
            self.logger.info(f"Processing date: {current_date}")
            daily_result = calculate_mat_bulk(self,
                fact_hotel_tags_df,
                md_hotels_df,
                dim_source_segment_df,
                current_date
            )

            daily_result.repartition(1).write.partitionBy("as_of_date") \
                .mode("append").parquet(final_output_path)

            current_date += timedelta(days=1)

        self.logger.info(f"[{RobMaterializeBulk.__name__}] Bulk MAT Job Completed Successfully.")


def calculate_mat_bulk(self, fact_hotel_tags_df, md_hotels_df, dim_source_segment_df, current_date) -> BaseJobRunner.DataFrame:

    F = self.F
    # Convert business_date to date type
    fact_hotel_tags_df = fact_hotel_tags_df.withColumn("business_date", F.to_date("business_date", "dd-MM-yyyy")). \
        filter(F.col("business_date") == current_date)

    # Step 2: Join with dimension tables
    transformed_df = (
        fact_hotel_tags_df
        .join(md_hotels_df, "hotel_code", "left")
        .join(dim_source_segment_df, "src_sgmnt_id", "left")
        .select(
            F.col("business_date").alias("as_of_date"),
            F.col("hotel_id"),
            F.col("hotel_code"),
            F.col("no_of_rooms").alias("inventory"),
            F.col("source").alias("source_nm"),
            F.col("segment").alias("segment_nm"),
            F.col("room_nights"),
            F.col("room_revenue")
        )
    )

    # Step 3: Aggregate MAT metrics
    aggregated_df = transformed_df.groupBy(
         "as_of_date", "hotel_id", "hotel_code", "inventory","source_nm","segment_nm"
    ).agg(
        F.sum("room_nights").alias("ROB"),
        F.sum("room_revenue").alias("total_room_revenue")
    )

    return aggregated_df
