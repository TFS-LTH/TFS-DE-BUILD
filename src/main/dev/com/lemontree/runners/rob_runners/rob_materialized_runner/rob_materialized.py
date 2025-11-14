from com.lemontree.runners.base.base_runner import BaseJobRunner
from com.lemontree.utils.utils_redshift import read_from_redshift
from com.lemontree.constants.redshift_tables import GOLD_FACT_HOTEL_TAGS, GOLD_DIM_SOURCE_SEGMENT, MD_HOTELS

from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from datetime import date


class Mat(BaseJobRunner):

    def run_job(self, spark_session, glue_context) -> None:
        self.logger.info(f"[{Mat.__name__}] Starting Local Job ...")

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
        # Step 2: Read dynamic parameters from config
        # -----------------------------------------------------------
        filter_start_date = self.config.get("filter_start_date")  # e.g., "2025-10-01"
        target_hotel_code = self.config.get("target_hotel_code")  # optional

        # Convert filter_start_date to date object if string
        if isinstance(filter_start_date, str):
            filter_start_date = date.fromisoformat(filter_start_date)

        self.logger.info(f"Filter Start Date: {filter_start_date}")
        self.logger.info(f"Target Hotel Code: {target_hotel_code}")

        # -----------------------------------------------------------
        # Step 3: Calculate MAT
        # -----------------------------------------------------------
        final_result = calculate_mat(
            fact_hotel_tags_df,
            md_hotels_df,
            dim_source_segment_df,
            filter_start_date,
            target_hotel_code
        )

        # -----------------------------------------------------------
        # Step 4: Write final output
        # -----------------------------------------------------------
        final_result.repartition(1).write.partitionBy('', ''). \
            mode("overwrite").option("header", True).\
            option("delimiter", ",").csv(final_output_path)
        self.logger.info(f"[{Mat.__name__}] Job Completed Successfully.")


def calculate_mat(
    fact_hotel_tags_df: DataFrame,
    md_hotels_df: DataFrame,
    dim_source_segment_df: DataFrame,
    filter_start_date: date,
    target_hotel_code: str
) -> DataFrame:
    """
    MAT calculation logic with dynamic filters.
    """

    # Ensure business_date is date type
    fact_hotel_tags_df = fact_hotel_tags_df.withColumn("business_date", to_date("business_date", "yyyy-MM-dd"))

    # Step 1: Apply filters
    filtered_df = fact_hotel_tags_df.filter(col("business_date") == lit(filter_start_date))
    if target_hotel_code:
        filtered_df = filtered_df.filter(col("hotel_code") == lit(target_hotel_code))

    # Step 2: Join with dimension tables
    # And Step 3 transform column
    transformed_df = (
        filtered_df
        .join(md_hotels_df, "hotel_code", "left")
        .join(dim_source_segment_df, "src_sgmnt_id", "left")
        .select(
            col("business_date").alias("as_of_date"),
            col("hotel_id"),
            col("hotel_code"),
            col("no_of_rooms").alias("inventory"),
            col("source").alias("source_nm"),
            col("segment").alias("segment_nm"),
            col("room_nights"),
            col("room_revenue")
        )
    )

    # Step 4: Aggregate
    aggregated_df = transformed_df.groupBy(
        "as_of_date", "hotel_code", "inventory"
    ).agg(
        sum("room_nights").alias("ROB"),
        sum("room_revenue").alias("total_room_revenue")
    )

    # Step 5: Sort
    final_result = aggregated_df.orderBy("as_of_date")
    return final_result
