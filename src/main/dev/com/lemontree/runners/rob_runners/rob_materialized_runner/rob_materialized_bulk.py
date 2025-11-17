from com.lemontree.runners.base.base_runner import BaseJobRunner
from com.lemontree.utils.utils_redshift import read_from_redshift
from com.lemontree.constants.redshift_tables import GOLD_FACT_HOTEL_TAGS, GOLD_DIM_SOURCE_SEGMENT, MD_HOTELS

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
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
        min_date = fact_hotel_tags_df.agg(F.min("business_date").alias("min_dt")).collect()[0]["min_dt"]
        max_date = date.today() - timedelta(days=1)  # yesterday
        self.logger.info(f"Processing MAT from {min_date} to {max_date}")

        # -----------------------------------------------------------
        # Step 3: Loop over dates to calculate MAT
        # -----------------------------------------------------------
        all_results_df = None
        current_date = min_date
        while current_date <= max_date:
            self.logger.info(f"Processing date: {current_date}")
            daily_result = calculate_mat_bulk(
                fact_hotel_tags_df,
                md_hotels_df,
                dim_source_segment_df,
                current_date
            )
            if all_results_df is None:
                all_results_df = daily_result
            else:
                all_results_df = all_results_df.unionByName(daily_result)

            current_date += timedelta(days=1)

        # -----------------------------------------------------------
        # Step 4: Write final output
        # -----------------------------------------------------------
        all_results_df.repartition(1).write.partitionBy("as_of_date", "hotel_id") \
            .mode("overwrite").option("header", True).option("delimiter", ",").csv(final_output_path)
        self.logger.info(f"[{RobMaterializeBulk.__name__}] Bulk MAT Job Completed Successfully.")


def calculate_mat_bulk(fact_hotel_tags_df: DataFrame, md_hotels_df: DataFrame, dim_source_segment_df: DataFrame,
                       as_of_date: date) -> DataFrame:
    """
    Calculate MAT for a single date (as_of_date)
    """
    # Convert business_date to date type
    fact_hotel_tags_df = fact_hotel_tags_df.withColumn("business_date", F.to_date("business_date"))

    # Step 1: Filter by date
    filtered_df = fact_hotel_tags_df.filter(F.col("business_date") == F.lit(as_of_date))

    # Step 2: Join with dimension tables
    transformed_df = (
        filtered_df
        .join(md_hotels_df, "hotel_code", "left")
        .join(dim_source_segment_df, "src_sgmnt_id", "left")
        .select(
            F.lit(as_of_date).alias("as_of_date"),
            "hotel_id",
            "hotel_code",
            F.col("no_of_rooms").alias("inventory"),
            F.col("source").alias("source_nm"),
            F.col("segment").alias("segment_nm"),
            "room_nights",
            "room_revenue"
        )
    )

    # Step 3: Aggregate MAT metrics
    aggregated_df = transformed_df.groupBy(
        "as_of_date", "hotel_id", "hotel_code", "inventory"
    ).agg(
        F.sum("room_nights").alias("ROB"),
        F.sum("room_revenue").alias("total_room_revenue")
    )

    return aggregated_df
