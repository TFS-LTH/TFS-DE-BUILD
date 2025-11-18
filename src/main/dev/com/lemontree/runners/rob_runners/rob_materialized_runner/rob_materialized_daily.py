from datetime import timedelta, date

from com.lemontree.runners.base.base_runner import BaseJobRunner
from com.lemontree.utils.utils_redshift import read_from_redshift
from com.lemontree.constants.redshift_tables import GOLD_FACT_HOTEL_TAGS, GOLD_DIM_SOURCE_SEGMENT, MD_HOTELS

class RobMaterializedDaily(BaseJobRunner):

    def run_job(self, spark_session, glue_context) -> None:
        self.logger.info(f"[{RobMaterializedDaily.__name__}] Starting Local Job ...")

        # -----------------------------------------------------------
        # Step 0: Set destination and output path
        # -----------------------------------------------------------
        destination_bucket = self.config.get("destination_bucket")
        output_path = self.config.get("output_path")
        final_output_path = f"{destination_bucket}{output_path}"
        print(f"final_output_path : {final_output_path}")

        # -----------------------------------------------------------
        # Step 1: Read source data from Redshift
        # -----------------------------------------------------------
        fact_hotel_tags_df = read_from_redshift(glue_context, table_name=GOLD_FACT_HOTEL_TAGS, query=None)
        md_hotels_df = read_from_redshift(glue_context, table_name=MD_HOTELS, query=None)
        dim_source_segment_df = read_from_redshift(glue_context, table_name=GOLD_DIM_SOURCE_SEGMENT, query=None)
        # -----------------------------------------------------------
        # Step 2: Read dynamic parameters from config
        # -----------------------------------------------------------
        filter_start_date = self.args.get("filter_start_date")
        target_hotel_code = self.args.get("target_hotel_code")

        if filter_start_date is None:
            filter_start_date = date.today()-timedelta(days=1)
        else:
            filter_start_date = filter_start_date.strip()
        self.logger.info(f"Filter Start Date: {filter_start_date}")
        self.logger.info(f"Target Hotel Code: {target_hotel_code}")

        # -----------------------------------------------------------
        # Step 3: Calculate MAT
        # -----------------------------------------------------------
        final_result = calculate_mat(self,
            fact_hotel_tags_df,
            md_hotels_df,
            dim_source_segment_df,
            filter_start_date
        )

        # -----------------------------------------------------------
        # Step 4: Write final output
        # -----------------------------------------------------------
        final_result.repartition(1).write.partitionBy('as_of_date', 'hotel_id'). \
            mode("append").option("header", True).\
            option("delimiter", ",").csv(final_output_path)
        self.logger.info(f"[{RobMaterializedDaily.__name__}] Job Completed Successfully.")


def calculate_mat( self, fact_hotel_tags_df, md_hotels_df, dim_source_segment_df, filter_start_date) -> BaseJobRunner.DataFrame:
    """
    MAT calculation logic with dynamic filters.
    """
    F = self.F


    # Ensure business_date is date type
    fact_hotel_tags_df = fact_hotel_tags_df.\
        withColumn("business_date", F.to_date("business_date", "dd-MM-yyyy")).\
        filter(F.col("business_date") == filter_start_date)

    # Step 2: Join with dimension tables
    # And Step 3 transform column
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

    # Step 4: Aggregate
    aggregated_df = transformed_df.groupBy(
        "as_of_date","hotel_id", "hotel_code", "inventory","source_nm","segment_nm"
    ).agg(
        F.sum("room_nights").alias("ROB"),
        F.sum("room_revenue").alias("total_room_revenue")
    )

    # Step 5: Sort
    final_result = aggregated_df.orderBy("as_of_date")
    return final_result
