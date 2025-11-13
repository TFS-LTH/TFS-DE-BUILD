from com.lemontree.runners.base.base_runner import BaseJobRunner
from com.lemontree.utils.utils_redshift import read_from_redshift
from com.lemontree.constants.redshift_tables import GOLD_FACT_RESERVATIONS, MD_HOTELS, DIM_SOURCE_SEGMENT
from com.lemontree.utils.utils_helper_methods import calculate_week_number_dynamic_year

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DateType
from pyspark.sql import DataFrame
from datetime import date, timedelta


class Rob(BaseJobRunner):

    def run_job(self, spark_session, glue_context) -> None:
        """
        Main job entry point for ROB (Room Nights & Room Revenue aggregation).
        """
        self.logger.info(f"[{Rob.__name__}] Starting Local Job ...")

        # -------------------------------
        # STEP 1: Read data from Redshift
        # -------------------------------
        self.logger.info("Reading data from Redshift tables...")

        fact_hotel_tags_df = read_from_redshift(
            spark_session, GOLD_FACT_RESERVATIONS, self.logger
        )

        hotels_df = read_from_redshift(
            spark_session, MD_HOTELS, self.logger
        )

        dim_source_segment_df = read_from_redshift(
            spark_session, DIM_SOURCE_SEGMENT, self.logger
        )

        # -------------------------------
        # STEP 2: Filter and Join Data
        # -------------------------------
        self.logger.info("Filtering and joining data...")

        total_hotel_mat = (
            fact_hotel_tags_df.alias("a")
            .join(hotels_df.alias("b"), F.col("a.hotel_code") == F.col("b.hotel_code"), "left")
            .join(dim_source_segment_df.alias("c"), F.col("a.src_sgmnt_id") == F.col("c.src_sgmnt_id"), "left")
            .filter(
                (F.col("a.business_date") >= F.lit("2025-10-01"))
                & (F.col("a.hotel_code") == F.lit("AHRMB1"))
            )
            .select(
                F.col("a.business_date").alias("as_of_date"),
                F.col("b.hotel_id"),
                F.when(F.col("a.hotel_code") == "RFHDL1", "LTHDL")
                 .when(F.col("a.hotel_code") == "KSHPP", "KPHPN")
                 .otherwise(F.col("a.hotel_code"))
                 .alias("hotel_code"),
                F.col("b.no_of_rooms").alias("inventory"),
                F.col("c.source").alias("source_nm"),
                F.col("c.segment").alias("segment_nm"),
                F.col("a.room_nights"),
                F.col("a.room_revenue")
            )
        )

        # -------------------------------
        # STEP 3: Aggregate Results
        # -------------------------------
        self.logger.info("Aggregating ROB and room revenue...")

        final_df = (
            total_hotel_mat
            .groupBy("as_of_date", "hotel_code", "inventory")
            .agg(
                F.sum("room_nights").alias("ROB"),
                F.sum("room_revenue").alias("total_room_revenue")
            )
            .orderBy("as_of_date")
        )

        # -------------------------------
        # STEP 4: Output or Save Results
        # -------------------------------
        self.logger.info("Job completed successfully. Displaying results sample:")
        final_df.show(10, truncate=False)

        # Optionally, write to Redshift, S3, or another sink:
        # self.write_to_redshift(final_df, "rob_results_table")

        self.logger.info(f"[{Rob.__name__}] Job Completed Successfully.")
