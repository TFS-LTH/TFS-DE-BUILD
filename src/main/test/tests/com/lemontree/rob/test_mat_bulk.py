from datetime import date
from pathlib import Path
from pyspark.sql.functions import col
import pytest
from com.lemontree.runners.rob_runners.rob_materialized_runner.rob_materialized_bulk import calculate_mat_bulk
from com.lemontree.utils.utils_helper_methods import delete_directory
from tests.com.lemontree.base.base_test import BaseTest
from pyspark.sql import functions as F

class TestMatBulk(BaseTest):

    @pytest.fixture(autouse=True)
    def setup_and_teardown(self, test_root_dir):
        # Setup
        print(f"[{self.__class__.__name__} setup] Preparing test resources")
        test_file_loc = test_root_dir / "test_files" / "rob_materialized_test_files"

        # These CSV files can be used if you want file-based tests
        self.fact_reservation = test_file_loc / "fact_hotel_tags.csv"
        self.md_hotels = test_file_loc / "md_hotels.csv"
        self.dim_source_segment = test_file_loc / "dim_source_segment.csv"

        # Dynamic filter params
        self.filter_date = date(2025, 11, 5)
        self.target_hotel_code = "AHRMB1"
        self.expected_rob = 665  # Sum of room_nights for that date
        self.expected_total_room_revenue = 6400587.99  # Sum of room_revenue

        self.test_config = {
            "output_path": r"test_outputs\rob_materialized_bulk"
        }

        yield

        # Teardown
        print(f"[{self.__class__.__name__} teardown] Cleaning up test resources\n{'#'*50}\n")
        delete_directory(self.test_config["output_path"])


    def test_calculate_mat_bulk(self):
        # ---------------------------
        # Create mock DataFrames
        # ---------------------------

        fact_reservation_df = self.spark_session.read.format("csv").option("header", "true").load(str(self.fact_reservation))
        md_hotels_df = self.spark_session.read.format("csv").option("header", "true").load(str(self.md_hotels))
        dim_source_segment_df = self.spark_session.read.format("csv").option("header", "true").load(str(self.dim_source_segment))


        # ---------------------------
        # Run MAT calculation
        # ---------------------------
        result_df = calculate_mat_bulk(self,
            fact_reservation_df,
            md_hotels_df,
            dim_source_segment_df, self.filter_date      )

        output_path = str(Path(self.test_config["output_path"]))
        print("output_path =", output_path)

        (result_df.repartition(1).write.partitionBy("as_of_date")
            .mode("append").
         option("header", True).
         option("delimiter", ",").csv(output_path))

        # ---------------------------
        # Validate results
        # ---------------------------

        test_rob_sum = (
            result_df
            .filter(
                (col("as_of_date") == F.lit(self.filter_date).cast("date")) &
                (col("hotel_code") == self.target_hotel_code)
            )
            .groupBy("as_of_date", "hotel_code")
            .agg(F.sum("ROB").alias("rob_sum"))
            .collect()[0]["rob_sum"]
        )

        print(test_rob_sum)
        assert test_rob_sum == self.expected_rob