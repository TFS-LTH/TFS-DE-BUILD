from typing_extensions import override
from datetime import date
from pathlib import Path
from pyspark.sql.functions import col
import pytest

from com.lemontree.runners.rob_runners.rob_materialized_runner.rob_materialized_bulk import calculate_mat_bulk
from com.lemontree.utils.utils_helper_methods import delete_directory
from tests.com.lemontree.base.base_test import BaseTest


class TestMatBulk(BaseTest):

    @pytest.fixture(autouse=True)
    def setup_and_teardown(self, test_root_dir):
        # Setup
        print(f"[{self.__class__.__name__} setup] Preparing test resources")
        test_file_loc = test_root_dir / "test_files" / "ROB_materialized_test_files"

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
        fact_data = [
            ("AHRMB1", "2025-11-05", 669, 665, 6400587.99, 1),
        ]
        fact_columns = ["hotel_code", "business_date", "no_of_rooms", "room_nights", "room_revenue", "src_sgmnt_id"]
        fact_reservation_df = self.spark_session.createDataFrame(fact_data, fact_columns)

        md_data = [("AHRMB1", 101)]
        md_columns = ["hotel_code", "hotel_id"]
        md_hotels_df = self.spark_session.createDataFrame(md_data, md_columns)

        segment_data = [(1, "OTA", "Leisure")]
        segment_columns = ["src_sgmnt_id", "source", "segment"]
        dim_source_segment_df = self.spark_session.createDataFrame(segment_data, segment_columns)

        # ---------------------------
        # Run MAT calculation
        # ---------------------------
        result_df = calculate_mat_bulk(
            fact_reservation_df,
            md_hotels_df,
            dim_source_segment_df,
            self.filter_date
        )

        # ---------------------------
        # Optional: Write output to CSV
        # ---------------------------
        output_path = str(Path(self.test_config["output_path"]))
        print("output_path =", output_path)
        result_df.repartition(1).write \
            .mode("overwrite") \
            .partitionBy('as_of_date', 'hotel_id') \
            .option("header", True) \
            .csv(output_path)

        # ---------------------------
        # Validate results
        # ---------------------------
        row = result_df.filter(col("as_of_date") == self.filter_date).collect()[0]
        assert row["ROB"] == self.expected_rob
        assert abs(row["total_room_revenue"] - self.expected_total_room_revenue) < 0.01
