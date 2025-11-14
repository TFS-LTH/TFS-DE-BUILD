from com.lemontree.runners.rob_runners.rob_materialized_runner.rob_materialized import calculate_mat
from tests.com.lemontree.base.base_test import BaseTest
from datetime import date
from pathlib import Path
from pyspark.sql.functions import col
import pytest

class TestMatDynamicParams(BaseTest):

    @pytest.fixture(autouse=True)
    def setup_and_teardown(self, test_root_dir):
        # Setup
        print(f"[{self.__class__.__name__} setup] Preparing test resources")
        test_file_loc = test_root_dir / "test_files" / "ROB_materialized_test_files"

        self.fact_reservation = test_file_loc / "fact_hotel_tags.csv"
        self.md_hotels = test_file_loc / "md_hotels.csv"
        self.dim_source_segment = test_file_loc / "dim_source_segment.csv"

        # Dynamic filter params
        self.filter_date = date(2025, 11, 5)
        self.target_hotel_code = "AHRMB1"
        self.expected_rob = 665

        yield

        print(f"[{self.__class__.__name__} teardown] Cleaning up test resources\n{'#'*50}\n")

    def test_calculate_mat(self):
        fact_reservation_df = self.spark_session.read.format("csv").option("header", "true").load(str(self.fact_reservation))
        md_hotels_df = self.spark_session.read.format("csv").option("header", "true").load(str(self.md_hotels))
        dim_source_segment_df = self.spark_session.read.format("csv").option("header", "true").load(str(self.dim_source_segment))

        result_df = calculate_mat(
            fact_reservation_df,
            md_hotels_df,
            dim_source_segment_df,
            self.filter_date,
            self.target_hotel_code
        )

        test_rob = result_df.filter(col("as_of_date") == self.filter_date).select("ROB").collect()[0]["ROB"]
        assert test_rob == self.expected_rob
