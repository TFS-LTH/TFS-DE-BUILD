from com.lemontree.runners.rob_daily import calculate_future_rob
from com.lemontree.runners.rob_bulk import calculate_future_rob_backdated_bulk
from tests.com.lemontree.base.base_test import BaseTest
from datetime import date, timedelta
from pathlib import Path
from pyspark.sql.functions import *
import pytest
from pyspark.sql import functions as F

class TestRobFromCurrentDtToFuture(BaseTest):

    @pytest.fixture(autouse=True)
    def setup_and_teardown(self, test_root_dir):
        # Setup before each test
        print(f"[{self.__class__.__name__} setup] Preparing test resources")

        test_file_loc = test_root_dir / "test_files" / "rob_test_files"

        self.fact_reservation = test_file_loc / "fact_reservations_future_rob.csv"
        self.md_mapping =  test_file_loc / "md_hotels_future_rob.csv"
        self.protel_reservation = test_file_loc / "protel_reservations_future_rob.csv"
        self.source_segment_df = test_file_loc / "source_segment_future_rob.csv"


        self.start_date = date(2025, 11, 29)
        self.expected_rob = 23

        print(f"Test Future ROB Using input fact_reservation file path: {self.fact_reservation}")
        print(f"Test Future ROB Using input md_mapping file path: {self.md_mapping}")
        print(f"Test Future ROB Using input protel reservation file path: {self.protel_reservation}")
        print(f"Test Future ROB Using input source segment file path: {self.source_segment_df}")
        print(f"Test Future ROB Using start_date : {self.start_date}")

        #########################################################
        yield  # Control passes to the test here
        #########################################################

        # Teardown after each test to clean up all the directories created during the run-time
        print(f"[{self.__class__.__name__} teardown] Cleaning up test resources")

        print('################################################')
        print('')

    def test_calculate_rob(self):

        fact_reservation_df = self.spark_session.read.format("csv").option("header", "true").load(str(Path(self.fact_reservation)))
        md_hotels_df = self.spark_session.read.format("csv").option("header", "true").load(str(Path(self.md_mapping)))
        protel_reservation_df = self.spark_session.read.format("csv").option("header","true").load(str(Path(self.protel_reservation)))
        source_segment_df = self.spark_session.read.format("csv").option("header","true").load(str(Path(self.source_segment_df)))

        # get rob using sample data
        result = calculate_future_rob(fact_reservation_df, md_hotels_df,protel_reservation_df,source_segment_df,self.start_date)

        test_rob_sum = result.filter((col("stay_date") == self.start_date) & (col("hotel_id") == 27)\
            ).agg(F.sum("rob").alias("total_rob")).first()["total_rob"]

        # check if the actual value is same as that of test data
        assert int(test_rob_sum) == self.expected_rob


    def test_calculate_rob_backdated_bulk(self):
        fact_reservation_df = self.spark_session.read.format("csv").option("header", "true").load(
            str(Path(self.fact_reservation)))
        md_hotels_df = self.spark_session.read.format("csv").option("header", "true").load(str(Path(self.md_mapping)))
        protel_reservation_df = self.spark_session.read.format("csv").option("header","true").load(str(Path(self.protel_reservation)))
        source_segment_df = self.spark_session.read.format("csv").option("header","true").load(str(Path(self.source_segment_df)))

        # get rob using sample data
        result = calculate_future_rob_backdated_bulk(fact_reservation_df, md_hotels_df, protel_reservation_df, source_segment_df,
                                      self.start_date)

        assert result.count() > 0



