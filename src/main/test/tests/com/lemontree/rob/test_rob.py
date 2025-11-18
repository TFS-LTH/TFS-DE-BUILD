from com.lemontree.runners.rob.rob_runner import calculate_rob
from tests.com.lemontree.base.base_test import BaseTest
from datetime import date, timedelta
from pathlib import Path
from pyspark.sql.functions import *
import pytest

class TestRobFromCurrentDtToYearEnd(BaseTest):

    @pytest.fixture(autouse=True)
    def setup_and_teardown(self, test_root_dir):
        # Setup before each test
        print(f"[{self.__class__.__name__} setup] Preparing test resources")

        test_file_loc = test_root_dir / "test_files" / "rob_test_files"

        self.fact_reservation = test_file_loc / "fact_reservations.csv"
        self.md_mapping =  test_file_loc / "md_hotels.csv"

        self.start_date = date(2025, 9, 17)
        self.end_date = date(self.start_date.year, 12, 31)
        self.filter_from_date = self.start_date + timedelta(days=1)
        self.expected_rob = 9

        print(f"Test ROB Using input fact_reservation file path: {self.fact_reservation}")
        print(f"Test ROB Using input md_mapping file path: {self.md_mapping}")
        print(f"Test ROB Using start_date : {self.start_date}")
        print(f"Test ROB Using end_date : {self.end_date}")
        print(f"Test ROB Using filter_from_date : {self.filter_from_date}")

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

        # get rob using sample data
        result = calculate_rob(self, fact_reservation_df, md_hotels_df, self.start_date, self.end_date, self.filter_from_date)
        test_rob = result.filter((col("Day of Stay") == self.start_date) & (col("rsrv_status") == "confirmed")).select("rob").collect()[0]["rob"]
        # check if the actual value is same as that of test data
        assert test_rob == self.expected_rob