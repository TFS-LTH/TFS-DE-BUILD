from com.lemontree.runners.base.base_runner import BaseJobRunner
import os
import pkgutil
import redshift_connector

class StoredProcedureTest(BaseJobRunner):
    def run_job(self, spark_session, glue_context) -> None:
        self.logger.info(f"[{StoredProcedureTest.__name__}] Starting Stored Procedure Job ...")
        #
        # path_in_package = 'stored_procedures/sp_create_and_insert_users.sql'
        # sql_bytes = pkgutil.get_data('com.lemontree', path_in_package)
        # if not sql_bytes:
        #     print(f'Loaded error from {path_in_package}')
        #     raise FileNotFoundError(f"Could not find {path_in_package}")
        #
        # # Convert bytes to string (assuming UTF-8 encoding)
        # sql_str = sql_bytes.decode('utf-8')
        # print(sql_str)
        #
        # # Connect to Redshift and execute
        # conn = redshift_connector.connect(
        #     host='ltree-redshift-prod-01.cjwibd5rhd7o.ap-south-1.redshift.amazonaws.com',
        #     port=5439,
        #     database='tfs_test',
        #     user='glue_dwh',
        #     password='btIbxGT3RwuyCPE'
        # )
        # cur = conn.cursor()
        #
        # try:
        #     # Step 1: Create or replace the stored procedure
        #     cur.execute(sql_str)
        #     conn.commit()
        #     print("Stored procedure stored successfully.")
        #
        #     # Step 2: Call the stored procedure to run it
        #     cur.execute("CALL sp_create_and_insert_users();")  # <-- call your stored procedure
        #     conn.commit()
        #     print("Stored procedure executed successfully.")
        #
        # except Exception as e:
        #     print(f"Error: {e}")
        #     conn.rollback()
        #
        # cur.close()
        # conn.close()