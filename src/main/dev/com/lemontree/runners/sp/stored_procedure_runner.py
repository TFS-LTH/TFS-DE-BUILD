from com.lemontree.runners.base.base_runner import BaseJobRunner
import os
import redshift_connector

class StoredProcedureTest(BaseJobRunner):
    def run_job(self, spark_session, glue_context) -> None:
        self.logger.info(f"[{StoredProcedureTest.__name__}] Starting Stored Procedure Job ...")

        # === Locate your SQL file from your local package ===
        # Get path of the current script
        SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
        # Move up two levels to get to the 'lemontree' directory
        ROOT_DIR = os.path.abspath(os.path.join(SCRIPT_DIR, '..', '..'))
        # Construct the full path to the .sql file
        SQL_FILE = os.path.join(ROOT_DIR, 'stored_procedures', 'sp_create_and_insert_users.sql')

        print("Looking for SQL at:", SQL_FILE)

        # === Read SQL from file ===
        with open(SQL_FILE, 'r') as f:
            sql = f.read()

        # Connect to Redshift and execute
        conn = redshift_connector.connect(
            host='ltree-redshift-prod-01.cjwibd5rhd7o.ap-south-1.redshift.amazonaws.com',
            port=5439,
            database='tfs_test',
            user='glue_dwh',
            password='btIbxGT3RwuyCPE'
        )
        cur = conn.cursor()

        try:
            cur.execute(sql)
            conn.commit()
            print("Stored procedure executed successfully.")
        except Exception as e:
            print(f"Error: {e}")
            conn.rollback()

        cur.close()
        conn.close()