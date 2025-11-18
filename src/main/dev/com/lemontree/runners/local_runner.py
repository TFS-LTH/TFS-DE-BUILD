from com.lemontree.runners.base.base_runner import BaseJobRunner

class LocalJobRunner(BaseJobRunner):

    def run_job(self, spark_session, glue_context) -> None:
        self.logger.info(f"[{LocalJobRunner.__name__}] Starting Local Job ...")
        run_job_local_pipeline(spark_session, glue_context, self.config)


def run_job_local_pipeline(spark_session, glue_context, config):
    print("Running Job local pipeline...")
    db = config.get("db")
    table = config.get("table")
    print(f"Processing {db}.{table}")
    print(f'spark_session: {spark_session}')
    print(f'glue_context: {glue_context}')
    # ETL logic here
