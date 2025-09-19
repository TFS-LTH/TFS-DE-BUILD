import logging
from abc import ABC, abstractmethod
from typing import Dict, Any
from com.lemontree.utils.utils_helper_methods import *
from com.lemontree.utils.utils_get_context import init_context

class BaseJobRunner(ABC):
    glue_context = None
    spark_session = None

    def __init__(self, args: Dict[str, Any]):
        self.args = args
        self.logger = logging.getLogger(self.__class__.__name__)

        # get job name
        self.job_name = self.args.get("job_name")
        if not self.job_name:
            raise ValueError("Missing required argument: 'job_name'")

        # load configs for the job
        self.config = load_config_for_job(self.job_name)

        # initialize spark_session and glue_context in parent class so that they are available to the child classes
        self.glue_context, self.spark_session = init_context(BaseJobRunner.__name__, self.config)

    def execute(self):
        self.run_job(self.spark_session, self.glue_context, self.args)

    @abstractmethod
    def run_job(self, spark_session, glue_context, args) -> None:
        pass
