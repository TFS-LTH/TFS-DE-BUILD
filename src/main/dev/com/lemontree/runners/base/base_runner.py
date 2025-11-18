from abc import ABC, abstractmethod
from typing import Dict, Any
from com.lemontree.utils.utils_helper_methods import *
from com.lemontree.configs.common_imports import F, T, DataFrame, W

class BaseJobRunner(ABC):

    glue_context = None
    spark_session = None

    F = F
    T = T
    DataFrame = DataFrame
    W = W

    def __init__(self, args: Dict[str, Any]):
        self.args = args
        self.logger = logging.getLogger(self.__class__.__name__)

        # get job name
        self.job_name = self.args.get("job_name")
        if not self.job_name:
            raise ValueError("Missing required argument: 'job_name'")

        # load configs for the job
        self.config = load_config_for_job(self.job_name)

        if self.config.get("job_type") is None:
            raise ValueError("Missing required argument in properties.yaml: 'job_type'")

        # initialize spark_session and glue_context in parent class for spark jobs so that they are available to the child classes
        if str(self.config.get("job_type")).lower().strip() == "spark":
            from com.lemontree.utils.utils_get_context import init_context
            self.glue_context, self.spark_session = init_context(BaseJobRunner.__name__, self.config)
        else:
            self.glue_context = None
            self.spark_session = None

    def execute(self):
        self.run_job(self.spark_session, self.glue_context)

    def not_implemented(self):
        raise NotImplementedError()

    @abstractmethod
    def run_job(self, spark_session, glue_context) -> None:
        pass
