from com.lemontree.runners.local_runner import LocalJobRunner
from com.lemontree.runners.pnl.aop_runner import AOPRunner
from com.lemontree.runners.pnl.ltr_preprocessing_runner import LtrPreprocessingRunner
from com.lemontree.runners.pnl.percentage_fee_runner import PercentageFeeRunner
from com.lemontree.runners.pnl.tb_actual_runner import TbActualRunner
from com.lemontree.runners.pnl.operational_data_runner import OperationalDataRunner
from com.lemontree.runners.rob.rob_runner import RobRunner

# Map job_name to runner class
runners_map = {
    "local": LocalJobRunner,
    "rob": RobRunner,

    #===============================
    # PNL runners
    #================================
    "percentage_fee": PercentageFeeRunner,
    "ltr": LtrPreprocessingRunner,
    "tb": TbActualRunner,
    "operational_data": OperationalDataRunner,
    "aop": AOPRunner,
    # TODO: Add more here
}