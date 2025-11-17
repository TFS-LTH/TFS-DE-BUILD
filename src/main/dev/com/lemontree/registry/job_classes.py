from com.lemontree.runners.local_runner import LocalJobRunner
from com.lemontree.runners.rob import Rob
from com.lemontree.runners.rob_daily import RobDaily
from com.lemontree.runners.rob_bulk import RobBulk

from com.lemontree.runners.rob_runners.rob_materialized_runner.rob_materialized_daily import RobMaterializedDaily
from com.lemontree.runners.rob_runners.rob_materialized_runner.rob_materialized_bulk import RobMaterializeBulk


# Map job_name to runner class
runners_map = {
    "local": LocalJobRunner,
    "robFromCurrentDtToYearEndRunner": Rob,
    "robDaily": RobDaily,
    "robBulk": RobBulk,
    "matRobDaily" : RobMaterializedDaily,
    "matRobBulk" : RobMaterializeBulk
    # TODO: Add more here
}