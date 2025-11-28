from com.lemontree.runners.rob.rob_daily import RobDaily
from com.lemontree.runners.rob.rob_bulk import RobBulk
from com.lemontree.runners.mat.materialized_daily import RobMaterializedDaily
from com.lemontree.runners.mat.materialized_bulk import RobMaterializeBulk
from com.lemontree.runners.pace.pace_runner import PaceRunner

# Map job_name to runner class
runners_map = {

    # TODO: Add more here
    "robDaily": RobDaily,
    "robBulk": RobBulk,
    "matRobDaily" : RobMaterializedDaily,
    "matRobBulk" : RobMaterializeBulk,
    "pace": PaceRunner,

}