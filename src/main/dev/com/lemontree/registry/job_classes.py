from com.lemontree.runners.local_runner import LocalJobRunner
from com.lemontree.runners.rob import Rob
from com.lemontree.runners.rob_daily import RobDaily
from com.lemontree.runners.rob_bulk import RobBulk

# Map job_name to runner class
runners_map = {
    "local": LocalJobRunner,
    "robFromCurrentDtToYearEndRunner": Rob,
    "robDaily": RobDaily,
    "robBulk": RobBulk
    # TODO: Add more here
}