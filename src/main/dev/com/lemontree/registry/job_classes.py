from com.lemontree.runners.local_runner import LocalJobRunner
from com.lemontree.runners.rob import Rob
from com.lemontree.runners.future_rob_daily import FutureRob

# Map job_name to runner class
runners_map = {
    "local": LocalJobRunner,
    "robFromCurrentDtToYearEndRunner": Rob,
    "robFromCurrentDtToFuture": FutureRob,
    # TODO: Add more here
}