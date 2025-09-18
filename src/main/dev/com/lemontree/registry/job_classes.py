from com.lemontree.runners.local_runner import LocalJobRunner
from com.lemontree.runners.rob import Rob

# Map job_name to runner class
runners_map = {
    "local": LocalJobRunner,
    "robFromCurrentDtToYearEndRunner": Rob,
    # TODO: Add more here
}