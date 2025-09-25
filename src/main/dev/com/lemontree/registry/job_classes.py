from com.lemontree.runners.local_runner import LocalJobRunner
from com.lemontree.runners.rob.rob_runner import Rob
from com.lemontree.runners.sp.stored_procedure_runner import StoredProcedureTest

# Map job_name to runner class
runners_map = {
    "local": LocalJobRunner,
    "robFromCurrentDtToYearEndRunner": Rob,
    "sp": StoredProcedureTest
    # TODO: Add more here
}