import sys
from com.lemontree.utils.utils_helper_methods import parse_cmd_line_args
from com.lemontree.registry.job_classes import runners_map

def main(args: dict):
    try:
        job_name = args.get("job_name").strip()
        if not job_name:
            raise ValueError("Missing required argument: 'job_name'")

        print(f"Dispatching job '{job_name}'")
        # get the class name from the job to run
        runner_class = runners_map.get(job_name)
        if runner_class is None:
            raise ValueError(f"Unknown job name: {job_name}")

        print(f'Running job: {job_name} . Class called {runner_class.__name__}')
        runner_class(args).execute()
        print(f'Completed job: {job_name}')
    except Exception as ex:
        raise ex

if __name__ == "__main__":
    # Running locally: parse CLI args then call main()
    try:
        args = parse_cmd_line_args()
        main(args)
    except Exception as e:
        print(f"Failed to run job: {e}")
        sys.exit(1)
