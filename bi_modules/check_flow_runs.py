from prefect.client.orchestration import get_client
from prefect.client.schemas.filters import FlowFilter,FlowFilterName, FlowFilterTags, DeploymentFilter, DeploymentFilterName
import time

def check_and_wait_for_running_flow_runs(deployment_name: str, flow_name: str = None, check_interval_minutes: int = 15):
    deployment_filter = DeploymentFilter(
        name=DeploymentFilterName(any_=[deployment_name])

    )
    flow_filter = FlowFilter(
        name=FlowFilterName(any_=[flow_name]) if flow_name else None
    )

    while True:
        with get_client(sync_client=True) as client:
            response = client.read_flow_runs(deployment_filter=deployment_filter, flow_filter=flow_filter)
            
            # Filter flow runs that have the state "Running"
            running_flow_runs = [flow_run for flow_run in response if flow_run.state.type == "RUNNING"]

            if running_flow_runs:
                print(f"Found {len(running_flow_runs)} running flow run(s) for deployment '{deployment_name}'. Waiting for {check_interval_minutes} minutes before retrying.")
                time.sleep(check_interval_minutes * 60)  # Wait for the specified interval before retrying
            else:
                print(f"No running flow runs found for deployment '{deployment_name}'. Proceeding with the next steps.")
                break

if __name__ == "__main__":
    check_and_wait_for_running_flow_runs( deployment_name='testdeployment', flow_name='testflow', check_interval_minutes=15)