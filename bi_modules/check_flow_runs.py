from prefect.client.orchestration import get_client
from prefect.client.schemas.filters import FlowFilter,FlowFilterName, FlowFilterTags, DeploymentFilter, DeploymentFilterName
import time
import prefect
from prefect import runtime,get_run_logger
from prefect.states import Cancelled
from prefect.deployments import run_deployment

def flow_run_handling(deployment_name: str = None, flow_name: str = None, check_interval_minutes: int = 15,self_action: str = 'wait',reschedule_interval_minutes: int = 15):
    if deployment_name is None:
        deployment_name = runtime.deployment.name
        self_deployment_id = runtime.deployment.id
    if flow_name is None:
        flow_name = runtime.flow_run.flow_name
        self_flow_run_id = runtime.flow_run.id

    flow_filter = FlowFilter(
        name=FlowFilterName(any_=[flow_name]) if flow_name else None
    )
    deployment_filter = DeploymentFilter(
        name=DeploymentFilterName(any_=[deployment_name])
    )


    print(prefect.context.get_run_context().flow_run.dict())
    while True:
        with get_client(sync_client=True) as client:
            response = client.read_flow_runs(deployment_filter=deployment_filter, flow_filter=flow_filter)
            
            # Filter flow runs that have the state "Running"
            running_flow_runs = [flow_run for flow_run in response if flow_run.state.type == "RUNNING"]

            if running_flow_runs:

                if self_action == 'wait':
                    print(f"Found {len(running_flow_runs)} running flow run(s) for deployment '{deployment_name}'. Waiting for {check_interval_minutes} minutes before retrying.")
                    time.sleep(check_interval_minutes * 60)  # Wait for the specified interval before retrying
                    
                elif self_action == 'continue':
                    print(f"Found {len(running_flow_runs)} running flow run(s) for deployment '{deployment_name}'. Proceeding with the next steps.")
                    break

                elif self_action == 'raise':
                    raise ValueError(f"Found {len(running_flow_runs)} running flow run(s) for deployment '{deployment_name}'.")
                
                elif self_action == 'cancel':
                    logger = get_run_logger()
                    logger.warning(f"Cancelled this flow run")
                    return Cancelled()

                elif self_action == 'reschedule':
                    reschedule_interval_minutes = reschedule_interval_minutes * 60
                    logger = get_run_logger()
                    run_deployment(name=f"{flow_name}/{deployment_name}",timeout=0)
                    print(f"Rescheduled run for flow {flow_name} for deployment '{deployment_name}'")
                    return Cancelled()
                
            else:
                print(f"No running flow runs found for flow {flow_name} deployment '{deployment_name}'. Proceeding with the next steps.")
                break

if __name__ == "__main__":
    flow_run_handling( deployment_name='testdeployment', flow_name='testflow', check_interval_minutes=15)