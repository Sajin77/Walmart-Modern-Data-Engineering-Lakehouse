# test.py
import os
import time
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import RunLifeCycleState, RunResultState

def trigger_and_monitor_databricks_job(host: str, token: str, job_id: str) -> str:
    ws = WorkspaceClient(host=host, token=token)
    
    print(f"Triggering Databricks job ID: {job_id}...")
    job_trigger = ws.jobs.run_now(job_id=job_id)
    run_id = job_trigger.run_id
    print(f"Job triggered successfully. Tracking Run ID: {run_id}")

    while True:
        job_run = ws.jobs.get_run(run_id)
        current_lifecycle = job_run.state.life_cycle_state

        if current_lifecycle in [RunLifeCycleState.TERMINATED, RunLifeCycleState.SKIPPED, RunLifeCycleState.INTERNAL_ERROR]:
            final_result = job_run.state.result_state
            
            if final_result == RunResultState.SUCCESS:
                success_msg = f"Databricks Job {job_id} (Run {run_id}) completed successfully!"
                print(success_msg)
                return success_msg
            else:
                raise Exception(f"Job failed! Lifecycle: {current_lifecycle}, Result: {final_result}")
                
        print(f"Job is still running (State: {current_lifecycle}). Checking again in 10 seconds...")
        time.sleep(10)

# # ─── testing ───
# if __name__ == "__main__":

#     db_host = os.getenv("DATABRICKS_HOST")
#     db_token = os.getenv("DATABRICKS_TOKEN")
#     db_job_id = os.getenv("DATABRICKS_WALMART_JOB_ID")   
    
#     try:
#         print("Starting Databricks utility test script...")
#         # Call your function using your parameters
#         output_result = trigger_and_monitor_databricks_job(host=db_host, token=db_token, job_id=db_job_id)
#         print("Test finished cleanly!")
#     except Exception as e:
#         print(f"\nTest Failed with Error: {e}")
