import json
import os
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

class JobAuditTable:
    def __init__(self, table_name):
        self.table_name = table_name
    
    def update_audit_record(self, job_name, snapshot_job_version, attribute, value):
        # Mocked implementation for demonstration
        return {"snapshot_date": snapshot_job_version}

def get_src_run_id_for_dependency(dataset_name, snapshot_date, edp_run_id, refined_dataset_path, job_audit_table):
    # Mocked function for demonstration
    return {
        'job1': {
            'job_status': {'S': 'DISABLED'},
            'dependencies': json.dumps({'dep1': {'runId': '123', 's3Path': 's3://path'}}),
            'job_version': '1'
        }
    }

def submit_new_step_to_cluster(dataset_name, snapshot_date, job_version, dependencies, cluster_name):
    # Mocked function for demonstration
    return {'StepIds': ['12345']}

def check_emr_step_status(emr_step_id, cluster_id):
    # Mocked function for demonstration
    return {'Step': {'Status': {'State': 'COMPLETED'}}}

def invoke_step_function(state_machine_arn, sf_input):
    # Mocked function for demonstration
    pass

def execute(event, context):
    logger.info("Received event: %s", json.dumps(event))
    
    AWS_ACCOUNT = str(os.environ.get("AWS_ACCOUNT"))
    CLUSTER_NAME = os.environ.get("CLUSTER_NAME")
    STEP_FUNCTION_NAME = os.environ.get("STEPFUNCTION_NAME")
    
    job_audit_table = JobAuditTable("data_dstr_job_audit")
    
    if 'Records' in event:
        return process_records_event(event, job_audit_table, AWS_ACCOUNT, CLUSTER_NAME, STEP_FUNCTION_NAME)
    
    elif 'Payload' not in event and event.get('next_step') == 'emr_job':
        return process_emr_job_event(event, job_audit_table, AWS_ACCOUNT, CLUSTER_NAME, STEP_FUNCTION_NAME)
    
    elif 'Payload' in event and event['Payload'].get('next_step') == 'check_job_status':
        return process_check_job_status_event(event, job_audit_table, AWS_ACCOUNT)
    
    else:
        return {"status": 200, "message": "No valid event found"}

def process_records_event(event, job_audit_table, aws_account, cluster_name, step_function_name):
    output_job = json.loads(event['Records'][0]['body'])
    task_configuration = json.loads(output_job['Message'])['task_configuration']
    
    refined_dataset_path = task_configuration['parsed_datasets'][0]['refined_dataset_path']
    edp_run_id = task_configuration['job_params']['edp_run_id']
    dataset_name = task_configuration['job_params']['output_datasets'][0]
    snapshot_date = task_configuration['job_info']['SNAPSHOT_DATE']
    
    job_list_dependencies_dict = get_src_run_id_for_dependency(dataset_name, snapshot_date, edp_run_id, refined_dataset_path, job_audit_table)
    
    if job_list_dependencies_dict:
        for view_job, view_dep in job_list_dependencies_dict.items():
            job_name = view_job
            
            if view_dep:
                if view_dep["job_status"]["S"] == "DISABLED":
                    return {"status": 200, "message": f"Job disabled {job_name}"}
                
                elif view_dep["job_status"]["S"] == "DEPS_COMPLETE":
                    dependencies = json.loads(view_dep["dependencies"])
                    
                    if None in dependencies.values():
                        response = {"status": 200, "message": "Dependency job yet to run"}
                        response.update(view_dep)
                        return response
                    
                    attribute = "dependencies"
                    up_response = job_audit_table.update_audit_record(job_name, snapshot_date + ":" + view_dep["job_version"], attribute, json.dumps(dependencies))
                    job_version = up_response["snapshot_date"].split(":")[-1]
                    
                    sf_input = json.dumps({
                        "job_name": job_name,
                        "next_step": "emr_job",
                        "snapshot_date": snapshot_date,
                        "job_version": job_version,
                        "dependencies": dependencies,
                        "aws_account": aws_account
                    })
                    invoke_step_function(f"arn:aws:states:us-east-1:{aws_account}:stateMachine:{step_function_name}", sf_input)
                
                else:
                    logger.info("Dependent jobs yet to run")
                    return {
                        "next_step": "check_job_status",
                        "job_name": job_name,
                        "snapshot_date": snapshot_date,
                        "job_version": view_dep["job_version"],
                        "aws_account": aws_account
                    }
    
    else:
        job_audit_table.update_audit_record(event['job_name'], event['snapshot_date'], event['job_version'], "description", f"Cluster not found for {cluster_name}")
        return {"status": 200, "message": "No job dependencies found"}

def process_emr_job_event(event, job_audit_table, aws_account, cluster_name, step_function_name):
    dataset_name = event["job_name"]
    snapshot_date = event["snapshot_date"]
    job_version = event["job_version"]
    dependencies = event["dependencies"]
    
    response = submit_new_step_to_cluster(dataset_name, snapshot_date, job_version, dependencies, cluster_name)
    
    if response == -1:
        job_audit_table.update_audit_record(dataset_name, snapshot_date + ":" + job_version, "description", f"Cluster not found for {cluster_name}")
        return {"status": 404, "message": f"Cluster not found for {cluster_name}"}
    
    else:
        step_id = response['StepIds'][0]
        job_audit_table.update_audit_record(dataset_name, snapshot_date + ":" + job_version, "step_id", step_id)
        job_audit_table.update_audit_record(event['job_name'], event['snapshot_date'] + ":" + event['job_version'], "cluster_name", cluster_name)
        
        return {
            'status': 200,
            'next_step': "check_job_status",
            'job_name': event['job_name'],
            'snapshot_date': event['snapshot_date'],
            'job_version': event['job_version'],
            'aws_account': aws_account
        }

def process_check_job_status_event(event, job_audit_table, aws_account):
    emr_step_id = event['Payload']['StepIds'][0]
    cluster_id = get_cluster_id(CLUSTER_NAME)  # Assuming get_cluster_id function is defined elsewhere
    response = check_emr_step_status(emr_step_id, cluster_id)
    job_status = response['Step']['Status']['State']
    
    res = {
        'check_job_status': {
            "job_name": event['Payload']['job_name'],
            "snapshot_date": event['Payload']['snapshot_date'],
            "job_version": event['Payload']['job_version'],
            "aws_account": aws_account
        }
    }
    
    if job_status == "COMPLETED":
        res["job_status"] = "COMPLETED"
        job_audit_table.update_audit_record(event['Payload']['job_name'], event['Payload']['snapshot_date'] + ":" + event['Payload']['job_version'], "job_status", "COMPLETED")
    
    elif job_status == "FAILED":
        res["job_status"] = "FAILED"
        job_audit_table.update_audit_record(event['Payload']['job_name'], event['Payload']['snapshot_date'] + ":" + event['Payload']['job_version'], "job_status", "FAILED")
    
    else:
        if job_status in ('RUNNING', "PENDING"):
            res['job_status'] = job_status
            res['next_step'] = "check_job_status"
            res['StepIds'] = [emr_step_id]
    
    return res
