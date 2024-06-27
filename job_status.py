import json
import unittest
from unittest.mock import patch
from your_module import execute  # Replace 'your_module' with the actual module name

# Simplified mock class for JobAuditTable
class MockJobAuditTable:
    def __init__(self, table_name):
        self.table_name = table_name
    
    def update_audit_record(self, dataset_name, snapshot_date, job_version, attribute, value):
        # Simplified mock implementation, returns a dictionary
        if attribute == "job_status":
            return {"job_status": value}
        elif attribute == "description":
            return {"description": value}
        else:
            return {"default": "mocked"}

# Unit test class
class TestExecuteFunction(unittest.TestCase):
    @patch('your_module.invoke_step_function')
    @patch('your_module.get_src_run_id_for_dependency')
    @patch('your_module.JobAuditTable', MockJobAuditTable)  # Use the simplified mock class
    @patch.dict('os.environ', {
        'AWS_ACCOUNT': '123456789012',
        'CLUSTER_NAME': 'test-cluster',
        'CONFIG_PATH': 's3://bucket/config',
        'SPARK_SCRIPT': 's3://bucket/script.py',
        'ENVIRONMENT': 'test',
        'EDP_UTILS_PATH': 's3://bucket/bootstrap.sh',
        'STEPFUNCTION_NAME': 'test-step-function'
    })
    def test_execute_with_records(self, mock_get_src_run_id_for_dependency, mock_invoke_step_function):
        mock_get_src_run_id_for_dependency.return_value = {
            'job1': {
                'job_status': 'DISABLED',
                'dependencies': json.dumps({'dep1': {'runId': '123', 's3Path': 's3://path'}}),
                'job_version': '1'
            }
        }

        event = {
            'Records': [{
                'body': json.dumps({
                    'Message': json.dumps({
                        'task_configuration': {
                            'parsed_datasets': [{'refined_dataset_path': 's3://path'}],
                            'job_params': {'edp_run_id': 'runId', 'output_datasets': ['dataset_name']},
                            'job_info': {'SNAPSHOT_DATE': '20230101'}
                        }
                    })
                })
            }]
        }
        context = {}

        response = execute(event, context)
        
        self.assertEqual(response['status'], 200)
        self.assertIn('message', response)
        self.assertEqual(response['message'], 'Job disabled job1')

        mock_get_src_run_id_for_dependency.assert_called_once()
        mock_invoke_step_function.assert_not_called()

if __name__ == '__main__':
    unittest.main()
