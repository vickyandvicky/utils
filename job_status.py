import json
import os
import unittest
from unittest.mock import patch, MagicMock
from your_module import execute  # Make sure to replace 'your_module' with the actual module name

class TestExecuteFunction(unittest.TestCase):
    @patch('your_module.invoke_step_function')
    @patch('your_module.get_src_run_id_for_dependency')
    @patch('your_module.JobAuditTable')
    @patch.dict('os.environ', {
        'AWS_ACCOUNT': '123456789012',
        'CLUSTER_NAME': 'test-cluster',
        'CONFIG_PATH': 's3://bucket/config',
        'SPARK_SCRIPT': 's3://bucket/script.py',
        'ENVIRONMENT': 'test',
        'EDP_UTILS_PATH': 's3://bucket/bootstrap.sh',
        'STEPFUNCTION_NAME': 'test-step-function'
    })
    def test_execute_with_records(self, mock_job_audit_table, mock_get_src_run_id_for_dependency, mock_invoke_step_function):
        mock_audit_table_instance = mock_job_audit_table.return_value
        job_audit_table = mock_audit_table_instance
        
        mock_get_src_run_id_for_dependency.return_value = {
            'job1': {
                'job_status': {'S': 'DISABLED'},
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
