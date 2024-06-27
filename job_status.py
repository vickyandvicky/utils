import unittest
from unittest.mock import MagicMock, patch
from your_module import execute, JobAuditTable

class TestYourModule(unittest.TestCase):
    def setUp(self):
        self.event = {
            'Records': [{
                'body': json.dumps({
                    'Message': json.dumps({
                        'task_configuration': {
                            'parsed_datasets': [{'refined_dataset_path': 's3://bucket/path'}],
                            'job_params': {
                                'edp_run_id': '1234',
                                'output_datasets': ['testJob']
                            },
                            'job_info': {'SNAPSHOT_DATE': '2023-01-01'}
                        }
                    })
                })
            }]
        }
        self.context = {}
        
        # Patch the environment variables
        patcher = patch.dict('os.environ', {
            'AWS_ACCOUNT': '123456789012',
            'CLUSTER_NAME': 'test-cluster',
            'CONFIG_PATH': 's3://bucket/config/path',
            'VIEWS_CONFIG': 'views_config.json',
            'STEPFUNCTION_NAME': 'test-step-function',
            'SPARK_SCRIPT': 's3://bucket/spark/script.py',
            'ENVIRONMENT': 'test',
            'EDP_UTILS_PATH': 's3://bucket/edp/utils/path'
        })
        self.addCleanup(patcher.stop)
        patcher.start()
        
        # Patch the JobAuditTable methods
        self.mock_job_audit_table = patch('your_module.JobAuditTable', autospec=True).start()
        self.addCleanup(self.mock_job_audit_table.stop)
        
        self.mock_job_audit_table_instance = self.mock_job_audit_table.return_value
        self.mock_job_audit_table_instance.get_audit_record_by_version.side_effect = [
            {"job_name": "dependencyJob", "snapshot_date": "2023-01-01:1", "job_status": "DISABLED"}
        ]
        self.mock_job_audit_table_instance.insert_audit_record.return_value = '1'
        self.mock_job_audit_table_instance.update_audit_record.return_value = {"job_status": "DISABLED"}

    def test_execute_with_records(self):
        with patch('your_module.invoke_step_function') as mock_invoke_step_function:
            # Execute the method
            response = execute(self.event, self.context)

            # Validate response and mock calls
            self.assertEqual(response, {"status": 200, "message": "Job disabled testJob"})
            self.mock_job_audit_table_instance.get_audit_record_by_version.assert_called_with(
                "testJob", "2023-01-01", "1"
            )
            mock_invoke_step_function.assert_not_called()

if __name__ == '__main__':
    unittest.main()
