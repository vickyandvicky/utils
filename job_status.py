import unittest
from unittest.mock import MagicMock
from main import execute, JobAuditTable, get_src_run_id_for_dependency, submit_new_step_to_cluster, check_emr_step_status

class TestExecuteFunction(unittest.TestCase):
    
    def setUp(self):
        # Set up any necessary mock objects or test data
        self.mock_job_audit_table = MagicMock(JobAuditTable)
    
    def test_process_records_event_job_disabled(self):
        # Test scenario where job is disabled
        event = {
            'Records': [
                {'body': '{"Message": {"task_configuration": {"job_params": {"output_datasets": ["dataset1"]}, "job_info": {"SNAPSHOT_DATE": "2024-06-27"}}}}'}
            ],
            'job_name': 'job1',
            'snapshot_date': '2024-06-27',
            'job_version': '1'
        }
        
        self.mock_job_audit_table.update_audit_record.return_value = {"snapshot_date": "2024-06-27:1"}
        get_src_run_id_for_dependency.return_value = {'job1': {'job_status': {'S': 'DISABLED'}}}
        
        result = execute(event, None)
        
        self.assertEqual(result, {"status": 200, "message": "Job disabled job1"})
    
    def test_process_records_event_deps_complete(self):
        # Test scenario where dependencies are complete
        event = {
            'Records': [
                {'body': '{"Message": {"task_configuration": {"job_params": {"output_datasets": ["dataset1"], "job_params": {"edp_run_id": "123"}}, "parsed_datasets": [{"refined_dataset_path": "path"}], "job_info": {"SNAPSHOT_DATE": "2024-06-27"}, "job_info": {"SNAPSHOT_DATE": "2024-06-27"}}}}'}
            ],
            'job_name': 'job1',
            'snapshot_date': '2024-06-27',
            'job_version': '1'
        }
        
        self.mock_job_audit_table.update_audit_record.return_value = {"snapshot_date": "2024-06-27:1"}
        get_src_run_id_for_dependency.return_value = {'job1': {'job_status': {'S': 'DEPS_COMPLETE'}, 'dependencies': json.dumps({'dep1': {'runId': '123', 's3Path': 's3://path'}}), 'job_version': '1'}}
        
        result = execute(event, None)
        
        self.assertEqual(result['next_step'], "check_job_status")
        self.assertEqual(result['job_name'], 'job1')
        self.assertEqual(result['snapshot_date'], '2024-06-27')
        self.assertEqual(result['job_version'], '1')
    
    def test_process_records_event_no_records(self):
        # Test scenario where 'Records' key is not present in event
        event = {'no_records_key': 'test'}
        
        result = execute(event, None)
        
        self.assertEqual(result, {"status": 200, "message": "No valid event found"})
    
    def test_process_emr_job_event_success(self):
        # Test scenario where 'next_step' is 'emr_job' and submission is successful
        event = {
            'next_step': 'emr_job',
            'job_name': 'job1',
            'snapshot_date': '2024-06-27',
            'job_version': '1',
            'dependencies': {'dep1': {'runId': '123', 's3Path': 's3://path'}}
        }
        
        submit_new_step_to_cluster.return_value = {'StepIds': ['12345']}
        self.mock_job_audit_table.update_audit_record.side_effect = lambda *args: {"snapshot_date": "2024-06-27:1"}
        
        result = execute(event, None)
        
        self.assertEqual(result['status'], 200)
        self.assertEqual(result['next_step'], "check_job_status")
        self.assertEqual(result['job_name'], 'job1')
        self.assertEqual(result['snapshot_date'], '2024-06-27')
        self.assertEqual(result['job_version'], '1')
    
    def test_process_emr_job_event_cluster_not_found(self):
        # Test scenario where 'next_step' is 'emr_job' but cluster is not found
        event = {
            'next_step': 'emr_job',
            'job_name': 'job1',
            'snapshot_date': '2024-06-27',
            'job_version': '1',
            'dependencies': {'dep1': {'runId': '123', 's3Path': 's3://path'}}
        }
        
        submit_new_step_to_cluster.return_value = -1
        self.mock_job_audit_table.update_audit_record.side_effect = lambda *args: {"snapshot_date": "2024-06-27:1"}
        
        result = execute(event, None)
        
        self.assertEqual(result['status'], 404)
        self.assertIn("Cluster not found", result['message'])
    
    def test_process_check_job_status_event_completed(self):
        # Test scenario where 'Payload' has 'next_step' as 'check_job_status' and job is completed
        event = {
            'Payload': {
                'next_step': 'check_job_status',
                'StepIds': ['12345'],
                'job_name': 'job1',
                'snapshot_date': '2024-06-27',
                'job_version': '1'
            }
        }
        
        check_emr_step_status.return_value = {'Step': {'Status': {'State': 'COMPLETED'}}}
        self.mock_job_audit_table.update_audit_record.side_effect = lambda *args: {"snapshot_date": "2024-06-27:1"}
        
        result = execute(event, None)
        
        self.assertEqual(result['check_job_status']['job_name'], 'job1')
        self.assertEqual(result['check_job_status']['snapshot_date'], '2024-06-27')
        self.assertEqual(result['check_job_status']['job_version'], '1')
        self.assertEqual(result['job_status'], 'COMPLETED')
    
    def test_process_check_job_status_event_failed(self):
        # Test scenario where 'Payload' has 'next_step' as 'check_job_status' and job has failed
        event = {
            'Payload': {
                'next_step': 'check_job_status',
                'StepIds': ['12345'],
                'job_name': 'job1',
                'snapshot_date': '2024-06-27',
                'job_version': '1'
            }
        }
        
        check_emr_step_status.return_value = {'Step': {'Status': {'State': 'FAILED'}}}
        self.mock_job_audit_table.update_audit_record.side_effect = lambda *args: {"snapshot_date": "2024-06-27:1"}
        
        result = execute(event, None)
        
        self.assertEqual(result['check_job_status']['job_name'], 'job1')
        self.assertEqual(result['check_job_status']['snapshot_date'], '2024-06-27')
        self.assertEqual(result['check_job_status']['job_version'], '1')
        self.assertEqual(result['job_status'], 'FAILED')
    
    def test_process_check_job_status_event_pending(self):
        # Test scenario where 'Payload' has 'next_step' as 'check_job_status' and job is pending
        event = {
            'Payload': {
                'next_step': 'check_job_status',
                'StepIds': ['12345'],
                'job_name': 'job1',
                'snapshot_date': '2024-06-27',
                'job_version': '1'
            }
        }
        
        check_emr_step_status.return_value = {'Step': {'Status': {'State': 'PENDING'}}}
        self.mock_job_audit_table.update_audit_record.side_effect = lambda *args: {"snapshot_date": "2024-06-27:1"}
        
        result = execute(event, None)
        
        self.assertEqual(result['check_job_status']['job_name'], 'job1')
        self.assertEqual(result['check_job_status']['snapshot_date'], '2024-06-27')
        self.assertEqual(result['check_job_status']['job_version'], '1')
        self.assertEqual(result['job_status'], 'PENDING')
    
    def test_process_check_job_status_event_running(self):
        # Test scenario where 'Payload' has 'next_step' as 'check_job_status' and job is running
        event = {
            'Payload': {
                'next_step': 'check_job_status',
                'StepIds': ['12345'],
                'job_name': 'job1',
                'snapshot_date': '2024-06-27',
                'job_version': '1'
            }
        }
        
        check_emr_step_status.return_value = {'Step': {'Status': {'State': 'RUNNING'}}}
        self.mock_job_audit_table.update_audit_record.side_effect = lambda *args: {"snapshot_date": "2024-06-27:1"}
        
        result = execute(event, None)
        
        self.assertEqual(result['check_job_status']['job_name'], 'job1')
        self.assertEqual(result['check_job_status']['snapshot_date'], '2024-06-27')
        self.assertEqual(result['check_job_status']['job_version'], '1')
        self.assertEqual(result['job_status'], 'RUNNING')

if __name__ == '__main__':
    unittest.main()

