import unittest
from unittest.mock import patch, MagicMock
import boto3
import datetime
from your_module import JobAuditTable  # Replace 'your_module' with the actual module name

class TestJobAuditTable(unittest.TestCase):
    def setUp(self):
        self.table_name = "test_audit_table"
        self.job_audit_table = JobAuditTable(self.table_name)

    @patch('boto3.client')
    @patch('boto3.resource')
    def test_insert_audit_record(self, mock_resource, mock_client):
        mock_dynamodb_client = MagicMock()
        mock_client.return_value = mock_dynamodb_client

        mock_table = MagicMock()
        mock_resource.return_value.Table.return_value = mock_table

        mock_table.query.return_value = {'Items': []}
        
        job_name = "test_job"
        snapshot_date = "2024-06-25"

        job_version = self.job_audit_table.insert_audit_record(job_name, snapshot_date)

        self.assertEqual(job_version, '1')
        mock_dynamodb_client.put_item.assert_called_once()
        args, kwargs = mock_dynamodb_client.put_item.call_args
        self.assertEqual(kwargs['TableName'], self.table_name)
        self.assertEqual(kwargs['Item']['job_name']['S'], job_name)
        self.assertEqual(kwargs['Item']['snapshot_date']['S'], f"{snapshot_date}:1")

    @patch('boto3.resource')
    def test_get_audit_record_by_version(self, mock_resource):
        mock_dynamodb_resource = MagicMock()
        mock_resource.return_value = mock_dynamodb_resource

        mock_table = MagicMock()
        mock_dynamodb_resource.Table.return_value = mock_table

        job_name = "test_job"
        snapshot_date = "2024-06-25"
        version = "1"

        mock_table.query.return_value = {
            'Items': [{'job_name': job_name, 'snapshot_date': f"{snapshot_date}:{version}", 'run_start_tm': '2024-06-25T12:00:00Z'}]
        }

        record = self.job_audit_table.get_audit_record_by_version(job_name, snapshot_date, version)

        self.assertIsNotNone(record)
        self.assertEqual(record['job_name'], job_name)
        self.assertEqual(record['snapshot_date'], f"{snapshot_date}:{version}")

    @patch('boto3.resource')
    def test_get_audit_record(self, mock_resource):
        mock_dynamodb_resource = MagicMock()
        mock_resource.return_value = mock_dynamodb_resource

        mock_table = MagicMock()
        mock_dynamodb_resource.Table.return_value = mock_table

        job_name = "test_job"
        snapshot_date = "2024-06-25"

        mock_table.query.return_value = {
            'Items': [{'job_name': job_name, 'snapshot_date': f"{snapshot_date}:1", 'run_start_tm': '2024-06-25T12:00:00Z'}]
        }

        records = self.job_audit_table.get_audit_record(job_name, snapshot_date)

        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]['job_name'], job_name)
        self.assertEqual(records[0]['snapshot_date'], f"{snapshot_date}:1")

    @patch('boto3.client')
    def test_update_audit_record(self, mock_client):
        mock_dynamodb_client = MagicMock()
        mock_client.return_value = mock_dynamodb_client

        job_name = "test_job"
        snapshot_date = "2024-06-25:1"
        attribute_name = "job_status"
        attribute_value = "completed"

        mock_dynamodb_client.update_item.return_value = {
            'Attributes': {'job_status': attribute_value, 'run_end_tm': '2024-06-25T12:00:00Z'}
        }

        updated_item = self.job_audit_table.update_audit_record(job_name, snapshot_date, attribute_name, attribute_value)

        self.assertIsNotNone(updated_item)
        self.assertEqual(updated_item['job_status'], attribute_value)
        mock_dynamodb_client.update_item.assert_called_once()
        args, kwargs = mock_dynamodb_client.update_item.call_args
        self.assertEqual(kwargs['TableName'], self.table_name)
        self.assertEqual(kwargs['Key']['job_name']['S'], job_name)
        self.assertEqual(kwargs['Key']['snapshot_date']['S'], snapshot_date)
        self.assertEqual(kwargs['ExpressionAttributeValues'][':value']['S'], attribute_value)

if __name__ == '__main__':
    unittest.main()
