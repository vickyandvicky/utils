import unittest
from unittest.mock import patch
from moto import mock_dynamodb2
import boto3
import datetime
from your_module import JobAuditTable  # Replace 'your_module' with the actual module name

class TestJobAuditTable(unittest.TestCase):
    @mock_dynamodb2
    def setUp(self):
        self.table_name = "test_audit_table"
        self.dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
        self.table = self.dynamodb.create_table(
            TableName=self.table_name,
            KeySchema=[
                {
                    'AttributeName': 'job_name',
                    'KeyType': 'HASH'  # Partition key
                },
                {
                    'AttributeName': 'snapshot_date',
                    'KeyType': 'RANGE'  # Sort key
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'job_name',
                    'AttributeType': 'S'
                },
                {
                    'AttributeName': 'snapshot_date',
                    'AttributeType': 'S'
                }
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 1,
                'WriteCapacityUnits': 1
            }
        )
        self.table.wait_until_exists()
        self.job_audit_table = JobAuditTable(self.table_name)

    @mock_dynamodb2
    def test_insert_audit_record(self):
        job_name = "test_job"
        snapshot_date = "2024-06-25"

        job_version = self.job_audit_table.insert_audit_record(job_name, snapshot_date)

        self.assertEqual(job_version, '1')
        response = self.table.get_item(
            Key={
                'job_name': job_name,
                'snapshot_date': f"{snapshot_date}:1"
            }
        )
        self.assertIn('Item', response)
        self.assertEqual(response['Item']['job_name'], job_name)
        self.assertEqual(response['Item']['snapshot_date'], f"{snapshot_date}:1")

    @mock_dynamodb2
    def test_get_audit_record_by_version(self):
        job_name = "test_job"
        snapshot_date = "2024-06-25"
        version = "1"

        self.table.put_item(
            Item={
                'job_name': job_name,
                'snapshot_date': f"{snapshot_date}:{version}",
                'run_start_tm': '2024-06-25T12:00:00Z'
            }
        )

        record = self.job_audit_table.get_audit_record_by_version(job_name, snapshot_date, version)

        self.assertIsNotNone(record)
        self.assertEqual(record['job_name'], job_name)
        self.assertEqual(record['snapshot_date'], f"{snapshot_date}:{version}")

    @mock_dynamodb2
    def test_get_audit_record(self):
        job_name = "test_job"
        snapshot_date = "2024-06-25"

        self.table.put_item(
            Item={
                'job_name': job_name,
                'snapshot_date': f"{snapshot_date}:1",
                'run_start_tm': '2024-06-25T12:00:00Z'
            }
        )

        records = self.job_audit_table.get_audit_record(job_name, snapshot_date)

        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]['job_name'], job_name)
        self.assertEqual(records[0]['snapshot_date'], f"{snapshot_date}:1")

    @mock_dynamodb2
    def test_update_audit_record(self):
        job_name = "test_job"
        snapshot_date = "2024-06-25"
        attribute_name = "job_status"
        attribute_value = "completed"

        self.table.put_item(
            Item={
                'job_name': job_name,
                'snapshot_date': f"{snapshot_date}:1",
                'run_start_tm': '2024-06-25T12:00:00Z'
            }
        )

        updated_item = self.job_audit_table.update_audit_record(job_name, f"{snapshot_date}:1", attribute_name, attribute_value)

        self.assertIsNotNone(updated_item)
        self.assertEqual(updated_item['job_status'], attribute_value)

if __name__ == '__main__':
    unittest.main()
