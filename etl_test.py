import unittest
from unittest.mock import patch, MagicMock
import json
import boto3
from botocore.stub import Stubber
from datetime import datetime
from yourmodule import ViewETL  # Replace 'yourmodule' with the actual module name


class TestViewETL(unittest.TestCase):

    @patch('yourmodule.SparkSession.builder.getOrCreate')
    @patch('yourmodule.boto3.client')
    @patch('yourmodule.boto3.resource')
    def setUp(self, mock_boto3_resource, mock_boto3_client, mock_spark_session):
        # Mock configuration returned from S3
        mock_boto3_client.return_value.get_object.return_value = {
            'Body': MagicMock(read=MagicMock(
                return_value=b"GLUE_DB=glue_db\nATHENA_WORKGROUP=workgroup\nREGION=region\nATHENA_ENABLED=true\nREFINED_BUCKET=refined_bucket\nVIEW_PATH=view_path\nJOB_CONFIG_BUCKET=job_config_bucket\nJOB_CONFIG_PATH=job_config_path\nAUDIT_BUCKET=audit_bucket\nEDP_RUN_ID_API=edp_run_id_api"))
        }

        self.env = 'dev'
        self.etl = ViewETL(self.env)

    def test_initialization(self):
        self.assertEqual(self.etl.config['GLUE_DB'], 'glue_db')
        self.assertEqual(self.etl.config['ATHENA_WORKGROUP'], 'workgroup')
        self.assertEqual(self.etl.config['REGION'], 'region')

    @patch('yourmodule.SparkSession.read.parquet')
    def test_read_from_s3(self, mock_read_parquet):
        mock_df = MagicMock()
        mock_read_parquet.return_value = mock_df

        result_df = self.etl.read_from_s3('s3_path')
        mock_read_parquet.assert_called_once_with('s3_path')
        self.assertEqual(result_df, mock_df)

    def test_joinDF(self):
        df1 = MagicMock()
        df2 = MagicMock()
        df3 = MagicMock()

        df1.join.return_value = df2
        df2.join.return_value = df3

        dfList = [df1, df2, df3]
        result_df = self.etl.joinDF(dfList)

        self.assertEqual(result_df, df3)

    @patch('yourmodule.boto3.client')
    def test_copy_s3_file(self, mock_boto3_client):
        mock_s3 = mock_boto3_client.return_value

        self.etl.copy_s3_file('source_bucket', 'source_key', 'destination_bucket', 'destination_key', 'format')
        mock_s3.copy_object.assert_called_once_with(
            Bucket='destination_bucket',
            CopySource={'Bucket': 'source_bucket', 'Key': 'source_key'},
            Key='destination_key.format'
        )

    @patch('yourmodule.boto3.client')
    def test_get_file_name(self, mock_boto3_client):
        mock_s3 = mock_boto3_client.return_value
        mock_s3.list_objects_v2.return_value = {
            'Contents': [{'Key': 'file_name.part.1'}]
        }

        result = self.etl.get_file_name('bucket_name', 'prefix')
        self.assertEqual(result, 'file_name.part.1')

    @patch('yourmodule.boto3.client')
    def test_delete_file(self, mock_boto3_client):
        mock_s3 = mock_boto3_client.return_value

        self.etl.delete_file('source_bucket', 'source_key')
        mock_s3.delete_object.assert_called_once_with(Bucket='source_bucket', Key='source_key')

    @patch('yourmodule.boto3.resource')
    def test_getAppConfig(self, mock_boto3_resource):
        mock_s3 = mock_boto3_resource.return_value
        mock_s3.Object.return_value.get.return_value = {
            'Body': MagicMock(read=MagicMock(
                return_value=b"GLUE_DB=glue_db\nATHENA_WORKGROUP=workgroup\nREGION=region\nATHENA_ENABLED=true"))
        }

        config = self.etl.getAppConfig('dev')
        self.assertEqual(config['GLUE_DB'], 'glue_db')

    @patch('yourmodule.ECDPConsumerClient.create_run_id')
    def test_create_edp_run_id(self, mock_create_run_id):
        mock_create_run_id.return_value = 'edp_run_id'

        edp_run_id = self.etl.create_edp_run_id()
        self.assertEqual(edp_run_id, 'edp_run_id')

    @patch('yourmodule.boto3.client')
    def test_update_audit_record(self, mock_boto3_client):
        mock_dynamodb = mock_boto3_client.return_value

        self.etl.update_audit_record('job_name', 'snapshot_date', 'run_version', 'edp_run_id')
        mock_dynamodb.update_item.assert_called_once()

    @patch('yourmodule.boto3.client')
    def test_audit_write(self, mock_boto3_client):
        mock_s3 = mock_boto3_client.return_value

        result = self.etl.audit_write('table_path', 'dataset_name', 'edp_run_id', 'snapshot_date')
        mock_s3.put_object.assert_called_once()

        self.assertEqual(result['run_id'], 'edp_run_id')

    @patch('yourmodule.boto3.client')
    def test_get_athena_client(self, mock_boto3_client):
        mock_athena_client = mock_boto3_client.return_value

        athena_client = self.etl.get_athena_client()
        self.assertEqual(athena_client, mock_athena_client)

    @patch('yourmodule.boto3.client')
    def test_submit_athena_query(self, mock_boto3_client):
        mock_athena = mock_boto3_client.return_value
        mock_athena.start_query_execution.return_value = {'QueryExecutionId': 'query_execution_id'}

        query_execution_id = self.etl.submit_athena_query(mock_athena, 'SELECT * FROM table')
        self.assertEqual(query_execution_id, 'query_execution_id')


if __name__ == '__main__':
    unittest.main()
