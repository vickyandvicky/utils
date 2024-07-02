import unittest
from unittest.mock import patch, MagicMock
import json
from your_module import JobConfigs3  # Replace with your actual module name

class TestJobConfigs3(unittest.TestCase):
    @patch('boto3.resource')
    def setUp(self, mock_s3_resource):
        # Mock S3 Object and its data
        self.mock_s3_resource = mock_s3_resource
        self.mock_s3_object = MagicMock()
        self.mock_s3_resource.return_value.Object.return_value = self.mock_s3_object
        
        # Mock content
        self.mock_content = {
            "job1": {"job_dependencies": ["dep1"], "active": True},
            "job2": {"job_dependencies": ["dep2"], "active": False},
            "job3": {"job_dependencies": ["dep1", "dep3"], "active": True}
        }
        self.mock_s3_object.get.return_value = {
            'Body': MagicMock(read=MagicMock(return_value=json.dumps(self.mock_content).encode('utf-8')))
        }
        
        self.bucket = 'test-bucket'
        self.key = 'test-key'
        self.job_configs = JobConfigs3(self.bucket, self.key)

    def test_get_jobs_by_dependency(self):
        result = self.job_configs.getJobsByDependency('dep1')
        expected = ['job1', 'job3']
        self.assertEqual(result, expected)

    def test_get_dependencies_by_job_active(self):
        result = self.job_configs.getDependenciesByJob('job1')
        expected = ['dep1']
        self.assertEqual(result, expected)

    def test_get_dependencies_by_job_inactive(self):
        result = self.job_configs.getDependenciesByJob('job2')
        expected = []
        self.assertEqual(result, expected)

    def test_get_dependencies_by_job_non_existent(self):
        result = self.job_configs.getDependenciesByJob('non_existent_job')
        self.assertEqual(result, [])

if __name__ == '__main__':
    unittest.main()
