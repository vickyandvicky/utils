import json

def get_dependencies_from_dynamo(dataset_name, snapshot_date, audit_table, dep_dict):
    def update_dependencies_and_status(version, dependencies, status):
        audit_table.update_audit_record(dataset_name, f"{snapshot_date}:{version}", "dependencies", json.dumps(dependencies))
        audit_table.update_audit_record(dataset_name, f"{snapshot_date}:{version}", "job_status", status)

    if not dep_dict:
        ver = audit_table.insert_audit_record(dataset_name, snapshot_date)
        audit_table.update_audit_record(dataset_name, f"{snapshot_date}:{ver}", "job_status", "DISABLED")
        return {
            "job_version": ver,
            "job_status": "DISABLED"
        }

    items = audit_table.get_audit_record(dataset_name, snapshot_date)
    if not items:
        ver = audit_table.insert_audit_record(dataset_name, snapshot_date)
        update_dependencies_and_status(ver, dep_dict, "WAITING")
        job_version = ver
    else:
        latest_rec = max(items, key=lambda rec: int(rec["snapshot_date"].split(":")[1]))
        job_version = int(latest_rec["snapshot_date"].split(":")[1])
        dependencies = json.loads(latest_rec["dependencies"])

        if latest_rec["job_status"] == "WAITING":
            for dep_job in dependencies:
                if dependencies[dep_job] is None:
                    dependencies[dep_job] = dep_dict[dep_job]
            update_dependencies_and_status(job_version, dependencies, "WAITING")
        else:
            job_version = audit_table.insert_audit_record(dataset_name, snapshot_date)
            update_dependencies_and_status(job_version, dep_dict, "WAITING")

    record = audit_table.get_audit_record_by_version(dataset_name, snapshot_date, job_version)
    dep_dict = json.loads(record["dependencies"])
    if None not in dep_dict.values():
        audit_table.update_audit_record(dataset_name, f"{snapshot_date}:{job_version}", "job_status", "DEPS_COMPLETE")
        record["job_status"] = "DEPS_COMPLETE"
    
    return {
        "job_version": job_version,
        "job_status": record["job_status"]
    }

==
import unittest
from unittest.mock import MagicMock
import json

class TestGetDependenciesFromDynamo(unittest.TestCase):

    def setUp(self):
        self.dataset_name = "test_dataset"
        self.snapshot_date = "2024-07-19"
        self.audit_table = MagicMock()

    def test_empty_dep_dict(self):
        # Test when dep_dict is empty
        self.audit_table.insert_audit_record.return_value = 1
        self.audit_table.update_audit_record.return_value = {"job_status": "DISABLED"}

        result = get_dependencies_from_dynamo(self.dataset_name, self.snapshot_date, self.audit_table, {})

        self.audit_table.insert_audit_record.assert_called_once_with(self.dataset_name, self.snapshot_date)
        self.audit_table.update_audit_record.assert_called_once_with(self.dataset_name, f"{self.snapshot_date}:1", "job_status", "DISABLED")

        expected_result = {
            "job_version": 1,
            "job_status": "DISABLED"
        }
        self.assertEqual(result, expected_result)

    def test_non_empty_dep_dict_no_items(self):
        # Test when dep_dict is not empty and no items in audit table
        self.audit_table.get_audit_record.return_value = []
        self.audit_table.insert_audit_record.return_value = 1
        self.audit_table.update_audit_record.return_value = None

        dep_dict = {"dep1": "value1"}
        result = get_dependencies_from_dynamo(self.dataset_name, self.snapshot_date, self.audit_table, dep_dict)

        self.audit_table.insert_audit_record.assert_called_once_with(self.dataset_name, self.snapshot_date)
        self.audit_table.update_audit_record.assert_any_call(self.dataset_name, f"{self.snapshot_date}:1", "dependencies", json.dumps(dep_dict))
        self.audit_table.update_audit_record.assert_any_call(self.dataset_name, f"{self.snapshot_date}:1", "job_status", "WAITING")

        self.assertEqual(result["job_version"], 1)
        self.assertEqual(result["job_status"], {"S": "WAITING"})

    def test_non_empty_dep_dict_with_items(self):
        # Test when dep_dict is not empty and there are items in audit table
        latest_snapshot_date = "2024-07-19:1"
        latest_record = {
            "snapshot_date": latest_snapshot_date,
            "job_status": "WAITING",
            "dependencies": json.dumps({"dep1": None})
        }
        self.audit_table.get_audit_record.return_value = [latest_record]
        self.audit_table.update_audit_record.return_value = None

        dep_dict = {"dep1": "value1"}
        result = get_dependencies_from_dynamo(self.dataset_name, self.snapshot_date, self.audit_table, dep_dict)

        self.audit_table.update_audit_record.assert_called_with(self.dataset_name, f"{self.snapshot_date}:1", "dependencies", json.dumps({"dep1": "value1"}))
        self.assertEqual(result["job_version"], 1)
        self.assertEqual(result["job_status"], {"S": "WAITING"})

    def test_all_dependencies_complete(self):
        # Test when all dependencies are complete
        latest_snapshot_date = "2024-07-19:1"
        latest_record = {
            "snapshot_date": latest_snapshot_date,
            "job_status": "WAITING",
            "dependencies": json.dumps({"dep1": "value1"})
        }
        self.audit_table.get_audit_record.return_value = [latest_record]
        self.audit_table.get_audit_record_by_version.return_value = latest_record
        self.audit_table.update_audit_record.return_value = {"job_status": "DEPS_COMPLETE"}

        dep_dict = {"dep1": "value1"}
        result = get_dependencies_from_dynamo(self.dataset_name, self.snapshot_date, self.audit_table, dep_dict)

        self.audit_table.update_audit_record.assert_called_with(self.dataset_name, f"{self.snapshot_date}:1", "job_status", "DEPS_COMPLETE")
        self.assertEqual(result["job_version"], 1)
        self.assertEqual(result["job_status"], "DEPS_COMPLETE")

if __name__ == '__main__':
    unittest.main()


====

import unittest
from unittest.mock import MagicMock
import json

class TestGetDependenciesFromDynamo(unittest.TestCase):

    def setUp(self):
        self.dataset_name = "test_dataset"
        self.snapshot_date = "2024-07-19"
        self.audit_table = MagicMock()

    def test_empty_dep_dict(self):
        # Test when dep_dict is empty
        self.audit_table.insert_audit_record.return_value = 1
        self.audit_table.update_audit_record.return_value = {"job_status": "DISABLED"}

        result = get_dependencies_from_dynamo(self.dataset_name, self.snapshot_date, self.audit_table, {})

        self.audit_table.insert_audit_record.assert_called_once_with(self.dataset_name, self.snapshot_date)
        self.audit_table.update_audit_record.assert_called_once_with(self.dataset_name, f"{self.snapshot_date}:1", "job_status", "DISABLED")

        expected_result = {
            "job_version": 1,
            "job_status": {"S": "DISABLED"}
        }
        self.assertEqual(result, expected_result)

    def test_non_empty_dep_dict_no_items(self):
        # Test when dep_dict is not empty and no items in audit table
        self.audit_table.get_audit_record.return_value = []
        self.audit_table.insert_audit_record.return_value = 1
        self.audit_table.update_audit_record.return_value = {"job_status": "WAITING"}

        dep_dict = {"dep1": "value1"}
        result = get_dependencies_from_dynamo(self.dataset_name, self.snapshot_date, self.audit_table, dep_dict)

        self.audit_table.insert_audit_record.assert_called_once_with(self.dataset_name, self.snapshot_date)
        self.audit_table.update_audit_record.assert_any_call(self.dataset_name, f"{self.snapshot_date}:1", "dependencies", json.dumps(dep_dict))
        self.audit_table.update_audit_record.assert_any_call(self.dataset_name, f"{self.snapshot_date}:1", "job_status", "WAITING")

        expected_result = {
            "job_version": 1,
            "job_status": {"S": "WAITING"}
        }
        self.assertEqual(result, expected_result)

    def test_non_empty_dep_dict_with_items_waiting(self):
        # Test when dep_dict is not empty and there are items in audit table with status WAITING
        latest_snapshot_date = "2024-07-19:1"
        latest_record = {
            "snapshot_date": latest_snapshot_date,
            "job_status": "WAITING",
            "dependencies": json.dumps({"dep1": None})
        }
        self.audit_table.get_audit_record.return_value = [latest_record]
        self.audit_table.update_audit_record.return_value = None

        dep_dict = {"dep1": "value1"}
        result = get_dependencies_from_dynamo(self.dataset_name, self.snapshot_date, self.audit_table, dep_dict)

        self.audit_table.update_audit_record.assert_called_with(self.dataset_name, f"{self.snapshot_date}:1", "dependencies", json.dumps({"dep1": "value1"}))

        expected_result = {
            "job_version": "1",
            "job_status": {"S": "WAITING"},
            "dependencies": json.dumps({"dep1": "value1"})
        }
        self.assertEqual(result["job_version"], "1")
        self.assertEqual(result["job_status"], {"S": "WAITING"})

    def test_non_empty_dep_dict_with_items_complete(self):
        # Test when dep_dict is not empty and there are items in audit table with status other than WAITING
        latest_snapshot_date = "2024-07-19:1"
        latest_record = {
            "snapshot_date": latest_snapshot_date,
            "job_status": "COMPLETE",
            "dependencies": json.dumps({"dep1": "value1"})
        }
        self.audit_table.get_audit_record.return_value = [latest_record]
        self.audit_table.insert_audit_record.return_value = 2
        self.audit_table.update_audit_record.return_value = None

        dep_dict = {"dep1": "value1"}
        result = get_dependencies_from_dynamo(self.dataset_name, self.snapshot_date, self.audit_table, dep_dict)

        self.audit_table.insert_audit_record.assert_called_once_with(self.dataset_name, self.snapshot_date)
        self.audit_table.update_audit_record.assert_any_call(self.dataset_name, f"{self.snapshot_date}:2", "job_status", "WAITING")

        expected_result = {
            "job_version": 2,
            "job_status": {"S": "WAITING"}
        }
        self.assertEqual(result["job_version"], 2)
        self.assertEqual(result["job_status"], {"S": "WAITING"})

    def test_all_dependencies_complete(self):
        # Test when all dependencies are complete
        latest_snapshot_date = "2024-07-19:1"
        latest_record = {
            "snapshot_date": latest_snapshot_date,
            "job_status": "WAITING",
            "dependencies": json.dumps({"dep1": "value1"})
        }
        self.audit_table.get_audit_record.return_value = [latest_record]
        self.audit_table.get_audit_record_by_version.return_value = latest_record
        self.audit_table.update_audit_record.return_value = {"job_status": "DEPS_COMPLETE"}

        dep_dict = {"dep1": "value1"}
        result = get_dependencies_from_dynamo(self.dataset_name, self.snapshot_date, self.audit_table, dep_dict)

        self.audit_table.update_audit_record.assert_called_with(self.dataset_name, f"{self.snapshot_date}:1", "job_status", "DEPS_COMPLETE")
        self.assertEqual(result["job_version"], "1")
        self.assertEqual(result["job_status"], "DEPS_COMPLETE")

if __name__ == '__main__':
    unittest.main()


