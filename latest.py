import unittest
from unittest.mock import MagicMock
import json

def get_dependencies_from_dynamo(dataset_name, snapshot_date, audit_table, dep_dict):
    if not dep_dict:
        ver = audit_table.insert_audit_record(dataset_name, snapshot_date)
        rec = audit_table.update_audit_record(dataset_name, snapshot_date + f":{str(ver)}", "job_status", "DISABLED")
        record = {
            "job_version": ver,
            "job_status": rec["job_status"]
        }
        return record
    else:
        items = audit_table.get_audit_record(dataset_name, snapshot_date)
        if not items:
            ver = audit_table.insert_audit_record(dataset_name, snapshot_date)
            audit_table.update_audit_record(dataset_name, snapshot_date + f":{str(ver)}", "dependencies", json.dumps(dep_dict))
            up_response = audit_table.update_audit_record(dataset_name, snapshot_date + f":{str(ver)}", "job_status", "WAITING")
            job_version = ver
        else:
            latest_rec = max(items, key=lambda rec: int(rec["snapshot_date"].split(":")[1]))
            job_version = str(int(latest_rec["snapshot_date"].split(":")[1]))
            dependencies = json.loads(latest_rec["dependencies"])
            if latest_rec["job_status"] == "WAITING":
                for dep_job in dependencies:
                    if dependencies[dep_job] is None:
                        dependencies[dep_job] = dep_dict[dep_job]
                audit_table.update_audit_record(dataset_name, snapshot_date + f":{job_version}", "dependencies", json.dumps(dependencies))
            else:
                job_version = audit_table.insert_audit_record(dataset_name, snapshot_date)
                up_response = audit_table.update_audit_record(dataset_name, snapshot_date + f":{job_version}", "job_status", "WAITING")
                for dep_job in dependencies:
                    if dep_dict[dep_job] is not None:
                        dependencies[dep_job] = dep_dict[dep_job]
                audit_table.update_audit_record(dataset_name, snapshot_date + f":{job_version}", "dependencies", json.dumps(dependencies))

        record = audit_table.get_audit_record_by_version(dataset_name, snapshot_date, job_version)
        record["job_version"] = job_version
        record["job_status"] = {"S": record["job_status"]}
        dep_dict = json.loads(record["dependencies"])
        if None not in dep_dict.values():
            rec_val = audit_table.update_audit_record(dataset_name, snapshot_date + f":{job_version}", "job_status", "DEPS_COMPLETE")
            record["job_status"] = rec_val["job_status"]
        return record


class TestGetDependenciesFromDynamo(unittest.TestCase):

    def setUp(self):
        self.dataset_name = "test_dataset"
        self.snapshot_date = "2024-07-19"
        self.audit_table = MagicMock()

    def test_non_empty_dep_dict_no_items(self):
        # Test when dep_dict is not empty and no items in audit table
        self.audit_table.get_audit_record.return_value = []
        self.audit_table.insert_audit_record.return_value = 1
        self.audit_table.update_audit_record.side_effect = [
            None,
            {"job_status": "WAITING"}
        ]

        dep_dict = {"dep1": "value1"}
        result = get_dependencies_from_dynamo(self.dataset_name, self.snapshot_date, self.audit_table, dep_dict)

        self.audit_table.insert_audit_record.assert_called_once_with(self.dataset_name, self.snapshot_date)
        self.audit_table.update_audit_record.assert_any_call(self.dataset_name, f"{self.snapshot_date}:1", "dependencies", json.dumps(dep_dict))
        self.audit_table.update_audit_record.assert_any_call(self.dataset_name, f"{self.snapshot_date}:1", "job_status", "WAITING")

        expected_result = {
            "job_version": 1,
            "job_status": {"S": "WAITING"}
        }
        self.assertEqual(result["job_version"], 1)
        self.assertEqual(result["job_status"], {"S": "WAITING"})

if __name__ == '__main__':
    unittest.main()
