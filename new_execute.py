import json

def get_dependencies_from_dynamo(dataset_name, snapshot_date, audit_table, dep_dict):
    if not dep_dict:
        ver = audit_table.insert_audit_record(dataset_name, snapshot_date)
        rec = audit_table.update_audit_record(dataset_name, snapshot_date + f":{ver}", "job_status", "DISABLED")
        record = {
            "job_version": ver,
            "job_status": rec["job_status"]
        }
        return record
    else:
        items = audit_table.get_audit_record(dataset_name, snapshot_date)
        if not items:
            ver = audit_table.insert_audit_record(dataset_name, snapshot_date)
            audit_table.update_audit_record(dataset_name, snapshot_date + f":{ver}", "dependencies", json.dumps(dep_dict))
            up_response = audit_table.update_audit_record(dataset_name, snapshot_date + f":{ver}", "job_status", "WAITING")
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
