import boto3

class JobAuditTable:
    def __init__(self, table_name):
        self.dynamodb = boto3.client('dynamodb')
        self.table_name = table_name

    def insert_record(self, run_id, job_id, dependency_job_ids=None):
        try:
            item = {
                'run_id': {'S': run_id},
                'job_id': {'S': job_id},
                'dependency_job_ids': {'L': [] if dependency_job_ids is None else [{'M': {'dependency_job': {'S': dep_job}, 'job_id': {'S': job}}} for dep_job, job in dependency_job_ids]},
                'all_dependencies_completed': {'BOOL': False}
            }

            self.dynamodb.put_item(
                TableName=self.table_name,
                Item=item
            )

            print("Job audit inserted successfully.")
        except Exception as e:
            print("Error inserting job audit:", e)

class JobAuditRecord:
    def __init__(self, run_id, job_id, dependency_job_ids=None):
        self.run_id = run_id
        self.job_id = job_id
        self.dependency_job_ids = dependency_job_ids if dependency_job_ids else {}
        self.all_dependencies_completed = False

    def to_dynamodb_item(self):
        return {
            'run_id': {'S': self.run_id},
            'job_id': {'S': self.job_id},
            'dependency_job_ids': {'L': [{'M': {'dependency_job': {'S': dep_job}, 'job_id': {'S': job}}} for dep_job, job in self.dependency_job_ids.items()]},
            'all_dependencies_completed': {'BOOL': self.all_dependencies_completed}
        }

# Example usage:
job_audit_table = JobAuditTable('job_audit')

# Insert record for job1
job1_record = JobAuditRecord(run_id='run123', job_id='job123', dependency_job_ids={'dependency1': 'job123'})
job_audit_table.insert_record(run_id=job1_record.run_id, job_id=job1_record.job_id, dependency_job_ids=job1_record.dependency_job_ids)

# Update record for job2
job2_record = JobAuditRecord(run_id='run123', job_id='job123')
job2_record.dependency_job_ids['dependency2'] = 'job124'
job2_record.all_dependencies_completed = True
job_audit_table.insert_record(run_id=job2_record.run_id, job_id=job2_record.job_id, dependency_job_ids=job2_record.dependency_job_ids)
