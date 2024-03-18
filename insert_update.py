import boto3

# Initialize DynamoDB client
dynamodb = boto3.client('dynamodb')

def insert_job_audit(run_id, job_id, dependency_job_ids=None):
    try:
        # Prepare the item to be inserted
        item = {
            'run_id': {'S': run_id},
            'job_id': {'S': job_id},
            'dependency_job_ids': {'L': [] if dependency_job_ids is None else [{'M': {'dependency_job': {'S': dep_job}} for dep_job in dependency_job_ids]},
            'all_dependencies_completed': {'BOOL': False}
        }

        # Insert the item into job_audit table
        dynamodb.put_item(
            TableName='job_audit',
            Item=item
        )

        print("Job audit inserted successfully.")
    except Exception as e:
        print("Error inserting job audit:", e)

def update_job_audit(run_id, job_id, dependency_job_id):
    try:
        # Retrieve existing record from job_audit table
        response = dynamodb.get_item(
            TableName='job_audit',
            Key={'run_id': {'S': run_id}, 'job_id': {'S': job_id}}
        )
        item = response.get('Item')

        if not item:
            # If record doesn't exist, create a new one
            item = {
                'run_id': {'S': run_id},
                'job_id': {'S': job_id},
                'dependency_job_ids': {'L': []},
                'all_dependencies_completed': {'BOOL': False}
            }

        # Update dependency_job_ids list with new dependency_job_id
        item['dependency_job_ids']['L'].append({'M': {'dependency_job': {'S': dependency_job_id}, 'job_id': {'S': job_id}}})

        # Check if all dependencies have run
        if set(dep['M']['dependency_job']['S'] for dep in item['dependency_job_ids']['L']) == set(['job1', 'job2']):
            item['all_dependencies_completed'] = {'BOOL': True}

        # Update the record in job_audit table
        dynamodb.put_item(
            TableName='job_audit',
            Item=item
        )

        print("Job audit updated successfully.")
    except Exception as e:
        print("Error updating job audit:", e)

# Example usage: Insert a record for job1
insert_job_audit(run_id='run123', job_id='job123', dependency_job_ids=['job1'])

# Example usage: Update a record for job2
update_job_audit(run_id='run123', job_id='job123', dependency_job_id='job2')
