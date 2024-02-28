import json
import time
from datetime import datetime

class Job:
    def __init__(self, name, dependencies=None, completed=False, snapshot_date=None, job_frequency=None,execution_snapshot_date=None):
        self.name = name
        self.dependencies = dependencies or []
        self.completed = completed
        self.snapshot_date = snapshot_date
        self.job_frequency = job_frequency
        self.execution_snapshot_date = execution_snapshot_date

    def add_dependency(self, job):
        self.dependencies.append(job)

    def mark_completed(self,snapshot_date):
            self.completed = True




    def is_ready_to_run(self, all_jobs):
        return all(all_jobs[dependency].completed for dependency in self.dependencies)

    def run(self, all_jobs):
        if  not self.completed and self.is_ready_to_run(all_jobs):
            print(f"Running job: {self.name}")
            # Simulate job execution
            time.sleep(2)  # Simulating job runtime
            self.mark_completed(self.execution_snapshot_date)
        elif not self.completed:
            waiting_dependencies = [dependency for dependency in self.dependencies if not (all_jobs[dependency].completed and all_jobs[dependency].snapshot_date==all_jobs[dependency].execution_snapshot_date)]
            print(f"Waiting for dependencies to be completed for job {self.name}: {', '.join(waiting_dependencies)}")

def create_jobs_from_json(jobs_data,execution_snapshot_date):
    jobs = {}
    for job_data in jobs_data["jobs"]:
        job = Job(
            job_data["name"],
            job_data["dependencies"],
            job_data["completed"],
            job_data["snapshot_date"],
            job_data.get("job_frequency", None),
            execution_snapshot_date
        )
        jobs[job_data["name"]] = job
    return jobs

def run_dependent_jobs(job_name, all_jobs):
    if job_name not in all_jobs:
        print("Invalid job name. Exiting.")
        return

    # Run the selected job
    all_jobs[job_name].run(all_jobs)

    # Update JSON file
    with open("../resources/jobs.json", "w") as json_file:
        updated_json_data = {"jobs": []}
        for job in all_jobs.values():
            updated_json_data["jobs"].append({
                "name": job.name,
                "completed": job.completed,
                "snapshot_date": job.snapshot_date,
                "job_frequency": job.job_frequency,
                "dependencies": job.dependencies
            })
        json.dump(updated_json_data, json_file, indent=2)

    # Run dependent jobs recursively
    for job in all_jobs.values():
        if job_name in job.dependencies:
            run_dependent_jobs(job.name, all_jobs)

def main():
    with open("../resources/jobs.json", "r") as json_file:
        workflow_data = json.load(json_file)



    # Allow the user to input the job name and snapshot date
    user_input_job_name = input("Enter the job name to run: ")
    user_input_snapshot_date = input("Enter the snapshot date (YYYY-MM-DD): ")
    jobs = create_jobs_from_json(workflow_data,user_input_snapshot_date)
    # Set snapshot_date for the selected job
    if user_input_job_name in jobs:
        pass
        #jobs[user_input_job_name].snapshot_date = user_input_snapshot_date

    run_dependent_jobs(user_input_job_name, jobs)

if __name__ == "__main__":
    main()
