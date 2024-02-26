import json
import time

class Job:
    def __init__(self, name, dependencies=None, completed=False):
        self.name = name
        self.dependencies = dependencies or []
        self.completed = completed

    def add_dependency(self, job):
        self.dependencies.append(job)

    def mark_completed(self):
        self.completed = True

    def is_ready_to_run(self, all_jobs):
        return all(all_jobs[dependency].completed for dependency in self.dependencies)

    def run(self, all_jobs):
        if not self.completed and self.is_ready_to_run(all_jobs):
            print(f"Running job: {self.name}")
            # Simulate job execution
            time.sleep(2)  # Simulating job runtime
            self.mark_completed()

def create_jobs_from_json(jobs_data):
    jobs = {}
    for job_data in jobs_data["jobs"]:
        job = Job(
            job_data["name"],
            job_data["dependencies"],
            job_data["completed"]
        )
        jobs[job_data["name"]] = job
    return jobs

def run_dependent_jobs(job_name, all_jobs):
    if job_name not in all_jobs:
        print("Invalid job name. Exiting.")
        return

    selected_job = all_jobs[job_name]
    selected_job.run(all_jobs)

    # Run dependent jobs
    for job in all_jobs.values():
        if job_name in job.dependencies:
            job.run(all_jobs)

def main():
    with open("../resources/jobs.json", "r") as json_file:
        workflow_data = json.load(json_file)

    jobs = create_jobs_from_json(workflow_data)

    # User input for job name
    user_input = input("Enter the job name to run: ")

    run_dependent_jobs(user_input, jobs)

if __name__ == "__main__":
    main()
