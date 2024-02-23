import json
import time
from datetime import date

class Job:
    def __init__(self, name, dependencies=None, completed=False, callback=None):
        self.name = name
        self.dependencies = dependencies or []
        self.completed = completed
        self.callback = callback
        self.completed_callback_jobs = set()

    def add_dependency(self, job):
        self.dependencies.append(job)

    def mark_completed(self, all_jobs):
        if not self.completed:
            print(f"Running job: {self.name}")
            # Simulate job execution
            time.sleep(2)  # Simulating job runtime
            self.completed = True

            if self.callback:
                for callback_job in self.callback:
                    print(f"Running callback job: {callback_job.name}")
                    callback_job.run(all_jobs)
                    self.completed_callback_jobs.add(callback_job)

            # Update JSON file with today's date and set completed flag to true
            with open("../resources/jobs.json", "r+") as json_file:
                data = json.load(json_file)
                for job_data in data["jobs"]:
                    if job_data["name"] == self.name:
                        job_data["completed"] = True
                        job_data["completion_date"] = str(date.today())
                json_file.seek(0)
                json.dump(data, json_file, indent=4)
                json_file.truncate()

    def is_ready_to_run(self, all_jobs):
        return all(all_jobs[dependency].completed for dependency in self.dependencies)

    def run(self, all_jobs):
        if not self.completed and self.is_ready_to_run(all_jobs):
            self.mark_completed(all_jobs)

def create_jobs_from_json(jobs_data):
    jobs = {}
    for job_data in jobs_data["jobs"]:
        job = Job(
            job_data["name"],
            job_data["dependencies"],
            job_data["completed"],
            [jobs_data[callback] for callback in job_data.get("callback", [])],  # Change this line
        )
        jobs[job_data["name"]] = job
    return jobs

# Other functions remain unchanged

# Load jobs from JSON file
def load_jobs():
    with open("../resources/jobs.json", "r") as json_file:
        workflow_data = json.load(json_file)

    jobs = create_jobs_from_json(workflow_data)
    return jobs

# Run the selected job based on user input
def run_selected_job(user_input, all_jobs):
    if user_input not in all_jobs:
        print("Invalid job name. Exiting.")
        return

    selected_job = all_jobs[user_input]
    selected_job.run(all_jobs)

# Main function
def main():
    all_jobs = load_jobs()

    # User input for job name
    user_input = input("Enter the job name to run: ")
    run_selected_job(user_input, all_jobs)

# Entry point
if __name__ == "__main__":
    main()
