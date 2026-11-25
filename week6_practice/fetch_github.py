import subprocess
import json

# Uses Codespaces' built-in authenticated GitHub API tunnel (never blocked!)
raw_output = subprocess.check_output(["gh", "api", "repos/apache/airflow"])
data = json.loads(raw_output)

print("Status: Success!")
print("Repository:", data["full_name"])
print("Total Stars:", data["stargazers_count"])
print("Open Issues:", data["open_issues_count"])

