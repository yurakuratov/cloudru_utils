# cloudru_utils

Python helpers for working with Cloud.ru training/HPC jobs from notebooks and scripts.

This project provides a lightweight wrapper around the public Cloud.ru API for common job operations:
- submit jobs
- list/filter jobs
- inspect job status
- read job logs
- stop/delete jobs

It also supports a `client_lib`-based helper for quick job listing inside Cloud.ru environments.

## What is implemented

### Public API wrapper (`CloudRuAPIClient`)
- Service authentication via `client_id` / `client_secret`
- Automatic access token refresh
- Job operations:
  - `submit_job(...)`
  - `jobs(...)` (rich table output)
  - `job_status(job_id)` (rich panel output)
  - `job_logs(job_id, tail=..., verbose=..., region=...)`
  - `kill_job(job_id, region=...)`

### `client_lib` helper
- `show_current_jobs(...)` for quick listing of current jobs via `client_lib` (if available)

## Requirements

- Python 3.9+
- Packages:
  - `requests`
  - `rich`
- Optional:
  - `client_lib` (for `show_current_jobs` and automatic workspace detection on Cloud.ru machines)

## Installation

Clone the repository and install dependencies:

```bash
pip install requests rich
```

If you run on Cloud.ru and want `client_lib` features, make sure `client_lib` is available in your environment.

## Authentication and workspace

`CloudRuAPIClient` needs:
- `client_id`
- `client_secret`

Additionally, Cloud.ru job endpoints require workspace headers:
- `x_api_key`
- `x_workspace_id`

You can provide these explicitly, or (inside Cloud.ru environments with `client_lib`) let the client try to read them automatically from `client_lib.Environment()`.

How to get credentials:
- API key guide: https://cloud.ru/docs/console_api/ug/topics/guides__api_key
- Workspace profile/dev func guide: https://cloud.ru/docs/aicloud/mlspace/concepts/guides/guides__profile/profile__develop-func

## Quick start

```python
from cloudru_utils import CloudRuAPIClient, show_current_jobs

# Optional: quick client_lib-based jobs view
show_current_jobs(n_last=10)

# Public API client
cloud_client = CloudRuAPIClient(
    client_id="YOUR_CLIENT_ID",
    client_secret="YOUR_CLIENT_SECRET",
    # Optional if not auto-detected:
    # x_api_key="YOUR_X_API_KEY",
    # x_workspace_id="YOUR_WORKSPACE_ID",
)
```

## Typical usage

### List jobs

```python
cloud_client.jobs(n_last=10)
cloud_client.jobs(n_last=10, status_in=["Running", "Pending"])
cloud_client.jobs(n_last=10, status_not_in=["Completed"])
```

### Inspect one job

```python
job_id = "lm-mpi-job-xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
cloud_client.job_status(job_id)
cloud_client.job_logs(job_id, tail=50)
```

### Get current workspace info

```python
workspace = cloud_client.get_workspace_info()
print(workspace["name"], workspace["namespace"])

# allocations are cached on the client after the call
print(cloud_client.workspace_allocations)

# pretty rich output (panel + allocations table)
cloud_client.workspace_info(refresh=False)

# show allocation resource availability (auto-resolves allocation from workspace)
cloud_client.available_resources()

# only resources with available > 0
cloud_client.available_resources(only_available=True)

# explicit allocation id
cloud_client.available_resources(allocation_id="19a9b0f1-34b2-4a2a-9e94-98f8b55b0538")

# force endpoint source
cloud_client.available_resources(source="instance_types_available")
cloud_client.available_resources(source="allocations_instance_types_availability")

# supported submit-ready instance_types for default region
cloud_client.instance_types()

# supported submit-ready instance_types for specific region
cloud_client.instance_types(region="SR006")
```

### Submit job

```python
resp = cloud_client.submit_job(
    script="ls",
    base_image="cr.ai.cloud.ru/aicloud-base-images/py3.11-torch2.4.0:0.0.40",
    instance_type="a100plus.1gpu.80vG.12C.96G",
    region="SR006",
    job_type="binary",
    n_workers=1,
    processes_per_worker=1,
    conda_env="/home/jovyan/your/env",
    env_variables={"HF_HOME": "/home/jovyan/data/.cache/huggingface"},
    job_desc="test job"
)
print(resp)
```

### Read job logs

```python
job_id = "lm-mpi-job-xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"

# last 10 lines
cloud_client.job_logs(job_id, tail=10)

# stream more logs with verbose output
cloud_client.job_logs(job_id, tail=100, verbose=True, region="SR006")
```

### Stop/delete job

```python
cloud_client.kill_job(job_id, region="SR006")
```

## API reference (implemented methods)

### `show_current_jobs(status_in=[], status_not_in=[], regions=['SR006'], n_last=-1)`
Display current jobs using `client_lib` output parsing.

### `CloudRuAPIClient(client_id, client_secret, x_api_key=None, x_workspace_id=None)`
Create API client and authenticate service account. Tries to auto-detect workspace info via `client_lib` if not provided.

### `submit_job(...)`
Submit a new job (binary/pytorch/pytorch2/horovod/pytorch_elastic/spark).
Many parameters are exposed; see method docstring for full list.

### `jobs(status_in=[], status_not_in=[], regions=['SR006'], n_last=1000, table_width=160)`
Show jobs in rich table with created time, status, region, GPU count, description, cost, and duration.

### `job_status(job_id)`
Show a formatted status panel for one job (created/pending/running/completed/error fields).

### `job_logs(job_id, tail=100, verbose=False, region='SR006')`
Stream and print logs for a job.

### `get_workspace_info(refresh=True)`
Get current workspace information from `/public/v2/workspaces/v3/{workspace_id}`.
Also stores connected allocations in `cloud_client.workspace_allocations`.

### `workspace_info(refresh=True)`
Print current workspace info in rich format (summary panel + allocations table).

### `available_resources(allocation_id=None, only_available=False, refresh_workspace=False, table_width=160, return_data=False, source='auto')`
Show allocation instance type availability in rich tables (one table per allocation).
Sort order: H100(A100+) -> A100 80GB -> A100 40GB -> V100 -> CPU, then by GPU count and RAM size.
If `allocation_id` is not provided, uses all current workspace allocations.
Includes `instance_type` mapping from `/public/v2/configs` (cached once per client) so values can be passed to `submit_job(instance_type=...)`.
In the output table, `instance_type` is submit-ready API value and `Instance Name` is human-readable label.
`source` controls backend endpoint: `auto` | `instance_types_available` | `allocations_instance_types_availability`.

### `instance_types(region=None, refresh_configs=False, table_width=160)`
Show supported instance types from `/public/v2/configs` for one region.
If `region` is not provided, uses workspace region when available, otherwise `SR006`.

### `kill_job(job_id, region='SR006')`
Delete/terminate a job.

## Notes and limitations

- The library is currently notebook-oriented (rich console output).
- `submit_job` includes many API parameters, but not all combinations are tested.
- Region defaults to `SR006` in most methods.
- Keep secrets out of notebooks and version control.
- If `client_lib` is not installed, `show_current_jobs` is unavailable, but `CloudRuAPIClient` still works with explicit workspace headers.

## Example notebook

See:
- `examples/cloudru_utils_example.ipynb`
