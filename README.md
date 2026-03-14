# cloudru_utils

Tools for working with Cloud.ru training/HPC jobs from CLI and Python.

Disclaimer: This is an unofficial tool, not developed, supported, or endorsed by Cloud.ru.

At a glance:
- `cloudru` CLI is built for day-to-day operations: initialize profiles, inspect workspace info, list/filter jobs, check status/logs, view supported/available instance types, and stop jobs.
- `CloudRuAPIClient` provides the same monitoring/control workflows programmatically and adds job submission (`submit_job`) for automation.
- The Python API is especially convenient in IPython/Jupyter notebooks for exploratory workflows, live monitoring, and quick iteration.
- Toolkit includes profile-based credentials, per-profile token cache, and mapping from available resources to submit-ready `instance_type` values.

## Installation

```bash
pip install -e .
```

## Credentials and workspace

Both CLI and Python API need:
- `client_id`
- `client_secret`

Most job endpoints also require workspace headers:
- `x_api_key`
- `x_workspace_id`

How to get credentials:
- API key guide: https://cloud.ru/docs/console_api/ug/topics/guides__api_key
- Workspace profile/dev func guide: https://cloud.ru/docs/aicloud/mlspace/concepts/guides/guides__profile/profile__develop-func

## CLI Quick Start

```bash
# Initialize profile (stored in ~/.cloudru)
cloudru init --profile default

# Install shell completion (Typer)
cloudru --install-completion

# Check workspace and jobs
cloudru workspace info
cloudru jobs list

# See currently available resources
cloudru resources available
```

## CLI Usage

Main commands:

```bash
cloudru workspace info
cloudru resources instance-types --region SR006
cloudru resources available
cloudru resources available --all
cloudru jobs list
cloudru jobs list --n 20 --status Running,Pending
cloudru jobs status lm-mpi-job-xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
cloudru jobs logs lm-mpi-job-xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx --tail 50
cloudru jobs kill lm-mpi-job-xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
```

Config files:
- `~/.cloudru/credentials` - `client_id`, `client_secret`, `x_api_key`, `x_workspace_id`
- `~/.cloudru/config` - defaults like `region`, `source`
- `~/.cloudru/token_cache` - cached access token per profile

Profile selection:
- `--profile`
- `CLOUDRU_PROFILE`

## Python API Quick Start

```python
from cloudru_utils import CloudRuAPIClient

cloud_client = CloudRuAPIClient(
    client_id="YOUR_CLIENT_ID",
    client_secret="YOUR_CLIENT_SECRET",
    x_api_key="YOUR_X_API_KEY",
    x_workspace_id="YOUR_WORKSPACE_ID",
)

cloud_client.jobs(n_last=10)
cloud_client.job_status("lm-mpi-job-xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx")
```

## Python API: Submit Job Example

```python
from cloudru_utils import CloudRuAPIClient

cloud_client = CloudRuAPIClient(
    client_id="YOUR_CLIENT_ID",
    client_secret="YOUR_CLIENT_SECRET",
    x_api_key="YOUR_X_API_KEY",
    x_workspace_id="YOUR_WORKSPACE_ID",
)

resp = cloud_client.submit_job(
    script="python train.py --epochs 1",
    base_image="cr.ai.cloud.ru/aicloud-base-images/py3.11-torch2.4.0:0.0.40",
    instance_type="a100plus.1gpu.80vG.12C.96G",
    region="SR006",
    job_type="binary",
    n_workers=1,
    processes_per_worker=1,
    conda_env="/home/jovyan/your/env",
    env_variables={"HF_HOME": "/home/jovyan/data/.cache/huggingface"},
    job_desc="quick smoke run",
)
print(resp)
```

Tip: use `cloud_client.instance_types(...)` and `cloud_client.available_resources(...)` to pick valid `instance_type` values.

## Python API Examples

```python
# Workspace info and allocations
cloud_client.workspace_info(refresh=False)

# Supported instance types in region
cloud_client.instance_types(region="SR006")

# Available resources (auto source/fallback)
cloud_client.available_resources(only_available=True)
cloud_client.available_resources(source="instance_types_available")

# Logs and stop job
job_id = "lm-mpi-job-xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
cloud_client.job_logs(job_id, tail=100, verbose=True, region="SR006")
cloud_client.kill_job(job_id, region="SR006")
```

## API Reference

- `show_current_jobs(status_in=[], status_not_in=[], regions=['SR006'], n_last=-1)`
- `CloudRuAPIClient(client_id, client_secret, x_api_key=None, x_workspace_id=None, ...)`
- `submit_job(...)`
- `jobs(status_in=[], status_not_in=[], regions=['SR006'], n_last=1000, table_width=160)`
- `job_status(job_id)`
- `job_logs(job_id, tail=100, verbose=False, region='SR006')`
- `kill_job(job_id, region='SR006')`
- `get_workspace_info(refresh=True)`
- `workspace_info(refresh=True)`
- `instance_types(region=None, refresh_configs=False, table_width=160, return_data=False)`
- `available_resources(allocation_id=None, only_available=True, refresh_workspace=False, table_width=160, return_data=False, source='auto')`

## Notes

- CLI currently focuses on monitoring/control workflows (no CLI submit command yet).
- `submit_job` exposes many API parameters; validate your runtime/env/image settings for your workspace.
- If `client_lib` is not installed, `show_current_jobs` is unavailable, but `CloudRuAPIClient` still works with explicit workspace headers.

## Example notebook

- `examples/cloudru_utils_example.ipynb`
