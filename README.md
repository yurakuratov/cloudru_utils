# cloudru_utils

Submit and monitor Cloud.ru training jobs and inspect resources from the `cloudru`
CLI or Python. You can use the tool on your computer or on a Cloud.ru machine.

This is an unofficial tool, not developed, supported, or endorsed by Cloud.ru.

Start here: [Installation](#installation) · [CLI Quick Start](#cli-quick-start) ·
[First job](#run-your-first-job) · [Run existing code](#run-code-already-available-for-the-job)

More: [Snapshot jobs](#run-a-snapshot-job) · [Common tasks](#common-tasks) ·
[Telegram bot](#optional-telegram-bot) · [Python API](#python-api) ·
[Configuration](#configuration-and-reference)

## Installation

Install Git and Python 3.9+. Use your existing Python environment, or optionally activate a virtual environment before installing.

```bash
git clone https://github.com/yurakuratov/cloudru_utils.git
cd cloudru_utils
pip install -e .
cloudru --version
```

Run the local example commands below from this repository's root directory.

## CLI Quick Start

A workspace is the Cloud.ru environment where your jobs run. Get your
[Cloud.ru access key](https://cloud.ru/docs/console_api/ug/topics/guides__api_key)
and open your workspace's
[Developer Parameters](https://cloud.ru/docs/aicloud/mlspace/concepts/guides/guides__profile/profile__develop-func).
You will enter these values during setup:

| Cloudru prompt | Value to enter |
| --- | --- |
| `client_id` | Cloud.ru Key ID |
| `client_secret` | Cloud.ru Key Secret |
| `x_api_key` | Workspace `x-api-key` |
| `x_workspace_id` | Workspace `x-workspace-id` |

Initialize the default profile, which stores your connection settings:

```bash
cloudru init
```

For `default region`, enter the region of the allocation you plan to use. An
allocation is the pool of computing resources assigned to your jobs.

Verify workspace access and list its allocations:

```bash
cloudru workspace info
cloudru allocations list
```

Initialization saves your settings; a successful workspace query confirms access.
Replace `<ALLOCATION_NAME_OR_ID>` below with a name or ID from the allocation list:

```bash
cloudru allocations use <ALLOCATION_NAME_OR_ID>
cloudru resources available
```

The selected allocation and its region are saved for later submissions. In the
resource report, find that allocation and copy an available `instance_type` value
for your first job.

## Run your first job

Create a local file named `job.yaml` with the following content. Replace
`REPLACE_WITH_INSTANCE_TYPE` with the value you selected above. Check that the
example image is available in your workspace, or choose an image that provides Python.

```yaml
job:
  script: python --version
  base_image: cr.ai.cloud.ru/aicloud-base-images/py3.11-torch2.4.0:0.0.40
  instance_type: REPLACE_WITH_INSTANCE_TYPE
  job_type: binary
  n_workers: 1
  processes_per_worker: 1
  job_desc: first-job
```

This uses the allocation and region saved in [CLI Quick Start](#cli-quick-start).
Preview the configuration and command:

```bash
cloudru jobs submit -f job.yaml --dry-run
```

This preview may authenticate with Cloud.ru. Fix any reported errors, then submit:

```bash
cloudru jobs submit -f job.yaml
```

Submission starts a job using the selected resources. It prints a `Job ID` and
ready-to-use status and log commands. Replace `<JOB_ID>` with that printed value:

```bash
cloudru jobs status <JOB_ID>
cloudru jobs logs <JOB_ID>
```

A successful run prints the Python version in its logs.

## Run code already available for the job

Script paths refer to files inside the image or on storage mounted into the job.
For this example, make sure a checkout of this repository is available at
`/home/jovyan/my-user/cloudru_utils` inside the job, in the selected region.
Replace that path with your actual checkout location.

The `setup` section prepares the job before your script runs. Use it to select a
Python environment, run preparation commands, and choose the working directory.
Here, `workdir` sets the directory from which the script runs:

Update `job.yaml`, keeping the image and instance type you used for the first job:

```yaml
setup:
  workdir: /home/jovyan/my-user/cloudru_utils
job:
  script: bash examples/example.sh
  base_image: cr.ai.cloud.ru/aicloud-base-images/py3.11-torch2.4.0:0.0.40
  instance_type: REPLACE_WITH_INSTANCE_TYPE
  job_type: binary
  n_workers: 1
  processes_per_worker: 1
  job_desc: example-script
```

Use the [preview, submit, and monitor commands above](#run-your-first-job).
The script creates `results/result.txt` under the remote working directory and
prints `Saved results/result.txt` in its logs.

By default, the script uses Python from the job image. To select an existing Conda
environment and check which Python will run, expand the same `setup` section:

```yaml
setup:
  conda_env: /home/jovyan/my-user/envs/training
  pre_command:
    - which python
    - python --version
  workdir: /home/jovyan/my-user/cloudru_utils
  print_pwd: true
```

Replace the environment path with one available inside the job. Its Python and
installed packages are then used by the preparation commands and your script.
`print_pwd` shows the working directory in the logs.

Setup steps run in this order, regardless of their order in YAML. Optional steps
run when configured:

| Order | Setting | What it does |
| --- | --- | --- |
| 1 | `setup.shell_init` | Initializes the shell, for example to make Conda available. |
| 2 | `setup.conda_env` | Activates your Python environment. |
| 3 | `setup.pre_command` | Runs preparation commands, in the order listed. |
| 4 | `setup.check_hf_auth` | Checks Hugging Face login with `hf auth whoami` when set to `true`. |
| 5 | `setup.workdir` | Changes to your project directory. |
| 6 | `setup.print_pwd` | Prints that directory when set to `true`. |
| 7 | `job.script` | Runs your command. |

Cloudru initializes Conda automatically when `conda_env` is set. Use `shell_init`
if your environment needs custom initialization. Preparation commands run before
the directory change, so use absolute paths for project files in `pre_command`.
Each step must succeed before the next one runs.

Use `--dry-run` to inspect the generated command. The
[full job example](examples/job.yaml) provides more settings; check its paths and
environment before using it.

## Run a snapshot job

A snapshot is a package of your local code. Cloudru uploads it to S3, starts a job,
and downloads the code before running your command. This example also uploads the
results to S3 through automatic output collection.

The source can be a directory, a single file, or a local Git repository. For sources
inside a Git repository, `.gitignore` rules apply to untracked files by default.

Complete [CLI Quick Start](#cli-quick-start) before continuing.

### 1. Prepare S3 access

Your computer uploads the source package. The job downloads it and uploads results.
Both need AWS CLI and its configuration and credentials files. Install
[AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html)
and get [Object Storage credentials](https://cloud.ru/docs/s3e/ug/topics/api__api-key)
with access to your bucket.

On your computer, configure the default AWS profile:

```bash
aws configure
```

For the example's `https://s3.cloud.ru` endpoint, enter these values as described in
[Cloud.ru's AWS CLI setup guide](https://cloud.ru/docs/s3e/ug/topics/tools__aws-cli):

| AWS prompt | Value |
| --- | --- |
| AWS Access Key ID | `<tenant_id>:<key_id>` from your Object Storage credentials |
| AWS Secret Access Key | The corresponding Key Secret |
| Default region name | `ru-central-1` |
| Default output format | `json` |

This creates `~/.aws/config` and `~/.aws/credentials`. In the example YAML, uncomment
`profile: default` under `s3.local` to select the local profile explicitly:

```yaml
s3:
  # Keep the other S3 settings in the example.
  local:
    profile: default
```

If `aws_profile` is already set in `~/.cloudru/config`, you can remove the whole
`s3.local` block instead. Do not leave it empty.

For the job, prepare these files on storage mounted at the path set by
`job.env_variables.HOME`: `<HOME>/.aws/config` and `<HOME>/.aws/credentials`.
For example, run `aws configure` in a notebook that shares this storage, using that
home directory and the default profile. The files must be visible at those paths
inside the job; they are **not copied from your computer**.
The job needs access to read the source package and write results in your bucket.

The job image must provide Bash and AWS CLI when it starts, before environment
activation. The job's Python environment must provide Python 3.9 or newer.

If you need a named AWS profile, use `--aws-profile NAME` or `s3.local.profile`
locally, and `job.env_variables.AWS_PROFILE` inside the job.

### 2. Configure the existing example

Edit [examples/snapshot-job-example.yaml](examples/snapshot-job-example.yaml).
Check these values before submitting:

| Setting | What to check or replace |
| --- | --- |
| `s3.endpoint_url` | Your S3 endpoint, such as `https://s3.cloud.ru`. |
| `s3.snapshot_prefix`, `s3.collect_outputs_to` | Replace `my-bucket/my-user` with your bucket and folder. Keep `${CLOUDRU_JOB_DIR_NAME}` in the output path. |
| `job.env_variables.HOME` | Your absolute home path inside the job, where the AWS files are available. |
| `job.base_image`, `job.instance_type`, `job.region` | An image and resources available in your workspace. |
| `job.allocation_name` | Your allocation, or omit it if you have selected a default with `cloudru allocations use`. |

The example packages the current directory, excluding data and generated files.
It runs `bash examples/example.sh`, which creates `results/result.txt`.
`outputs` selects the directories to upload; `s3.collect_outputs_to` gives each job a separate S3 folder.

### 3. Submit and monitor

Replace `<JOB_ID>` with the `Job ID` printed after submission.

```bash
cloudru jobs submit -f examples/snapshot-job-example.yaml --dry-run
cloudru jobs submit -f examples/snapshot-job-example.yaml
cloudru jobs status <JOB_ID>
cloudru jobs logs <JOB_ID>
```

Dry-run checks local configuration and previews the job. Remote credentials and
the image are checked when the job runs. Fix any preview errors before submitting.

In the logs, look for `Saved results/result.txt` to confirm the example script wrote its result.

### 4. Find the results

After `cloudru jobs submit`, find the `Collect outputs to` line in your terminal.
For example:

```text
Collect outputs to: s3://my-bucket/my-user/outputs/cloudru_utils-20261003-120000-ab12cd34ef56-1234-7f2a
```

Copy the final folder name after `outputs/` into `JOB_FOLDER`; the value below
matches the example above. Use the folder from your own submission.

After the job finishes, download all its outputs into a local folder. Replace the
bucket and endpoint with your settings. This uses your default AWS profile:

```bash
JOB_FOLDER='cloudru_utils-20261003-120000-ab12cd34ef56-1234-7f2a'
aws s3 cp --recursive "s3://my-bucket/my-user/outputs/$JOB_FOLDER/" "./$JOB_FOLDER/" --endpoint-url https://s3.cloud.ru
```

The example result is now at `./$JOB_FOLDER/results/result.txt`.

Collection runs after the script exits, including when it fails. Missing directories
are skipped. Resources remain allocated until uploads finish.

### Optional: use an existing Python environment

Set `conda_env` in the example's existing `setup` section:

```yaml
setup:
  conda_env: /home/jovyan/my-user/envs/training
```

Use an environment already available inside the job. See the startup reference
below if your image needs shell initialization to make Conda available.

### Optional: link shared data or output directories

A symlink lets the script use a directory on shared storage through a local path.
The target directories must be accessible inside the job.

Add these variables to the existing `job.env_variables` section, keeping `HOME`
and the other settings:

```yaml
job:
  env_variables:
    JOBS_DATA_DIR: /home/jovyan/my-user/data/my-dataset
    JOBS_RESULTS_DIR: /home/jovyan/my-user/shared-results
```

Append these commands to the existing `setup.pre_command` list:

```yaml
setup:
  pre_command:
    - 'test -d "$JOBS_DATA_DIR"'
    - 'mkdir -p "$JOBS_RESULTS_DIR"'
    - 'ln -sT "$JOBS_DATA_DIR" "$CLOUDRU_SOURCE_DIR/data"'
    - 'ln -sT "$JOBS_RESULTS_DIR" "$CLOUDRU_SOURCE_DIR/results"'
```

This checks that the dataset exists and creates the output directory if needed.
`ln -sT` refuses to overwrite an existing path. Keep `data` and `results` in
`snapshot.exclude`, as in the example, so these link paths are free after extraction.

If several jobs write through symlinks to the same output directory, do not use
automatic output collection for that directory. It can upload files from other jobs
while they are still being written.

What matters is the shared target directory, not the link name: collection follows
symlinks. Remove that directory from `outputs`, or turn collection off as shown below.

### Optional: turn off automatic output collection

Remove `outputs` and the unused `s3.collect_outputs_to` setting. Keep the other S3
settings needed to transfer the source code.

Results stay where the script writes them, including shared storage reached through
a symlink. Download or manage those files yourself when needed.

### Snapshot job reference

<details>
<summary>Reuse a snapshot; exclusions and size limits</summary>

Choose exactly one mode in `snapshot`:

| Field | Behavior |
| --- | --- |
| `source: .` | Package the current directory and upload it. |
| `archive: ./snapshots/project.tar.gz` | Validate and upload an existing Cloudru snapshot. |
| `uri: s3://my-bucket/my-user/snapshots/project.tar.gz` | Download an existing snapshot inside the job. No local AWS setup or upload prefix is needed. |

For `source`, options are `output_dir` (default `./snapshots`), `use_gitignore`
(default `true`, applied to untracked files), `exclude` (patterns), `exclude_from`
(pattern file), and `max_bytes` (default `1073741824`, or 1 GiB). The output directory
is excluded automatically when inside the source. Git is required for Git sources.
These capture options do not apply to `archive` or `uri`.

Each source submission captures a new snapshot. To retry uploading an existing
archive, reuse `snapshot.archive` or use `aws s3 cp`; copying to the same S3 key
replaces the object.

</details>

<details>
<summary>Configuration overrides and path rules</summary>

Snapshot storage and local AWS settings resolve in this order: CLI options, YAML,
selected Cloud.ru profile, then defaults. You can save defaults in `~/.cloudru/config`:

```ini
[default]
s3_snapshot_prefix = s3://my-bucket/my-user/snapshots
s3_endpoint_url = https://s3.cloud.ru
aws_profile = default
```

`--profile NAME` or `CLOUDRU_PROFILE` selects the Cloud.ru section; this is separate
from the AWS profile. Optional profile keys `aws_cli`, `aws_config_file`, and
`aws_credentials_file` override the AWS executable and file locations. In YAML,
these are `s3.local.aws_cli`, `s3.local.config_file`, and `s3.local.credentials_file`.
Both AWS files must exist. The credentials file must contain the selected profile
with an access key and secret key (and an optional session token).

Submit overrides are `--snapshot-s3-prefix`, `--s3-endpoint-url`, `--aws-profile`,
`--aws-cli`, `--aws-config-file`, and `--aws-credentials-file`. Explicit
`--use-gitignore` / `--no-use-gitignore` overrides YAML for source capture.

Relative local paths resolve from your terminal's current directory, not the YAML
file's location. This includes source, archive, output, exclusion-file, and AWS
paths. Local `~` expands on your computer.

`outputs` takes absolute directory paths with distinct final names. Job environment
variables can be used in these paths and `s3.collect_outputs_to`. Each directory is
uploaded under its final name (for example, `results/`). Files remain on the job's
storage after collection. Use `--collect-outputs-to` to override the S3 destination
and `--dry-run` to preview the paths.

</details>

<details>
<summary>Remote settings and startup order</summary>

Remote settings come from `job.env_variables`, `--env` overrides, and these defaults.
`HOME` is required and must be an absolute path. Explicit remote file and directory
paths must also be absolute.

| Variable | Default |
| --- | --- |
| `AWS_PROFILE` | `default` |
| `CLOUDRU_JOBS_ROOT` | `<HOME>/data/jobs` |
| `AWS_CONFIG_FILE` | `<HOME>/.aws/config` |
| `AWS_SHARED_CREDENTIALS_FILE` | `<HOME>/.aws/credentials` |
| `CLOUDRU_AWS_CLI` | `aws` in the startup PATH |

Cloudru sets four reserved variables: `CLOUDRU_JOB_DIR_NAME`, `CLOUDRU_JOB_DIR`,
`CLOUDRU_SOURCE_DIR`, and `CLOUDRU_SNAPSHOT_URI`. The job folder name combines the
sanitized snapshot name and a four-digit hexadecimal suffix. Source extracts into
`<CLOUDRU_JOBS_ROOT>/<CLOUDRU_JOB_DIR_NAME>/source`. An existing job directory is
rejected.

Startup runs in this order:

1. Apply remote settings and locate AWS CLI.
2. Run `setup.shell_init`, then activate `setup.conda_env` if set.
3. Download, validate, and extract the snapshot.
4. Run `setup.pre_command`, then the optional Hugging Face authentication check.
5. Change to `setup.workdir` (default: `$CLOUDRU_SOURCE_DIR`) and record diagnostics.
6. Run `job.script`, then collect outputs if enabled.

If needed, set `setup.shell_init` to `eval "$(conda shell.bash hook)"`. Initialization
and activation run once, before the snapshot is available. Put commands that need
source files in `setup.pre_command`; use `$CLOUDRU_SOURCE_DIR` because these commands
run before the working-directory change. Python is selected after activation:
`python`, or `python3` if `python` is absent, with version 3.9 or newer.

For snapshot jobs, `job.conda_env` also activates through setup and is omitted from
the API payload. Put command arguments in `job.script`; nonempty `job.flags` is
unsupported.

</details>

<details>
<summary>Diagnostics and supported jobs</summary>

`<CLOUDRU_JOB_DIR>/.cloudru/` contains the resolved `config.yaml`, `packages.txt`,
`system.txt`, `environment.startup.json`, `environment.prepared.json`, and
`status.json`. Environment captures may contain secrets. Job directories use mode
`0700` and diagnostic files `0600`. Optional diagnostic failures do not stop the
script. Console logs remain in Cloud.ru.

Snapshot jobs support one binary job, one worker, and one process per worker.
The startup wrapper is required; `--no-bootstrap` is unsupported.

</details>

### Create a snapshot without submitting a job

This is optional: snapshot jobs create the package during submission. To create a
local `.tar.gz` package separately:

```bash
cloudru snapshot . --exclude data --exclude runs --exclude results --dry-run
cloudru snapshot . --exclude data --exclude runs --exclude results
```

The package is saved in `./snapshots`. To create and upload a new snapshot using the
local S3 defaults from the reference above:

```bash
cloudru snapshot . --exclude data --exclude runs --exclude results --upload
```

The standalone command uses `--s3-prefix` instead of submit's `--snapshot-s3-prefix`.
Use `--dry-run` to preview without creating or uploading files, `--json` for structured
output, or `cloudru snapshot --help` for all options.

## Common tasks

Use the `Job ID` printed by submission or shown by `cloudru jobs list` wherever
`<JOB_ID>` appears below. Use `--help` on any command for its full options.

### Monitor and stop jobs

```bash
cloudru jobs list
```

| Task | Command |
| --- | --- |
| Inspect one job | `cloudru jobs status <JOB_ID>` |
| Read its latest log lines | `cloudru jobs logs <JOB_ID> --tail 50` |
| See recent finished jobs | `cloudru jobs finished --n 20` |
| Stop and delete a job | `cloudru jobs kill <JOB_ID>` |

<details>
<summary>Job filters and multiple deletions</summary>

Replace `<ALLOCATION_NAME>` with a name from `cloudru allocations list`.

```bash
cloudru jobs list --n 20 --status Running,Pending
cloudru jobs list --allocation-name <ALLOCATION_NAME>
cloudru jobs finished --status Completed,Succeeded --n 20
```

`cloudru jobs kill` accepts multiple job IDs. Add `--yes` to confirm deletion
without an interactive prompt.

</details>

### Inspect allocations and resources

Show the selected allocation's resource usage:

```bash
cloudru allocations resources
```

| Task | Command |
| --- | --- |
| Inspect allocation details | `cloudru allocations info` |
| Inspect jobs and notebooks on nodes | `cloudru allocations workloads` |
| Inspect running and waiting jobs in custom queues | `cloudru allocations queue` |
| Find available instance types | `cloudru resources available` |
| Report GPU usage | `cloudru resources used` |
| List supported instance types | `cloudru resources instance-types` |
| List accessible workspaces | `cloudru workspace list` |

<details>
<summary>Allocation defaults, filters, and overrides</summary>

`cloudru allocations use` shows your saved default; `--clear` removes it.
Allocation inspection commands accept either a UUID or an exact, case-sensitive
name. Replace `<ALLOCATION_NAME_OR_ID>` and `<REGION>` with your allocation and region:

```bash
cloudru allocations info <ALLOCATION_NAME_OR_ID> --json
cloudru allocations queue --status Pending
cloudru allocations workloads --type notebook
cloudru resources available --allocation <ALLOCATION_NAME_OR_ID>
cloudru resources available --all
cloudru resources used --region <REGION> --n 2000
```

For `resources available`, `--all` includes instance types with zero availability.
For `resources used`, `--all` queries all configured profiles and reports individual
profile errors while continuing with successful profiles.

</details>

### Wait for free GPUs

Retry submission when Cloud.ru reports that too few GPUs are free:

```bash
cloudru jobs submit -f job.yaml --retry
```

Cloudru retries every 60 seconds until the job is accepted, another error occurs,
or you press Ctrl+C.

<details>
<summary>Retry timing and snapshot preparation</summary>

Retries handle `PROJECT_GPU_LIMIT_REACHED_ONLY_<N>_FREE`. Use `--retry-interval 30`
to change the delay or `--retry-timeout 2h` to limit waiting to two hours. Both
require `--retry`; the default waiting time is unlimited.

Timeouts accept `s`, `m`, `h`, or `d`. Timing starts after preparation and upload;
a request already in progress may finish later. Snapshot preparation and upload
happen once, and local archives are kept if submission fails. With `--json`, retry
progress goes to stderr and the final response goes to stdout.

</details>

### Connect over SSH

For a running job, connect using your local OpenSSH client and a matching private
key from `ssh-agent` or OpenSSH's default key files:

```bash
cloudru jobs ssh <JOB_ID>
```

<details>
<summary>Explicit keys, worker selection, and connection preview</summary>

Replace `<PRIVATE_KEY_PATH>` with your local private-key file:

```bash
cloudru jobs ssh <JOB_ID> -i <PRIVATE_KEY_PATH>
cloudru jobs ssh <JOB_ID> --rank 1 -i <PRIVATE_KEY_PATH>
cloudru jobs ssh <JOB_ID> --dry-run
```

The default rank is 0, the master pod. Rank 1 selects `mpiworker-0`, rank 2 selects
`mpiworker-1`, and so on. Cloudru gets the workspace namespace and region-specific
SSH gateway from the API. `--dry-run` prints the resolved SSH command.

</details>

### Use an SSH configuration with VS Code

Generate an entry with an alias of your choice:

```bash
cloudru jobs ssh-config <JOB_ID> --alias cloudru-training
```

Paste the printed block into `~/.ssh/config`. Then run `ssh cloudru-training` or
select `cloudru-training` in VS Code Remote SSH.

<details>
<summary>SSH configuration options</summary>

Add `-i <PRIVATE_KEY_PATH>` to include a key file, or `--rank N` to select a worker.
With the default identity handling, the output includes comments explaining how to
configure your key. The default alias is `cloudru-<last-8-job-id-characters>-r<rank>`.

</details>

### Execute a command on a running job

Put the remote command after `--`:

```bash
cloudru jobs exec <JOB_ID> -- nvidia-smi
```

<details>
<summary>Interactive commands, keys, and shell syntax</summary>

`jobs exec` uses the SSH connection settings above and returns the remote command's
exit status. It accepts `-i <PRIVATE_KEY_PATH>`, `--rank N`, and `--dry-run`.

```bash
cloudru jobs exec <JOB_ID> --tty -- top
cloudru jobs exec <JOB_ID> -- bash -lc 'nvidia-smi | grep A100'
```

Arguments are quoted for the remote shell. Use `bash -lc` for pipes, redirects,
variable expansion, directory changes, or multiple commands.

</details>

### Report job costs

Show API-reported training-job costs for the configured region over the last 30 days:

```bash
cloudru resources cost --since 30d
```

<details>
<summary>Cost filters and interpretation</summary>

```bash
cloudru resources cost --all --group-by profile,region,n_gpus
cloudru resources cost --n-gpus 1
cloudru resources cost --region SR003 --region SR006 --json
```

Replace the example regions with your own. `--since` accepts durations such as
`30m`, `12h`, `30d`, and `4w`, and filters by job creation time in UTC.
Grouping supports `profile`, `region`, and `n_gpus`; the default is `profile,region`.
With `--all`, each profile uses its configured region unless `--region` overrides it.

Regions with zero total reported cost are omitted, including their job and GPU-hour
counts. Running jobs use their current accrued cost and duration. Values come from
the training-jobs API and are shown without an assumed currency. Use your billing
report for the complete project invoice or continuous allocation charges.

</details>

## Optional Telegram bot

The bot runs on your computer, checks configured profiles every 60 seconds, and
sends notifications when job statuses change.

Create a bot with [BotFather](https://core.telegram.org/bots/tutorial#obtain-your-bot-token).
In `~/.cloudru/telegram.ini`, replace `YOUR_TELEGRAM_BOT_TOKEN` with its token and
`123456789` with the numeric ID of the chat allowed to use the bot and receive notifications:

```ini
[bot]
token=YOUR_TELEGRAM_BOT_TOKEN
allowed_chat_ids=123456789
poll_interval_sec=60
```

For a private chat, send your bot a message and find `message.chat.id` in the
Telegram [getUpdates response](https://core.telegram.org/bots/api#getupdates) before
starting the bot. Multiple allowed chat IDs can be separated by commas.

```bash
cloudru bot run
```

Open the bot in Telegram and send `/start` to use its menus. Keep the local process
running to receive updates.

<details>
<summary>Bot commands and configuration overrides</summary>

Text commands are also available: `/help`, `/jobs [n] [profile]`,
`/status <job_id> [profile]`, `/logs <job_id> [tail] [profile]`,
`/resources_used [profile|all]`, `/resources_available [profile|all] [region]`, and
`/instance_types [profile|all] [region]`.

Environment overrides are `CLOUDRU_TELEGRAM_BOT_TOKEN`,
`CLOUDRU_TELEGRAM_ALLOWED_CHAT_IDS`, and `CLOUDRU_TELEGRAM_POLL_INTERVAL_SEC`.
Use `cloudru bot run --profile NAME --no-all` to monitor one profile.

</details>

## Python API

Create a client using the credentials described in [CLI Quick Start](#cli-quick-start).
Replace the four credential placeholders, then reuse `cloud_client` in the examples below:

```python
from cloudru_utils import CloudRuAPIClient

cloud_client = CloudRuAPIClient(
    client_id="YOUR_CLIENT_ID",
    client_secret="YOUR_CLIENT_SECRET",
    x_api_key="YOUR_X_API_KEY",
    x_workspace_id="YOUR_WORKSPACE_ID",
)
cloud_client.jobs(n_last=10)
```

To submit a job, choose an image, instance type, region, and allocation available
to your workspace. Replace the uppercase placeholders and check the example image:

```python
response = cloud_client.submit_job(
    script="python --version",
    base_image="cr.ai.cloud.ru/aicloud-base-images/py3.11-torch2.4.0:0.0.40",
    instance_type="REPLACE_WITH_INSTANCE_TYPE",
    region="REPLACE_WITH_REGION",
    allocation_name="REPLACE_WITH_ALLOCATION_NAME",
    job_type="binary",
    n_workers=1,
    processes_per_worker=1,
    job_desc="first-python-job",
)
job_id = response["job_name"]
cloud_client.job_status(job_id)
```

<details>
<summary>More Python examples</summary>

Replace the allocation and region placeholders with your own values. The `job_id`
variable below comes from a successful submission above.

```python
allocation = "REPLACE_WITH_ALLOCATION_NAME_OR_ID"
region = "REPLACE_WITH_REGION"
cloud_client.workspace_info(refresh=False)
cloud_client.allocations()
cloud_client.allocation_info(allocation)
cloud_client.allocation_resources(allocation)
cloud_client.allocation_queue(allocation, status_in=["Running"], n_last=50)
rows = cloud_client.allocation_queue(allocation, return_data=True, show_table=False)
cloud_client.allocation_workloads(allocation)
notebooks = cloud_client.allocation_workloads(
    allocation, types=["notebook"], return_data=True, show_table=False,
)
cloud_client.instance_types(region=region)
cloud_client.available_resources(allocation_id=allocation, only_available=True)
cloud_client.available_resources(allocation_id=allocation, source="allocation_instance_types")
cloud_client.used_resources(regions=[region], n_last=1000)
cloud_client.jobs(n_last=10, allocation_name="REPLACE_WITH_ALLOCATION_NAME")
cloud_client.job_logs(job_id, tail=100, verbose=True, region=region)
```

To stop and delete a job, call `cloud_client.kill_job(job_id, region=region)`.
Allocation inspection methods accept UUIDs or exact, case-sensitive names.

</details>

## Configuration and reference

Use `cloudru --help` or a command's `--help`, such as `cloudru jobs submit --help`,
for all CLI options.

<details>
<summary>Profiles, saved settings, and environment variables</summary>

The CLI uses the `default` profile unless you select another. To add and use a profile:

```bash
cloudru init --profile work
cloudru --profile work jobs list
```

Profile selection is `--profile`, then `CLOUDRU_PROFILE`, then `default`.
Each profile stores its credentials, defaults, and cached access token in these files:

| File | Contents |
| --- | --- |
| `~/.cloudru/credentials` | `client_id`, `client_secret`, `x_api_key`, `x_workspace_id` |
| `~/.cloudru/config` | Region, resource source, selected allocation, and snapshot storage defaults |
| `~/.cloudru/token_cache` | Cached access tokens |

`CLOUDRU_CLIENT_ID`, `CLOUDRU_CLIENT_SECRET`, `CLOUDRU_X_API_KEY`, and
`CLOUDRU_X_WORKSPACE_ID` override saved credentials. `CLOUDRU_REGION` and
`CLOUDRU_SOURCE` override the corresponding defaults.

Resource sources are `auto` (regional availability with allocation fallback),
`instance_types_available` (regional only), and `allocation_instance_types`
(allocation only). Keep `auto` for ordinary use.

For submission, explicit CLI options override YAML fields. The selected allocation
fills an omitted `job.allocation_name`. Its region fills an omitted `job.region`
when using that allocation default; otherwise the configured region is used, with
`SR006` as the final fallback. See [snapshot job reference](#snapshot-job-reference)
for S3 settings and path rules.

</details>

<details>
<summary>Setup options for jobs using existing code</summary>

See [Run code already available for the job](#run-code-already-available-for-the-job)
for a setup example and execution order.

When `conda_env` is set, the default shell initialization is
`eval "$(conda shell.bash hook)"`. Set `shell_init` to customize it.
`print_pwd` defaults to `false`. `pre_command` accepts a string or a list of strings.
Use `job.env_variables` to set environment variables.

Setup and the script form one submitted command. Dry-run displays the merged
configuration and `Command to run`. The [full example](examples/job.yaml) shows
additional settings. Snapshot startup follows the order in its
[own reference](#snapshot-job-reference).

</details>

<details>
<summary>Submission overrides and structured output</summary>

```bash
cloudru jobs submit -f job.yaml --job-desc exp-001 --env WANDB_MODE=offline
cloudru jobs submit -f job.yaml --pre-command 'python -V'
cloudru jobs submit -f job.yaml --json
```

`--env KEY=VALUE` overrides individual environment variables. `--pre-command`
replaces the configured pre-command list; repeat the option to supply multiple
commands. Other overrides include `--allocation-name`, `--region`, `--workdir`,
and `--conda-env`.

Ordinary submission's `--json` returns the API response, whose `job_name` identifies
an accepted job. Snapshot submission returns `job_id`, `job_dir`, `snapshot_uri`,
and the API response, plus output and archive paths when applicable.

</details>

<details>
<summary>Shell completion</summary>

Install completion for your shell, then follow the printed instructions:

```bash
cloudru --install-completion
```

</details>

<details>
<summary>Python API reference</summary>

- `show_current_jobs(status_in=[], status_not_in=[], regions=['SR006'], n_last=-1)`
- `CloudRuAPIClient(client_id, client_secret, x_api_key=None, x_workspace_id=None, ...)`
- `submit_job(...)`
- `jobs(status_in=[], status_not_in=[], regions=['SR006'], n_last=1000, table_width=160, allocation_name=None)`
- `finished_jobs(regions=['SR006'], n_last=1000, status_in=None, table_width=160, return_data=False)`
- `job_status(job_id)`
- `job_ssh_target(job_id, rank=0)`
- `job_logs(job_id, tail=100, verbose=False, region='SR006')`
- `kill_job(job_id, region='SR006')`
- `get_workspace_info(refresh=True)`
- `workspace_info(refresh=True)`
- `workspaces(table_width=160, return_data=False, show_table=True)`
- `allocations(table_width=160, return_data=False, show_table=True)`
- `allocation_info(allocation_id, table_width=160, return_data=False, show_table=True)`
- `allocation_resources(allocation_id, table_width=160, return_data=False, show_table=True)`
- `allocation_queue(allocation_id, status_in=None, status_not_in=None, regions=None, queues=None, workspace_id=None, n_last=20, table_width=160, return_data=False, show_table=True, workspace_names=None, workspace_ids=None)`
- `allocation_workloads(allocation_id, types=None, status_in=None, status_not_in=None, n_last=None, table_width=160, return_data=False, show_table=True)`
- `instance_types(region=None, refresh_configs=False, table_width=160, return_data=False)`
- `available_resources(allocation_id=None, only_available=True, refresh_workspace=False, table_width=160, return_data=False, source='auto')`
- `used_resources(regions=['SR006'], n_last=1000, table_width=160, return_data=False, show_table=True)`

`allocation_info()`, `allocation_resources()`, `allocation_queue()`, and `allocation_workloads()` accept either an allocation UUID or an exact, case-sensitive allocation name.

`show_current_jobs` uses the optional Cloud.ru `client_lib` package.
`CloudRuAPIClient` uses the explicit credentials shown in the Python example.

</details>

## Example files

- [Example script](examples/example.sh): writes `results/result.txt`.
- [Job using existing code](examples/job.yaml): customize its remote paths and environment.
- [Snapshot job](examples/snapshot-job-example.yaml): packages local code and collects outputs.
- [Older example notebook](examples/cloudru_utils_example.ipynb): some examples may need updating.
