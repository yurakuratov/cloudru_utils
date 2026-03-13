try:
    import client_lib
except ImportError:
    print("client_lib is not available. Make sure you have client_lib installed.")
    CLIENT_LIB_AVAILABLE = False
CLIENT_LIB_AVAILABLE = True

import contextlib
import io

from rich.table import Table
from rich.console import Console
from rich.panel import Panel
from rich.text import Text

import requests
import time
from datetime import timedelta, datetime


def get_jobs(status_in=[], status_not_in=[], regions=['SR006']):
    if not CLIENT_LIB_AVAILABLE:
        print("Error: client_lib is not available. Cannot get jobs information.")
        return []

    jobs = []
    for region in regions:
        f = io.StringIO()
        with contextlib.redirect_stdout(f):
            client_lib.jobs(region=region)

        for line in f.getvalue().split('\n'):
            if len(line.strip()) > 0 and 'Cluster is not available' not in line:
                t, i, status = line.split(' : ')
                if (status in status_in or len(status_in) == 0) and status not in status_not_in:
                    jobs += [{'time': t, 'id': i, 'status': status, 'region': region}]
    jobs = sorted(jobs, key=lambda x: x['time'], reverse=True)
    return jobs


def show_current_jobs(status_in=[], status_not_in=[], regions=['SR006'], n_last=-1):
    if not CLIENT_LIB_AVAILABLE:
        print("Error: client_lib is not available. Cannot show jobs information.")
        return

    jobs = get_jobs(status_in=status_in, status_not_in=status_not_in, regions=regions)
    table = Table(title="Jobs")

    table.add_column("Created", justify="left", style="cyan")
    table.add_column("Job ID", justify="left", style="magenta")
    table.add_column("Status", justify="center", style="green")
    table.add_column("Region", justify="center", style="yellow")

    # Only show n_last jobs if n_last is positive
    if n_last > 0:
        jobs = jobs[:n_last]

    for job in jobs:
        status_style = {
            'Running': 'green',
            'Failed': 'red',
            'Terminated': 'red',
            'Pending': 'yellow',
            'Completed': 'cyan'
        }.get(job['status'], 'white')

        table.add_row(
            job['time'],
            job['id'],
            f"[{status_style}]{job['status']}[/{status_style}]",
            job['region']
        )

    console = Console()
    console.print(table)


class CloudRuAPIClient:
    """
    This class uses public cloud.ru API: https://api.ai.cloud.ru/public/v2/docs
    It can be used out of the cloud.ru machines if x_api_key and x_workspace_id are set.
    It can provide more detailed information than default client_lib, such as job description, duration, etc.

    usage:

    client = CloudRuAPIClient(client_id, client_secret, x_api_key, x_workspace_id)
    base_image = 'cr.ai.cloud.ru/aicloud-base-images/py3.11-torch2.4.0:0.0.40'
    client.submit_job('ls', base_image=base_image, instance_type='a100plus.1gpu.80vG.12C.96G', region='SR006',
                      job_type='binary', n_workers=1, job_desc='test job')
    job_id = '...'
    client.job_status(job_id)
    client.job_logs(job_id)
    client.job_logs(job_id, tail=10)
    client.kill_job(job_id)

    cloud_client.jobs(n_last=10)
    cloud_client.jobs(n_last=10, status_in=['Running', 'Pending'])
    cloud_client.jobs(n_last=10, status_not_in=['Completed'])
    cloud_client.jobs(n_last=10, table_width=150)
    """
    API_URL = 'https://api.ai.cloud.ru/public/v2'

    JOB_STATUSES = ['Completed', 'Completing', 'Deleted', 'Failed', 'Pending',
                    'Running', 'Stopped', 'Succeeded', 'Terminated']

    STATUS_STYLES = {
        'Running': 'green',
        'Failed': 'red', 
        'Terminated': 'red',
        'Stopped': 'red',
        'Pending': 'yellow',
        'Completed': 'cyan',
        'Succeeded': 'cyan',
        }

    def __init__(self, client_id, client_secret, x_api_key=None, x_workspace_id=None):
        """
        how to get client_id and client_secret:
        https://cloud.ru/docs/console_api/ug/topics/guides__api_key
        how to get x_api_key and x_workspace_id:
        https://cloud.ru/docs/aicloud/mlspace/concepts/guides/guides__profile/profile__develop-func
        """
        # todo: support multiple workspaces (multiple x_api_key and x_workspace_id)
        self.client_id = client_id
        self.client_secret = client_secret
        if x_api_key is None or x_workspace_id is None:
            try:
                import client_lib
                self.environment = client_lib.Environment()
                self.x_api_key = self.environment.GW_API_KEY
                self.x_workspace_id = self.environment.WORKSPACE_ID
            except ImportError:
                raise RuntimeError("client_lib is not installed, set x_api_key and x_workspace_id manually. Refer to:\n"
                    "https://cloud.ru/docs/aicloud/mlspace/concepts/guides/guides__profile/profile__develop-func")
        else:
            self.x_api_key = x_api_key
            self.x_workspace_id = x_workspace_id
        self._refresh_token()

    def _service_auth(self):
        response = requests.post(
            f'{self.API_URL}/service_auth',
            headers={
                'Content-Type': 'application/json',
                'Accept': 'application/json'
            },
            json={'client_id': self.client_id, 'client_secret': self.client_secret}
        )
        return response.json()

    def _refresh_token(self):
        """Refresh access token only when needed based on expiration time"""
        current_time = time.time()

        # check if we have a valid token that is not close to expiring
        # if token is about to expire in 60 seconds, refresh it
        if hasattr(self, 'access_token_expires_at') and current_time < self.access_token_expires_at - 60:
            return
        # get new access_token
        auth_response = self._service_auth()
        self.access_token = auth_response['token']['access_token']
        expires_in = auth_response['token']['expires_in']
        self.access_token_expires_at = current_time + expires_in

    def _get_jobs(self, region='SR006', offset=0, limit=1000, status_in=[], status_not_in=[]):
        """Get all jobs in workspace for specified region

        Args:
            region (str): Region code (default: SR006)
            offset (int): Pagination offset (default: 0)
            limit (int): Maximum number of jobs to return (default: 1000)

        Returns:
            Response from jobs API endpoint
        """
        self._refresh_token()
        url = f'{self.API_URL}/jobs'

        headers = {
            'accept': 'application/json',
            'x-api-key': self.x_api_key,
            'authorization': self.access_token,
            'x-workspace-id': self.x_workspace_id,
        }

        status = self.JOB_STATUSES[:] if len(status_in) == 0 else status_in
        status = [s for s in status if s not in status_not_in]

        params = {
            'region': region,
            'offset': offset,
            'limit': limit,
            'status': status,
        }

        response = requests.get(url, headers=headers, params=params)
        jobs_data = response.json()
        sorted_jobs = sorted(jobs_data['jobs'], key=lambda x: x['created_dt'], reverse=True)
        return sorted_jobs

    def _get_job_status(self, job_id):
        """Get status of a specific job

        Args:
            job_id (str): ID of the job to get status for

        Returns:
            Response from job status API endpoint
        """
        self._refresh_token()
        url = f'{self.API_URL}/jobs/{job_id}'

        headers = {
            'accept': 'application/json',
            'x-api-key': self.x_api_key,
            'authorization': self.access_token,
            'x-workspace-id': self.x_workspace_id,
        }

        response = requests.get(url, headers=headers)
        return response.json()

    def _get_job_logs(self, job_id, tail=100, verbose=False, region='SR006'):
        """Get logs for a specific job

        Args:
            job_id (str): ID of the job to get logs for
            tail (int): Number of log lines to return from the end, 0 - get all logs.
            verbose (bool): Whether to include verbose logs
            region (str): Region where the job is running

        Returns:
            Iterator yielding log lines as they arrive
        """
        url = f'{self.API_URL}/jobs/{job_id}/logs'

        headers = {
            'accept': 'text/plain',
            'x-api-key': self.x_api_key,
            'authorization': self.access_token,
            'x-workspace-id': self.x_workspace_id,
        }

        while True:
            self._refresh_token()
            params = {'tail': tail, 'verbose': verbose, 'region': region}
            response = requests.get(url, headers=headers, params=params, stream=True)
            try:
                for line in response.iter_lines():
                    if line:
                        yield line.decode('utf-8')
                break
            except requests.exceptions.ChunkedEncodingError:
                print('retrying to get job logs...')
                tail = 1  # get only last line
                time.sleep(1)

    def submit_job(self, script, base_image, instance_type, region='SR006', job_type='binary', n_workers=1,
                   processes_per_worker='default', job_desc=None, internet=True, conda_env=None, max_retry=None,
                   priority_class='medium', checkpoint_dir=None, flags=None, env_variables=None, pytorch_use_env=False,
                   elastic_min_workers='default', elastic_max_workers='default', elastic_max_restarts=5,
                   spark_executor_memory=None, health_params=None, stop_timer=0):
        """Submit a new training job to the AI Cloud platform.

        # Caution: not all parameters were tested, only those that are related to running gpu jobs

        Please refer to the following docs for more information:
        - https://cloud.ru/docs/aicloud/mlspace/concepts/client-lib__job
        - https://api.ai.cloud.ru/public/v2/docs#/training%20jobs/run_job_public_v2_jobs_post

        Args:
            script (str): Path to the script to run. The mount point is the root directory, so it should not be at the
                          beginning of the path. If your script is located at /data/demo_examples/script.py, you need to
                          specify /home/jovyan/data/demo_examples/script.py.
                          Note: The script must be located on the NFS of the region where the training task will be
                          launched.
            base_image (str): Base image in which the training script will be executed. The image must be from
                              cr.ai.cloud.ru/aicloud-base-images or from the Docker Registry project for the current
                              workspace.
            instance_type (str): Configuration of computing resources used to solve tasks.
                                E.g., a100plus.1gpu.80vG.12C.96G, check for all available instance types:
                                client_lib.get_instance_types(cluster_type=client_lib.ClusterType.MT)
            region (str, optional): Parameter allows you to select the region where computing resources are located.
                                   Available region keys:
                                   - DGX2-MT - Christofari.V100
                                   - A100-MT - Christofari.A100
                                   - SR002-MT - Cloud.Region.A100 (GPU Tesla A100)
                                   - SR003 - Cloud.Region.HP1
                                   - SR006 - Cloud.Region.HP
                                   Defaults to 'SR006'.
            job_type (str, optional): Type of job. Can be a machine learning framework or a binary executable file.
                                     Possible values:
                                     - 'binary' for executing binary files and shell scripts.
                                     - 'pytorch' for using the built-in pytorch.distributed mechanism.
                                     - 'pytorch2' for using the built-in pytorch.distributed mechanism.
                                     - 'horovod' for using the horovod library.
                                     - 'pytorch_elastic' for running a job with Pytorch Elastic Learning type.
                                     - 'spark' for running distributed training jobs using Spark.
                                     Defaults to 'binary'.
            n_workers (int, optional): Number of worker nodes in the region where the script will be executed.
                                      Defaults to 1.
            processes_per_worker (str or int, optional): Sets the number of processes per worker node if the number of
                                                        processes equal to the number of GPUs is not suitable.
                                                        Defaults to 'default'.
            job_desc (str, optional): Parameter allows you to set custom descriptions for the launched tasks.
                                     Defaults to None.
            internet (bool, optional): If True, internet and S3 access will be available. If False, internet and S3
                                      access will be blocked. Defaults to True.
            conda_env (str, optional): Parameter allows you to specify the name of the conda environment if the image
                                      has one and a non-standard python environment is used. Defaults to None.
            max_retry (int, optional): Maximum number of attempts to launch a task in case the first of these attempts
                                      failed with an error. Valid values are from 3 to 100 inclusive. Defaults to None.
            priority_class (str, optional): Priority class for the job. Possible values: 'low', 'medium', 'high'.
                                           Defaults to 'medium'.
            checkpoint_dir (str, optional): Path to the directory where training checkpoints are written.
                                          Example: /home/jovyan/my-checkpoints. Defaults to None.
            flags (dict, optional): Flags with which to run the script (if any in the training script).
                                   Defaults to None.
            env_variables (dict, optional): Parameter sets environment variables. Defaults to None.
            pytorch_use_env (bool, optional): Parameter duplicates the "use_env" flag in torch.distributed.launch and is
                                             needed if "local_rank" is passed through environments in the script, not
                                             through "argparse". Defaults to False.
            elastic_min_workers (str or int, optional): Parameter sets the minimum number of workers for Pytorch Elastic
                                                       Learning tasks. Possible to pass values greater than 0 or the
                                                       string 'default'. Defaults to 'default'.
            elastic_max_workers (str or int, optional): Parameter sets the maximum number of workers for Pytorch Elastic
                                                      Learning tasks. Possible to pass values greater than 0 or the
                                                      string 'default'. Defaults to 'default'.
            elastic_max_restarts (int, optional): Parameter sets the maximum number of restarts for Pytorch Elastic
                                                Learning tasks. Defaults to 5.
            spark_executor_memory (float, optional): Amount of memory in GB used by each Spark worker. Defaults to None.
            health_params (dict, optional): Set of parameters for monitoring hung tasks. Defaults to None.
            stop_timer (int, optional): Time in minutes until forced deletion of a task that has transitioned to the
                                       "Running" status. Defaults to 0 (task will not be forcibly deleted).

        Returns:
            dict: Response from job submission API endpoint
        """
        self._refresh_token()
        url = f'{self.API_URL}/jobs'

        headers = {
            'accept': 'application/json',
            'x-api-key': self.x_api_key,
            'authorization': self.access_token,
            'x-workspace-id': self.x_workspace_id,
            'Content-Type': 'application/json'
        }

        flags = flags or {}
        env_variables = env_variables or {}

        # Build request payload
        payload = {
            "script": script,
            "base_image": base_image,
            "instance_type": instance_type,
            "region": region,
            "type": job_type,
            "n_workers": n_workers,
            "processes_per_worker": processes_per_worker,
            "pytorch_use_env": pytorch_use_env,
            "elastic_min_workers": elastic_min_workers,
            "elastic_max_workers": elastic_max_workers,
            "elastic_max_restarts": elastic_max_restarts,
            "stop_timer": stop_timer,
            "internet": internet,
            "flags": flags,
            "env_variables": env_variables,
            "priority_class": priority_class,
        }

        # Add optional parameters if provided
        if job_desc:
            payload["job_desc"] = job_desc
        if conda_env:
            payload["conda_env"] = conda_env
        if max_retry:
            payload["max_retry"] = max_retry
        if checkpoint_dir:
            payload["checkpoint_dir"] = checkpoint_dir
        if spark_executor_memory:
            payload["spark_executor_memory"] = spark_executor_memory
        if health_params:
            payload["health_params"] = health_params

        response = requests.post(url, headers=headers, json=payload)
        return response.json()

    def kill_job(self, job_id, region='SR006'):
        """Kill/delete a specific job.

        Args:
            job_id (str): ID of the job to kill
            region (str, optional): Region where the job is running. Defaults to 'SR006'.

        Returns:
            dict: Response from job deletion API endpoint
        """
        self._refresh_token()
        url = f'{self.API_URL}/jobs/{job_id}'

        headers = {
            'accept': 'application/json',
            'x-api-key': self.x_api_key,
            'authorization': self.access_token,
            'x-workspace-id': self.x_workspace_id,
        }

        params = {'region': region}

        response = requests.delete(url, headers=headers, params=params)
        return response.json()

    def job_logs(self, job_id, tail=100, verbose=False, region='SR006'):
        """Print logs for a specific job.

        Args:
            job_id (str): ID of the job to get logs for
            tail (int, optional): Number of log lines to return from the end. Defaults to 100. 0 will get all logs.
            verbose (bool, optional): Whether to include verbose logs. Defaults to False.
            region (str, optional): Region where the job is running. Defaults to 'SR006'.
        """
        try:
            for log in self._get_job_logs(job_id, tail=tail, verbose=verbose, region=region):
                print(log)
        except KeyboardInterrupt:
            ...

    def job_status(self, job_id):
        """Get human readable status information for a job

        Args:
            job_id (str): ID of the job to get status for

        Returns:
            str: Formatted string with job status information
        """
        status = self._get_job_status(job_id)

        # Convert timestamps to datetime objects
        created = datetime.fromtimestamp(status.get('created_at', 0))
        pending = datetime.fromtimestamp(status.get('pending_at', 0))
        running = datetime.fromtimestamp(status.get('running_at', 0))
        completed = datetime.fromtimestamp(status.get('completed_at', 0))

        console = Console()

        status_text = Text()
        status_text.append("Job ID: ", style="bold")
        status_text.append(f"{status.get('job_name', 'Unknown')}\n")

        status_text.append("Status: ", style="bold")
        job_status = status.get('status', 'Unknown').capitalize()
        status_text.append(f"{job_status}\n", style=self.STATUS_STYLES.get(job_status, 'white'))

        status_text.append("Created: ", style="bold")
        status_text.append(f"{created.strftime('%Y-%m-%d %H:%M:%S')}\n")

        status_text.append("Pending: ", style="bold")
        status_text.append(f"{pending.strftime('%Y-%m-%d %H:%M:%S')}\n")

        status_text.append("Running: ", style="bold")
        status_text.append(f"{running.strftime('%Y-%m-%d %H:%M:%S')}\n")

        status_text.append("Completed: ", style="bold")
        status_text.append(f"{completed.strftime('%Y-%m-%d %H:%M:%S')}\n")

        status_text.append("Error code: ", style="bold red")
        status_text.append(f"{status['error_code']}\n")

        status_text.append("Error message: ", style="bold red")
        status_text.append(status['error_message'])

        panel = Panel(status_text, title="Job Status")
        return console.print(panel)

    def jobs(self, status_in=[], status_not_in=[], regions=['SR006'], n_last=1000, table_width=160):
        """Display a formatted table of jobs sorted by creation date"""
        jobs_data = []
        for region in regions:
            jobs_data += self._get_jobs(region=region, offset=0, limit=n_last, status_in=status_in,
                                        status_not_in=status_not_in)
        jobs_data = sorted(jobs_data, key=lambda x: x['created_dt'], reverse=True)

        # Create rich table
        table = Table(title="Jobs")

        # Add columns
        table.add_column("Created", justify="left", style="cyan")
        table.add_column("Job ID", no_wrap=True, justify="left", style="magenta")
        table.add_column("Status", justify="center", style="green")
        table.add_column("Region", justify="center", style="yellow")
        table.add_column("GPUs", justify="center")
        table.add_column("Description", overflow="fold")
        table.add_column("Cost", justify="right")
        table.add_column("Duration", justify="right")

        # job keys:
        # ['uid', 'job_name', 'status', 'region', 'instance_type', 'job_desc', 'created_dt', 'updated_dt',
        # 'completed_dt', 'cost', 'gpu_count', 'duration', 'namespace']

        # Add rows
        for job in jobs_data:
            status_style = self.STATUS_STYLES.get(job['status'], 'white')

            # Convert UTC to +3 timezone
            # todo: convert all dates in jobs_data to +3 timezone
            created_dt = datetime.strptime(job['created_dt'], '%Y-%m-%dT%H:%M:%SZ')
            created_dt = created_dt + timedelta(hours=3)
            created_dt_str = created_dt.strftime('%Y-%m-%dT%H:%M:%S')

            table.add_row(
                created_dt_str,
                job['job_name'],
                f"[{status_style}]{job['status']}[/{status_style}]",
                job['region'],
                str(job['gpu_count']),
                job['job_desc'],
                f"{float(job['cost']):.01f}",
                str(timedelta(seconds=int(job['duration'][:-1]))).zfill(8)
            )

        # Display table
        console = Console(width=table_width)
        console.print(table)
