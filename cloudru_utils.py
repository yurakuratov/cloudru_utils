try:
    import client_lib
    CLIENT_LIB_AVAILABLE = True
except ImportError:
    CLIENT_LIB_AVAILABLE = False

import contextlib
import io
import re

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
    client.workspace_info()
    client.instance_types()
    client.available_resources()
    client.kill_job(job_id)

    cloud_client.jobs(n_last=10)
    cloud_client.jobs(n_last=10, status_in=['Running', 'Pending'])
    cloud_client.jobs(n_last=10, status_not_in=['Completed'])
    cloud_client.jobs(n_last=10, table_width=150)
    """
    API_URL = 'https://api.ai.cloud.ru/public/v2'

    JOB_STATUSES = ['Completed', 'Completing', 'Deleted', 'Failed', 'Pending',
                    'Running', 'Stopped', 'Succeeded', 'Terminated']

    TERMINAL_JOB_STATUSES = ['Completed', 'Succeeded', 'Failed', 'Terminated', 'Stopped', 'Deleted']

    STATUS_STYLES = {
        'Running': 'green',
        'Failed': 'red',
        'Terminated': 'red',
        'Stopped': 'red',
        'Pending': 'yellow',
        'Completed': 'cyan',
        'Succeeded': 'cyan',
        }

    def __init__(
        self,
        client_id,
        client_secret,
        x_api_key=None,
        x_workspace_id=None,
        access_token=None,
        access_token_expires_at=None,
        token_persist_callback=None,
    ):
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
        self._workspace_info_cache = None
        self._workspace_allocations_cache = []
        self._configs_cache = None
        self._instance_types_by_region_cache = {}
        self._instance_types_normalized_by_region_cache = {}
        self._token_persist_callback = token_persist_callback

        if access_token:
            self.access_token = access_token
        if access_token_expires_at is not None:
            try:
                self.access_token_expires_at = float(access_token_expires_at)
            except (TypeError, ValueError):
                self.access_token_expires_at = 0

        self._refresh_token()

    def __repr__(self):
        return (
            "CloudRuAPIClient("
            f"client_id={self.client_id!r}, "
            f"client_secret={self.client_secret!r}, "
            f"x_api_key={self.x_api_key!r}, "
            f"x_workspace_id={self.x_workspace_id!r}, "
            f"access_token={getattr(self, 'access_token', None)!r}, "
            f"access_token_expires_at={getattr(self, 'access_token_expires_at', None)!r}, "
            f"workspace_info_cache={self._workspace_info_cache!r}, "
            f"workspace_allocations={self._workspace_allocations_cache!r}, "
            f"configs_cache={self._configs_cache!r}, "
            f"instance_types_by_region_cache={self._instance_types_by_region_cache!r}, "
            f"instance_types_normalized_by_region_cache={self._instance_types_normalized_by_region_cache!r}"
            ")"
        )

    def _service_auth(self):
        response = requests.post(
            f'{self.API_URL}/service_auth',
            headers={
                'Content-Type': 'application/json',
                'Accept': 'application/json'
            },
            json={'client_id': self.client_id, 'client_secret': self.client_secret},
            timeout=30,
        )
        try:
            data = response.json()
        except ValueError as exc:
            raise RuntimeError(
                f"Service auth failed (HTTP {response.status_code}) with non-JSON response"
            ) from exc

        if response.status_code >= 400:
            raise RuntimeError(f"Service auth failed (HTTP {response.status_code}): {data}")

        return data

    def _persist_token_cache(self):
        if not self._token_persist_callback:
            return
        try:
            self._token_persist_callback(self.access_token, self.access_token_expires_at)
        except Exception:
            pass

    def _refresh_token(self, force=False):
        """Refresh access token only when needed based on expiration time"""
        current_time = time.time()

        # check if we have a valid token that is not close to expiring
        # if token is about to expire in 60 seconds, refresh it
        if not force and hasattr(self, 'access_token_expires_at') and current_time < self.access_token_expires_at - 60:
            return
        # get new access_token
        auth_response = self._service_auth()
        token_data = auth_response.get('token') if isinstance(auth_response, dict) else None
        if not token_data or 'access_token' not in token_data:
            raise RuntimeError(
                "Service auth response does not contain access token. "
                "Check client_id/client_secret. "
                f"Response: {auth_response}"
            )

        self.access_token = token_data['access_token']
        expires_in = token_data.get('expires_in', 3600)
        try:
            expires_in = float(expires_in)
        except (TypeError, ValueError):
            expires_in = 3600
        self.access_token_expires_at = current_time + expires_in
        self._persist_token_cache()

    def _request_with_auth(self, method, url, headers=None, retry_on_auth=True, timeout=30, **kwargs):
        self._refresh_token()
        req_headers = dict(headers or {})
        if 'authorization' in req_headers:
            req_headers['authorization'] = self.access_token
        if 'Authorization' in req_headers:
            req_headers['Authorization'] = f'Bearer {self.access_token}'

        response = requests.request(method, url, headers=req_headers, timeout=timeout, **kwargs)
        if retry_on_auth and response.status_code in (401, 403):
            self._refresh_token(force=True)
            if 'authorization' in req_headers:
                req_headers['authorization'] = self.access_token
            if 'Authorization' in req_headers:
                req_headers['Authorization'] = f'Bearer {self.access_token}'
            response = requests.request(method, url, headers=req_headers, timeout=timeout, **kwargs)

        return response

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

        response = self._request_with_auth('get', url, headers=headers, params=params)
        try:
            jobs_data = response.json()
        except ValueError as exc:
            raise RuntimeError(
                f"Failed to decode jobs response for region={region} (HTTP {response.status_code})"
            ) from exc

        if response.status_code >= 400:
            raise RuntimeError(
                f"Jobs request failed for region={region} (HTTP {response.status_code}): {jobs_data}"
            )

        if not isinstance(jobs_data, dict) or 'jobs' not in jobs_data:
            raise RuntimeError(
                f"Unexpected jobs response format for region={region}. "
                f"Expected object with 'jobs'. Got: {jobs_data}"
            )

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

        response = self._request_with_auth('get', url, headers=headers)
        return response.json()

    def _get_workspace_info(self, workspace_id=None):
        """Get workspace information including connected allocations.

        Args:
            workspace_id (str, optional): Workspace ID. Defaults to current client workspace.

        Returns:
            dict: Response from workspace API endpoint
        """
        self._refresh_token()
        workspace_id = workspace_id or self.x_workspace_id
        url = f'{self.API_URL}/workspaces/v3/{workspace_id}'

        headers = {
            'accept': 'application/json',
            'authorization': self.access_token,
        }

        response = self._request_with_auth('get', url, headers=headers)
        return response.json()

    def _get_allocation_instance_types_availability(self, allocation_id):
        """Get current resource availability for allocation instance types.

        Args:
            allocation_id (str): Allocation ID

        Returns:
            list[dict]: Availability rows with `instance_type` and `available`
        """
        self._refresh_token()
        url = f'{self.API_URL}/allocations/{allocation_id}/instance_types_availability'

        headers = {
            'accept': 'application/json',
            'x-workspace-id': self.x_workspace_id,
            'x-api-key': self.x_api_key,
            'authorization': self.access_token,
        }

        response = self._request_with_auth('get', url, headers=headers)
        return response.json()

    def _get_instance_types_available(self, region, allocation_name):
        """Get available instance types for allocation using region endpoint.

        Args:
            region (str): Region key, e.g. SR006
            allocation_name (str): Allocation name

        Returns:
            dict: Response with `instance_types` array
        """
        self._refresh_token()
        url = f'{self.API_URL}/instance_types/{region}/available'

        headers = {
            'accept': 'application/json',
            'x-workspace-id': self.x_workspace_id,
            'x-api-key': self.x_api_key,
            'authorization': self.access_token,
        }

        params = {'allocation_name': allocation_name}
        response = self._request_with_auth('get', url, headers=headers, params=params)
        return response.json()

    def _get_configs(self, cluster_type='MT'):
        """Get platform configs (regions, instance types, images)."""
        self._refresh_token()
        url = f'{self.API_URL}/configs'

        headers = {
            'accept': 'application/json',
            'x-workspace-id': self.x_workspace_id,
            'x-api-key': self.x_api_key,
            'authorization': self.access_token,
        }

        params = {'cluster_type': cluster_type}
        response = self._request_with_auth('get', url, headers=headers, params=params)
        return response.json()

    @staticmethod
    def _normalize_instance_type_name(instance_type_name):
        normalized = instance_type_name.lower().strip()
        normalized = normalized.replace('gb ram', 'gbram')
        normalized = normalized.replace('gb', 'gb')
        normalized = normalized.replace('cpu-cores', 'cpu')
        normalized = normalized.replace('vcpu-cores', 'vcpu')
        normalized = re.sub(r'\s+', '', normalized)
        normalized = re.sub(r'[^a-z0-9+.]', '', normalized)
        return normalized

    def _load_instance_types_cache(self, refresh=False, cluster_type='MT'):
        """Load instance types by region from /configs once and cache them."""
        if self._configs_cache is not None and self._instance_types_by_region_cache and not refresh:
            return

        configs = self._get_configs(cluster_type=cluster_type)
        self._configs_cache = configs
        self._instance_types_by_region_cache = {}
        self._instance_types_normalized_by_region_cache = {}

        for region in configs.get('regions', []):
            region_key = region.get('key')
            if not region_key:
                continue

            exact_map = {}
            normalized_map = {}
            for instance_type in region.get('instances_types', []):
                instance_key = instance_type.get('key')
                instance_name = instance_type.get('name')
                if not instance_key or not instance_name:
                    continue
                exact_map[instance_name] = instance_key

                normalized_name = self._normalize_instance_type_name(instance_name)
                if normalized_name not in normalized_map:
                    normalized_map[normalized_name] = instance_key

            self._instance_types_by_region_cache[region_key] = exact_map
            self._instance_types_normalized_by_region_cache[region_key] = normalized_map

    def _resolve_instance_type_key(self, instance_type_name, region_key=None):
        """Resolve public instance type name to API instance key."""
        normalized_name = self._normalize_instance_type_name(instance_type_name)

        if region_key and region_key in self._instance_types_by_region_cache:
            exact_map = self._instance_types_by_region_cache.get(region_key, {})
            if instance_type_name in exact_map:
                return exact_map[instance_type_name]

            normalized_map = self._instance_types_normalized_by_region_cache.get(region_key, {})
            if normalized_name in normalized_map:
                return normalized_map[normalized_name]

        found_keys = set()
        for current_region, exact_map in self._instance_types_by_region_cache.items():
            if instance_type_name in exact_map:
                found_keys.add(exact_map[instance_type_name])

            normalized_map = self._instance_types_normalized_by_region_cache.get(current_region, {})
            if normalized_name in normalized_map:
                found_keys.add(normalized_map[normalized_name])

        if len(found_keys) == 1:
            return list(found_keys)[0]
        return None

    def _default_region_from_workspace(self):
        """Resolve default region from current workspace allocations."""
        if not self._workspace_allocations_cache:
            self.get_workspace_info(refresh=False)

        for allocation in self._workspace_allocations_cache:
            cluster_key = allocation.get('cluster_key')
            if cluster_key:
                return cluster_key
        return 'SR006'

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
            params = {'tail': tail, 'verbose': verbose, 'region': region}
            response = self._request_with_auth('get', url, headers=headers, params=params, stream=True)
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

        response = self._request_with_auth('post', url, headers=headers, json=payload)
        return response.json()

    def kill_job(self, job_id, region='SR006'):
        """Kill/delete a specific job.

        Args:
            job_id (str): ID of the job to kill
            region (str, optional): Region where the job is running. Defaults to 'SR006'.

        Returns:
            dict: Response from job deletion API endpoint
        """
        url = f'{self.API_URL}/jobs/{job_id}'

        headers = {
            'accept': 'application/json',
            'x-api-key': self.x_api_key,
            'authorization': self.access_token,
            'x-workspace-id': self.x_workspace_id,
        }

        params = {'region': region}

        response = self._request_with_auth('delete', url, headers=headers, params=params)
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

    def get_workspace_info(self, refresh=True):
        """Get current workspace info and cache connected allocations.

        Args:
            refresh (bool, optional): Force refresh from API. Defaults to True.

        Returns:
            dict: Workspace information
        """
        if not refresh and self._workspace_info_cache is not None:
            return self._workspace_info_cache

        self._workspace_info_cache = self._get_workspace_info(self.x_workspace_id)
        self._workspace_allocations_cache = self._workspace_info_cache.get('allocations', [])
        return self._workspace_info_cache

    @property
    def workspace_info_cache(self):
        return self._workspace_info_cache

    @property
    def workspace_allocations(self):
        return self._workspace_allocations_cache

    def _workspace_title_label(self):
        """Return human-readable workspace name for table titles."""
        if self._workspace_info_cache is None:
            try:
                self.get_workspace_info(refresh=False)
            except Exception:
                return 'Unknown workspace'

        info = self._workspace_info_cache or {}
        workspace_name = info.get('name') or 'Unknown workspace'
        return workspace_name

    def workspace_info(self, refresh=True):
        """Show human-readable workspace information in rich format.

        Args:
            refresh (bool, optional): Force refresh from API. Defaults to True.

        Returns:
            None: Prints formatted workspace info
        """
        info = self.get_workspace_info(refresh=refresh)
        allocations = info.get('allocations', [])

        info_text = Text()
        info_text.append("Workspace ID: ", style="bold")
        info_text.append(f"{info.get('id', 'Unknown')}\n")

        info_text.append("Name: ", style="bold")
        info_text.append(f"{info.get('name', 'Unknown')}\n")

        info_text.append("Namespace: ", style="bold")
        info_text.append(f"{info.get('namespace', 'Unknown')}\n")

        info_text.append("Project ID: ", style="bold")
        info_text.append(f"{info.get('project_id', 'Unknown')}\n")

        info_text.append("Project name: ", style="bold")
        info_text.append(f"{info.get('project_name', 'Unknown')}\n")

        info_text.append("Owner email: ", style="bold")
        info_text.append(f"{info.get('owner_email', 'Unknown')}\n")

        info_text.append("Allocations count: ", style="bold")
        info_text.append(f"{len(allocations)}")

        console = Console()
        console.print(Panel(info_text, title="Workspace Info"))

        allocations_table = Table(title="Workspace Allocations")
        allocations_table.add_column("Allocation ID", style="cyan")
        allocations_table.add_column("Name", style="magenta")
        allocations_table.add_column("Cluster key", style="yellow")
        allocations_table.add_column("Cluster name", style="green")

        for allocation in allocations:
            allocations_table.add_row(
                allocation.get('id', ''),
                allocation.get('name', ''),
                allocation.get('cluster_key', ''),
                allocation.get('cluster_name') or '',
            )

        console.print(allocations_table)

    @staticmethod
    def _resource_gpu_family(instance_type_name):
        upper_name = instance_type_name.upper()
        if 'H100' in upper_name or 'A100+' in upper_name:
            return 'H100(A100+)'
        if 'V100' in upper_name:
            return 'V100'
        if 'A100' in upper_name:
            vram_gb = CloudRuAPIClient._resource_gpu_vram_gb(instance_type_name)
            if vram_gb == 40:
                return 'A100 40GB'
            return 'A100 80GB'
        return 'CPU'

    @staticmethod
    def _resource_gpu_count(instance_type_name):
        match = re.search(r'(\d+)\s*GPU', instance_type_name, flags=re.IGNORECASE)
        return int(match.group(1)) if match else 0

    @staticmethod
    def _resource_gpu_vram_gb(instance_type_name):
        upper_name = instance_type_name.upper()
        if 'A100+' in upper_name or 'H100' in upper_name:
            return 80
        if 'V100' in upper_name:
            return 32

        match = re.search(r'A100\s*(\d+)\s*GB', upper_name)
        if not match:
            match = re.search(r'TESLA\s*A100\s*(\d+)\s*GB', upper_name)
        if match:
            return int(match.group(1))
        return 0

    @staticmethod
    def _resource_ram_gb(instance_type_name):
        match = re.search(r'(\d+)\s*(?:GB|Gb)\s*RAM', instance_type_name, flags=re.IGNORECASE)
        return int(match.group(1)) if match else 0

    @staticmethod
    def _resource_cpu_count(instance_type_name):
        match = re.search(r'(\d+(?:\.\d+)?)\s*(?:v)?CPU(?:-cores)?', instance_type_name, flags=re.IGNORECASE)
        return float(match.group(1)) if match else 0.0

    def instance_types(self, region=None, refresh_configs=False, table_width=160, return_data=False, show_table=True):
        """Show supported instance types for selected region.

        Args:
            region (str, optional): Region key. If not set, uses workspace region or SR006.
            refresh_configs (bool, optional): Force refresh of /configs cache.
            table_width (int, optional): Console table width.
            return_data (bool, optional): If True, returns parsed rows. Defaults to False.
            show_table (bool, optional): Print rich table output. Defaults to True.

        Returns:
            list[dict] | None: Sorted rows with instance type info when return_data=True.
        """
        self._load_instance_types_cache(refresh=refresh_configs, cluster_type='MT')

        region_key = region or self._default_region_from_workspace()

        if not self._configs_cache:
            console = Console(width=table_width)
            console.print(Panel('Unable to load configs.', title='Instance Types'))
            return []

        selected_region = None
        for region_data in self._configs_cache.get('regions', []):
            if region_data.get('key') == region_key:
                selected_region = region_data
                break

        console = Console(width=table_width)
        if selected_region is None:
            console.print(Panel(f'Region {region_key} was not found in /configs response.', title='Instance Types'))
            return []

        rows = []
        for instance in selected_region.get('instances_types', []):
            instance_type = instance.get('key')
            instance_name = instance.get('name', '')
            if not instance_type:
                continue

            gpu_family = self._resource_gpu_family(instance_name)
            gpu_count = self._resource_gpu_count(instance_name)
            ram_gb = self._resource_ram_gb(instance_name)
            cpu_count = self._resource_cpu_count(instance_name)

            if (ram_gb == 0 or cpu_count == 0.0) and instance.get('resource'):
                limits = instance.get('resource', {}).get('limits', {})
                memory = limits.get('memory', '')
                cpu = limits.get('cpu', '')
                mem_match = re.match(r'^(\d+)', str(memory))
                if mem_match and ram_gb == 0:
                    ram_gb = int(mem_match.group(1))
                try:
                    if cpu_count == 0.0:
                        cpu_count = float(cpu)
                except (TypeError, ValueError):
                    pass

            rows.append({
                'region': region_key,
                'instance_type': instance_type,
                'instance_name': instance_name,
                'gpu_family': gpu_family,
                'gpu_count': gpu_count,
                'cpu_count': cpu_count,
                'ram_gb': ram_gb,
            })

        family_order = {
            'H100(A100+)': 0,
            'A100 80GB': 1,
            'A100 40GB': 2,
            'V100': 3,
            'CPU': 4,
        }

        rows = sorted(
            rows,
            key=lambda row: (
                family_order.get(row['gpu_family'], 99),
                row['gpu_count'],
                row['ram_gb'],
                row['cpu_count'],
                row['instance_name'],
            ),
        )

        table = Table(title=f'Instance Types (Region: {region_key})')
        table.add_column('region', style='yellow')
        table.add_column('GPU Type', style='yellow')
        table.add_column('GPUs', justify='center')
        table.add_column('CPU', justify='right')
        table.add_column('RAM (GB)', justify='right')
        table.add_column('instance_type', style='cyan')
        table.add_column('Instance Name', style='magenta', overflow='fold')

        for row in rows:
            cpu_value = str(int(row['cpu_count'])) if row['cpu_count'].is_integer() else str(row['cpu_count'])
            table.add_row(
                row['region'],
                row['gpu_family'],
                str(row['gpu_count']),
                cpu_value,
                str(row['ram_gb']),
                row['instance_type'],
                row['instance_name'],
            )

        if show_table:
            if rows:
                console.print(table)
            else:
                console.print(Panel(f'No instance types found for region {region_key}.', title='Instance Types'))

        if return_data:
            return rows
        return None

    def available_resources(self, allocation_id=None, only_available=True, refresh_workspace=False, table_width=160,
                            return_data=False, source='auto', show_table=True):
        """Show current allocation resource availability in sorted rich tables.

        Args:
            allocation_id (str, optional): Allocation ID. If not provided, use all current workspace allocations.
            only_available (bool, optional): Show only rows with available > 0. Defaults to True.
            refresh_workspace (bool, optional): Refresh workspace info when resolving allocation automatically.
            table_width (int, optional): Console table width. Defaults to 160.
            return_data (bool, optional): If True, returns parsed rows by allocation. Defaults to False.
            source (str, optional): Data source strategy:
                - 'auto': try instance_types/{region}/available first, fallback to allocations endpoint
                - 'instance_types_available': use only /instance_types/{region}/available
                - 'allocations_instance_types_availability': use only /allocations/{id}/instance_types_availability
            show_table (bool, optional): Print rich table output. Defaults to True.

        Returns:
            dict[str, list[dict]] | None: Sorted rows by allocation when return_data=True.
        """
        valid_sources = {'auto', 'instance_types_available', 'allocations_instance_types_availability'}
        if source not in valid_sources:
            raise ValueError(f"Invalid source={source!r}. Use one of: {sorted(valid_sources)}")

        console = Console(width=table_width)

        if refresh_workspace or not self._workspace_allocations_cache:
            self.get_workspace_info(refresh=refresh_workspace)

        workspace_label = self._workspace_title_label()

        allocation_meta_by_id = {
            allocation.get('id'): {
                'region': allocation.get('cluster_key'),
                'name': allocation.get('name'),
            }
            for allocation in self._workspace_allocations_cache
            if allocation.get('id')
        }

        self._load_instance_types_cache(refresh=False, cluster_type='MT')

        if allocation_id is None:
            if not self._workspace_allocations_cache:
                if show_table:
                    console.print(Panel('No allocations found for current workspace.', title='Available Resources'))
                return {} if return_data else None

            allocation_ids = [allocation.get('id') for allocation in self._workspace_allocations_cache if allocation.get('id')]
            if not allocation_ids:
                if show_table:
                    console.print(Panel('No valid allocation IDs found in workspace.', title='Available Resources'))
                return {} if return_data else None
        else:
            allocation_ids = [allocation_id]

        all_results = {}

        for current_allocation_id in allocation_ids:
            if not current_allocation_id:
                if show_table:
                    console.print(Panel('Allocation ID is empty. Provide allocation_id explicitly.', title='Available Resources'))
                continue

            allocation_meta = allocation_meta_by_id.get(current_allocation_id, {})
            allocation_region = allocation_meta.get('region')
            allocation_name = allocation_meta.get('name')
            row_region = allocation_region or 'Unknown'

            normalized = []
            endpoint_errors = []

            use_new_endpoint = source in {'auto', 'instance_types_available'}
            use_old_endpoint = source in {'auto', 'allocations_instance_types_availability'}

            if use_new_endpoint:
                if allocation_region and allocation_name:
                    new_data = self._get_instance_types_available(allocation_region, allocation_name)
                    rows = new_data.get('instance_types', []) if isinstance(new_data, dict) else None
                    if isinstance(rows, list):
                        for row in rows:
                            instance_name = row.get('name', '')
                            instance_type = row.get('key')
                            available = int(row.get('count', 0))
                            normalized.append({
                                'region': row_region,
                                'instance_type': instance_type,
                                'instance_name': instance_name,
                                'available': available,
                                'gpu_family': self._resource_gpu_family(instance_name),
                                'gpu_count': self._resource_gpu_count(instance_name),
                                'ram_gb': self._resource_ram_gb(instance_name),
                                'cpu_count': self._resource_cpu_count(instance_name),
                            })
                    else:
                        endpoint_errors.append(new_data)
                else:
                    endpoint_errors.append({
                        'error': 'Cannot call instance_types_available without allocation name/region',
                        'allocation_id': current_allocation_id,
                    })

            if not normalized and use_old_endpoint:
                old_data = self._get_allocation_instance_types_availability(current_allocation_id)
                if isinstance(old_data, list):
                    for row in old_data:
                        instance_name = row.get('instance_type', '')
                        available = int(row.get('available', 0))
                        instance_type = self._resolve_instance_type_key(instance_name, region_key=allocation_region)
                        normalized.append({
                            'region': row_region,
                            'instance_type': instance_type,
                            'instance_name': instance_name,
                            'available': available,
                            'gpu_family': self._resource_gpu_family(instance_name),
                            'gpu_count': self._resource_gpu_count(instance_name),
                            'ram_gb': self._resource_ram_gb(instance_name),
                            'cpu_count': self._resource_cpu_count(instance_name),
                        })
                else:
                    endpoint_errors.append(old_data)

            if not normalized and endpoint_errors:
                if show_table:
                    console.print(Panel(str(endpoint_errors[-1]), title=f'Available Resources Error ({current_allocation_id})'))
                all_results[current_allocation_id] = []
                continue

            if only_available:
                normalized = [row for row in normalized if row['available'] > 0]

            family_order = {
                'H100(A100+)': 0,
                'A100 80GB': 1,
                'A100 40GB': 2,
                'V100': 3,
                'CPU': 4,
            }

            normalized = sorted(
                normalized,
                key=lambda row: (
                    family_order.get(row['gpu_family'], 99),
                    row['gpu_count'],
                    row['ram_gb'],
                    row['cpu_count'],
                    -row['available'],
                    row['instance_name'],
                )
            )

            table = Table(title=(
                f'Available Resources (Workspace: {workspace_label}, '
                f'Allocation: {current_allocation_id})'
            ))
            table.add_column('region', style='yellow')
            table.add_column('GPU Type', style='yellow')
            table.add_column('GPUs', justify='center')
            table.add_column('CPU', justify='right')
            table.add_column('RAM (GB)', justify='right')
            table.add_column('Available', justify='right', style='green')
            table.add_column('instance_type', style='cyan')
            table.add_column('Instance Name', style='magenta', overflow='fold')

            for row in normalized:
                cpu_value = str(int(row['cpu_count'])) if row['cpu_count'].is_integer() else str(row['cpu_count'])
                table.add_row(
                    row['region'],
                    row['gpu_family'],
                    str(row['gpu_count']),
                    cpu_value,
                    str(row['ram_gb']),
                    str(row['available']),
                    row['instance_type'] or '',
                    row['instance_name'],
                )

            if show_table:
                if normalized:
                    console.print(table)
                else:
                    message = 'No rows to display.'
                    if only_available:
                        message = 'No currently available resources (all rows have available=0).'
                    console.print(Panel(
                        message,
                        title=(
                            f'Available Resources (Workspace: {workspace_label}, '
                            f'Allocation: {current_allocation_id})'
                        ),
                    ))

            all_results[current_allocation_id] = normalized

        if return_data:
            return all_results
        return None

    def used_resources(self, regions=['SR006'], n_last=1000, table_width=160, return_data=False, show_table=True):
        """Show currently used GPU resources by region.

        Aggregates Running and Pending jobs and GPU counts per region.

        Args:
            regions (list[str], optional): Regions to inspect. Defaults to ['SR006'].
            n_last (int, optional): Max jobs to read per region. Defaults to 1000.
            table_width (int, optional): Console table width. Defaults to 160.
            return_data (bool, optional): Return aggregated rows/totals. Defaults to False.
            show_table (bool, optional): Print table/panel output. Defaults to True.

        Returns:
            dict | None: Aggregated data when return_data=True.
        """
        rows = []

        total_running_jobs = 0
        total_pending_jobs = 0
        total_running_gpus = 0
        total_pending_gpus = 0

        for region in regions:
            jobs_data = self._get_jobs(region=region, offset=0, limit=n_last, status_in=['Running', 'Pending'])

            running_jobs = 0
            pending_jobs = 0
            running_gpus = 0
            pending_gpus = 0

            for job in jobs_data:
                status = str(job.get('status', ''))
                gpu_count_raw = job.get('gpu_count', 0)
                try:
                    gpu_count = int(gpu_count_raw)
                except (TypeError, ValueError):
                    gpu_count = 0

                if status == 'Running':
                    running_jobs += 1
                    running_gpus += gpu_count
                elif status == 'Pending':
                    pending_jobs += 1
                    pending_gpus += gpu_count

            total_running_jobs += running_jobs
            total_pending_jobs += pending_jobs
            total_running_gpus += running_gpus
            total_pending_gpus += pending_gpus

            rows.append({
                'region': region,
                'running_jobs': running_jobs,
                'pending_jobs': pending_jobs,
                'gpus_running': running_gpus,
                'gpus_pending': pending_gpus,
                'gpus_total': running_gpus + pending_gpus,
            })

        workspace_label = self._workspace_title_label()

        totals_text = Text()
        totals_text.append('Running jobs: ', style='bold')
        totals_text.append(str(total_running_jobs))
        totals_text.append(' | Pending jobs: ', style='bold')
        totals_text.append(str(total_pending_jobs))
        totals_text.append('\n')
        totals_text.append('GPUs running: ', style='bold green')
        totals_text.append(str(total_running_gpus), style='green')
        totals_text.append(' | GPUs pending: ', style='bold yellow')
        totals_text.append(str(total_pending_gpus), style='yellow')
        totals_text.append(' | GPUs total: ', style='bold cyan')
        totals_text.append(str(total_running_gpus + total_pending_gpus), style='cyan')

        if show_table:
            table = Table(title=f'Used Resources (Workspace: {workspace_label})')
            table.add_column('region', style='yellow')
            table.add_column('running_jobs', justify='right')
            table.add_column('pending_jobs', justify='right')
            table.add_column('gpus_running', justify='right', style='green')
            table.add_column('gpus_pending', justify='right', style='yellow')
            table.add_column('gpus_total', justify='right', style='cyan')

            for row in rows:
                table.add_row(
                    row['region'],
                    str(row['running_jobs']),
                    str(row['pending_jobs']),
                    str(row['gpus_running']),
                    str(row['gpus_pending']),
                    str(row['gpus_total']),
                )

            console = Console(width=table_width)
            console.print(table)
            console.print(Panel(totals_text, title=f'Used Resources Summary (Workspace: {workspace_label})'))

        if return_data:
            return {
                'rows': rows,
                'totals': {
                    'running_jobs': total_running_jobs,
                    'pending_jobs': total_pending_jobs,
                    'gpus_running': total_running_gpus,
                    'gpus_pending': total_pending_gpus,
                    'gpus_total': total_running_gpus + total_pending_gpus,
                },
                'workspace': workspace_label,
            }
        return None

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

    @staticmethod
    def _format_job_datetime(dt_raw):
        if not dt_raw:
            return 'Unknown'
        try:
            dt = datetime.strptime(dt_raw, '%Y-%m-%dT%H:%M:%SZ') + timedelta(hours=3)
            return dt.strftime('%Y-%m-%dT%H:%M:%S')
        except (TypeError, ValueError):
            return str(dt_raw)

    @staticmethod
    def _format_job_cost(cost_raw):
        try:
            return f"{float(cost_raw):.01f}"
        except (TypeError, ValueError):
            return '0.0'

    @staticmethod
    def _format_job_duration(duration_raw):
        duration_raw = str(duration_raw or '')
        try:
            return str(timedelta(seconds=int(duration_raw[:-1]))).zfill(8)
        except (TypeError, ValueError):
            return duration_raw or 'Unknown'

    def _render_jobs_table(self, jobs_data, table_title, time_column, time_getter, table_width=160):
        table = Table(title=table_title)
        table.add_column(time_column, justify='left', style='cyan')
        table.add_column('Job ID', no_wrap=True, justify='left', style='magenta')
        table.add_column('Status', justify='center', style='green')
        table.add_column('Region', justify='center', style='yellow')
        table.add_column('GPUs', justify='center')
        table.add_column('Description', overflow='fold')
        table.add_column('Cost', justify='right')
        table.add_column('Duration', justify='right')

        rendered_rows = []
        for job in jobs_data:
            status = job.get('status', '')
            status_style = self.STATUS_STYLES.get(status, 'white')
            time_value = time_getter(job)

            row = {
                'time': time_value,
                'job_id': job.get('job_name', ''),
                'status': status,
                'region': job.get('region', ''),
                'gpus': str(job.get('gpu_count', '0')),
                'description': job.get('job_desc', ''),
                'cost': self._format_job_cost(job.get('cost', 0.0)),
                'duration': self._format_job_duration(job.get('duration', '')),
            }
            rendered_rows.append(row)

            table.add_row(
                row['time'],
                row['job_id'],
                f"[{status_style}]{row['status']}[/{status_style}]",
                row['region'],
                row['gpus'],
                row['description'],
                row['cost'],
                row['duration'],
            )

        console = Console(width=table_width)
        console.print(table)
        return rendered_rows

    def jobs(self, status_in=[], status_not_in=[], regions=['SR006'], n_last=1000, table_width=160):
        """Display a formatted table of jobs sorted by creation date"""
        jobs_data = []
        for region in regions:
            jobs_data += self._get_jobs(region=region, offset=0, limit=n_last, status_in=status_in,
                                        status_not_in=status_not_in)
        jobs_data = sorted(jobs_data, key=lambda x: x['created_dt'], reverse=True)

        workspace_label = self._workspace_title_label()
        self._render_jobs_table(
            jobs_data=jobs_data,
            table_title=f'Jobs (Workspace: {workspace_label})',
            time_column='Created',
            time_getter=lambda job: self._format_job_datetime(job.get('created_dt')),
            table_width=table_width,
        )

    def finished_jobs(self, regions=['SR006'], n_last=1000, status_in=None, table_width=160, return_data=False):
        """Display recently finished jobs with completion time.

        Args:
            regions (list[str], optional): Regions to query.
            n_last (int, optional): Max jobs to fetch per region.
            status_in (list[str] | None, optional): Terminal statuses to include.
            table_width (int, optional): Console table width.
            return_data (bool, optional): Return rows instead of only printing.
        """
        statuses = self.TERMINAL_JOB_STATUSES if not status_in else status_in

        jobs_data = []
        for region in regions:
            jobs_data += self._get_jobs(region=region, offset=0, limit=n_last, status_in=statuses, status_not_in=[])

        def _parse_job_dt(job):
            for key in ['completed_dt', 'updated_dt', 'created_dt']:
                value = job.get(key)
                if not value:
                    continue
                try:
                    return datetime.strptime(value, '%Y-%m-%dT%H:%M:%SZ')
                except (TypeError, ValueError):
                    continue
            return datetime.fromtimestamp(0)

        jobs_data = sorted(jobs_data, key=_parse_job_dt, reverse=True)

        workspace_label = self._workspace_title_label()
        rendered_rows = self._render_jobs_table(
            jobs_data=jobs_data,
            table_title=f'Finished Jobs (Workspace: {workspace_label})',
            time_column='Finished',
            time_getter=lambda job: self._format_job_datetime(
                job.get('completed_dt') or job.get('updated_dt') or job.get('created_dt')
            ),
            table_width=table_width,
        )

        if return_data:
            return rendered_rows
        return None
