try:
    import client_lib
    CLIENT_LIB_AVAILABLE = True
except ImportError:
    CLIENT_LIB_AVAILABLE = False

import contextlib
import io
import json
import re
import uuid

from rich.table import Table
from rich.console import Console
from rich.panel import Panel
from rich.text import Text

import requests
import time
from datetime import timedelta, datetime, timezone


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

    def _get_jobs_page(self, region='SR006', offset=0, limit=1000, status_in=[], status_not_in=[],
                       allocation_name=None):
        """Get one page of jobs in a workspace for the specified region.

        Args:
            region (str): Region code (default: SR006)
            offset (int): Pagination offset (default: 0)
            limit (int): Maximum number of jobs to return (default: 1000)
            allocation_name (str, optional): Allocation name to filter jobs by.

        Returns:
            dict: Validated jobs API response containing ``jobs`` and ``count``.
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
        if allocation_name is not None:
            params['allocation_name'] = allocation_name

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

        if not isinstance(jobs_data, dict) or not isinstance(jobs_data.get('jobs'), list):
            raise RuntimeError(
                f"Unexpected jobs response format for region={region}. "
                f"Expected object with 'jobs'. Got: {jobs_data}"
            )

        jobs = jobs_data['jobs']
        raw_count = jobs_data.get('count')
        try:
            count = int(raw_count) if raw_count is not None else None
        except (TypeError, ValueError):
            count = None
        return {'jobs': jobs, 'count': count}

    def _get_jobs(self, region='SR006', offset=0, limit=1000, status_in=[], status_not_in=[], allocation_name=None):
        """Get one page of jobs as a list, preserving the historical helper API."""
        jobs_data = self._get_jobs_page(
            region=region,
            offset=offset,
            limit=limit,
            status_in=status_in,
            status_not_in=status_not_in,
            allocation_name=allocation_name,
        )
        return sorted(jobs_data['jobs'], key=lambda x: x.get('created_dt', ''), reverse=True)

    def _get_all_jobs(self, region='SR006', status_in=[], status_not_in=[], page_size=1000):
        """Get every job for a region using the jobs endpoint pagination metadata."""
        if page_size < 1:
            raise RuntimeError("Jobs page size must be positive")

        jobs = []
        offset = 0
        total_count = None
        seen_pages = set()

        while True:
            page = self._get_jobs_page(
                region=region,
                offset=offset,
                limit=page_size,
                status_in=status_in,
                status_not_in=status_not_in,
            )
            page_jobs = page['jobs']
            if not page_jobs:
                break

            page_ids = tuple(job.get('job_name') for job in page_jobs)
            if page_ids in seen_pages:
                raise RuntimeError(f"Jobs pagination did not advance for region={region}")
            seen_pages.add(page_ids)

            jobs.extend(page_jobs)
            offset += len(page_jobs)

            page_count = page['count']
            if page_count is not None:
                total_count = max(total_count or 0, page_count)
                if offset >= total_count:
                    break
            elif len(page_jobs) < page_size:
                break

        return jobs

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

    def _get_workspaces(self, timeout=30):
        """List the authenticated user's workspaces, including namespace/name mappings."""
        self._refresh_token()
        response = self._request_with_auth(
            'get', f'{self.API_URL}/workspaces/v3/',
            headers={'accept': 'application/json', 'authorization': self.access_token},
            timeout=timeout,
        )
        try:
            data = response.json()
        except ValueError as exc:
            raise RuntimeError(f'List workspaces failed (HTTP {response.status_code}) with non-JSON response') from exc
        if response.status_code >= 400:
            raise RuntimeError(f'List workspaces failed (HTTP {response.status_code}): {data}')
        workspaces = data.get('workspaces') if isinstance(data, dict) else None
        if not isinstance(workspaces, list) or not all(
            isinstance(workspace, dict) and all(
                isinstance(workspace.get(key), str) and workspace[key]
                for key in ('id', 'name', 'namespace')
            ) for workspace in workspaces
        ):
            raise RuntimeError("Unexpected workspace list. Expected 'workspaces' objects with id, name, and namespace.")
        return workspaces

    def workspaces(self, table_width=160, return_data=False, show_table=True):
        """Show workspaces accessible to the authenticated user, sorted by name."""
        data = sorted(self._get_workspaces(), key=lambda workspace: workspace['name'].casefold())
        if show_table:
            table = Table(title='Workspaces')
            table.add_column('Name', style='magenta', overflow='fold')
            table.add_column('Namespace', style='cyan', overflow='fold')
            table.add_column('Workspace ID', overflow='fold')
            table.add_column('Project', overflow='fold')
            for workspace in data:
                table.add_row(
                    Text(workspace['name']), Text(workspace['namespace']), Text(workspace['id']),
                    Text(workspace.get('project_name') or workspace.get('project_id') or ''),
                )
            Console(width=table_width).print(table)
        return data if return_data else None

    def _get_allocation_api(self, path, operation, expected_type, params=None):
        """Call a read-only allocation endpoint and validate its top-level response."""
        self._refresh_token()
        url = f'{self.API_URL}{path}'
        headers = {
            'accept': 'application/json',
            'x-workspace-id': self.x_workspace_id,
            'x-api-key': self.x_api_key,
            'authorization': self.access_token,
        }

        request_options = {'params': params} if params is not None else {}
        response = self._request_with_auth('get', url, headers=headers, **request_options)
        try:
            data = response.json()
        except ValueError as exc:
            raise RuntimeError(
                f'{operation} failed (HTTP {response.status_code}) with non-JSON response'
            ) from exc

        if response.status_code >= 400:
            raise RuntimeError(f'{operation} failed (HTTP {response.status_code}): {data}')
        if not isinstance(data, expected_type):
            expected_name = 'array' if expected_type is list else 'object'
            raise RuntimeError(
                f'Unexpected response from {operation}. Expected {expected_name}, got: {data}'
            )
        return data

    def _get_allocations(self):
        """List allocations available to the current workspace."""
        data = self._get_allocation_api('/allocations/', 'List allocations', list)
        if not all(isinstance(item, dict) for item in data):
            raise RuntimeError('Unexpected response from List allocations. Expected an array of objects.')
        return data

    def _get_allocation_queues(self, allocation_id):
        try:
            data = self._get_allocation_api(
                '/queues/', f'List queues for allocation {allocation_id}', list,
                params={'allocation_id': allocation_id},
            )
        except RuntimeError as exc:
            if 'HTTP 409' in str(exc) and 'Custom queues are not activated' in str(exc):
                raise RuntimeError(
                    f'{exc}. Use cloudru allocations workloads {allocation_id} '
                    'to see jobs and notebooks assigned to nodes.'
                ) from exc
            raise
        if not all(isinstance(queue, dict) and isinstance(queue.get('id'), str)
                   and queue['id'] for queue in data):
            raise RuntimeError('Unexpected queue list. Expected objects with non-empty string IDs.')
        return data

    def _get_queue_jobs(self, queue_id):
        data = self._get_allocation_api(
            f'/queues/{queue_id}/jobs', f'Get jobs for queue {queue_id}', dict,
        )
        jobs = data.get('jobs')
        if not isinstance(jobs, list) or not all(
            isinstance(job, dict) and isinstance(job.get('id'), str) and job['id']
            for job in jobs
        ):
            raise RuntimeError(
                f'Unexpected jobs response for queue {queue_id}. '
                "Expected 'jobs' array of objects with non-empty string IDs."
            )
        return jobs

    @staticmethod
    def _parse_allocation_job_datetime(value):
        try:
            parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
            return parsed.replace(tzinfo=timezone.utc) if parsed.tzinfo is None else parsed.astimezone(timezone.utc)
        except (ValueError, TypeError, AttributeError, OverflowError):
            return None

    def _get_allocation_workspace_names(self, allocation_id, strict=False):
        """Best-effort display names, or required metadata for workspace filtering."""
        try:
            allocation = self._get_allocation(allocation_id)
        except (RuntimeError, requests.RequestException) as exc:
            if strict:
                raise RuntimeError(f'Cannot resolve workspace names: {exc}') from exc
            return {}
        workspaces = allocation.get('workspace_access')
        if not isinstance(workspaces, list):
            if strict:
                raise RuntimeError('Cannot resolve workspace names: invalid or missing workspace_access metadata.')
            return {}
        if strict and not all(
            isinstance(workspace, dict)
            and isinstance(workspace.get('id'), str) and workspace['id']
            and isinstance(workspace.get('name'), str) and workspace['name']
            for workspace in workspaces
        ):
            raise RuntimeError('Cannot resolve workspace names: invalid workspace_access metadata.')
        return {
            workspace['id']: workspace['name']
            for workspace in workspaces
            if isinstance(workspace, dict)
            and isinstance(workspace.get('id'), str) and workspace['id']
            and isinstance(workspace.get('name'), str) and workspace['name']
        }

    def _resolve_workload_workspaces(self, rows, workloads, allocation_id):
        """Resolve displayed rows using existing job metadata before one optional lookup."""
        namespace_ids = {}
        for workload in workloads:
            if workload['namespace'] and workload['workspace_id']:
                namespace_ids.setdefault(workload['namespace'], set()).add(workload['workspace_id'])
        for row in rows:
            matches = namespace_ids.get(row['namespace'], set())
            if not row['workspace_id'] and len(matches) == 1:
                row['workspace_id'] = next(iter(matches))

        if any(row['workspace_id'] for row in rows):
            names = self._get_allocation_workspace_names(allocation_id)
            for row in rows:
                row['workspace_name'] = names.get(row['workspace_id'])

        unresolved = [row for row in rows if not row['workspace_name']
                      and (row['workspace_id'] or row['namespace'])]
        if not unresolved:
            return
        try:
            workspaces = self._get_workspaces(timeout=5)
        except (RuntimeError, requests.RequestException):
            # Name enrichment must not prevent displaying workloads on API failure.
            return
        by_id = {workspace['id']: workspace for workspace in workspaces}
        by_namespace = {}
        for workspace in workspaces:
            by_namespace.setdefault(workspace['namespace'], []).append(workspace)
        for row in unresolved:
            if row['workspace_id']:
                workspace = by_id.get(row['workspace_id'])
            else:
                matches = by_namespace.get(row['namespace'], [])
                workspace = matches[0] if len(matches) == 1 else None
            if workspace:
                row['workspace_id'] = workspace['id']
                row['workspace_name'] = workspace['name']

    def allocation_queue(self, allocation_id, status_in=None, status_not_in=None, regions=None,
                        queues=None, workspace_id=None, n_last=20, table_width=160,
                        return_data=False, show_table=True, workspace_names=None, workspace_ids=None):
        """List jobs exposed by allocation queues across visible workspaces.

        Allocation and queue selectors accept UUIDs or exact names. Filters are
        applied locally, followed by a global newest-first limit. An omitted
        region never inherits the profile region. This is not a job-history API.
        workspace_names selects any of the exact, case-sensitive names and cannot
        be combined with workspace_id or workspace_ids. workspace_ids selects any
        of the supplied IDs and cannot be combined with workspace_id.
        """
        if isinstance(n_last, bool) or not isinstance(n_last, int) or n_last < 1:
            raise RuntimeError('Job limit must be a positive integer')
        if workspace_ids is not None:
            if workspace_id is not None or workspace_names is not None:
                raise RuntimeError('Cannot combine workspace_ids with workspace_id or workspace_names.')
            if (isinstance(workspace_ids, str) or not workspace_ids
                    or any(not isinstance(wid, str) or not wid.strip() for wid in workspace_ids)):
                raise RuntimeError('workspace_ids must contain at least one non-empty workspace ID.')
        if workspace_names is not None:
            if workspace_id is not None:
                raise RuntimeError('Cannot combine workspace_names with workspace_id; use --workspace or --workspace-id.')
            if (isinstance(workspace_names, str) or not workspace_names
                    or any(not isinstance(name, str) or not name.strip() for name in workspace_names)):
                raise RuntimeError('workspace_names must contain at least one non-empty workspace name.')
        resolved_id, resolved_name = self._resolve_allocation_selector(allocation_id)
        workspace_names_by_id = None
        selected_workspace_ids = set(workspace_ids or [])
        if workspace_names is not None:
            workspace_names_by_id = self._get_allocation_workspace_names(resolved_id, strict=True)
            for name in dict.fromkeys(workspace_names):
                matches = [wid for wid, wname in workspace_names_by_id.items() if wname == name]
                if not matches:
                    raise RuntimeError(f'Workspace {name!r} was not found by exact name in allocation {allocation_id!r}.')
                if len(matches) > 1:
                    raise RuntimeError(
                        f'Workspace name {name!r} is ambiguous. Matching IDs: {", ".join(matches)}. '
                        'Use --workspace-id.'
                    )
                selected_workspace_ids.add(matches[0])
        available_queues = self._get_allocation_queues(resolved_id)
        selected_ids = set()
        for selector in queues or []:
            matches = [q for q in available_queues if selector in (q['id'], q.get('name'))]
            if not matches:
                raise RuntimeError(f'Queue {selector!r} was not found in allocation {allocation_id!r}.')
            if len(matches) > 1:
                raise RuntimeError(f'Queue {selector!r} is ambiguous; use its UUID.')
            selected_ids.add(matches[0]['id'])

        jobs_data = []
        seen = set()
        for queue in available_queues:
            if queues and queue['id'] not in selected_ids:
                continue
            for job in self._get_queue_jobs(queue['id']):
                identity = (job.get('workspace'), job['id'])
                if identity in seen:
                    continue
                seen.add(identity)
                if status_in and job.get('status') not in status_in:
                    continue
                if job.get('status') in (status_not_in or []):
                    continue
                if regions and job.get('region') not in regions:
                    continue
                if workspace_id is not None and job.get('workspace') != workspace_id:
                    continue
                if ((workspace_names is not None or workspace_ids is not None)
                        and job.get('workspace') not in selected_workspace_ids):
                    continue
                created = self._parse_allocation_job_datetime(job.get('created_at'))
                jobs_data.append({
                    **job,
                    'job_name': job.get('name') or '',
                    'job_desc': job.get('description') or '',
                    'created_dt': created.strftime('%Y-%m-%dT%H:%M:%SZ') if created else None,
                    '_created_sort': created.timestamp() if created else float('-inf'),
                    'api_job_id': job['id'],
                    'allocation_id': resolved_id,
                    'allocation_name': resolved_name or job.get('allocation_label'),
                    'workspace_id': job.get('workspace'),
                    'queue_id': queue['id'],
                    'queue_name': queue.get('name') or job.get('queue_name'),
                })
        jobs_data.sort(key=lambda job: job['_created_sort'], reverse=True)
        jobs_data = jobs_data[:n_last]
        if jobs_data:
            if workspace_names_by_id is None:
                workspace_names_by_id = self._get_allocation_workspace_names(resolved_id)
            for job in jobs_data:
                job['workspace_name'] = workspace_names_by_id.get(job['workspace_id'])
        rows = self._render_jobs_table(
            jobs_data, f'Job Queue (Allocation: {resolved_name or resolved_id})',
            'Created', lambda job: self._format_job_datetime(job.get('created_dt')),
            lambda job: job.get('created_dt'), table_width=table_width,
            show_table=show_table, allocation_context=True,
        )
        return rows if return_data else None

    def _get_allocation_nodes(self, allocation_id):
        data = self._get_allocation_api(
            f'/allocations/{allocation_id}/nodes', f'Get nodes for allocation {allocation_id}', dict,
        )
        nodes = data.get('nodes')
        if not isinstance(nodes, list) or not all(
            isinstance(node, dict) and isinstance(node.get('name'), str) and node['name']
            for node in nodes
        ):
            raise RuntimeError('Unexpected allocation nodes response. Expected nodes with non-empty names.')
        return data

    def _get_allocation_node_loads(self, allocation_id, node_names):
        # The live API requires both allocation_id and node_names despite its
        # description saying exactly one selector should be supplied.
        data = self._get_allocation_api(
            '/nodes/load', f'Get node loads for allocation {allocation_id}', dict,
            params={'allocation_id': allocation_id, 'node_names': node_names},
        )
        loads = data.get('loads')
        if not isinstance(loads, list):
            raise RuntimeError("Unexpected node loads response. Expected a 'loads' array.")
        for load in loads:
            if not isinstance(load, dict) or not isinstance(load.get('node_name'), str):
                raise RuntimeError('Unexpected node load. Expected a node_name.')
            for field in ('jobs', 'notebooks'):
                items = load.get(field)
                if not isinstance(items, list) or not all(
                    isinstance(item, dict) and isinstance(item.get('id'), str) and item['id']
                    for item in items
                ):
                    raise RuntimeError(f'Unexpected node load. Expected {field} with non-empty IDs.')
        if {load['node_name'] for load in loads} != set(node_names):
            raise RuntimeError('Incomplete node loads response: returned nodes do not match allocation nodes.')
        return loads

    def allocation_workloads(self, allocation_id, types=None, status_in=None, status_not_in=None,
                             n_last=None, table_width=160, return_data=False, show_table=True):
        """Show jobs and notebooks assigned to allocation nodes, preserving their statuses.

        All workloads are shown by default. This view does not include work waiting
        for nodes and is not historical. Notebook workspace identity may be unknown;
        its namespace is displayed when workspace identity is unavailable.
        """
        if types and any(kind not in ('job', 'notebook') for kind in types):
            raise RuntimeError('Workload type must be job or notebook')
        if n_last is not None and (isinstance(n_last, bool) or not isinstance(n_last, int) or n_last < 1):
            raise RuntimeError('Workload limit must be a positive integer')
        resolved_id, resolved_name = self._resolve_allocation_selector(allocation_id)
        allocation = self._get_allocation_nodes(resolved_id)
        node_names = list(dict.fromkeys(node['name'] for node in allocation['nodes']))
        loads = self._get_allocation_node_loads(resolved_id, node_names) if node_names else []
        grouped = {}
        for load in loads:
            for kind, field in (('job', 'jobs'), ('notebook', 'notebooks')):
                for item in load[field]:
                    # A workload can occur on several nodes. Its declared GPU
                    # count is a workload total, not a value to sum per node.
                    key = (kind, item.get('workspace') or item.get('namespace'), item['id'])
                    if key in grouped:
                        if load['node_name'] not in grouped[key]['nodes']:
                            grouped[key]['nodes'].append(load['node_name'])
                        continue
                    limits = item.get('limits') or {}
                    raw_gpus = item.get('gpu_count') if kind == 'job' else limits.get('gpu')
                    try:
                        gpu_count = int(raw_gpus) if raw_gpus is not None else None
                    except (ValueError, TypeError):
                        gpu_count = None
                    grouped[key] = {
                        'type': kind, 'id': item['id'], 'name': item.get('name') or item['id'],
                        'status': item.get('status'), 'gpu_count': gpu_count,
                        'workspace_id': item.get('workspace'), 'workspace_name': None,
                        'namespace': item.get('namespace'), 'nodes': [load['node_name']],
                        'allocation_id': resolved_id,
                        'allocation_name': resolved_name or allocation.get('name'),
                        'region': item.get('region'), 'created_at': item.get('created_at'),
                        'description': item.get('description'), 'instance_type': item.get('instance_type'),
                        'user_id': item.get('user_id'), 'user_email': item.get('user_email'),
                    }
        include = {value.casefold() for value in status_in or []}
        exclude = {value.casefold() for value in status_not_in or []}
        rows = [row for row in grouped.values()
                if (not types or row['type'] in types)
                and (not include or str(row['status']).casefold() in include)
                and str(row['status']).casefold() not in exclude]
        def created_order(row):
            parsed = self._parse_allocation_job_datetime(row['created_at'])
            return parsed.timestamp() if parsed else float('-inf')
        rows.sort(key=created_order, reverse=True)
        if n_last is not None:
            rows = rows[:n_last]
        self._resolve_workload_workspaces(rows, grouped.values(), resolved_id)
        for row in rows:
            row['nodes'].sort()
        if show_table:
            label = resolved_name or allocation.get('name') or resolved_id
            table = Table(title=f'Workloads assigned to nodes (Allocation: {label})')
            table.add_column('Created', style='cyan', min_width=19, overflow='fold')
            table.add_column('Type', style='cyan')
            table.add_column('Name', style='magenta', overflow='fold')
            table.add_column('Status', justify='center')
            table.add_column('GPUs', justify='right')
            table.add_column('Description', overflow='fold')
            table.add_column('Workspace', overflow='fold')
            table.add_column('Nodes', overflow='fold')
            for row in rows:
                workspace = row['workspace_name'] or row['workspace_id']
                if not workspace and row['namespace']:
                    workspace = row['namespace'] if row['type'] == 'notebook' else f"namespace: {row['namespace']}"
                status = str(row['status'] or 'Unknown')
                created = self._parse_allocation_job_datetime(row['created_at'])
                created_display = self._format_job_datetime(
                    created.strftime('%Y-%m-%dT%H:%M:%SZ') if created else None,
                )
                table.add_row(
                    Text(created_display),
                    Text(row['type']), Text(row['name']), Text(status, style=self.STATUS_STYLES.get(status, 'white')),
                    str(row['gpu_count']) if row['gpu_count'] is not None else '—',
                    Text(row['description'] or ''),
                    Text(workspace or 'Unknown'), Text(', '.join(row['nodes'])),
                )
            Console(width=table_width).print(table)
        return rows if return_data else None

    def _resolve_allocation_selector(self, allocation_id):
        """Resolve an allocation UUID or exact allocation name to its UUID and optional name."""
        selector = str(allocation_id)
        try:
            resolved_id = str(uuid.UUID(selector))
            return resolved_id, None
        except (ValueError, AttributeError, TypeError):
            pass

        matches = [
            allocation
            for allocation in self._get_allocations()
            if allocation.get('name') == selector
        ]
        if not matches:
            raise RuntimeError(
                f"Allocation {selector!r} was not found by exact name in the current workspace."
            )
        if len(matches) > 1:
            matching_ids = ', '.join(str(allocation.get('id') or '') for allocation in matches)
            raise RuntimeError(
                f"Allocation name {selector!r} is ambiguous. Matching IDs: {matching_ids}"
            )

        resolved_id = matches[0].get('id')
        if not resolved_id:
            raise RuntimeError(f"Allocation {selector!r} does not contain an allocation ID.")
        return str(resolved_id), selector

    def _get_allocation(self, allocation_id):
        """Get detailed information for an allocation."""
        data = self._get_allocation_api(
            f'/allocations/{allocation_id}',
            f'Get allocation {allocation_id}',
            dict,
        )
        if not data.get('id') or not data.get('name') or not isinstance(data.get('resources'), dict):
            raise RuntimeError(
                f'Unexpected response from Get allocation {allocation_id}. '
                "Expected fields 'id', 'name', and object 'resources'."
            )
        self._validate_allocation_resources_status(data['resources'], f'Get allocation {allocation_id}')
        return data

    def _get_allocation_resources_status(self, allocation_id):
        """Get current resource metrics for an allocation."""
        data = self._get_allocation_api(
            f'/allocations/{allocation_id}/resources_status',
            f'Get allocation resources status {allocation_id}',
            dict,
        )
        self._validate_allocation_resources_status(
            data,
            f'Get allocation resources status {allocation_id}',
        )
        return data

    @staticmethod
    def _validate_allocation_resources_status(data, operation):
        required_metrics = ('cpu', 'gpu', 'ram', 'nodes_status')
        missing = [name for name in required_metrics if not isinstance(data.get(name), dict)]
        if missing:
            raise RuntimeError(
                f"Unexpected response from {operation}. Missing resource metrics: {', '.join(missing)}"
            )

        missing_fields = []
        for metric_name in ('cpu', 'gpu', 'ram'):
            for field in ('current', 'available', 'all', 'timestamp'):
                if field not in data[metric_name]:
                    missing_fields.append(f'{metric_name}.{field}')
        for field in ('available', 'all', 'timestamp'):
            if field not in data['nodes_status']:
                missing_fields.append(f'nodes_status.{field}')
        if missing_fields:
            raise RuntimeError(
                f"Unexpected response from {operation}. Missing resource fields: {', '.join(missing_fields)}"
            )

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
                   spark_executor_memory=None, health_params=None, stop_timer=0, allocation_name=None,
                   queue_name=None):
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
            allocation_name (str, optional): Name of the allocation in which the job will run. When omitted, Cloud.ru
                                            uses the default allocation. Defaults to None.
            queue_name (str, optional): Name of the queue in which the job will run. When omitted, Cloud.ru uses the
                                       default queue. Defaults to None.

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
        if allocation_name is not None:
            payload["allocation_name"] = allocation_name
        if queue_name is not None:
            payload["queue_name"] = queue_name

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

    def job_logs(self, job_id, tail=100, verbose=False, region='SR006', return_data=False, show_output=True):
        """Print logs for a specific job.

        Args:
            job_id (str): ID of the job to get logs for
            tail (int, optional): Number of log lines to return from the end. Defaults to 100. 0 will get all logs.
            verbose (bool, optional): Whether to include verbose logs. Defaults to False.
            region (str, optional): Region where the job is running. Defaults to 'SR006'.
            return_data (bool, optional): Return collected log lines. Defaults to False.
            show_output (bool, optional): Print logs while reading. Defaults to True.
        """
        lines = []
        try:
            for log in self._get_job_logs(job_id, tail=tail, verbose=verbose, region=region):
                lines.append(log)
                if show_output:
                    print(log)
        except KeyboardInterrupt:
            ...

        if return_data:
            return lines
        return None

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
    def _format_allocation_metric(value):
        if value is None:
            return ''
        if isinstance(value, float) and value.is_integer():
            return str(int(value))
        return str(value)

    def _render_allocation_resources_status(self, resources, title, console):
        table = Table(title=title)
        table.add_column('Resource', style='cyan')
        table.add_column('Used', justify='right')
        table.add_column('Available', justify='right', style='green')
        table.add_column('Total', justify='right')

        for key, label in (('cpu', 'CPU'), ('gpu', 'GPU'), ('ram', 'RAM')):
            metric = resources[key]
            table.add_row(
                label,
                self._format_allocation_metric(metric.get('current')),
                self._format_allocation_metric(metric.get('available')),
                self._format_allocation_metric(metric.get('all')),
            )

        nodes = resources['nodes_status']
        nodes_total = nodes.get('all')
        nodes_available = nodes.get('available')
        nodes_used = None
        if isinstance(nodes_total, (int, float)) and isinstance(nodes_available, (int, float)):
            nodes_used = nodes_total - nodes_available
        table.add_row(
            'Nodes',
            self._format_allocation_metric(nodes_used),
            self._format_allocation_metric(nodes_available),
            self._format_allocation_metric(nodes_total),
        )
        console.print(table)

    def allocations(self, table_width=160, return_data=False, show_table=True):
        """List allocations available to the current workspace."""
        data = self._get_allocations()

        if show_table:
            console = Console(width=table_width)
            if not data:
                console.print(Panel('No allocations found for current workspace.', title='Allocations'))
            else:
                table = Table(title='Allocations')
                table.add_column('Allocation ID', style='cyan')
                table.add_column('Name', style='magenta')
                table.add_column('Region', style='yellow')
                table.add_column('Project')
                table.add_column('Description', overflow='fold')

                for allocation in data:
                    project = allocation.get('project_name') or allocation.get('project_id') or ''
                    table.add_row(
                        str(allocation.get('id') or ''),
                        str(allocation.get('name') or ''),
                        str(allocation.get('region_key') or ''),
                        str(project),
                        str(allocation.get('description') or ''),
                    )
                console.print(table)

        if return_data:
            return data
        return None

    def allocation_info(self, allocation_id, table_width=160, return_data=False, show_table=True):
        """Show details for an allocation selected by UUID or exact name."""
        resolved_id, _ = self._resolve_allocation_selector(allocation_id)
        data = self._get_allocation(resolved_id)

        if show_table:
            console = Console(width=table_width)
            target_resource = data.get('target_resource') or {}
            overview = Text()
            overview_fields = (
                ('Allocation ID', data.get('id')),
                ('Name', data.get('name')),
                ('Status', data.get('status')),
                ('Region', data.get('region_key')),
                ('Description', data.get('description')),
                ('Project ID', data.get('project_id')),
                ('Project name', data.get('project_name')),
                ('Target nodes', target_resource.get('nodes')),
                ('Target GPUs', target_resource.get('gpus')),
            )
            for label, value in overview_fields:
                overview.append(f'{label}: ', style='bold')
                overview.append(f'{value if value is not None else ""}\n')
            console.print(Panel(
                overview,
                title=f"Allocation: {data.get('name')} ({data.get('id')})",
            ))

            self._render_allocation_resources_status(
                data['resources'],
                title='Allocation Resources',
                console=console,
            )

            nodes = data.get('nodes') or []
            if nodes:
                nodes_table = Table(title='Allocation Nodes')
                nodes_table.add_column('Node ID', style='cyan')
                for node in nodes:
                    nodes_table.add_row(str(node))
                console.print(nodes_table)

            workspace_access = data.get('workspace_access') or []
            if workspace_access:
                access_table = Table(title='Workspace Access')
                access_table.add_column('Workspace ID', style='cyan')
                access_table.add_column('Name', style='magenta')
                for workspace in workspace_access:
                    if isinstance(workspace, dict):
                        access_table.add_row(
                            str(workspace.get('id') or ''),
                            str(workspace.get('name') or ''),
                        )
                console.print(access_table)

        if return_data:
            return data
        return None

    def allocation_status(self, allocation_id, table_width=160, return_data=False, show_table=True):
        """Show resource metrics for an allocation selected by UUID or exact name."""
        resolved_id, resolved_name = self._resolve_allocation_selector(allocation_id)
        data = self._get_allocation_resources_status(resolved_id)

        if show_table:
            console = Console(width=table_width)
            allocation_label = resolved_id
            if resolved_name:
                allocation_label = f'{resolved_name} ({resolved_id})'
            self._render_allocation_resources_status(
                data,
                title=f'Allocation Resources Status ({allocation_label})',
                console=console,
            )

        if return_data:
            return data
        return None

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
            allocation_id (str, optional): Allocation UUID or exact name. If omitted, use all current workspace allocations.
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
            allocation_ids = [self._resolve_allocation_selector(allocation_id)[0]]

        all_results = {}

        for current_allocation_id in allocation_ids:
            if not current_allocation_id:
                if show_table:
                    console.print(Panel('Allocation ID is empty. Provide allocation_id explicitly.', title='Available Resources'))
                continue

            allocation_meta = allocation_meta_by_id.get(current_allocation_id, {})
            allocation_region = allocation_meta.get('region')
            allocation_name = allocation_meta.get('name')
            if not allocation_region:
                try:
                    allocation = self._get_allocation(current_allocation_id)
                except (RuntimeError, requests.RequestException):
                    pass  # Missing metadata must not hide resource availability.
                else:
                    allocation_region = allocation.get('region_key')
                    allocation_name = allocation_name or allocation.get('name')
            row_region = allocation_region or 'Unknown'
            if allocation_name:
                allocation_title_label = f'Allocation: {allocation_name} ({current_allocation_id})'
            else:
                allocation_title_label = f'Allocation ID: {current_allocation_id}'
            title_context = f'Workspace: {workspace_label}, {allocation_title_label}'
            resources_title = f'Available Resources ({title_context})'

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
                    console.print(Panel(
                        str(endpoint_errors[-1]),
                        title=f'Available Resources Error ({title_context})',
                    ))
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

            table = Table(title=resources_title)
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
                        title=resources_title,
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

    def job_status(self, job_id, return_data=False, show_output=True):
        """Get human readable status information for a job

        Args:
            job_id (str): ID of the job to get status for
            return_data (bool, optional): Return normalized status dictionary. Defaults to False.
            show_output (bool, optional): Print rich panel output. Defaults to True.

        Returns:
            dict | None: Normalized status data when return_data=True, otherwise None.
        """
        status = self._get_job_status(job_id)

        created = self._format_unix_timestamp(status.get('created_at'))
        pending = self._format_unix_timestamp(status.get('pending_at'))
        running = self._format_unix_timestamp(status.get('running_at'))
        completed = self._format_unix_timestamp(status.get('completed_at'))

        normalized = {
            'job_id': status.get('job_name', ''),
            'status': str(status.get('status', 'Unknown')).capitalize(),
            'allocation_id': status.get('allocation_id'),
            'allocation_name': status.get('allocation_name'),
            'queue_id': status.get('queue_id'),
            'queue_name': status.get('queue_name'),
            'error_code': status.get('error_code'),
            'error_message': status.get('error_message', ''),
            'created_at_raw': status.get('created_at'),
            'pending_at_raw': status.get('pending_at'),
            'running_at_raw': status.get('running_at'),
            'completed_at_raw': status.get('completed_at'),
            'created_at_display': created,
            'pending_at_display': pending,
            'running_at_display': running,
            'completed_at_display': completed,
            'raw': status,
        }

        if show_output:
            console = Console()
            status_text = Text()
            status_text.append("Job ID: ", style="bold")
            status_text.append(f"{normalized['job_id'] or 'Unknown'}\n")

            status_text.append("Status: ", style="bold")
            job_status = normalized['status']
            status_text.append(f"{job_status}\n", style=self.STATUS_STYLES.get(job_status, 'white'))

            scheduling_fields = (
                ('Allocation ID', 'allocation_id'),
                ('Allocation name', 'allocation_name'),
                ('Queue ID', 'queue_id'),
                ('Queue name', 'queue_name'),
            )
            for label, key in scheduling_fields:
                if normalized[key] is not None:
                    status_text.append(f"{label}: ", style="bold")
                    status_text.append(f"{normalized[key]}\n")

            status_text.append("Created: ", style="bold")
            status_text.append(f"{normalized['created_at_display']}\n")

            status_text.append("Pending: ", style="bold")
            status_text.append(f"{normalized['pending_at_display']}\n")

            status_text.append("Running: ", style="bold")
            status_text.append(f"{normalized['running_at_display']}\n")

            status_text.append("Completed: ", style="bold")
            status_text.append(f"{normalized['completed_at_display']}\n")

            status_text.append("Error code: ", style="bold red")
            status_text.append(f"{normalized['error_code']}\n")

            status_text.append("Error message: ", style="bold red")
            status_text.append(str(normalized['error_message']))

            panel = Panel(status_text, title="Job Status")
            console.print(panel)

        if return_data:
            return normalized
        return None

    def job_ssh_target(self, job_id, rank=0):
        """Resolve SSH connection details for a running training job.

        Args:
            job_id (str): ID of the job to connect to.
            rank (int, optional): Job rank. Rank 0 is the master pod; rank N
                maps to worker pod N-1. Defaults to 0.

        Returns:
            dict: Normalized SSH target details, including destination and port.

        Raises:
            RuntimeError: If the rank is invalid or required API data is missing.
        """
        if isinstance(rank, bool) or not isinstance(rank, int) or rank < 0:
            raise RuntimeError("SSH rank must be a non-negative integer")

        status = self._get_job_status(job_id)
        if not isinstance(status, dict):
            raise RuntimeError(f"Unexpected status response for job '{job_id}'")

        resolved_job_id = status.get('job_name')
        if not resolved_job_id:
            raise RuntimeError(f"Job '{job_id}' was not found or returned an invalid status response")

        job_status = str(status.get('status', '')).strip()
        if job_status.lower() != 'running':
            display_status = job_status.capitalize() if job_status else 'Unknown'
            raise RuntimeError(
                f"Job '{resolved_job_id}' must be Running for SSH access; current status: {display_status}"
            )

        region = status.get('region')
        if not region:
            raise RuntimeError(f"Job '{resolved_job_id}' status response does not contain a region")

        workspace = self.get_workspace_info(refresh=False)
        if not isinstance(workspace, dict):
            raise RuntimeError("Unexpected workspace response while resolving SSH target")

        namespace = workspace.get('namespace')
        if not namespace:
            raise RuntimeError("Workspace response does not contain an SSH namespace")

        configs = self._get_configs()
        if not isinstance(configs, dict) or not isinstance(configs.get('regions'), list):
            raise RuntimeError("Unexpected configs response while resolving SSH target")

        region_config = next(
            (item for item in configs['regions'] if isinstance(item, dict) and item.get('key') == region),
            None,
        )
        if region_config is None:
            raise RuntimeError(f"Region '{region}' was not found in the Cloud.ru configs response")

        ssh_config = region_config.get('ssh')
        if not isinstance(ssh_config, dict):
            raise RuntimeError(f"SSH access is not configured for region '{region}'")

        host = ssh_config.get('url')
        port = ssh_config.get('port')
        if not host or not port:
            raise RuntimeError(f"SSH host or port is missing for region '{region}'")

        pod = 'mpimaster-0' if rank == 0 else f'mpiworker-{rank - 1}'
        destination = f'{resolved_job_id}-{pod}.{namespace}@{host}'

        return {
            'job_id': resolved_job_id,
            'status': job_status,
            'region': region,
            'namespace': namespace,
            'pod': pod,
            'host': host,
            'port': str(port),
            'destination': destination,
        }

    @staticmethod
    def _format_unix_timestamp(ts_raw):
        try:
            if ts_raw is None:
                return 'Unknown'
            return datetime.fromtimestamp(float(ts_raw)).strftime('%Y-%m-%d %H:%M:%S')
        except (TypeError, ValueError, OSError):
            return str(ts_raw)

    def render_job_delete_response(self, result, console=None):
        """Render single-job delete response and return parsed outcome."""
        console = console or Console()

        if not isinstance(result, dict) or not result.get('job_name'):
            console.print(Panel(json.dumps(result, ensure_ascii=False, indent=2), title='Job Delete Response'))
            return {
                'recognized': False,
                'ok': False,
                'job_id': '',
                'error_summary': 'unexpected response format',
            }

        status_raw = str(result.get('status', 'Unknown'))
        status = status_raw.capitalize()
        status_style = self.STATUS_STYLES.get(status, 'cyan' if status.lower() == 'deleted' else 'white')

        deleted_at_str = self._format_unix_timestamp(result.get('deleted_at'))
        error_code = result.get('error_code', 'Unknown')
        error_message = str(result.get('error_message', ''))

        status_text = Text()
        status_text.append('Job ID: ', style='bold')
        status_text.append(f"{result.get('job_name')}\n")
        status_text.append('Status: ', style='bold')
        status_text.append(f"{status}\n", style=status_style)
        status_text.append('Deleted: ', style='bold')
        status_text.append(f"{deleted_at_str}\n")
        status_text.append('Error code: ', style='bold red')
        status_text.append(f"{error_code}\n")
        status_text.append('Error message: ', style='bold red')
        status_text.append(error_message)
        console.print(Panel(status_text, title='Job Delete Status'))

        ok = str(error_code) in {'0', '0.0'} and status.lower() == 'deleted'
        return {
            'recognized': True,
            'ok': ok,
            'job_id': str(result.get('job_name') or ''),
            'error_summary': f"error_code={error_code}, status={status}, error_message={error_message}",
        }

    @staticmethod
    def render_job_delete_summary(requested, deleted, failed, console=None):
        """Render bulk delete summary and failed list."""
        console = console or Console()

        summary = Text()
        summary.append('Requested: ', style='bold')
        summary.append(str(requested))
        summary.append(' | Deleted: ', style='bold green')
        summary.append(str(deleted), style='green')
        summary.append(' | Failed: ', style='bold red')
        summary.append(str(len(failed)), style='red')
        console.print(Panel(summary, title='Job Delete Summary'))

        if failed:
            failed_text = Text()
            for job_id, err in failed:
                failed_text.append(f"- {job_id}: {err}\n")
            console.print(Panel(failed_text, title='Failed Deletes'))

    def render_submit_response(self, result, console=None):
        """Render submit response in rich format and return parsed outcome."""
        console = console or Console()

        if isinstance(result, dict) and result.get('job_name'):
            status = str(result.get('status', 'Unknown'))
            status_style = self.STATUS_STYLES.get(status.capitalize(), 'white')
            created_str = self._format_unix_timestamp(result.get('created_at'))

            info = Text()
            info.append('Job ID: ', style='bold')
            info.append(f"{result.get('job_name')}\n")
            info.append('Status: ', style='bold')
            info.append(f"{status}\n", style=status_style)
            info.append('Created: ', style='bold')
            info.append(created_str)

            console.print(Panel(info, title='Job Submitted'))
            return {'recognized': True, 'job_id': str(result.get('job_name'))}

        console.print(Panel(json.dumps(result, ensure_ascii=False, indent=2), title='Submit Response'))
        return {'recognized': False, 'job_id': None}

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

    @staticmethod
    def _parse_api_datetime(dt_raw):
        if not dt_raw:
            return datetime.fromtimestamp(0)
        try:
            return datetime.strptime(dt_raw, '%Y-%m-%dT%H:%M:%SZ')
        except (TypeError, ValueError):
            return datetime.fromtimestamp(0)

    @staticmethod
    def _job_finished_dt_raw(job):
        return job.get('completed_dt') or job.get('updated_dt') or job.get('created_dt')

    def _normalize_job_row(self, job, primary_time_raw, primary_time_display, allocation_context=False):
        created_dt_raw = job.get('created_dt')
        finished_dt_raw = self._job_finished_dt_raw(job)
        gpu_count_raw = job.get('gpu_count', 0)
        try:
            gpu_count = int(gpu_count_raw)
        except (TypeError, ValueError):
            gpu_count = 0
        row = {
            'time': primary_time_display,
            'time_raw': primary_time_raw,
            'time_display': primary_time_display,
            'created_dt_raw': created_dt_raw,
            'created_dt_display': self._format_job_datetime(created_dt_raw),
            'finished_dt_raw': finished_dt_raw,
            'finished_dt_display': self._format_job_datetime(finished_dt_raw),
            'job_id': job.get('job_name', ''),
            'status': job.get('status', ''),
            'region': job.get('region', ''),
            'gpu_count': gpu_count,
            'gpus': str(gpu_count),
            'job_desc': job.get('job_desc', ''),
            'description': job.get('job_desc', ''),
            'cost_raw': job.get('cost', 0.0),
            'cost_display': self._format_job_cost(job.get('cost', 0.0)),
            'cost': self._format_job_cost(job.get('cost', 0.0)),
            'duration_raw': job.get('duration', ''),
            'duration_display': self._format_job_duration(job.get('duration', '')),
            'duration': self._format_job_duration(job.get('duration', '')),
        }
        if allocation_context:
            row.update({key: job.get(key) for key in (
                'api_job_id', 'allocation_id', 'allocation_name', 'workspace_id', 'workspace_name',
                'queue_id', 'queue_name', 'namespace', 'instance_type', 'user_id',
                'user_email', 'created_at',
            )})
            row.update(
                finished_dt_raw=None, finished_dt_display='Unknown',
                cost_raw=None, cost_display='—', cost='—',
                duration_raw=None, duration_display='—', duration='—',
            )
        return row

    def _render_jobs_table(self, jobs_data, table_title, time_column, time_getter, time_raw_getter,
                           table_width=160, show_table=True, allocation_context=False):
        table = Table(title=table_title)
        table.add_column(time_column, justify='left', style='cyan',
                         overflow='fold' if allocation_context else 'ellipsis',
                         min_width=19 if allocation_context else None)
        table.add_column('Job ID', no_wrap=True, justify='left', style='magenta')
        table.add_column('Status', justify='center', style='green')
        table.add_column('Region', justify='center', style='yellow')
        table.add_column('GPUs', justify='center')
        table.add_column('Description', overflow='fold')
        if allocation_context:
            table.add_column('Workspace', overflow='fold')
        else:
            table.add_column('Cost', justify='right')
            table.add_column('Duration', justify='right')

        rendered_rows = []
        for job in jobs_data:
            status = job.get('status', '')
            status_style = self.STATUS_STYLES.get(status, 'white')
            time_raw = time_raw_getter(job)
            time_value = time_getter(job)
            row = self._normalize_job_row(job, primary_time_raw=time_raw, primary_time_display=time_value,
                                          allocation_context=allocation_context)
            rendered_rows.append(row)

            if show_table:
                table.add_row(
                    row['time'],
                    row['job_id'],
                    f"[{status_style}]{row['status']}[/{status_style}]",
                    row['region'],
                    row['gpus'],
                    row['description'],
                    *([str(row['workspace_name'] or row['workspace_id'] or '')]
                      if allocation_context else [row['cost'], row['duration']]),
                )

        if show_table:
            console = Console(width=table_width)
            console.print(table)
        return rendered_rows

    def jobs(self, status_in=[], status_not_in=[], regions=['SR006'], n_last=1000, table_width=160,
             return_data=False, show_table=True, allocation_name=None):
        """Display jobs sorted by creation date.

        Args:
            status_in (list[str], optional): Status filter include list.
            status_not_in (list[str], optional): Status exclude list.
            regions (list[str], optional): Regions to query.
            n_last (int, optional): Max jobs to fetch per region.
            table_width (int, optional): Console table width.
            return_data (bool, optional): Return normalized rows.
            show_table (bool, optional): Print rich table output.
            allocation_name (str, optional): Allocation name to filter jobs by.

        Returns:
            list[dict] | None: Normalized rows when return_data=True.
        """
        jobs_data = []
        for region in regions:
            jobs_data += self._get_jobs(region=region, offset=0, limit=n_last, status_in=status_in,
                                        status_not_in=status_not_in, allocation_name=allocation_name)
        jobs_data = sorted(jobs_data, key=lambda x: self._parse_api_datetime(x.get('created_dt')), reverse=True)

        workspace_label = self._workspace_title_label()
        rendered_rows = self._render_jobs_table(
            jobs_data=jobs_data,
            table_title=f'Jobs (Workspace: {workspace_label})',
            time_column='Created',
            time_getter=lambda job: self._format_job_datetime(job.get('created_dt')),
            time_raw_getter=lambda job: job.get('created_dt'),
            table_width=table_width,
            show_table=show_table,
        )
        if return_data:
            return rendered_rows
        return None

    def finished_jobs(self, regions=['SR006'], n_last=1000, status_in=None, table_width=160,
                      return_data=False, show_table=True):
        """Display recently finished jobs with completion time.

        Args:
            regions (list[str], optional): Regions to query.
            n_last (int, optional): Max jobs to fetch per region.
            status_in (list[str] | None, optional): Terminal statuses to include.
            table_width (int, optional): Console table width.
            return_data (bool, optional): Return rows instead of only printing.
            show_table (bool, optional): Print rich table output.
        """
        statuses = self.TERMINAL_JOB_STATUSES if not status_in else status_in

        jobs_data = []
        for region in regions:
            jobs_data += self._get_jobs(region=region, offset=0, limit=n_last, status_in=statuses, status_not_in=[])

        jobs_data = sorted(jobs_data, key=lambda job: self._parse_api_datetime(self._job_finished_dt_raw(job)), reverse=True)

        workspace_label = self._workspace_title_label()
        rendered_rows = self._render_jobs_table(
            jobs_data=jobs_data,
            table_title=f'Finished Jobs (Workspace: {workspace_label})',
            time_column='Finished',
            time_getter=lambda job: self._format_job_datetime(self._job_finished_dt_raw(job)),
            time_raw_getter=lambda job: self._job_finished_dt_raw(job),
            table_width=table_width,
            show_table=show_table,
        )

        if return_data:
            return rendered_rows
        return None
