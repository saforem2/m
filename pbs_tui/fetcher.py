"""Utilities for gathering PBS Pro scheduler information."""

from __future__ import annotations

import asyncio
import logging
import os
from collections import Counter, defaultdict
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple
from xml.etree import ElementTree as ET

from .data import Job, Node, Queue, SchedulerSnapshot
from .samples import sample_snapshot

_LOGGER = logging.getLogger(__name__)


def _parse_bool(value: Optional[str]) -> Optional[bool]:
    if value is None:
        return None
    value = value.strip().lower()
    if value in {"true", "t", "1", "yes", "y"}:
        return True
    if value in {"false", "f", "0", "no", "n"}:
        return False
    return None


def _parse_int(value: Optional[str]) -> Optional[int]:
    if value is None:
        return None
    value = value.strip()
    if not value:
        return None
    try:
        return int(value)
    except ValueError:
        return None


_PBS_DATETIME_FORMATS = (
    "%Y-%m-%dT%H:%M:%SZ",
    "%Y-%m-%dT%H:%M:%S%z",
    "%Y-%m-%d %H:%M:%S",
    "%a %b %d %H:%M:%S %Y",
)


def _parse_timestamp(value: Optional[str]) -> Optional[datetime]:
    if value is None:
        return None
    value = value.strip()
    if not value:
        return None
    if value.isdigit():
        try:
            return datetime.fromtimestamp(int(value), tz=timezone.utc)
        except (ValueError, OSError):
            return None
    for fmt in _PBS_DATETIME_FORMATS:
        try:
            if fmt.endswith("%z") or value.endswith("Z"):
                normalised = value.replace("Z", "+0000")
                return datetime.strptime(normalised, fmt)
            return datetime.strptime(value, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def _collect_child_text(element: Optional[ET.Element]) -> Dict[str, str]:
    result: Dict[str, str] = {}
    if element is None:
        return result
    for child in element:
        key = child.tag
        text = child.text.strip() if child.text else ""
        result[key] = text
    return result


class PBSDataFetcher:
    """Fetch PBS Pro scheduler data via the command-line utilities."""

    def __init__(
        self,
        *,
        qstat_path: str = "qstat",
        pbsnodes_path: str = "pbsnodes",
        include_nodes: bool = True,
        include_queues: bool = True,
        command_timeout: float = 15.0,
        fallback_to_sample: bool = True,
        force_sample: Optional[bool] = None,
    ) -> None:
        env_force_sample = os.getenv("PBS_TUI_SAMPLE_DATA")
        if force_sample is None and env_force_sample is not None:
            force_sample = env_force_sample.strip().lower() not in {"0", "false", "no", ""}
        self.force_sample = bool(force_sample)
        self.qstat_path = qstat_path
        self.pbsnodes_path = pbsnodes_path
        self.include_nodes = include_nodes
        self.include_queues = include_queues
        self.command_timeout = command_timeout
        self.fallback_to_sample = fallback_to_sample
        self._qstat_jobs_cmd = [self.qstat_path, "-f", "-x"]
        self._qstat_queue_cmd = [self.qstat_path, "-Q", "-f", "-x"]
        self._pbsnodes_cmd = [self.pbsnodes_path, "-x"]

    async def fetch_snapshot(self) -> SchedulerSnapshot:
        """Collect scheduler information.

        Returns a :class:`SchedulerSnapshot` either from the live PBS system or
        from bundled sample data when the PBS utilities are not available.
        """

        if self.force_sample:
            snapshot = sample_snapshot()
            snapshot.errors.insert(
                0,
                "Using bundled sample data because PBS_TUI_SAMPLE_DATA was set.",
            )
            return snapshot

        jobs, job_errors = await self._fetch_jobs()
        nodes: List[Node] = []
        node_errors: List[str] = []
        queues: List[Queue] = []
        queue_errors: List[str] = []

        if self.include_nodes:
            nodes, node_errors = await self._fetch_nodes()
        if self.include_queues:
            queues, queue_errors = await self._fetch_queues()

        errors = job_errors + node_errors + queue_errors

        if not jobs and not nodes and self.fallback_to_sample:
            snapshot = sample_snapshot()
            snapshot.errors = errors + snapshot.errors
            return snapshot

        snapshot = SchedulerSnapshot(
            jobs=jobs,
            nodes=nodes,
            queues=queues,
            timestamp=datetime.now(timezone.utc),
            source="pbs",
            errors=errors,
        )
        self._populate_queue_job_counts(snapshot)
        return snapshot

    async def _fetch_jobs(self) -> Tuple[List[Job], List[str]]:
        output, error = await self._run_command(self._qstat_jobs_cmd)
        if output is None:
            return [], [error or "Unable to execute qstat"]
        try:
            jobs = self._parse_jobs_xml(output)
            return jobs, []
        except ET.ParseError as exc:
            message = f"Failed to parse qstat XML output: {exc}"
            _LOGGER.warning(message)
            return [], [message]

    async def _fetch_nodes(self) -> Tuple[List[Node], List[str]]:
        output, error = await self._run_command(self._pbsnodes_cmd)
        if output is None:
            return [], [error or "Unable to execute pbsnodes"]
        try:
            nodes = self._parse_nodes_xml(output)
            return nodes, []
        except ET.ParseError as exc:
            message = f"Failed to parse pbsnodes XML output: {exc}"
            _LOGGER.warning(message)
            return [], [message]

    async def _fetch_queues(self) -> Tuple[List[Queue], List[str]]:
        output, error = await self._run_command(self._qstat_queue_cmd)
        if output is None:
            return [], [error or "Unable to execute qstat for queues"]
        try:
            queues = self._parse_queues_xml(output)
            return queues, []
        except ET.ParseError as exc:
            message = f"Failed to parse queue XML output: {exc}"
            _LOGGER.warning(message)
            return [], [message]

    async def _run_command(self, cmd: List[str]) -> Tuple[Optional[str], Optional[str]]:
        """Execute *cmd* asynchronously and return ``(stdout, error)``."""

        try:
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
        except FileNotFoundError:
            message = f"Command not found: {' '.join(cmd)}"
            _LOGGER.debug(message)
            return None, message
        try:
            stdout, stderr = await asyncio.wait_for(
                process.communicate(), timeout=self.command_timeout
            )
        except asyncio.TimeoutError:
            process.kill()
            await process.communicate()
            message = f"Command timed out after {self.command_timeout}s: {' '.join(cmd)}"
            _LOGGER.warning(message)
            return None, message
        if process.returncode != 0:
            message = stderr.decode().strip() or f"Command failed: {' '.join(cmd)}"
            _LOGGER.warning("%s (exit %s)", message, process.returncode)
            return None, message
        return stdout.decode(), None

    def _parse_jobs_xml(self, xml_text: str) -> List[Job]:
        root = ET.fromstring(xml_text)
        jobs: List[Job] = []
        for job_el in root.findall(".//Job"):
            job_id = job_el.findtext("Job_Id", "").strip()
            if not job_id:
                continue
            job = Job(
                id=job_id,
                name=job_el.findtext("Job_Name", "").strip(),
                user=self._normalise_user(job_el.findtext("Job_Owner") or job_el.findtext("owner")),
                queue=job_el.findtext("queue", "").strip(),
                state=job_el.findtext("job_state", "").strip(),
                exec_host=(job_el.findtext("exec_host") or "").strip() or None,
                create_time=_parse_timestamp(job_el.findtext("ctime")),
                start_time=_parse_timestamp(
                    job_el.findtext("start_time")
                    or job_el.findtext("stime")
                    or job_el.findtext("etime")
                ),
                end_time=_parse_timestamp(job_el.findtext("comp_time") or job_el.findtext("mtime")),
                walltime=(
                    job_el.findtext("Resource_List/walltime")
                    or job_el.findtext("resources_default/walltime")
                ),
                nodes=job_el.findtext("Resource_List/nodes"),
                resources_requested=_collect_child_text(job_el.find("Resource_List")),
                resources_used=_collect_child_text(job_el.find("resources_used")),
                comment=(job_el.findtext("comment") or job_el.findtext("sched_comment")),
                exit_status=job_el.findtext("Exit_status"),
            )
            jobs.append(job)
        return jobs

    def _parse_nodes_xml(self, xml_text: str) -> List[Node]:
        root = ET.fromstring(xml_text)
        nodes: List[Node] = []
        for node_el in root.findall(".//Node"):
            name = node_el.findtext("name", "").strip()
            if not name:
                continue
            resources_available = _collect_child_text(node_el.find("resources_available"))
            resources_assigned = _collect_child_text(node_el.find("resources_assigned"))
            jobs_field = (node_el.findtext("jobs") or "").strip()
            jobs_list = self._parse_node_jobs(jobs_field)
            node = Node(
                name=name,
                state=(node_el.findtext("state") or "").strip(),
                np=_parse_int(node_el.findtext("np")),
                ncpus=_parse_int(resources_available.get("ncpus")),
                properties=self._parse_properties(node_el.findtext("properties")),
                jobs=jobs_list,
                resources_available=resources_available,
                resources_assigned=resources_assigned,
                comment=(node_el.findtext("comment") or "").strip() or None,
            )
            nodes.append(node)
        return nodes

    def _parse_queues_xml(self, xml_text: str) -> List[Queue]:
        root = ET.fromstring(xml_text)
        queues: List[Queue] = []
        for queue_el in root.findall(".//Queue"):
            name = queue_el.findtext("queue_name", "").strip()
            if not name:
                continue
            queue = Queue(
                name=name,
                state=(queue_el.findtext("state_count") or queue_el.findtext("state") or "").strip() or None,
                enabled=_parse_bool(queue_el.findtext("enabled")),
                started=_parse_bool(queue_el.findtext("started")),
                total_jobs=_parse_int(queue_el.findtext("total_jobs")),
                job_states=self._parse_state_counts(queue_el.findtext("state_count")),
                resources_default=_collect_child_text(queue_el.find("resources_default")),
                resources_max=_collect_child_text(queue_el.find("resources_max")),
                comment=(queue_el.findtext("comment") or "").strip() or None,
            )
            queues.append(queue)
        return queues

    def _populate_queue_job_counts(self, snapshot: SchedulerSnapshot) -> None:
        if not snapshot.queues and not snapshot.jobs:
            return
        queue_map = {queue.name: queue for queue in snapshot.queues}
        counts: Dict[str, Counter[str]] = defaultdict(Counter)
        for job in snapshot.jobs:
            if not job.queue:
                continue
            counts[job.queue][job.state] += 1
        for queue_name, counter in counts.items():
            queue = queue_map.get(queue_name)
            if queue is None:
                queue = Queue(name=queue_name)
                snapshot.queues.append(queue)
                queue_map[queue_name] = queue
            if counter:
                queue.job_states = dict(counter)
                queue.total_jobs = sum(counter.values())
        for queue in snapshot.queues:
            if queue.total_jobs is None and queue.job_states:
                queue.total_jobs = sum(queue.job_states.values())

    @staticmethod
    def _normalise_user(owner: Optional[str]) -> str:
        if not owner:
            return ""
        owner = owner.strip()
        if "@" in owner:
            owner = owner.split("@", 1)[0]
        return owner

    @staticmethod
    def _parse_properties(properties: Optional[str]) -> List[str]:
        if not properties:
            return []
        return [prop.strip() for prop in properties.split(",") if prop.strip()]

    @staticmethod
    def _parse_node_jobs(jobs_field: str) -> List[str]:
        if not jobs_field:
            return []
        jobs: List[str] = []
        for part in jobs_field.replace("\n", " ").split():
            cleaned = part.strip().strip(",")
            if cleaned:
                jobs.append(cleaned)
        return jobs

    @staticmethod
    def _parse_state_counts(state_count_text: Optional[str]) -> Dict[str, int]:
        counts: Dict[str, int] = {}
        if not state_count_text:
            return counts
        tokens = state_count_text.replace("\n", " ").replace(",", "").split()
        i = 0
        mapping = {
            "transit": "T",
            "queued": "Q",
            "held": "H",
            "waiting": "W",
            "running": "R",
            "exiting": "E",
            "suspended": "S",
            "begun": "B",
            "finished": "F",
        }
        while i < len(tokens) - 1:
            key = tokens[i].rstrip(":").lower()
            value = tokens[i + 1]
            i += 2
            if not key:
                continue
            code = mapping.get(key, key.upper()[:1])
            try:
                counts[code] = int(value)
            except ValueError:
                continue
        return counts


__all__ = ["PBSDataFetcher"]
