import asyncio
from datetime import datetime, timezone

from pbs_tui.fetcher import PBSDataFetcher


def test_fetch_snapshot_force_sample_uses_bundled_data():
    fetcher = PBSDataFetcher(force_sample=True)
    snapshot = asyncio.run(fetcher.fetch_snapshot())
    assert snapshot.source == "sample"
    assert snapshot.jobs
    assert any("sample data" in message.lower() for message in snapshot.errors)


def test_parse_state_counts_extracts_known_states():
    counts = PBSDataFetcher._parse_state_counts("queued: 2 running: 1 held: 3")
    assert counts == {"Q": 2, "R": 1, "H": 3}


def test_parse_state_counts_ignores_noise():
    counts = PBSDataFetcher._parse_state_counts("queued: notanumber waiting: 4, unknown: 2")
    assert counts == {"W": 4, "U": 2}


def test_parse_node_jobs_splits_entries():
    jobs = PBSDataFetcher._parse_node_jobs("0/123.a r/456.b, 789.c")
    assert jobs == ["0/123.a", "r/456.b", "789.c"]


def test_parse_jobs_xml_extracts_fields():
    xml = """
    <Data>
      <Job>
        <Job_Id>123.a</Job_Id>
        <Job_Name>analysis</Job_Name>
        <Job_Owner>user@host</Job_Owner>
        <queue>batch</queue>
        <job_state>R</job_state>
        <exec_host>nid0001</exec_host>
        <ctime>2024-05-11T10:00:00Z</ctime>
        <start_time>2024-05-11T10:05:00Z</start_time>
        <Resource_List>
          <walltime>01:00:00</walltime>
          <nodes>1:ppn=4</nodes>
        </Resource_List>
        <resources_used>
          <walltime>00:10:00</walltime>
        </resources_used>
      </Job>
    </Data>
    """
    fetcher = PBSDataFetcher(force_sample=True)
    job = fetcher._parse_jobs_xml(xml)[0]
    assert job.id == "123.a"
    assert job.user == "user"
    assert job.resources_requested["walltime"] == "01:00:00"
    assert job.resources_used["walltime"] == "00:10:00"
    assert job.runtime(datetime(2024, 5, 11, 10, 15, tzinfo=timezone.utc))
