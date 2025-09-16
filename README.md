# PBS Pro Textual TUI

A terminal user interface built with [Textual](https://textual.textualize.io/) for monitoring
[PBS Pro](https://altair.com/pbs-professional) schedulers at the
[Argonne Leadership Computing Facility](https://alcf.anl.gov). The dashboard surfaces job,
queue, and node activity in a single view and refreshes itself automatically so operators can
track workload health in real time.

## Features

- **Live PBS data** – executes `qstat -f -x`, `qstat -Q -f -x`, and `pbsnodes -x` to gather
  jobs, queues, and node information.
- **Automatic refresh** – updates every 30 seconds by default with a manual refresh binding
  (`r`).
- **Summary cards** – quick totals for job states, node states, and queue health.
- **Rich tables** – sortable (via cursor) tables for jobs, nodes, and queues with detail views
  for the selected record.
- **Fallback sample data** – optional bundled data makes it easy to demo the interface without
  connecting to a production scheduler (`PBS_TUI_SAMPLE_DATA=1`).

## Installation

1. Ensure Python 3.10 or newer is available.
2. Install the project (and Textual) in your environment:

   ```bash
   pip install -e .
   ```

   Development extras (formatting, etc.) can be installed with `pip install -e .[dev]`.

## Usage

Launch the dashboard once the PBS CLI utilities (`qstat`, `pbsnodes`) are on the `PATH`:

```bash
pbs-tui
```

The same entry point is available via `python -m pbs_tui`. The interface displays a summary
panel, tables for jobs/nodes/queues, and a detail pane for the selected row. Refreshing happens
automatically; press `r` to force an immediate update.

### Key bindings

| Key | Action |
| --- | ------ |
| `q` | Quit the application |
| `r` | Refresh immediately |
| `j` | Focus the jobs table |
| `n` | Focus the nodes table |
| `u` | Focus the queues table |

Use the arrow keys/`PageUp`/`PageDown` to move through rows once a table has focus.

### Sample mode

If you want to explore the UI without a live PBS cluster, export `PBS_TUI_SAMPLE_DATA=1`
(or pass `force_sample=True` to `PBSDataFetcher`). The application will display bundled example
jobs, nodes, and queues along with a warning banner indicating that the data is synthetic.

## Architecture

- `pbs_tui.fetcher.PBSDataFetcher` orchestrates `qstat`/`pbsnodes` calls, parses XML output, and
  converts it into structured dataclasses (`Job`, `Node`, `Queue`).
- `pbs_tui.app.PBSTUI` is the Textual application that renders the dashboard, periodically asks
  the fetcher for new data, and updates the widgets.
- `pbs_tui.samples.sample_snapshot` provides the demonstration snapshot used when PBS commands
  cannot be executed.

The UI styles are defined in `pbs_tui/app.tcss`. Adjust the CSS to change layout or theme
attributes.

## Development notes

- The application refresh interval defaults to 30 seconds. Pass a different value to
  `PBSTUI(refresh_interval=...)` if desired.
- Errors encountered while running PBS commands are surfaced in the status bar so operators can
  quickly see when data is stale.
- When both PBS utilities are unavailable and the fallback is disabled, the UI will show an empty
  dashboard with an error message in the status bar.
