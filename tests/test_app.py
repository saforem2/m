from pbs_tui.app import PBSTUI, _env_flag, run


def test_env_flag_truthy(monkeypatch):
    monkeypatch.delenv("TEST_FLAG", raising=False)
    assert not _env_flag("TEST_FLAG")
    monkeypatch.setenv("TEST_FLAG", "1")
    assert _env_flag("TEST_FLAG")
    monkeypatch.setenv("TEST_FLAG", "false")
    assert not _env_flag("TEST_FLAG")


def test_run_honours_environment(monkeypatch):
    monkeypatch.setenv("PBS_TUI_HEADLESS", "1")
    monkeypatch.setenv("PBS_TUI_AUTOPILOT", "quit")
    captured = {}

    def fake_run(self, *, headless=False, auto_pilot=None, **kwargs):
        captured["headless"] = headless
        captured["auto_pilot"] = auto_pilot

    monkeypatch.setattr(PBSTUI, "run", fake_run, raising=False)
    run()
    assert captured["headless"] is True
    assert captured["auto_pilot"] is not None
