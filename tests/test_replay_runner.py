"""The child runner: deadline, tail, progress, terminate"""

import os
import sys
import threading
import time

import pytest

from maas_engine.replay import runner as runner_module
from maas_engine.replay.runner import (
    KILLED_EXIT_CODE,
    STOPPED_EXIT_CODE,
    TAIL_LINES,
    ChildRunner,
)


def python(code: str) -> list[str]:
    return [sys.executable, "-u", "-c", code]


def run(runner, code, timeout=30, progress=None, interval=30, env=None):
    return runner.run(
        python(code),
        env if env is not None else dict(os.environ),
        timeout,
        progress or (lambda lines: None),
        interval,
    )


def test_the_exit_code_and_the_output_come_back():
    code, tail = run(ChildRunner(), "print('hello'); raise SystemExit(3)")
    assert code == 3
    assert tail == "hello"


def test_stderr_is_in_the_tail():
    code, tail = run(
        ChildRunner(), "import sys; print('out'); print('err', file=sys.stderr)"
    )
    assert code == 0
    assert tail.splitlines() == ["out", "err"]


def test_only_the_last_lines_are_kept():
    code, tail = run(ChildRunner(), "\n".join(["for i in range(100): print(i)"]))
    assert code == 0
    lines = tail.splitlines()
    assert len(lines) == TAIL_LINES
    assert lines[-1] == "99"


def test_output_that_is_not_utf8_does_not_break_the_run():
    code, tail = run(
        ChildRunner(),
        "import sys; sys.stdout.buffer.write(b'caf\\xe9\\n'); sys.stdout.flush()",
    )
    assert code == 0
    assert tail.startswith("caf")


def test_the_environment_is_the_one_given():
    code, tail = run(
        ChildRunner(),
        "import os; print(os.environ['REPLAY_MARK'])",
        env={**os.environ, "REPLAY_MARK": "yes"},
    )
    assert (code, tail) == (0, "yes")


def test_a_silent_child_is_killed_at_the_deadline():
    started = time.monotonic()
    code, tail = run(ChildRunner(), "import time; time.sleep(60)", timeout=0.5)
    assert code == KILLED_EXIT_CODE
    assert "killed after 0.5s" in tail
    assert time.monotonic() - started < 10


def test_progress_gets_the_tail_every_interval():
    seen = []
    code, _ = run(
        ChildRunner(),
        "import time; print('a'); time.sleep(1.2); print('b')",
        progress=seen.append,
        interval=0.3,
    )
    assert code == 0
    assert len(seen) >= 2
    assert seen[0] == "a"


def test_terminate_before_the_run_refuses_to_start():
    runner = ChildRunner()
    runner.terminate()
    assert run(runner, "print('never')") == (STOPPED_EXIT_CODE, "stopped before start")


def test_terminate_during_the_run_stops_the_child():
    runner = ChildRunner()
    result = []
    thread = threading.Thread(
        target=lambda: result.append(
            run(runner, "import time; print('up', flush=True); time.sleep(60)")
        )
    )
    thread.start()
    deadline = time.monotonic() + 10
    while runner._child is None and time.monotonic() < deadline:  # noqa: SLF001
        time.sleep(0.05)
    runner.terminate()
    thread.join(15)
    assert not thread.is_alive()
    assert result[0][0] == -15


def test_a_child_that_ignores_terminate_is_killed_after_the_grace(monkeypatch):
    monkeypatch.setattr(runner_module, "TERMINATE_GRACE", 0.5)
    runner = ChildRunner()
    result = []
    code = (
        "import signal, time; signal.signal(signal.SIGTERM, signal.SIG_IGN); "
        "print('up', flush=True); time.sleep(60)"
    )
    thread = threading.Thread(target=lambda: result.append(run(runner, code)))
    thread.start()
    deadline = time.monotonic() + 10
    while runner._child is None and time.monotonic() < deadline:  # noqa: SLF001
        time.sleep(0.05)
    time.sleep(0.5)  # let it install its handler
    started = time.monotonic()
    runner.terminate()
    thread.join(15)
    assert not thread.is_alive()
    assert result[0][0] == -9
    assert time.monotonic() - started < 10


def test_the_runner_is_reusable_until_terminated():
    runner = ChildRunner()
    assert run(runner, "print(1)")[0] == 0
    assert run(runner, "print(2)")[0] == 0
    runner.terminate()
    assert run(runner, "print(3)")[0] == STOPPED_EXIT_CODE


@pytest.mark.parametrize("missing", ["/nonexistent/interpreter"])
def test_a_command_that_cannot_start_raises(missing):
    with pytest.raises(OSError):
        ChildRunner().run([missing], dict(os.environ), 5, lambda lines: None)
