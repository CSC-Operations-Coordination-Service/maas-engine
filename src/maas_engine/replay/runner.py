"""Run a child process under a deadline that does not depend on its output"""

import collections
import subprocess
import sys
import threading
import time
from collections.abc import Callable

TAIL_LINES = 20
# seconds a child gets to exit after terminate() before it is killed
TERMINATE_GRACE = 10
KILLED_EXIT_CODE = -9
# exit code of a run refused because the runner was terminated before it started
STOPPED_EXIT_CODE = -15
# longest wait on the child, in seconds: terminate() runs in another thread and
# sets the moment the child must die, which the loop only learns at its next turn
WAIT_SLICE = 0.5


class ChildRunner:
    """Run a child process, keep its last lines, kill it after a deadline

    The deadline is a clock, not a check made when the child prints: a child that
    hangs silently is killed on time.
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._child = None
        self._kill_at = None
        self._closed = False

    def terminate(self) -> None:
        """Terminate the running child, if any, and start no other one; safe to call
        from another thread"""
        with self._lock:
            # sticky: a run about to start would otherwise escape the stop
            self._closed = True
            if self._child is not None and self._child.poll() is None:
                self._kill_at = time.monotonic() + TERMINATE_GRACE
                self._child.terminate()

    @staticmethod
    def _kill(child) -> None:
        try:
            child.kill()
        except OSError:  # already gone
            pass

    # one loop with three clocks: exit, progress, deadline
    # pylint: disable-next=too-many-locals
    def run(
        self,
        command: list[str],
        env: dict,
        timeout: float,
        progress: Callable[[str], None],
        progress_interval: float = 30,
    ) -> tuple[int, str]:
        """Run command, echo its output, and keep its last lines

        progress receives those lines every progress_interval seconds. The child
        is killed after timeout seconds even when it prints nothing.

        Returns:
            tuple[int, str]: exit code (-9 when killed for the timeout, -15 without
                a child once terminate() was called), output tail
        """
        tail = collections.deque(maxlen=TAIL_LINES)
        with self._lock:
            if self._closed:
                return STOPPED_EXIT_CODE, "stopped before start"
            self._kill_at = None
            # pylint: disable-next=consider-using-with
            child = subprocess.Popen(
                command,
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                encoding="utf-8",
                errors="replace",
                bufsize=1,
            )
            self._child = child

        def read():
            for line in child.stdout:
                sys.stdout.write(line)
                tail.append(line.rstrip())

        reader = threading.Thread(target=read, daemon=True)
        reader.start()

        started = time.monotonic()
        deadline = started + timeout
        next_progress = started + progress_interval
        timed_out = False
        try:
            while True:
                with self._lock:
                    kill_at = self._kill_at
                wake = min(deadline, next_progress)
                if kill_at is not None:
                    wake = min(wake, kill_at)
                try:
                    child.wait(
                        timeout=min(WAIT_SLICE, max(0.0, wake - time.monotonic()))
                    )
                    break
                except subprocess.TimeoutExpired:
                    pass
                now = time.monotonic()
                if now >= deadline:
                    timed_out = True
                    self._kill(child)
                    child.wait()
                    break
                if kill_at is not None and now >= kill_at:
                    self._kill(child)
                    child.wait()
                    break
                if now >= next_progress:
                    progress("\n".join(tail))
                    next_progress = now + progress_interval
            reader.join(timeout=5)
        finally:
            with self._lock:
                self._child = None
            if child.poll() is None:
                self._kill(child)
                child.wait()

        if timed_out:
            tail.append(f"killed after {timeout}s")
            return KILLED_EXIT_CODE, "\n".join(tail)
        return child.returncode, "\n".join(tail)
