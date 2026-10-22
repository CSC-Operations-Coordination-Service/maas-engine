"""What a caller provides: the parts of a replay that are specific to it"""

import importlib
from typing import Any, Protocol, runtime_checkable

from maas_engine.replay.request import ReplayRequest


class ReplayNotPossible(ValueError):
    """The request cannot be replayed; the message goes to the operation record"""


@runtime_checkable
class ReplayHandler(Protocol):
    """Prepare, build and clean up one replay

    The worker calls the three methods in a lane's thread, one request at a time
    per lane, but lanes run side by side: a handler shared by two lanes must be safe
    to call from both.

    An optional fourth method, `describe_failure(code, tail) -> str`, turns a
    non-zero exit code and the output tail into the text stored in the record. The
    default is `exit code <code>`, then the tail.
    """

    def prepare(self, request: ReplayRequest, lane: str, workdir: str) -> Any:
        """Check the request can run and make what the command needs

        Args:
            request: the parsed request
            lane: name of the lane it was taken from
            workdir: a folder the handler may create its own temporary folder in

        Returns:
            whatever `command` and `cleanup` need

        Raises:
            ReplayNotPossible: the request cannot be replayed. `prepare` removes
                what it made before it raises: `cleanup` is only called after a
                successful `prepare`.
        """

    def command(
        self, prepared: Any, request: ReplayRequest, lane: str
    ) -> tuple[list[str], dict[str, str]]:
        """The child's argv and environment"""

    def cleanup(self, prepared: Any) -> None:
        """Remove what `prepare` made; called even when the child failed"""


def load_handler(spec: str, kwargs: dict | None = None) -> ReplayHandler:
    """Instantiate a handler from `package.module:Class`

    Raises:
        ValueError: the spec is malformed, or the class lacks a handler method
    """
    module_name, separator, class_name = spec.partition(":")
    if not (module_name and separator and class_name):
        raise ValueError(f"handler must be 'package.module:Class', got {spec!r}")
    try:
        module = importlib.import_module(module_name)
    except ImportError as error:
        raise ValueError(
            f"cannot import the handler module {module_name}: {error}"
        ) from error
    try:
        cls = getattr(module, class_name)
    except AttributeError as error:
        raise ValueError(f"{module_name} has no {class_name}") from error
    handler = cls(**(kwargs or {}))
    for name in ("prepare", "command", "cleanup"):
        if not callable(getattr(handler, name, None)):
            raise ValueError(f"{spec} has no {name}() method")
    return handler
