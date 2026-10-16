"""Engines reporting on fixed routes, for the telemetry tests (import once)."""

from maas_engine.engine.base import Engine, EngineReport
from maas_engine.exceptions import CannotProcessMessageException


class FakeA(Engine):
    """reports two ids on next.rk"""

    ENGINE_ID = "FAKE_A"

    def run(self, routing_key, payload):
        yield EngineReport(
            action="next.rk", data_ids=["x", "y"], document_class="Doc"
        )


class FakeB(Engine):
    """reports three ids on next.rk"""

    ENGINE_ID = "FAKE_B"

    def run(self, routing_key, payload):
        yield EngineReport(
            action="next.rk", data_ids=["p", "q", "r"], document_class="Doc"
        )


class FakeFailing(Engine):
    """cannot process the message"""

    ENGINE_ID = "FAKE_FAIL"

    def run(self, routing_key, payload):
        raise CannotProcessMessageException("nope")
        yield  # pylint: disable=unreachable


class FakeUndeclaredExchange(Engine):
    """reports two ids on an exchange the configuration does not declare"""

    ENGINE_ID = "FAKE_UNDECLARED"

    def run(self, routing_key, payload):
        yield EngineReport(
            action="next.rk",
            data_ids=["u", "v"],
            document_class="Doc",
            exchange="undeclared-exchange",
        )
