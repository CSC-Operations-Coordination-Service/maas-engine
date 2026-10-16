"""
Dummy conftest.py for maas_engine.

If you don't know what this is for, just leave it empty.
Read more about conftest.py under:
- https://docs.pytest.org/en/stable/fixture.html
- https://docs.pytest.org/en/stable/writing_plugins.html
"""

import pytest

from maas_model.testing.fakevault import FakeVault
from maas_model.telemetry.testing import telemetry_capture  # noqa: F401


@pytest.fixture
def fake_vault():
    """a running in-process OpenBao / Vault"""
    vault = FakeVault()
    vault.start()
    yield vault
    vault.stop()
