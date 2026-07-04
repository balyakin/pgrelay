"""Doctor command tests."""

import pytest

from pgrelay.cli import commands_doctor


class FakeSettings:
    """Fake settings for doctor tests."""

    def validate_runtime(self) -> None:
        """Validate fake runtime settings."""
        return None


class FakeEngine:
    """Fake async engine for doctor tests."""

    def __init__(self) -> None:
        """Initialize fake engine."""
        self.disposed = False

    async def dispose(self) -> None:
        """Dispose fake engine."""
        self.disposed = True


class FakeSession:
    """Fake async session for doctor tests."""

    async def __aenter__(self) -> "FakeSession":
        """Enter fake session."""
        return self

    async def __aexit__(self, exc_type: object, exc: object, traceback: object) -> None:
        """Exit fake session."""
        return None

    async def scalar(self, statement: object, parameters: dict[str, object] | None = None) -> str | None:
        """Return fake scalar query results."""
        query = str(statement)
        if "SHOW server_version_num" in query:
            return "150000"
        if "SELECT to_regclass" in query:
            return None
        if "SELECT version_num FROM pgrelay_alembic_version" in query:
            raise RuntimeError("migration was queried before table existence")
        return None


class FakeSessionFactory:
    """Fake async session factory."""

    def __call__(self) -> FakeSession:
        """Create fake session."""
        return FakeSession()


async def test_doctor_reports_missing_migration_table_before_reading_migration(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Doctor reports missing migration table before reading migration."""
    # ARRANGE
    engine = FakeEngine()
    monkeypatch.setattr(commands_doctor, "load_settings", FakeSettings)
    monkeypatch.setattr(commands_doctor, "create_engine", lambda _settings: engine)
    monkeypatch.setattr(commands_doctor, "create_session_factory", lambda _engine: FakeSessionFactory())

    # ACT
    result = await commands_doctor.doctor_async()
    output = capsys.readouterr().out

    # ASSERT
    assert result == 1
    assert output == "Missing table: pgrelay_queue\n"
    assert engine.disposed is True
