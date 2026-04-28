from __future__ import annotations

import json
from typing import TYPE_CHECKING
from unittest import mock

import pymssql
import pytest
from meltano.core.state_store import MeltanoState
from meltano.core.state_store.base import MissingStateBackendSettingsError, StateIDLockedError

if TYPE_CHECKING:
    from meltano_state_backend_mssql.backend import MssqlStateStoreManager


# ---------------------------------------------------------------------------
# set / get
# ---------------------------------------------------------------------------


def test_set_state(
    subject: tuple[MssqlStateStoreManager, mock.Mock],
) -> None:
    """Execute is called with the correct parameters when setting state."""
    manager, mock_cursor = subject

    state = MeltanoState(
        state_id="test_job",
        partial_state={"singer_state": {"partial": 1}},
        completed_state={"singer_state": {"complete": 1}},
    )
    manager.set(state)

    mock_cursor.execute.assert_called()
    call_args = mock_cursor.execute.call_args
    assert call_args[0][1] == (
        "test_job",
        json.dumps({"singer_state": {"partial": 1}}),
        json.dumps({"singer_state": {"complete": 1}}),
    )


def test_get_state(
    subject: tuple[MssqlStateStoreManager, mock.Mock],
) -> None:
    """State is returned and correctly deserialised."""
    manager, mock_cursor = subject

    mock_cursor.fetchone.return_value = (
        '{"singer_state": {"partial": 1}}',
        '{"singer_state": {"complete": 1}}',
    )

    state = manager.get("test_job")
    assert state is not None
    assert state.state_id == "test_job"
    assert state.partial_state == {"singer_state": {"partial": 1}}
    assert state.completed_state == {"singer_state": {"complete": 1}}

    mock_cursor.execute.assert_called()
    assert mock_cursor.execute.call_args[0][1] == ("test_job",)


@pytest.mark.parametrize(
    ("partial_raw", "completed_raw", "expected_partial", "expected_completed"),
    (
        pytest.param(
            None,
            '{"singer_state": {"complete": 1}}',
            {},
            {"singer_state": {"complete": 1}},
            id="null_partial",
        ),
        pytest.param(None, None, {}, {}, id="both_null"),
    ),
)
def test_get_state_null_columns(
    subject: tuple[MssqlStateStoreManager, mock.Mock],
    partial_raw: str | None,
    completed_raw: str | None,
    expected_partial: dict[str, object],
    expected_completed: dict[str, object],
) -> None:
    """NULL NVARCHAR(MAX) columns are returned as empty dicts."""
    manager, mock_cursor = subject
    mock_cursor.fetchone.return_value = (partial_raw, completed_raw)

    state = manager.get("test_job")
    assert state is not None
    assert state.state_id == "test_job"
    assert state.partial_state == expected_partial
    assert state.completed_state == expected_completed


def test_get_state_not_found(
    subject: tuple[MssqlStateStoreManager, mock.Mock],
) -> None:
    """None is returned when the state does not exist."""
    manager, mock_cursor = subject
    mock_cursor.fetchone.return_value = None

    assert manager.get("nonexistent") is None


# ---------------------------------------------------------------------------
# delete
# ---------------------------------------------------------------------------


def test_delete_state(
    subject: tuple[MssqlStateStoreManager, mock.Mock],
) -> None:
    """Execute is called with the correct state_id when deleting."""
    manager, mock_cursor = subject

    manager.delete("test_job")

    mock_cursor.execute.assert_called()
    assert mock_cursor.execute.call_args[0][1] == ("test_job",)


# ---------------------------------------------------------------------------
# get_state_ids
# ---------------------------------------------------------------------------


def test_get_state_ids(
    subject: tuple[MssqlStateStoreManager, mock.Mock],
) -> None:
    """All state IDs are returned when no pattern is given."""
    manager, mock_cursor = subject
    mock_cursor.fetchall.return_value = [("job1",), ("job2",), ("job3",)]

    state_ids = list(manager.get_state_ids())

    mock_cursor.execute.assert_called()
    assert state_ids == ["job1", "job2", "job3"]


def test_get_state_ids_with_pattern(
    subject: tuple[MssqlStateStoreManager, mock.Mock],
) -> None:
    """Glob pattern is converted to SQL LIKE and used in the query."""
    manager, mock_cursor = subject
    mock_cursor.fetchall.return_value = [("test_job_1",), ("test_job_2",)]

    state_ids = list(manager.get_state_ids("test_*"))

    mock_cursor.execute.assert_called()
    assert mock_cursor.execute.call_args[0][1] == ("test_%",)
    assert state_ids == ["test_job_1", "test_job_2"]


# ---------------------------------------------------------------------------
# clear_all
# ---------------------------------------------------------------------------


def test_clear_all(
    subject: tuple[MssqlStateStoreManager, mock.Mock],
) -> None:
    """clear_all returns the count returned by the pre-truncate query."""
    manager, mock_cursor = subject
    mock_cursor.fetchone.return_value = (5,)

    count = manager.clear_all()

    assert mock_cursor.execute.call_count >= 2
    assert count == 5


# ---------------------------------------------------------------------------
# acquire_lock
# ---------------------------------------------------------------------------


def test_acquire_lock(
    subject: tuple[MssqlStateStoreManager, mock.Mock],
) -> None:
    """Lock is acquired and released without error."""
    manager, mock_cursor = subject

    with manager.acquire_lock("test_job", retry_seconds=0):
        mock_cursor.execute.assert_called()

    # table setup (x2) + stale cleanup + insert + delete
    assert mock_cursor.execute.call_count >= 3


def test_acquire_lock_retry(
    subject: tuple[MssqlStateStoreManager, mock.Mock],
) -> None:
    """Lock is retried when INSERT raises IntegrityError (lock held by another process)."""
    manager, mock_cursor = subject
    mock_cursor.execute.side_effect = [
        None,  # _ensure_tables state table
        None,  # _ensure_tables lock table
        None,  # _cleanup_stale_locks (first attempt)
        pymssql.IntegrityError,  # INSERT fails — lock held
        None,  # _cleanup_stale_locks (second attempt)
        None,  # INSERT succeeds
        None,  # DELETE (release)
    ]

    with (
        mock.patch("meltano_state_backend_mssql.backend.sleep"),
        manager.acquire_lock("test_job", retry_seconds=0.01),
    ):
        pass


def test_acquire_lock_max_retries_exceeded(
    subject: tuple[MssqlStateStoreManager, mock.Mock],
) -> None:
    """StateIDLockedError is raised when the lock cannot be acquired within the timeout."""
    manager, mock_cursor = subject

    # _ensure_tables() was already called during fixture __init__, so reset the mock
    # and make every INSERT unconditionally raise IntegrityError (lock always held).
    mock_cursor.execute.reset_mock()

    def execute_side_effect(*args: object, **_kwargs: object) -> None:
        if "INSERT" in str(args[0]):
            raise pymssql.IntegrityError

    mock_cursor.execute.side_effect = execute_side_effect

    with (
        mock.patch("meltano_state_backend_mssql.backend.sleep"),
        pytest.raises(
            StateIDLockedError,
            match="Could not acquire lock for state_id: test_job",
        ),
        manager.acquire_lock("test_job", retry_seconds=0.01),
    ):
        pass  # pragma: no cover


# ---------------------------------------------------------------------------
# connection_params_from_uri
# ---------------------------------------------------------------------------


def test_connection_params_from_uri() -> None:
    """URI is parsed into pymssql connect kwargs."""
    from meltano_state_backend_mssql.backend import connection_params_from_uri

    params = connection_params_from_uri("mssql+pymssql://myuser:mypass@myhost:1433/mydb")
    assert params["server"] == "myhost"
    assert params["port"] == 1433
    assert params["user"] == "myuser"
    assert params["password"] == "mypass"  # noqa: S105
    assert params["database"] == "mydb"


def test_connection_params_overrides() -> None:
    """Explicit overrides take precedence over the URI values."""
    from meltano_state_backend_mssql.backend import connection_params_from_uri

    params = connection_params_from_uri(
        "mssql+pymssql://olduser:oldpass@oldhost:1433/olddb",
        host="newhost",
        user="newuser",
        password="newpass",  # noqa: S106
        database="newdb",
    )
    assert params["server"] == "newhost"
    assert params["user"] == "newuser"
    assert params["database"] == "newdb"


def test_connection_params_minimal_uri_with_overrides() -> None:
    """Overrides fill in all fields when the URI has no components (covers False branches)."""
    from meltano_state_backend_mssql.backend import connection_params_from_uri

    # Bare URI with no host/port/user/pass/db → all False branches in the URI parsing block
    params = connection_params_from_uri(
        "mssql+pymssql://",
        host="h",
        user="u",
        password="p",  # noqa: S106
        database="d",
    )
    assert params["server"] == "h"
    assert params["user"] == "u"
    assert params["password"] == "p"  # noqa: S105
    assert params["database"] == "d"


@pytest.mark.parametrize(
    ("uri", "match"),
    (
        pytest.param(
            "mssql+pymssql://:pass@host/db",
            "MSSQL user is required",
            id="missing_user",
        ),
        pytest.param(
            "mssql+pymssql://user@host/db",
            "MSSQL password is required",
            id="missing_password",
        ),
        pytest.param(
            "mssql+pymssql://user:pass@host",
            "MSSQL database is required",
            id="missing_database",
        ),
    ),
)
def test_connection_params_missing_required(uri: str, match: str) -> None:
    """MissingStateBackendSettingsError is raised for missing required credentials."""
    from meltano_state_backend_mssql.backend import connection_params_from_uri

    with pytest.raises(MissingStateBackendSettingsError, match=match):
        connection_params_from_uri(uri)


# ---------------------------------------------------------------------------
# context manager / close
# ---------------------------------------------------------------------------


def test_context_manager(
    mock_connection: tuple[mock.MagicMock, mock.Mock, mock.Mock],
) -> None:
    """Manager can be used as a context manager; close() is called on exit."""
    from meltano_state_backend_mssql.backend import MssqlStateStoreManager

    _, mock_conn, _ = mock_connection
    manager = MssqlStateStoreManager(
        uri="mssql+pymssql://testuser:testpass@testhost:1433/testdb",
        host="testhost",
        port=1433,
        user="testuser",
        password="testpass",  # noqa: S106
        database="testdb",
    )
    manager.connection = mock_conn

    with manager as m:
        assert m is manager

    mock_conn.close.assert_called_once()


@pytest.mark.usefixtures("mock_connection")
def test_close_without_connection() -> None:
    """close() is a no-op when connection has never been opened."""
    from meltano_state_backend_mssql.backend import MssqlStateStoreManager

    manager = MssqlStateStoreManager(
        uri="mssql+pymssql://testuser:testpass@testhost:1433/testdb",
        host="testhost",
        port=1433,
        user="testuser",
        password="testpass",  # noqa: S106
        database="testdb",
    )
    # Remove any cached connection (set during _ensure_tables in __init__)
    manager.__dict__.pop("connection", None)
    manager.close()  # should not raise


def test_explicit_table_name(
    mock_connection: tuple[mock.MagicMock, mock.Mock, mock.Mock],
) -> None:
    """Passing an explicit table name skips the default-table-name log message."""
    from meltano_state_backend_mssql.backend import MssqlStateStoreManager

    _, mock_conn, _ = mock_connection
    manager = MssqlStateStoreManager(
        uri="mssql+pymssql://testuser:testpass@testhost:1433/testdb",
        host="testhost",
        port=1433,
        user="testuser",
        password="testpass",  # noqa: S106
        database="testdb",
        table="custom_state",
    )
    manager.connection = mock_conn
    assert manager.table == "custom_state"
    assert "[dbo].[custom_state]" in manager.state_table
