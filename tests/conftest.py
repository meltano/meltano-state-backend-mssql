from __future__ import annotations

from importlib.metadata import version
from typing import TYPE_CHECKING
from unittest import mock

import pytest
from testcontainers.mssql import SqlServerContainer

from meltano_state_backend_mssql.backend import MssqlStateStoreManager

if TYPE_CHECKING:
    from collections.abc import Generator


def pytest_report_header() -> list[str]:
    """Add Meltano version to test report header."""
    return [f"Meltano v{version('meltano')}"]


@pytest.fixture(scope="module")
def mssql_container() -> Generator[SqlServerContainer, None, None]:  # pragma: no cover
    """Start an MSSQL container for integration tests."""
    with SqlServerContainer("mcr.microsoft.com/mssql/server:2022-latest") as mssql:
        yield mssql


@pytest.fixture
def mock_connection() -> Generator[tuple[mock.MagicMock, mock.Mock, mock.Mock], None, None]:
    """Patch pymssql.connect and yield (mock_connect, mock_conn, mock_cursor)."""
    with mock.patch("pymssql.connect") as mock_connect:
        mock_conn = mock.Mock()
        mock_cursor = mock.Mock()

        mock_cursor_context = mock.Mock()
        mock_cursor_context.__enter__ = mock.Mock(return_value=mock_cursor)
        mock_cursor_context.__exit__ = mock.Mock(return_value=None)
        mock_conn.cursor.return_value = mock_cursor_context

        mock_connect.return_value = mock_conn
        yield mock_connect, mock_conn, mock_cursor


@pytest.fixture
def subject(
    mock_connection: tuple[mock.MagicMock, mock.Mock, mock.Mock],
) -> tuple[MssqlStateStoreManager, mock.Mock]:
    """MssqlStateStoreManager with a mocked connection."""
    _, mock_conn, mock_cursor = mock_connection
    manager = MssqlStateStoreManager(
        uri="mssql+pymssql://testuser:testpass@testhost:1433/testdb",
        host="testhost",
        port=1433,
        user="testuser",
        password="testpass",  # noqa: S106
        database="testdb",
        schema="dbo",
    )
    manager.connection = mock_conn
    return manager, mock_cursor
