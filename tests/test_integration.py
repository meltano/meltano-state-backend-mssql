from __future__ import annotations

import os
import platform
from typing import TYPE_CHECKING, cast

import pymssql
import pytest

from meltano_state_backend_mssql.backend import MssqlStateStoreManager

if TYPE_CHECKING:
    from testcontainers.mssql import SqlServerContainer


def _connect(container: SqlServerContainer) -> pymssql.Connection:
    return pymssql.connect(
        server=container.get_container_host_ip(),
        port=str(container.get_exposed_port(1433)),
        user=container.username,
        password=container.password,
        database=container.dbname,
    )


def _schema_exists(container: SqlServerContainer, schema: str) -> bool:
    conn = _connect(container)
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM sys.schemas WHERE name = %s", (schema,))
            row = cast("tuple[int] | None", cursor.fetchone())
            return bool(row and row[0] > 0)
    finally:
        conn.close()


@pytest.mark.skipif(
    os.environ.get("CI") == "true" and platform.system() != "Linux",
    reason="Integration tests are only supported on Linux",
)
class TestIntegration:
    """Integration tests using a real SQL Server container."""

    def test_ensure_tables_creates_schema_if_not_exists(
        self,
        mssql_container: SqlServerContainer,
    ) -> None:
        """MssqlStateStoreManager creates the schema automatically when it does not exist."""
        schema = "meltano_schema_test"

        assert not _schema_exists(mssql_container, schema), (
            f"Schema '{schema}' should not exist before the manager is initialised"
        )

        manager = MssqlStateStoreManager(
            uri=mssql_container.get_connection_url(),
            schema=schema,
        )
        manager.close()

        assert _schema_exists(mssql_container, schema), f"Schema '{schema}' should have been created by _ensure_tables"
