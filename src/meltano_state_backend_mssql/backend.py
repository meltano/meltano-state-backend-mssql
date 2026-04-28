"""StateStoreManager for MSSQL state backend."""

from __future__ import annotations

import json
import logging
import uuid
from contextlib import contextmanager
from functools import cached_property
from time import sleep
from typing import TYPE_CHECKING, Any, cast
from urllib.parse import unquote, urlparse

import pymssql
from meltano.core.error import MeltanoError
from meltano.core.setting_definition import SettingDefinition, SettingKind
from meltano.core.state_store.base import (
    MeltanoState,
    MissingStateBackendSettingsError,
    StateIDLockedError,
    StateStoreManager,
)
from typing_extensions import override

if TYPE_CHECKING:
    from collections.abc import Generator, Iterable
    from types import TracebackType


DEFAULT_TABLE_NAME = "state"
DEFAULT_SCHEMA_NAME = "dbo"
DEFAULT_PORT = 1433
LOCK_TIMEOUT_SECONDS = 30
STALE_LOCK_SECONDS = 300  # 5 minutes

logger = logging.getLogger(__name__)


class MssqlStateBackendError(MeltanoError):
    """Base error for MSSQL state backend."""


MSSQL_HOST = SettingDefinition(
    name="state_backend.mssql.host",
    label="MSSQL Host",
    description="MSSQL server hostname",
    kind=SettingKind.STRING,
    env_specific=True,
)

MSSQL_PORT = SettingDefinition(
    name="state_backend.mssql.port",
    label="MSSQL Port",
    description="MSSQL server port",
    kind=SettingKind.INTEGER,
    env_specific=True,
)

MSSQL_DATABASE = SettingDefinition(
    name="state_backend.mssql.database",
    label="MSSQL Database",
    description="MSSQL database name",
    kind=SettingKind.STRING,
    env_specific=True,
)

MSSQL_USER = SettingDefinition(
    name="state_backend.mssql.user",
    label="MSSQL User",
    description="MSSQL username",
    kind=SettingKind.STRING,
    env_specific=True,
)

MSSQL_PASSWORD = SettingDefinition(
    name="state_backend.mssql.password",
    label="MSSQL Password",
    description="MSSQL password",
    kind=SettingKind.STRING,
    sensitive=True,
    env_specific=True,
)

MSSQL_SCHEMA = SettingDefinition(
    name="state_backend.mssql.schema",
    label="MSSQL Schema",
    description="MSSQL schema name (default: dbo)",
    kind=SettingKind.STRING,
    env_specific=True,
)

MSSQL_TABLE = SettingDefinition(
    name="state_backend.mssql.table",
    label="MSSQL Table",
    description="MSSQL table name for state storage (default: state)",
    kind=SettingKind.STRING,
    env_specific=True,
)


def connection_params_from_uri(
    uri: str,
    *,
    host: str | None = None,
    port: int | None = None,
    database: str | None = None,
    user: str | None = None,
    password: str | None = None,
) -> dict[str, Any]:
    """Create pymssql connection params from a URI and optional parameter overrides.

    Args:
        uri: SQLAlchemy-style MSSQL URI (e.g. mssql+pymssql://user:pass@host:port/db)
        host: Override hostname
        port: Override port
        database: Override database name
        user: Override username
        password: Override password

    Returns:
        A dict of kwargs suitable for passing to pymssql.connect()

    Raises:
        MissingStateBackendSettingsError: If required parameters are missing

    """
    parsed = urlparse(uri)

    params: dict[str, Any] = {}

    if parsed.hostname:
        params["server"] = parsed.hostname
    if parsed.port:
        params["port"] = parsed.port
    if parsed.username:
        params["user"] = unquote(parsed.username)
    if parsed.password:
        params["password"] = unquote(parsed.password)
    if parsed.path and parsed.path.lstrip("/"):
        params["database"] = parsed.path.lstrip("/")

    # Apply explicit overrides
    if host:
        params["server"] = host
    if port is not None:
        params["port"] = port
    if user:
        params["user"] = user
    if password:
        params["password"] = password
    if database:
        params["database"] = database

    # Validate required
    if not params.get("user"):
        msg = "MSSQL user is required"
        raise MissingStateBackendSettingsError(msg)
    if not params.get("password"):
        msg = "MSSQL password is required"
        raise MissingStateBackendSettingsError(msg)
    if not params.get("database"):
        msg = "MSSQL database is required"
        raise MissingStateBackendSettingsError(msg)

    params.setdefault("server", "localhost")
    params.setdefault("port", DEFAULT_PORT)

    return params


class MssqlStateStoreManager(StateStoreManager):
    """State backend for MSSQL."""

    @property
    @override
    def label(self) -> str:
        """Return a human-readable label for this backend."""
        return "MSSQL"  # pragma: no cover

    def __enter__(self) -> MssqlStateStoreManager:
        """Enter the context manager.

        Returns:
            The manager instance.

        """
        return self

    @override
    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """Exit the context manager, closing the connection."""
        self.close()

    @override
    def close(self) -> None:
        """Close the MSSQL connection if it has been opened."""
        if "connection" in self.__dict__:
            self.connection.close()

    def __init__(
        self,
        uri: str,
        *,
        host: str | None = None,
        port: int | None = None,
        database: str | None = None,
        user: str | None = None,
        password: str | None = None,
        schema: str | None = None,
        table: str | None = None,
        **kwargs: Any,
    ) -> None:
        """Initialise the MssqlStateStoreManager.

        Args:
            uri: The state backend URI (mssql+pymssql://user:pass@host:port/db)
            host: MSSQL server hostname override
            port: MSSQL server port override
            database: MSSQL database name override
            user: MSSQL username override
            password: MSSQL password override
            schema: MSSQL schema name (default: dbo)
            table: MSSQL table name for state storage (default: state)
            kwargs: Additional keyword args passed to the parent

        """
        super().__init__(**kwargs)
        self.uri = uri
        self.conn_params = connection_params_from_uri(
            uri,
            host=host,
            port=port,
            database=database,
            user=user,
            password=password,
        )

        self.schema = schema or DEFAULT_SCHEMA_NAME

        if not table:
            logger.info(
                "No explicit table name provided, using default table name: %s",
                DEFAULT_TABLE_NAME,
            )
            table = DEFAULT_TABLE_NAME

        self.table = table
        # Bracket-quoted identifiers for MSSQL
        self.state_table = f"[{self.schema}].[{self.table}]"
        self.lock_table = f"[{self.schema}].[{self.table}_lock]"

        self._ensure_tables()

    @cached_property
    def connection(self) -> pymssql.Connection:
        """Get a pymssql connection with autocommit enabled.

        Returns:
            A pymssql connection object.

        """
        conn = pymssql.connect(**self.conn_params)
        conn.autocommit(True)  # noqa: FBT003
        return conn

    def _ensure_tables(self) -> None:
        """Ensure the state and lock tables exist."""
        with self.connection.cursor() as cursor:
            # State table
            cursor.execute(f"""
                IF NOT EXISTS (
                    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
                    WHERE TABLE_SCHEMA = '{self.schema}'
                    AND TABLE_NAME = '{self.table}'
                )
                BEGIN
                    CREATE TABLE {self.state_table} (
                        state_id NVARCHAR(900) NOT NULL,
                        partial_state NVARCHAR(MAX) NULL,
                        completed_state NVARCHAR(MAX) NULL,
                        updated_at DATETIME2 NOT NULL DEFAULT GETUTCDATE(),
                        CONSTRAINT PK_{self.table}_state_id PRIMARY KEY (state_id)
                    )
                END
            """)  # noqa: S608
            # Lock table — used for acquire_lock
            lock_table_name = f"{self.table}_lock"
            cursor.execute(f"""
                IF NOT EXISTS (
                    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
                    WHERE TABLE_SCHEMA = '{self.schema}'
                    AND TABLE_NAME = '{lock_table_name}'
                )
                BEGIN
                    CREATE TABLE {self.lock_table} (
                        state_id NVARCHAR(900) NOT NULL,
                        lock_id NVARCHAR(36) NOT NULL,
                        locked_at DATETIME2 NOT NULL DEFAULT GETUTCDATE(),
                        CONSTRAINT PK_{lock_table_name}_state_id PRIMARY KEY (state_id)
                    )
                END
            """)  # noqa: S608

    @override
    def set(self, state: MeltanoState) -> None:
        """Set the job state for the given state_id.

        Args:
            state: the state to set.

        """
        partial_json = json.dumps(state.partial_state) if state.partial_state else None
        completed_json = json.dumps(state.completed_state) if state.completed_state else None

        with self.connection.cursor() as cursor:
            # MSSQL MERGE for upsert
            cursor.execute(
                f"""
                MERGE INTO {self.state_table} AS target
                USING (SELECT %s AS state_id, %s AS partial_state, %s AS completed_state) AS source
                ON target.state_id = source.state_id
                WHEN MATCHED THEN
                    UPDATE SET
                        partial_state = source.partial_state,
                        completed_state = source.completed_state,
                        updated_at = GETUTCDATE()
                WHEN NOT MATCHED THEN
                    INSERT (state_id, partial_state, completed_state, updated_at)
                    VALUES (source.state_id, source.partial_state, source.completed_state, GETUTCDATE());
                """,  # noqa: S608
                (state.state_id, partial_json, completed_json),
            )

    @override
    def get(self, state_id: str) -> MeltanoState | None:
        """Get the job state for the given state_id.

        Args:
            state_id: the name of the job to get state for.

        Returns:
            The current state for the given job, or None if not found.

        """
        with self.connection.cursor() as cursor:
            cursor.execute(
                f"SELECT partial_state, completed_state FROM {self.state_table} WHERE state_id = %s",  # noqa: S608
                (state_id,),
            )
            row = cursor.fetchone()

            if not row:
                return None

            partial_state, completed_state = row
            return MeltanoState(
                state_id=state_id,
                partial_state=json.loads(str(partial_state)) if partial_state is not None else {},
                completed_state=json.loads(str(completed_state)) if completed_state is not None else {},
            )

    @override
    def delete(self, state_id: str) -> None:
        """Delete state for the given state_id.

        Args:
            state_id: the state_id to clear state for.

        """
        with self.connection.cursor() as cursor:
            cursor.execute(
                f"DELETE FROM {self.state_table} WHERE state_id = %s",  # noqa: S608
                (state_id,),
            )

    @override
    def clear_all(self) -> int:
        """Clear all states.

        Returns:
            The number of states cleared from the store.

        """
        with self.connection.cursor() as cursor:
            cursor.execute(f"SELECT COUNT(*) FROM {self.state_table}")  # noqa: S608
            row = cursor.fetchone()
            assert row is not None, "Got an empty result from the database"  # noqa: S101
            count = cast("int", row[0])

            cursor.execute(f"TRUNCATE TABLE {self.state_table}")
            return count

    def get_state_ids(self, pattern: str | None = None) -> Iterable[str]:
        """Get all state_ids available in this state store manager.

        Args:
            pattern: glob-style pattern to filter by.

        Returns:
            An iterable of state_ids.

        """
        with self.connection.cursor() as cursor:
            if pattern and pattern != "*":
                sql_pattern = pattern.replace("*", "%").replace("?", "_")
                cursor.execute(
                    f"SELECT state_id FROM {self.state_table} WHERE state_id LIKE %s",  # noqa: S608
                    (sql_pattern,),
                )
            else:
                cursor.execute(f"SELECT state_id FROM {self.state_table}")  # noqa: S608

            yield from (str(row[0]) for row in cursor.fetchall())

    def _cleanup_stale_locks(self) -> None:
        """Remove locks that are older than STALE_LOCK_SECONDS."""
        with self.connection.cursor() as cursor:
            cursor.execute(
                f"DELETE FROM {self.lock_table} WHERE DATEDIFF(SECOND, locked_at, GETUTCDATE()) > %s",  # noqa: S608
                (STALE_LOCK_SECONDS,),
            )

    @override
    @contextmanager
    def acquire_lock(
        self,
        state_id: str,
        *,
        retry_seconds: float = 1,
    ) -> Generator[None, None, None]:
        """Acquire a lock for the given job's state using a lock table.

        Args:
            state_id: the state_id to lock.
            retry_seconds: the number of seconds to wait before retrying.

        Yields:
            None

        Raises:
            StateIDLockedError: if the lock cannot be acquired within the timeout.

        """
        lock_id = str(uuid.uuid4())
        max_seconds = LOCK_TIMEOUT_SECONDS
        seconds_waited = 0.0

        while seconds_waited < max_seconds:  # pragma: no branch
            self._cleanup_stale_locks()
            with self.connection.cursor() as cursor:
                try:
                    cursor.execute(
                        f"INSERT INTO {self.lock_table} (state_id, lock_id, locked_at) VALUES (%s, %s, GETUTCDATE())",  # noqa: S608
                        (state_id, lock_id),
                    )
                    break  # lock acquired
                except pymssql.IntegrityError:
                    pass  # lock held by another process, retry

            seconds_waited += retry_seconds
            if seconds_waited >= max_seconds:
                msg = f"Could not acquire lock for state_id: {state_id}"
                raise StateIDLockedError(msg)
            sleep(retry_seconds)

        try:
            yield
        finally:
            with self.connection.cursor() as cursor:
                cursor.execute(
                    f"DELETE FROM {self.lock_table} WHERE state_id = %s AND lock_id = %s",  # noqa: S608
                    (state_id, lock_id),
                )
