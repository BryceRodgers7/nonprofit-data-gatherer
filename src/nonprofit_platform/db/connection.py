from __future__ import annotations

from contextlib import contextmanager
from typing import Iterator

import psycopg
from psycopg.rows import dict_row

from nonprofit_platform.config import DatabaseSettings


class Database:
    def __init__(self, settings: DatabaseSettings) -> None:
        self.settings = settings
        self._connection: psycopg.Connection | None = None

    def __enter__(self) -> "Database":
        self._connection = psycopg.connect(
            self.settings.dsn,
            autocommit=False,
            row_factory=dict_row,
        )
        self._connection.execute(f"set application_name = '{self.settings.application_name}'")
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if self._connection is None:
            return
        if exc:
            self._connection.rollback()
        self._connection.close()
        self._connection = None

    @property
    def connection(self) -> psycopg.Connection:
        if self._connection is None:
            raise RuntimeError("Database connection is not open.")
        return self._connection

    @contextmanager
    def transaction(self) -> Iterator[psycopg.Connection]:
        try:
            yield self.connection
            self.connection.commit()
        except Exception:
            self.connection.rollback()
            raise
