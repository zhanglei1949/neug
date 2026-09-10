#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Scoped persistent COPY session."""

from __future__ import annotations

from typing import TYPE_CHECKING
from typing import Any
from typing import Dict
from typing import Optional

if TYPE_CHECKING:
    from neug.connection import Connection
    from neug.query_result import QueryResult


class BulkLoadSession:
    """Accumulate persistent ``COPY FROM`` queries into one checkpoint."""

    def __init__(self, connection: "Connection") -> None:
        self._connection = connection
        self._active = True
        self._failed = False

    @property
    def active(self) -> bool:
        return self._active and self._connection.is_open

    @property
    def failed(self) -> bool:
        return self._failed

    def execute(
        self,
        query: str,
        access_mode: str = "",
        parameters: Optional[Dict[str, Any]] = None,
    ) -> "QueryResult":
        self._require_active()
        try:
            return self._connection._execute_bulk_load(query, access_mode, parameters)
        except Exception:
            self._failed = True
            raise

    def commit(self) -> None:
        self._require_active()
        if self._failed:
            raise RuntimeError("Bulk-load session failed; rollback is required.")
        self._connection._commit_bulk_load()
        self._active = False

    def rollback(self) -> None:
        self._require_active()
        self._connection._rollback_bulk_load()
        self._active = False

    def __enter__(self) -> "BulkLoadSession":
        self._require_active()
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> bool:
        if exc_type is None:
            try:
                self.commit()
            except Exception:
                if self.active:
                    try:
                        self.rollback()
                    except Exception:
                        self._active = False
                raise
        else:
            try:
                self.rollback()
            except Exception:
                # Preserve the exception that caused the session to abort.
                self._active = False
        return False

    def __del__(self) -> None:
        if self._active:
            if self._connection.is_open:
                try:
                    self._connection._rollback_bulk_load()
                except Exception:
                    pass
            self._active = False

    def _require_active(self) -> None:
        if not self._active or not self._connection.is_open:
            self._active = False
            raise RuntimeError("Bulk-load session is no longer active.")
