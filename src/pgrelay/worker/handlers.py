"""Explicit async handler registry."""

import inspect
from collections.abc import Awaitable, Callable
from typing import Any, overload

from pgrelay.errors import ValidationError

Handler = Callable[[dict[str, Any]], Awaitable[dict[str, Any] | None]]


class HandlerRegistry:
    """Registry for explicitly registered async handlers."""

    def __init__(self) -> None:
        """Initialize an empty registry."""
        self._handlers: dict[str, Handler] = {}

    @overload
    def register(self, name: str) -> Callable[[Handler], Handler]:
        """Return a decorator that registers a handler."""

    @overload
    def register(self, name: str, handler: Handler) -> Handler:
        """Register and return a handler."""

    def register(self, name: str, handler: Handler | None = None) -> Handler | Callable[[Handler], Handler]:
        """Register an async handler directly or as a decorator."""
        if not name:
            raise ValidationError("Handler name is required")

        def decorator(candidate: Handler) -> Handler:
            if not inspect.iscoroutinefunction(candidate):
                raise ValidationError("Handler must be async")
            if name in self._handlers:
                raise ValidationError("Handler name already registered")
            self._handlers[name] = candidate
            return candidate

        if handler is None:
            return decorator
        return decorator(handler)

    def get(self, name: str) -> Handler:
        """Return a registered handler."""
        return self._handlers[name]
