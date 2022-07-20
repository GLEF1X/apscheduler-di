import functools
from typing import Any, Callable

from apscheduler.schedulers.base import BaseScheduler
from rodi import Container


class ApschedulerEvent:
    def __init__(self, scheduler: BaseScheduler, ctx: Container, on_event: int) -> None:
        self._scheduler = scheduler
        self._on_event = on_event
        self._ctx = ctx

    def __iadd__(self, handler: Callable[..., Any]) -> 'ApschedulerEvent':
        with_context = functools.partial(handler, ctx=self._ctx)
        self._scheduler.add_listener(callback=with_context, mask=self._on_event)
        return self

    def __isub__(self, handler: Callable[..., Any]) -> 'ApschedulerEvent':
        self._scheduler.remove_listener(callback=handler)
        return self

    def __len__(self) -> int:
        return len(self._scheduler._listeners)  # noqa
