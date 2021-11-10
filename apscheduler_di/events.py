from typing import (
    Any,
    Callable,
)

from apscheduler.schedulers.base import BaseScheduler


class ApschedulerEvent:
    def __init__(self, scheduler: BaseScheduler, on_event: int) -> None:
        self._scheduler = scheduler
        self.on_event = on_event

    def __iadd__(self, handler: Callable[..., Any]) -> "ApschedulerEvent":
        self._scheduler.add_listener(callback=handler, mask=self.on_event)
        return self

    def __isub__(self, handler: Callable[..., Any]) -> "ApschedulerEvent":
        self._scheduler.remove_listener(callback=handler)
        return self

    def __len__(self) -> int:
        return len(self._scheduler._listeners)  # noqa
