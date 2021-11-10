from typing import (
    Any,
    Callable,
)

from apscheduler.schedulers.base import BaseScheduler


class ApschedulerEventAlias:
    def __init__(self, scheduler: BaseScheduler, on_event: int) -> None:
        self.scheduler = scheduler
        self._on_event = on_event

    def __iadd__(self, handler: Callable[..., Any]) -> "ApschedulerEventAlias":
        self.scheduler.add_listener(callback=handler, mask=self._on_event)
        return self

    def __isub__(self, handler: Callable[..., Any]) -> "ApschedulerEventAlias":
        self.scheduler.remove_listener(callback=handler)
        return self

    def __call__(self, *args) -> Any:
        if args:
            self.__iadd__(args[0])
            return args[0]

        def decorator(fn):
            self.__iadd__(fn)
            return fn

        return decorator
