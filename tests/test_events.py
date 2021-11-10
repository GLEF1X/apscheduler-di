from typing import List

import pytest
from apscheduler.events import EVENT_ALL
from apscheduler.events import EVENT_SCHEDULER_STARTED

from apscheduler_di.events import ApschedulerEvent
from tests.mocks.mock_schedulers import MockScheduler


class StubScheduler(MockScheduler):
    _listeners: List[int] = []

    def add_listener(self, callback, mask=EVENT_ALL):
        self._listeners.append(1)

    def remove_listener(self, callback):
        self._listeners.remove(callback)

    def clear(self):
        self._listeners = []

    @property
    def listeners(self):
        return self._listeners


def some_event_handler(event):
    pass


@pytest.fixture(name="event_collection", scope="function")
def event_fixture() -> ApschedulerEvent:
    stub_scheduler = StubScheduler()
    event = ApschedulerEvent(stub_scheduler, on_event=EVENT_SCHEDULER_STARTED)
    yield event
    stub_scheduler.clear()


def test_add_event(event_collection: ApschedulerEvent):
    event_collection += some_event_handler
    assert event_collection._scheduler.listeners == [1]


def test_remove_event(event_collection: ApschedulerEvent):
    event_collection._scheduler._listeners = [some_event_handler]
    event_collection -= some_event_handler
    assert event_collection._scheduler.listeners == []


def test_get_len(event_collection: ApschedulerEvent):
    event_collection += some_event_handler
    assert len(event_collection) == 1
