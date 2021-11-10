from rodi import Container

from apscheduler_di.inject import _inject_dependencies
from tests.mocks.mock_injectable import Dependency, InjectableMockOfScheduler


def test_inject_dependencies():
    container = Container()
    scheduler = InjectableMockOfScheduler()
    container.add_instance(Dependency(), Dependency)
    _inject_dependencies(scheduler, container)

    patched_jobs = scheduler.get_jobs()
    assert patched_jobs[0].func() == 1
