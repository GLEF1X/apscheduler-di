from rodi import Container

from apscheduler_di._inject import inject_dependencies_to_scheduler
from tests.mocks.mock_injectable import Dependency, InjectableMockOfScheduler


def test_inject_dependencies():
    container = Container()
    scheduler = InjectableMockOfScheduler()
    container.add_instance(Dependency(), Dependency)
    inject_dependencies_to_scheduler(scheduler, container)

    patched_jobs = scheduler.get_jobs()
    assert patched_jobs[0].func() == 1
