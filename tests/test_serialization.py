import pickle
import ssl
from datetime import datetime

from apscheduler.events import EVENT_ALL
from apscheduler.triggers.cron import CronTrigger
from rodi import Container

from apscheduler_di.inject import set_serialization_options
from apscheduler_di.serialization import SharedJob
from tests.mocks.mock_schedulers import MockScheduler, MockBlockingScheduler


def example_job():
    pass


def example_job2(ssl_context: ssl.SSLContext):
    pass


def test_pickle_special_job():
    container = Container()
    provider = container.build_provider()
    job = SharedJob(MockScheduler(), provider,
                    func=example_job,
                    trigger=CronTrigger(second=5),
                    kwargs={}, args=tuple(), executor='some_executor',
                    misfire_grace_time=10,
                    coalesce=False,
                    max_instances=1,
                    next_run_time=datetime.now())
    dumped = pickle.dumps(job)
    assert pickle.loads(dumped) == job


def test_pickle_special_job_with_ssl_context():
    container = Container()
    provider = container.build_provider()
    scheduler = MockBlockingScheduler()
    set_serialization_options(EVENT_ALL, scheduler)
    job = SharedJob(scheduler, provider,
                    func=example_job2,
                    trigger=CronTrigger(second=5),
                    kwargs={},
                    args=(ssl.SSLContext(),),
                    executor='some_executor',
                    misfire_grace_time=10,
                    coalesce=False,
                    max_instances=1,
                    next_run_time=datetime.now())
    dumped = pickle.dumps(job)
    assert pickle.loads(dumped) == job
