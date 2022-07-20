import pickle
from datetime import datetime

from apscheduler.triggers.cron import CronTrigger
from apscheduler.util import obj_to_ref
from rodi import Container

from apscheduler_di._serialization import SharedJob
from tests.mocks.mock_schedulers import MockScheduler


def example_job():
    pass


def test_pickle_shared_job_with_ref_to_func():
    ctx = Container().build_provider()
    shared_job = SharedJob(
        MockScheduler(),
        ctx,
        func=obj_to_ref(example_job),
        trigger=CronTrigger(second=5),
        kwargs={},
        args=tuple(),
        executor='some_executor',
        misfire_grace_time=10,
        coalesce=False,
        max_instances=1,
        next_run_time=datetime.now(),
    )

    pickled = pickle.dumps(shared_job)
    assert pickle.loads(pickled) == shared_job
