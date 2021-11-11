from datetime import datetime

from apscheduler.job import Job
from apscheduler.jobstores.base import BaseJobStore
from apscheduler.util import obj_to_ref

from tests.mocks.mock_schedulers import MockScheduler


class Dependency:
    pass


def injectable(d: Dependency):
    return 1


class MockJobStore(BaseJobStore):

    def lookup_job(self, job_id):
        pass

    def get_due_jobs(self, now):
        return [MockJob(MockScheduler(), injectable)]

    def get_next_run_time(self):
        pass

    def get_all_jobs(self):
        pass

    def add_job(self, job):
        pass

    def update_job(self, job):
        pass

    def remove_job(self, job_id):
        pass

    def remove_all_jobs(self):
        pass


class MockJob(Job):

    def __init__(self, scheduler, func):
        super().__init__(scheduler)
        self.func = func
        self.func_ref = obj_to_ref(func)

    def _modify(self, **changes):
        pass

    def _get_run_times(self, now):
        pass


class InjectableMockOfScheduler(MockScheduler):

    def __init__(self, **options):
        super().__init__(**options)
        self._jobstores = {
            "default": MockJobStore()
        }

    def get_jobs(self, jobstore=None, pending=None):
        mock_jobstore = self._jobstores["default"]
        return mock_jobstore.get_due_jobs(datetime.now())
