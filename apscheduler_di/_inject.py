import types
from datetime import datetime
from typing import List

from apscheduler.job import Job
from apscheduler.jobstores.base import BaseJobStore
from apscheduler.schedulers.base import BaseScheduler
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.util import ref_to_obj
from rodi import Container, Services

from apscheduler_di._binding import normalize_job_executable
from apscheduler_di._serialization import SharedJob


def inject_dependencies_to_scheduler(
    scheduler: BaseScheduler, container: Container
) -> None:
    services = container.build_provider()
    for job_store in scheduler._jobstores.values():  # type: BaseJobStore  # noqa

        def func_get_due_jobs_with_context(c: BaseJobStore, now: datetime) -> List[Job]:
            jobs: List[Job] = type(c).get_due_jobs(c, now)
            return _convert_raw_to_easy_maintainable_jobs(scheduler, jobs, services)

        job_store.get_due_jobs = types.MethodType(
            func_get_due_jobs_with_context, job_store
        )


def _convert_raw_to_easy_maintainable_jobs(
    scheduler: BaseScheduler, jobs: List[Job], services: Services
) -> List[Job]:
    unsafe_pickling = False
    if isinstance(scheduler, BlockingScheduler):
        unsafe_pickling = True  # pragma: no cover
    if unsafe_pickling:
        return _make_jobs_shared(jobs, scheduler, services)
    for job in jobs:
        origin_func = ref_to_obj(job.func_ref)
        job.func = normalize_job_executable(origin_func, services)
    return jobs


def _make_jobs_shared(
    jobs: List[Job], scheduler: BaseScheduler, services: Services
) -> List[SharedJob]:
    shared_jobs: List[SharedJob] = []
    for job in jobs:
        if isinstance(job, SharedJob):
            jobs.append(job)
            continue
        shared_jobs.append(SharedJob(scheduler, services, **job.__getstate__()))
    return shared_jobs
