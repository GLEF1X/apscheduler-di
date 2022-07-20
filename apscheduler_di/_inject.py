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


def inject_dependencies_to_scheduler(scheduler: BaseScheduler, ctx: Container):
    prepared_context = ctx.build_provider()
    for job_store in scheduler._jobstores.values():  # type: BaseJobStore  # noqa

        def func_get_due_jobs_with_context(c: BaseJobStore, now: datetime):
            jobs: List[Job] = type(job_store).get_due_jobs(c, now)
            return _convert_raw_to_easy_maintainable_jobs(
                scheduler, jobs, prepared_context
            )

        job_store.get_due_jobs = types.MethodType(
            func_get_due_jobs_with_context, job_store
        )


def _convert_raw_to_easy_maintainable_jobs(
    scheduler: BaseScheduler, jobs: List[Job], ctx: Services
) -> List[Job]:
    unsafe_pickling = False
    if isinstance(scheduler, BlockingScheduler):
        unsafe_pickling = True  # pragma: no cover
    if unsafe_pickling:
        return _make_jobs_shared(jobs, scheduler, ctx)
    for job in jobs:
        origin_func = ref_to_obj(job.func_ref)
        job.func = normalize_job_executable(origin_func, ctx)
    return jobs


def _make_jobs_shared(
    jobs: List[Job], scheduler: BaseScheduler, ctx: Services
) -> List[SharedJob]:
    shared_jobs: List[SharedJob] = []
    for job in jobs:
        if isinstance(job, SharedJob):
            jobs.append(job)
            continue
        shared_jobs.append(SharedJob(scheduler, ctx, **job.__getstate__()))
    return shared_jobs
