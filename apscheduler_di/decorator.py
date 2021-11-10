import functools
from datetime import datetime
from threading import RLock
from typing import Any, Dict, Callable, Optional, List, Tuple

from apscheduler.events import EVENT_ALL, SchedulerEvent, EVENT_SCHEDULER_START, \
    EVENT_JOBSTORE_ADDED, EVENT_JOB_ERROR, EVENT_SCHEDULER_SHUTDOWN
from apscheduler.job import Job
from apscheduler.jobstores.base import BaseJobStore
from apscheduler.schedulers.base import BaseScheduler
from apscheduler.util import undefined
from rodi import Container

from apscheduler_di.binding.util import get_method_annotations_base
from apscheduler_di.events import ApschedulerEvent
from apscheduler_di.inject import listen_new_job_store_added, listen_startup


class ContextSchedulerDecorator(BaseScheduler):

    def __init__(self, scheduler: BaseScheduler):
        self.ctx = Container()
        self._scheduler = scheduler
        self.on_startup = ApschedulerEvent(scheduler, on_event=EVENT_SCHEDULER_START)
        self.on_error = ApschedulerEvent(scheduler, on_event=EVENT_JOB_ERROR)
        self.on_shutdown = ApschedulerEvent(scheduler, on_event=EVENT_SCHEDULER_SHUTDOWN)

        self.on_startup += functools.partial(listen_startup, scheduler=scheduler, ctx=self.ctx)
        self._scheduler.add_listener(
            functools.partial(listen_new_job_store_added, scheduler=scheduler, ctx=self.ctx),
            mask=EVENT_JOBSTORE_ADDED
        )
        super().__init__()

    def wakeup(self) -> None:
        self._scheduler.wakeup()

    def shutdown(self, wait: bool = True) -> None:
        self._scheduler.shutdown(wait=wait)

    def add_job(self,
                func: Callable[..., Any],
                trigger: Optional[str] = None,
                args: Optional[Tuple[Any, ...]] = None,
                kwargs: Optional[Dict[Any, Any]] = None,
                id: Optional[str] = None,
                name: Optional[str] = None,
                misfire_grace_time: int = undefined,
                coalesce: bool = undefined,
                max_instances: int = undefined,
                next_run_time: datetime = undefined,
                jobstore: str = 'default',
                executor: str = 'default',
                replace_existing: bool = False,
                **trigger_args) -> Job:
        if kwargs is None:
            kwargs = {}
        for key in get_method_annotations_base(func).keys():
            kwargs.update({key: None})
        job = self._scheduler.add_job(
            func, trigger, args, kwargs, id, name, misfire_grace_time,
            coalesce, max_instances, next_run_time, jobstore, executor,
            replace_existing, **trigger_args
        )
        job.kwargs = {}
        return job

    def scheduled_job(self,
                      trigger: Optional[str] = None,
                      args: Optional[Tuple[Any, ...]] = None,
                      kwargs: Optional[Dict[Any, Any]] = None,
                      id: Optional[str] = None,
                      name: Optional[str] = None,
                      misfire_grace_time: int = undefined,
                      coalesce: bool = undefined,
                      max_instances: int = undefined,
                      next_run_time: datetime = undefined,
                      jobstore: str = 'default',
                      executor: str = 'default',
                      **trigger_args) -> Job:
        return self._scheduler.scheduled_job(
            trigger, args, kwargs, id, name, misfire_grace_time, coalesce,
            max_instances, next_run_time, jobstore, executor, **trigger_args
        )

    def resume_job(self, job_id: str, jobstore: Optional[str] = None) -> Optional[Job]:
        return self._scheduler.resume_job(job_id, jobstore)

    def resume(self):
        self._scheduler.resume()

    def reschedule_job(self, job_id: str, jobstore: Optional[str] = None,
                       trigger: Optional[str] = None,
                       **trigger_args: Any) -> Job:
        return self._scheduler.reschedule_job(job_id, jobstore, trigger, **trigger_args)

    def remove_listener(self, callback: Callable[..., Any]) -> None:
        self._scheduler.remove_listener(callback)

    def start(self, paused: bool = False):
        self._scheduler.start(paused=paused)

    def pause(self):
        self._scheduler.pause()

    def remove_jobstore(self, alias: str, shutdown: bool = True):
        self._scheduler.remove_jobstore(alias, shutdown)

    def remove_job(self, job_id: str, jobstore: Optional[str] = None):
        self._scheduler.remove_job(job_id, jobstore)

    def remove_executor(self, alias: str, shutdown: bool = True):
        self._scheduler.remove_executor(alias, shutdown)

    def remove_all_jobs(self, jobstore: Optional[str] = None):
        self._scheduler.remove_all_jobs(jobstore)

    def print_jobs(self, jobstore: Optional[str] = None, out: Optional[Any] = None):
        self._scheduler.print_jobs(jobstore, out)

    def pause_job(self, job_id: str, jobstore: Optional[str] = None) -> Job:
        return self._scheduler.pause_job(job_id, jobstore)

    def modify_job(self, job_id: str, jobstore: Optional[str] = None, **changes: Any) -> Job:
        return self._scheduler.modify_job(job_id, jobstore, **changes)

    def get_jobs(self, jobstore: Optional[str] = None, pending=None) -> List[Job]:
        return self._scheduler.get_jobs(jobstore, pending)

    def get_job(self, job_id: str, jobstore: Optional[str] = None) -> Job:
        return self._scheduler.get_job(job_id, jobstore)

    def add_listener(self, callback: Callable[..., Any], mask=EVENT_ALL):
        return self._scheduler.add_listener(callback, mask)

    def add_jobstore(self, jobstore: str, alias: str = 'default', **jobstore_opts):
        self._scheduler.add_jobstore(jobstore, alias, **jobstore_opts)

    def add_executor(self, executor: str, alias: str = 'default', **executor_opts):
        self._scheduler.add_executor(executor, alias, **executor_opts)

    def _dispatch_event(self, event: SchedulerEvent):
        self._scheduler._dispatch_event(event)

    def _create_trigger(self, trigger: str, trigger_args: Any):
        return self._scheduler._create_trigger(trigger, trigger_args)

    def _real_add_job(self, job: Job, jobstore_alias: str, replace_existing: bool):
        self._scheduler._real_add_job(job, jobstore_alias, replace_existing)

    def _check_uwsgi(self) -> None:
        self._scheduler._check_uwsgi()

    def _create_default_executor(self):
        return self._scheduler._create_default_executor()

    def _lookup_jobstore(self, alias: str) -> BaseJobStore:
        return self._scheduler._lookup_jobstore(alias)

    def _lookup_job(self, job_id: str, jobstore_alias: str) -> Tuple[Job, str]:
        return self._scheduler._lookup_job(job_id, jobstore_alias)

    def _create_default_jobstore(self):
        return self._scheduler._create_default_jobstore()

    def _create_lock(self) -> RLock:
        return self._scheduler._create_lock()

    def _create_plugin_instance(self, type_, alias, constructor_kwargs):
        return self._scheduler._create_plugin_instance(type, alias, constructor_kwargs)

    def _lookup_executor(self, alias):
        return self._scheduler._lookup_executor(alias)

    def _process_jobs(self) -> Optional[float]:
        return self._scheduler._process_jobs()
