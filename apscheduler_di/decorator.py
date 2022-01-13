import asyncio
import functools
import inspect
import types
import warnings
from datetime import datetime
from threading import RLock
from typing import Any, Dict, Callable, Optional, List, Tuple

import six
from apscheduler.events import (
    EVENT_ALL,
    SchedulerEvent,
    EVENT_SCHEDULER_STARTED,
    EVENT_JOBSTORE_ADDED,
    EVENT_JOB_ERROR,
    EVENT_SCHEDULER_SHUTDOWN,
    EVENT_SCHEDULER_PAUSED,
    EVENT_SCHEDULER_RESUMED,
    EVENT_EXECUTOR_ADDED,
    EVENT_EXECUTOR_REMOVED,
    EVENT_JOB_ADDED,
    EVENT_JOB_MODIFIED,
    EVENT_JOB_SUBMITTED,
    EVENT_JOB_MISSED,
    EVENT_ALL_JOBS_REMOVED
)
from apscheduler.job import Job
from apscheduler.jobstores.base import BaseJobStore
from apscheduler.schedulers.asyncio import AsyncIOScheduler, run_in_event_loop
from apscheduler.schedulers.base import BaseScheduler, STATE_STOPPED
from apscheduler.util import undefined
from rodi import Container

from apscheduler_di.events import ApschedulerEvent
from apscheduler_di.helper import get_missing_arguments
from apscheduler_di.inject import _inject_dependencies


class ContextSchedulerDecorator(BaseScheduler):

    def __init__(self, scheduler: BaseScheduler):
        self.ctx = Container()
        self._scheduler = scheduler
        # Scheduler events
        self.on_startup = ApschedulerEvent(scheduler, self.ctx, on_event=EVENT_SCHEDULER_STARTED)
        self.on_shutdown = ApschedulerEvent(scheduler, self.ctx, on_event=EVENT_SCHEDULER_SHUTDOWN)
        self.on_pause = ApschedulerEvent(scheduler, self.ctx, on_event=EVENT_SCHEDULER_PAUSED)
        self.on_resume = ApschedulerEvent(scheduler, self.ctx, on_event=EVENT_SCHEDULER_RESUMED)

        # executor events
        self.on_executor_add = ApschedulerEvent(scheduler, self.ctx,
                                                on_event=EVENT_EXECUTOR_ADDED)
        self.on_executor_removed = ApschedulerEvent(scheduler, self.ctx,
                                                    on_event=EVENT_EXECUTOR_REMOVED)

        # job events
        self.on_job_error = ApschedulerEvent(scheduler, self.ctx, on_event=EVENT_JOB_ERROR)
        self.on_job_added = ApschedulerEvent(scheduler, self.ctx, on_event=EVENT_JOB_ADDED)
        self.on_job_modified = ApschedulerEvent(scheduler, self.ctx, on_event=EVENT_JOB_MODIFIED)
        self.on_job_submitted = ApschedulerEvent(scheduler, self.ctx, on_event=EVENT_JOB_SUBMITTED)
        self.on_job_missed = ApschedulerEvent(scheduler, self.ctx, on_event=EVENT_JOB_MISSED)
        self.on_all_jobs_removed = ApschedulerEvent(
            scheduler, self.ctx, on_event=EVENT_ALL_JOBS_REMOVED
        )

        self.on_startup += lambda event, ctx: _inject_dependencies(scheduler, ctx)
        self._scheduler.add_listener(
            lambda event: _inject_dependencies(scheduler, self.ctx),
            mask=EVENT_JOBSTORE_ADDED
        )
        self._scheduler._dispatch_event = types.MethodType(_dispatch_event, self._scheduler)
        super().__init__()

    def wakeup(self) -> None:
        if isinstance(self._scheduler, AsyncIOScheduler):
            return run_in_event_loop(self._scheduler.wakeup)()
        self._scheduler.wakeup()

    def shutdown(self, wait: bool = True) -> None:
        if isinstance(self._scheduler, AsyncIOScheduler):
            return run_in_event_loop(self._scheduler.shutdown)()
        self._scheduler.shutdown(wait=wait)

    def add_job(self,
                func: Callable[..., Any],
                trigger: Optional[str] = None,
                args: Tuple[Any, ...] = (),
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
                **trigger_args) -> Job:  # pragma: no cover
        if kwargs is None:
            kwargs = {}

        kwargs.update(get_missing_arguments(func, args, kwargs))

        job_kwargs = {
            'trigger': self._scheduler._create_trigger(trigger, trigger_args),
            'executor': executor,
            'func': func,
            'args': tuple(args) if args is not None else (),
            'kwargs': dict(kwargs) if kwargs is not None else {},
            'id': id,
            'name': name,
            'misfire_grace_time': misfire_grace_time,
            'coalesce': coalesce,
            'max_instances': max_instances,
            'next_run_time': next_run_time
        }
        job_kwargs = dict((key, value) for key, value in six.iteritems(job_kwargs) if
                          value is not undefined)
        job = Job(self._scheduler, **job_kwargs)

        job.kwargs = {}

        # Don't really add jobs to job stores before the scheduler is up and running
        with self._scheduler._jobstores_lock:
            if self._scheduler.state == STATE_STOPPED:
                self._scheduler._pending_jobs.append((job, jobstore, replace_existing))
                self._scheduler._logger.info('Adding job tentatively -- it will be properly scheduled when '
                                             'the scheduler starts')
            else:
                self._scheduler._real_add_job(job, jobstore, replace_existing)

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

    def add_listener(self, callback: Callable[..., Any], mask: int = EVENT_ALL):
        return self._scheduler.add_listener(callback, mask)

    def add_jobstore(self, jobstore: str, alias: str = 'default', **jobstore_opts):
        self._scheduler.add_jobstore(jobstore, alias, **jobstore_opts)

    def add_executor(self, executor: str, alias: str = 'default', **executor_opts):
        self._scheduler.add_executor(executor, alias, **executor_opts)

    def _dispatch_event(self, event: SchedulerEvent) -> None:
        return self._scheduler._dispatch_event(event)

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


def _dispatch_event(self: BaseScheduler, event: SchedulerEvent):
    with self._listeners_lock:
        listeners = tuple(self._listeners)
    for cb, mask in listeners:
        if event.code & mask:
            _run_callback(self, callback=cb, event=event)


def _run_callback(scheduler: BaseScheduler, callback: Callable[..., Any], event: SchedulerEvent):
    try:
        if _is_function_coroutine(callback):
            if isinstance(scheduler, AsyncIOScheduler):
                scheduler._eventloop.create_task(callback(event))
            else:
                warnings.warn(
                    "running async events with sync scheduler"
                    " can lead to unpredictable behavior and unclosed descriptors or sockets",
                    UserWarning,
                    stacklevel=3
                )
                asyncio.create_task(callback(event))
        else:
            callback(event)
    except BaseException:
        scheduler._logger.exception('Error notifying listener')


def _is_function_coroutine(fn: Callable[..., Any]) -> bool:
    is_coroutine = inspect.iscoroutinefunction(fn)
    if is_coroutine is False:
        if not isinstance(fn, functools.partial):
            return is_coroutine
        is_coroutine = inspect.iscoroutinefunction(fn.func)
    return is_coroutine
