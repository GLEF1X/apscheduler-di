import asyncio
from datetime import timedelta, datetime
from threading import TIMEOUT_MAX
from typing import Any, Dict, Callable, Optional, List, Tuple

import six
from apscheduler.events import JobSubmissionEvent, EVENT_JOB_SUBMITTED, EVENT_JOB_MAX_INSTANCES, \
    EVENT_ALL
from apscheduler.executors.base import MaxInstancesReachedError, BaseExecutor
from apscheduler.job import Job
from apscheduler.jobstores.base import BaseJobStore
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.schedulers.base import STATE_PAUSED, BaseScheduler
from apscheduler.util import timedelta_seconds, undefined
from rodi import Container


class ContextSchedulerDecorator(BaseScheduler):

    def __init__(self, scheduler: BaseScheduler, **options: Any):
        self.ctx = Container()
        self._scheduler = scheduler
        if isinstance(scheduler, AsyncIOScheduler):
            self._eventloop = asyncio.get_event_loop()  # for compat with AsyncIOScheduler
            self._timeout = None  # for compat with AsyncIOScheduler
        self._scheduler._process_jobs = self._process_jobs
        super().__init__(**options)

    def wakeup(self) -> None:
        self._scheduler.wakeup()

    def _start_timer(self, wait_seconds):
        self._stop_timer()
        if wait_seconds is not None:
            self._timeout = self._eventloop.call_later(wait_seconds, self.wakeup)

    def _stop_timer(self):
        if self._timeout:
            self._timeout.cancel()
            del self._timeout

    def shutdown(self, wait: bool = True):
        self._scheduler.shutdown(wait=wait)

    def add_job(self,
                func: Callable[..., Any],
                trigger: Optional[str] = None,
                args: Optional[Tuple[Any, ...]] = None,
                kwargs: Optional[Dict[Any, Any]] = None,
                id: Optional[str] = None,
                name: Optional[str] = None,
                misfire_grace_time=undefined,
                coalesce=undefined,
                max_instances: int = undefined,
                next_run_time: datetime = undefined,
                jobstore: str = 'default',
                executor: str = 'default',
                replace_existing: bool = False,
                **trigger_args) -> Job:
        if kwargs is None:
            kwargs = {}
        kwargs["ctx"] = None  # just trick
        return self._scheduler.add_job(
            func, trigger, args, kwargs, id, name, misfire_grace_time,
            coalesce, max_instances, next_run_time, jobstore, executor,
            replace_existing, **trigger_args
        )

    def scheduled_job(self,
                      trigger: Optional[str] = None,
                      args: Optional[Tuple[Any, ...]] = None,
                      kwargs: Optional[Dict[Any, Any]] = None,
                      id: Optional[str] = None,
                      name: Optional[str] = None,
                      misfire_grace_time=undefined,
                      coalesce=undefined,
                      max_instances: int = undefined,
                      next_run_time: datetime = undefined,
                      jobstore: str = 'default',
                      executor: str = 'default',
                      **trigger_args) -> Job:
        return self._scheduler.scheduled_job(
            trigger, args, kwargs, id, name, misfire_grace_time, coalesce,
            max_instances, next_run_time, jobstore, executor, **trigger_args
        )

    def resume_job(self, job_id, jobstore=None):
        self._scheduler.resume_job(job_id, jobstore)

    def resume(self):
        self._scheduler.resume()

    def reschedule_job(self, job_id: str, jobstore: Optional[str] = None,
                       trigger: Optional[str] = None,
                       **trigger_args: Any) -> Job:
        return self._scheduler.reschedule_job(job_id, jobstore, trigger, **trigger_args)

    def remove_listener(self, callback: Callable[..., Any]) -> None:
        self._scheduler.remove_listener(callback)

    def _configure(self, config: Dict[Any, Any]) -> None:
        self._scheduler._configure(config)

    def configure(self, gconfig: Dict[Any, Any] = {}, prefix: str = 'apscheduler.',
                  **options: Any) -> None:
        self._scheduler.configure(gconfig, prefix, **options)

    def _create_trigger(self, trigger: str, trigger_args: Any):
        return self._scheduler._create_trigger(trigger, trigger_args)

    def start(self, paused: bool = False):
        self._scheduler.start(paused)

    def pause(self):
        self._scheduler.pause()

    def _dispatch_event(self, event):
        self._scheduler._dispatch_event(event)

    def _real_add_job(self, job: Job, jobstore_alias: str, replace_existing: bool):
        self._scheduler._real_add_job(job, jobstore_alias, replace_existing)

    def _check_uwsgi(self) -> None:
        self._scheduler._check_uwsgi()

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

    def pause_job(self, job_id: str, jobstore: Optional[str] = None):
        self._scheduler.pause_job(job_id, jobstore)

    def modify_job(self, job_id: str, jobstore: Optional[str] = None, **changes: Any) -> Job:
        return self._scheduler.modify_job(job_id, jobstore, **changes)

    def get_jobs(self, jobstore: Optional[str] = None, pending=None) -> List[Job]:
        return self._scheduler.get_jobs(jobstore, pending)

    def get_job(self, job_id: str, jobstore: Optional[str] = None):
        return self._scheduler.get_job(job_id, jobstore)

    def add_listener(self, callback: Callable[..., Any], mask=EVENT_ALL):
        return self._scheduler.add_listener(callback, mask)

    def _create_default_executor(self):
        return self._scheduler._create_default_executor()

    def add_jobstore(self, jobstore: str, alias: str = 'default', **jobstore_opts):
        return self._scheduler.add_jobstore(jobstore, alias, **jobstore_opts)

    def add_executor(self, executor: BaseExecutor, alias: str = 'default', **executor_opts):
        self._scheduler.add_executor(executor, alias, **executor_opts)

    def _lookup_jobstore(self, alias: str) -> BaseJobStore:
        return self._scheduler._lookup_jobstore(alias)

    def _lookup_job(self, job_id: str, jobstore_alias: str) -> Job:
        return self._scheduler._lookup_job(job_id, jobstore_alias)

    def _create_default_jobstore(self):
        return self._scheduler._create_default_jobstore()

    def _create_lock(self):
        return self._scheduler._create_lock()

    def _create_plugin_instance(self, type_, alias, constructor_kwargs):
        return self._scheduler._create_plugin_instance(type, alias, constructor_kwargs)

    def _lookup_executor(self, alias):
        return self._scheduler._lookup_executor(alias)

    def _process_jobs(self):
        if self._scheduler.state == STATE_PAUSED:
            self._scheduler._logger.debug('Scheduler is paused -- not processing jobs')
            return None

        self._scheduler._logger.debug('Looking for jobs to run')
        now = datetime.now(self._scheduler.timezone)
        next_wakeup_time = None
        events = []
        prepared_context = self.ctx.build_provider()

        with self._scheduler._jobstores_lock:
            for jobstore_alias, jobstore in six.iteritems(self._scheduler._jobstores):
                try:
                    due_jobs = jobstore.get_due_jobs(now)
                except Exception as e:
                    # Schedule a wakeup at least in jobstore_retry_interval seconds
                    self._scheduler._logger.warning('Error getting due jobs from job store %r: %s',
                                                    jobstore_alias, e)
                    retry_wakeup_time = now + timedelta(
                        seconds=self._scheduler.jobstore_retry_interval)
                    if not next_wakeup_time or next_wakeup_time > retry_wakeup_time:
                        next_wakeup_time = retry_wakeup_time

                    continue

                for job in due_jobs:
                    # Look up the job's executor
                    try:
                        executor = self._scheduler._lookup_executor(job.executor)
                    except BaseException:
                        self._scheduler._logger.error(
                            'Executor lookup ("%s") failed for job "%s" -- removing it from the '
                            'job store', job.executor, job)
                        self._scheduler.remove_job(job.id, jobstore_alias)
                        continue

                    run_times = job._get_run_times(now)
                    run_times = run_times[-1:] if run_times and job.coalesce else run_times
                    if run_times:
                        try:
                            job.kwargs.update(ctx=prepared_context)
                            executor.submit_job(job, run_times)
                        except MaxInstancesReachedError:
                            self._scheduler._logger.warning(
                                'Execution of job "%s" skipped: maximum number of running '
                                'instances reached (%d)', job, job.max_instances)
                            event = JobSubmissionEvent(EVENT_JOB_MAX_INSTANCES, job.id,
                                                       jobstore_alias, run_times)
                            events.append(event)
                        except BaseException:
                            self._scheduler._logger.exception(
                                'Error submitting job "%s" to executor "%s"',
                                job, job.executor)
                        else:
                            event = JobSubmissionEvent(EVENT_JOB_SUBMITTED, job.id, jobstore_alias,
                                                       run_times)
                            events.append(event)

                        # Update the job if it has a next execution time.
                        # Otherwise remove it from the job store.
                        job_next_run = job.trigger.get_next_fire_time(run_times[-1], now)
                        if job_next_run:
                            job._modify(next_run_time=job_next_run)
                            jobstore.update_job(job)
                        else:
                            self._scheduler.remove_job(job.id, jobstore_alias)

                # Set a new next wakeup time if there isn't one yet or
                # the jobstore has an even earlier one
                jobstore_next_run_time = jobstore.get_next_run_time()
                if jobstore_next_run_time and (next_wakeup_time is None or
                                               jobstore_next_run_time < next_wakeup_time):
                    next_wakeup_time = jobstore_next_run_time.astimezone(self._scheduler.timezone)

        # Dispatch collected events
        for event in events:
            self._scheduler._dispatch_event(event)

        # Determine the delay until this method should be called again
        if self.state == STATE_PAUSED:
            wait_seconds = None
            self._scheduler._logger.debug('Scheduler is paused; waiting until resume() is called')
        elif next_wakeup_time is None:
            wait_seconds = None
            self._scheduler._logger.debug('No jobs; waiting until a job is added')
        else:
            wait_seconds = min(max(timedelta_seconds(next_wakeup_time - now), 0), TIMEOUT_MAX)
            self._scheduler._logger.debug('Next wakeup is due at %s (in %f seconds)',
                                          next_wakeup_time,
                                          wait_seconds)

        return wait_seconds
