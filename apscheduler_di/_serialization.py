import pickle
from typing import Any, Callable

from apscheduler.job import Job
from apscheduler.schedulers.base import BaseScheduler
from apscheduler.util import ref_to_obj
from rodi import Services

from apscheduler_di._binding import normalize_job_executable
from apscheduler_di._helper import get_missing_arguments


def _load_func_from_ref(func_ref: str, ctx: Services) -> Callable[..., Any]:
    original_func = ref_to_obj(func_ref)
    return normalize_job_executable(original_func, ctx)


class SharedJob(Job):
    def __init__(self, scheduler: BaseScheduler, ctx: Services, **kwargs):
        fn_args = kwargs.get('args', ())
        fn_kwargs = kwargs.get('kwargs', {})
        fn = kwargs['func']
        if not callable(fn):
            fn = ref_to_obj(fn)
        kwargs['kwargs'].update(get_missing_arguments(fn, fn_args, fn_kwargs))

        if kwargs.get('version') is not None:
            kwargs.pop('version')  # pragma: no cover
        super().__init__(scheduler, **kwargs)
        self.kwargs = {}
        self._ctx = ctx

    def __getstate__(self):
        state = super().__getstate__()
        state.update(ctx=pickle.dumps(self._ctx))
        return state

    def __setstate__(self, state):
        if state.get('version', 1) > 1:
            raise ValueError(  # pragma: no cover
                'Job has version %s, but only version 1 can be handled'
                % state['version']
            )
        self._ctx = pickle.loads(state['ctx'])
        self.id = state['id']
        self.func_ref = state['func']
        self.args = state['args']
        self.kwargs = state['kwargs']
        self.func = _load_func_from_ref(self.func_ref, self._ctx)
        self.trigger = state['trigger']
        self.executor = state['executor']
        self.name = state['name']
        self.misfire_grace_time = state['misfire_grace_time']
        self.coalesce = state['coalesce']
        self.max_instances = state['max_instances']
        self.next_run_time = state['next_run_time']
