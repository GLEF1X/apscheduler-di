import pickle
import ssl
from typing import Callable, Any, Type, Tuple

from apscheduler.job import Job
from apscheduler.schedulers.base import BaseScheduler
from apscheduler.util import ref_to_obj
from rodi import Services

from apscheduler_di.binding.util import normalize_job_executable, get_method_annotations_base


def _load_func_from_ref(func_ref: str, ctx: Services) -> Callable[..., Any]:
    original_func = ref_to_obj(func_ref)
    return normalize_job_executable(original_func, ctx)


def save_ssl_context(obj: ssl.SSLContext) -> Tuple[Type[ssl.SSLContext], Tuple[int, ...]]:
    return obj.__class__, (obj.protocol,)


class TransferredBetweenProcessesJob(Job):

    def __init__(self, scheduler: BaseScheduler, ctx: Services, **kwargs):
        version = kwargs.get("version")
        if version is not None:
            kwargs.pop("version")
        fn = kwargs["func"]
        if not callable(fn):
            fn = ref_to_obj(fn)
        if len(kwargs) + len(kwargs["args"]) < len(get_method_annotations_base(fn).keys()):
            for key in get_method_annotations_base(fn).keys():
                kwargs["kwargs"].update({key: None})  # hacking exception
        super().__init__(scheduler, **kwargs)
        self.kwargs = {}
        self._ctx = ctx

    def __getstate__(self):
        state = super().__getstate__()
        state.update(ctx=pickle.dumps(self._ctx))
        return state

    def __setstate__(self, state):
        if state.get('version', 1) > 1:
            raise ValueError('Job has version %s, but only version 1 can be handled' %
                             state['version'])
        self._ctx = pickle.loads(state["ctx"])
        self.id = state['id']
        self.func_ref = state['func']
        self.func = _load_func_from_ref(self.func_ref, self._ctx)
        self.trigger = state['trigger']
        self.executor = state['executor']
        self.args = state['args']
        self.kwargs = state['kwargs']
        self.name = state['name']
        self.misfire_grace_time = state['misfire_grace_time']
        self.coalesce = state['coalesce']
        self.max_instances = state['max_instances']
        self.next_run_time = state['next_run_time']
