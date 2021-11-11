import inspect
import types
from functools import wraps
from inspect import _ParameterKind, Signature
from typing import Callable, Any, TypeVar, get_type_hints, Mapping, Dict

from rodi import Services

_T = TypeVar("_T", bound=Callable[..., Any])


class NormalizationError(Exception):
    ...


class ParamInfo:
    __slots__ = ("name", "annotation", "kind", "default", "_str")

    def __init__(self, name, annotation, kind, default, str_repr):
        self.name = name
        self.annotation = annotation
        self.kind = kind
        self.default = default
        self._str = str_repr

    def __str__(self) -> str:
        return self._str


class UnsupportedSignatureError(NormalizationError):
    def __init__(self, method: types.FunctionType):
        super().__init__(
            f"Cannot normalize method `{method.__qualname__}` because its "
            f"signature contains *args, or *kwargs, or keyword only parameters. "
            f"If you use a decorator, please use `functools.@wraps` "
            f"with your wrapper, to fix this error."
        )


def _get_method_annotations_or_throw(method: Callable[..., Any]) -> Dict[str, Any]:
    method_locals = getattr(method, "_locals", None)
    method_globals = getattr(method, "_globals", None)
    return get_type_hints(method, globalns=method_globals, localns=method_locals)


def get_method_annotations_base(method: Callable[..., Any]) -> Dict[str, ParamInfo]:
    signature = Signature.from_callable(method)
    params = {
        key: ParamInfo(
            value.name, value.annotation, value.kind, value.default, str(value)
        )
        for key, value in signature.parameters.items()
    }

    annotations = _get_method_annotations_or_throw(method)
    for key, value in params.items():
        if key in annotations:
            value.annotation = annotations[key]
    return params


def get_sync_wrapper(
        services: Services,
        job_fn: _T,
        params: Mapping[str, ParamInfo],
        params_len: int,
) -> _T:
    if params_len == 0:
        return job_fn  # pragma: no cover

    @wraps(job_fn)
    def wrapped_job():
        values = []
        for param in params.values():
            values.append(services.get(param.annotation))
        return job_fn(*values)

    return wrapped_job


def get_async_wrapper(
        services: Services,
        job_fn: _T,
        params: Mapping[str, ParamInfo],
        params_len: int
) -> _T:
    if params_len == 0:
        return job_fn  # pragma: no cover

    @wraps(job_fn)
    async def wrapped_job():
        values = []
        for param in params.values():
            values.append(services.get(param.annotation))
        return await job_fn(*values)

    return wrapped_job


def normalize_job_executable(job_fn: types.FunctionType, services: Services) -> Callable[..., Any]:
    params = get_method_annotations_base(job_fn)
    params_len = len(params)

    if any(
            str(param).startswith("*") or param.kind.value == _ParameterKind.KEYWORD_ONLY
            for param in params.values()
    ):
        raise UnsupportedSignatureError(job_fn)

    if inspect.iscoroutinefunction(job_fn):
        normalized = get_async_wrapper(services, job_fn, params, params_len)

    else:
        normalized = get_sync_wrapper(services, job_fn, params, params_len)

    return normalized
