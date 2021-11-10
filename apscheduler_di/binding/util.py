import inspect
from functools import wraps
from inspect import _ParameterKind, Signature
from typing import Callable, Any, TypeVar, get_type_hints, Mapping

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
    def __init__(self, method):
        super().__init__(
            f"Cannot normalize method `{method.__qualname__}` because its "
            f"signature contains *args, or *kwargs, or keyword only parameters. "
            f"If you use a decorator, please use `functools.@wraps` "
            f"with your wrapper, to fix this error."
        )


class UnsupportedForwardRefInSignatureError(NormalizationError):
    def __init__(self, unsupported_type):
        super().__init__(  # pragma: no cover
            f"Cannot normalize method `{unsupported_type}` because its "
            f"signature contains a forward reference (type annotation as string). "
            f"Use type annotations to exact types to fix this error. "
        )


def _get_method_annotations_or_throw(method):
    method_locals = getattr(method, "_locals", None)
    method_globals = getattr(method, "_globals", None)

    try:
        return get_type_hints(method, globalns=method_globals, localns=method_locals)
    except TypeError:
        if inspect.isclass(method) or hasattr(method, "__call__"):
            # can be a callable class
            return get_type_hints(
                method.__call__, globalns=method_globals, localns=method_locals
            )
        raise  # pragma: no cover


def get_method_annotations_base(method: Callable[..., Any]):
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
        func: _T,
        params: Mapping[str, ParamInfo],
        params_len: int,
) -> _T:
    if params_len == 0:
        return func

    @wraps(func)
    def job():
        values = []
        for param in params.values():
            values.append(services.get(param.kind))
        return func(*values)

    return job


def get_async_wrapper(
        services: Services,
        func: _T,
        params: Mapping[str, ParamInfo],
        params_len: int,
) -> _T:
    if params_len == 0:
        return func

    @wraps(func)
    async def job():
        values = []
        for param in params.values():
            values.append(services.get(param.annotation))

        return await func(*values)

    return job


def normalize_job(func: _T, services: Services) -> _T:
    params = get_method_annotations_base(func)
    params_len = len(params)

    if any(
            str(param).startswith("*") or param.kind.value == _ParameterKind.KEYWORD_ONLY
            for param in params.values()
    ):
        raise UnsupportedSignatureError(func)

    if inspect.iscoroutinefunction(func):
        normalized = get_async_wrapper(services, func, params, params_len)
    else:
        normalized = get_sync_wrapper(services, func, params, params_len)

    return normalized
