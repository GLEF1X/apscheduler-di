import functools
import inspect
from inspect import Signature, _ParameterKind
from typing import Any, Callable, Dict, List, TypeVar, get_type_hints

from rodi import CannotResolveTypeException, GetServiceContext, Services

T = TypeVar('T', bound=Callable[..., Any])


class NormalizationError(Exception):
    ...


class ParamSpec:
    __slots__ = ('name', 'annotation', 'kind', 'default', '_str')

    def __init__(self, name, annotation, kind, default, str_repr):
        self.name = name
        self.annotation = annotation
        self.kind = kind
        self.default = default
        self._str = str_repr

    def __str__(self) -> str:
        return self._str


class UnsupportedSignatureError(NormalizationError):
    def __init__(self, method: Callable[..., Any]):
        super().__init__(
            f'Cannot normalize method `{method.__qualname__}` because its '
            f'signature contains *args, or *kwargs, or keyword only parameters. '
            f'If you use a decorator, please use `functools.@wraps` '
            f'with your wrapper, to fix this error.'
        )


class UnableToResolveDependencyError(Exception):
    def __init__(self, message: str, third_party_di_lib_exception: Exception):
        super().__init__(f'Unable to resolve the dependency: {message}')
        self.third_party_di_lib_exception = third_party_di_lib_exception


def normalize_job_executable(
    func: Callable[..., Any],
    services: Services,
) -> Callable[..., Any]:
    check_if_signature_is_supported(func)

    if inspect.iscoroutinefunction(func) or inspect.iscoroutine(func):
        normalized = get_async_wrapper(services, func)
    else:
        normalized = get_sync_wrapper(services, func)

    return normalized


def check_if_signature_is_supported(func: Callable[..., Any]) -> None:
    params = get_func_param_specs(func)

    if any(
        str(param).startswith('*') or param.kind.value == _ParameterKind.KEYWORD_ONLY
        for param in params.values()
    ):
        raise UnsupportedSignatureError(func)


def get_func_param_specs(method: Callable[..., Any]) -> Dict[str, ParamSpec]:
    signature = Signature.from_callable(method)
    params = {
        key: ParamSpec(
            value.name, value.annotation, value.kind, value.default, str(value)
        )
        for key, value in signature.parameters.items()
    }

    annotations = _get_method_annotations_or_throw(method)
    for key, value in params.items():
        if key in annotations:
            value.annotation = annotations[key]
    return params


def _get_method_annotations_or_throw(method: Callable[..., Any]) -> Dict[str, Any]:
    method_locals = getattr(method, '_locals', None)
    method_globals = getattr(method, '_globals', None)
    return get_type_hints(method, globalns=method_globals, localns=method_locals)


def get_sync_wrapper(services: Services, func: T) -> T:
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        return func(*resolve_dependencies(services, func, **kwargs))

    return wrapper


def get_async_wrapper(services: Services, func: T) -> T:
    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        return await func(*resolve_dependencies(services, func, **kwargs))

    return wrapper


def resolve_dependencies(
    services: Services, func: Callable[..., Any], **kwargs: Any
) -> List[Any]:
    dependencies = []
    for param_spec in get_func_param_specs(func).values():
        with GetServiceContext() as context:
            try:
                instance = services.get(param_spec.annotation, context)
            except CannotResolveTypeException as ex:
                instance = kwargs.get(param_spec.name)
                if instance is None:
                    raise UnableToResolveDependencyError(
                        f'Instance of {param_spec.name} argument found neither in '
                        'apscheduler-di context nor in kwargs argument',
                        ex,
                    ) from ex
            dependencies.append(instance)
    return dependencies
