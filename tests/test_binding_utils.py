import pytest
from rodi import Container, Services

from apscheduler_di.binding import UnsupportedSignatureError, normalize_job_executable


class ExampleOne:
    def __init__(self, a, b):
        self.a = a
        self.b = int(b)


class ExampleTwo:
    def __init__(self, a, b, **kwargs):
        self.a = a
        self.b = b


def sync_example_job(example_one: ExampleOne) -> int:
    return 1


async def async_example_job(example_one: ExampleOne) -> int:
    return 2


def test_normalize_sync_job():
    container = Container()
    container.add_instance(ExampleOne(1, 2), ExampleOne)
    provider = container.build_provider()

    normalized_job = normalize_job_executable(sync_example_job, provider)

    assert normalized_job() == 1


@pytest.mark.asyncio
async def test_normalize_async_job():
    container = Container()
    container.add_instance(ExampleOne(1, 2), ExampleOne)
    provider = container.build_provider()

    normalized_job = normalize_job_executable(async_example_job, provider)
    assert await normalized_job() == 2


def test_raises_for_unsupported_signature():
    app_services = Services()

    def job(services, *args):
        assert services is app_services
        return services

    def job2(services, **kwargs):
        assert services is app_services
        return services

    def job3(services, *, key_only):
        assert services is app_services
        return services

    with pytest.raises(UnsupportedSignatureError):
        normalize_job_executable(job, app_services)

    with pytest.raises(UnsupportedSignatureError):
        normalize_job_executable(job2, app_services)

    with pytest.raises(UnsupportedSignatureError):
        normalize_job_executable(job3, app_services)
