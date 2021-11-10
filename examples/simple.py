import asyncio
from abc import abstractmethod, ABC
from typing import Dict

from apscheduler.jobstores.redis import RedisJobStore
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from rodi import Services

from src.decorator import ContextSchedulerDecorator

# pip install redis
job_defaults: Dict[str, RedisJobStore] = {
    "default": RedisJobStore(
        jobs_key="dispatched_trips_jobs", run_times_key="dispatched_trips_running"
    )
}
job_stores: Dict[str, RedisJobStore] = {
    "default": RedisJobStore(
        jobs_key="dispatched_trips_jobs", run_times_key="dispatched_trips_running"
    )
}


# domain object:
class Cat:
    def __init__(self, name):
        self.name = name


# abstract interface
class ICatsRepository(ABC):
    @abstractmethod
    def get_by_id(self, _id) -> Cat:
        pass


class PostgresCatsRepository(ICatsRepository):
    def get_by_id(self, _id) -> Cat:
        # TODO: implement logic to use a connection to the db
        return Cat("...")


async def some_job(ctx: Services):
    repository = ctx.get(ICatsRepository)
    print(repository.get_by_id(id))  # type: Cat


async def some_infinite_cycle():
    while True:
        await asyncio.sleep(.5)


def run_scheduler():
    scheduler = ContextSchedulerDecorator(AsyncIOScheduler(jobstores=job_stores,
                                                           job_defaults=job_defaults))
    scheduler.ctx.add_instance(PostgresCatsRepository(), ICatsRepository)
    scheduler.add_job(some_job, trigger="interval", seconds=5)
    scheduler.start()


async def main():
    run_scheduler()
    await some_infinite_cycle()


if __name__ == '__main__':
    asyncio.run(main())
