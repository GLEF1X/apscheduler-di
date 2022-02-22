import asyncio
import dataclasses
from abc import abstractmethod, ABC
from typing import Dict

from apscheduler.jobstores.redis import RedisJobStore
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from apscheduler_di.decorator import ContextSchedulerDecorator

# pip install redis
job_stores: Dict[str, RedisJobStore] = {
    "default": RedisJobStore(
        jobs_key="dispatched_trips_jobs", run_times_key="dispatched_trips_running"
    )
}


# domain object:
class Cat:
    def __init__(self, name):
        self.name = name


@dataclasses.dataclass()
class Config:
    some_param: int


class ICatsRepository(ABC):
    @abstractmethod
    def get_by_id(self, _id) -> Cat:
        pass


class PostgresCatsRepository(ICatsRepository):
    def get_by_id(self, _id) -> Cat:
        # TODO: implement logic to use a connection to the db
        return Cat("...")


async def some_job(repository: ICatsRepository, config: Config):
    cat = repository.get_by_id(config.some_param)
    print(cat.name)


async def some_infinite_cycle():
    while True:
        await asyncio.sleep(.5)


def run_scheduler():
    scheduler = ContextSchedulerDecorator(AsyncIOScheduler(jobstores=job_stores))
    scheduler.ctx.add_instance(PostgresCatsRepository(), ICatsRepository)
    scheduler.ctx.add_instance(Config(some_param=1), Config)
    scheduler.add_job(some_job, trigger="interval", seconds=5)
    scheduler.start()


async def main():
    run_scheduler()
    await some_infinite_cycle()


if __name__ == '__main__':
    asyncio.run(main())
