import asyncio
from typing import Any, Dict, Optional

from apscheduler.jobstores.redis import RedisJobStore
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from apscheduler_di import ContextSchedulerDecorator

# pip install redis
job_stores: Dict[str, RedisJobStore] = {
    'default': RedisJobStore(
        jobs_key='dispatched_trips_jobs', run_times_key='dispatched_trips_running'
    )
}

_previous_id: Optional[int] = None


class DatabaseSession:
    async def query(self) -> Any:
        ...


async def make_request_to_database(session: DatabaseSession):
    await session.query()


async def main():
    scheduler = ContextSchedulerDecorator(AsyncIOScheduler(jobstores=job_stores))
    scheduler.ctx.add_scoped_by_factory(lambda: DatabaseSession(), DatabaseSession)
    scheduler.add_job(make_request_to_database, 'interval', seconds=3)

    scheduler.start()

    try:
        await asyncio.Future()
    except (SystemExit, KeyboardInterrupt):
        pass


if __name__ == '__main__':
    asyncio.run(main())
