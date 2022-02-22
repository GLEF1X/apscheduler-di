import asyncio
from typing import Dict, Any

from apscheduler.jobstores.redis import RedisJobStore
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from apscheduler_di import ContextSchedulerDecorator

# pip install redis
job_stores: Dict[str, RedisJobStore] = {
    "default": RedisJobStore(
        jobs_key="dispatched_trips_jobs", run_times_key="dispatched_trips_running"
    )
}


class Broadcaster:

    def start(self) -> None:
        print("Tack!")


def broadcast(user_id: int, broadcaster: Broadcaster, additional_data: Dict[str, Any]):
    print(f"Executing broadcast task using {user_id=} and {broadcaster=} and {additional_data}")


async def main():
    scheduler = ContextSchedulerDecorator(AsyncIOScheduler(jobstores=job_stores))
    scheduler.ctx.add_instance(Broadcaster(), Broadcaster)
    scheduler.add_job(broadcast, 'interval', seconds=3, kwargs={
        "user_id": 543534,
        "additional_data": {
            "hello": "world"
        }
    })

    scheduler.start()

    try:
        await asyncio.Future()
    except (SystemExit, KeyboardInterrupt):
        pass


if __name__ == '__main__':
    asyncio.run(main())
