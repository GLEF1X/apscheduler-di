import asyncio
from typing import Dict

from aiogram import Bot
from apscheduler.jobstores.redis import RedisJobStore
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from apscheduler_di.decorator import ContextSchedulerDecorator

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


async def send_message_by_timer(bot: Bot):
    await bot.send_message(chat_id=4434, text="Hello world!")


async def some_infinite_cycle():
    while True:
        await asyncio.sleep(.5)


def run_scheduler():
    token = "1443372:AAEL5UPVPoruXeX9fqoD_6f-1Qk7AHQ"
    bot = Bot(token)
    scheduler = ContextSchedulerDecorator(AsyncIOScheduler(jobstores=job_stores,
                                                           job_defaults=job_defaults))
    scheduler.ctx.add_instance(bot, Bot)
    scheduler.add_job(send_message_by_timer, trigger="interval", seconds=5)
    scheduler.start()


async def main():
    run_scheduler()
    await some_infinite_cycle()


if __name__ == '__main__':
    asyncio.run(main())
