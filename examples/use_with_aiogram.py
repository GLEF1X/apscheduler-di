import asyncio
from typing import Dict

from aiogram import Bot
from apscheduler.jobstores.redis import RedisJobStore
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from apscheduler_di.decorator import ContextSchedulerDecorator

# pip install redis
job_stores: Dict[str, RedisJobStore] = {
    'default': RedisJobStore(
        jobs_key='dispatched_trips_jobs', run_times_key='dispatched_trips_running'
    )
}


async def send_message_by_timer(bot: Bot):
    await bot.send_message(chat_id=1219185039, text='Hello world!')


def run_scheduler():
    token = 'BOT TOKEN'
    bot = Bot(token)
    scheduler = ContextSchedulerDecorator(AsyncIOScheduler(jobstores=job_stores))
    scheduler.ctx.add_instance(bot, Bot)
    scheduler.add_job(send_message_by_timer, trigger='interval', seconds=5)
    scheduler.start()


async def main():
    run_scheduler()
    try:
        await asyncio.Future()
    except (SystemExit, KeyboardInterrupt):
        pass


if __name__ == '__main__':
    asyncio.run(main())
