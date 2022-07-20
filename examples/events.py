import asyncio

from apscheduler.events import JobExecutionEvent
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from rodi import Container

from apscheduler_di import ContextSchedulerDecorator


def tick():
    raise Exception('Ooops, something went wrong(')


async def handle_job_error(event: JobExecutionEvent, ctx: Container):
    print('OH NO!! WE GOT EXCEPTION')  # handling exception here


async def main():
    scheduler = ContextSchedulerDecorator(AsyncIOScheduler())
    scheduler.on_job_error += handle_job_error
    scheduler.add_job(tick, 'interval', seconds=3)
    scheduler.start()

    try:
        await asyncio.Future()
    except (SystemExit, KeyboardInterrupt):
        pass


if __name__ == '__main__':
    asyncio.run(main())
