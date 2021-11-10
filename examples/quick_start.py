import os
from typing import Dict

from apscheduler.jobstores.redis import RedisJobStore
from apscheduler.schedulers.blocking import BlockingScheduler

from apscheduler_di import ContextSchedulerDecorator

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


class Tack:

    def tack(self):
        print("Tack!")


def tick(tack: Tack):
    print(tack)


def main():
    scheduler = ContextSchedulerDecorator(BlockingScheduler(jobstores=job_stores,
                                                            job_defaults=job_defaults))
    scheduler.ctx.add_instance(Tack(), Tack)
    scheduler.add_executor('processpool')
    scheduler.add_job(tick, 'interval', seconds=3)
    print('Press Ctrl+{0} to exit'.format('Break' if os.name == 'nt' else 'C'))

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        pass


if __name__ == '__main__':
    main()
