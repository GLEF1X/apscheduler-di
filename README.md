# Implementation of dependency injection for `apscheduler`
[![PyPI version](https://img.shields.io/pypi/v/apscheduler-di.svg)](https://pypi.org/project/apscheduler-di/)[![codecov](https://codecov.io/gh/GLEF1X/apscheduler-di/branch/master/graph/badge.svg?token=X71JFESNL5)](https://codecov.io/gh/GLEF1X/apscheduler-di)[![Downloads](https://pepy.tech/badge/apscheduler-di/week)](https://pepy.tech/project/apscheduler-di)

### Motivation:

* `apscheduler-di` addresses the issue of `apscheduler` not natively supporting Dependency Injection, making it challenging for developers to pass complex objects to jobs without encountering issues.

## Features:

* Supports type hints ([PEP 561](https://www.python.org/dev/peps/pep-0561/))
* Extend `apscheduler` and provide handy aliases for events(such as `on_startup`, `on_shutdown` and
  etc)
* Offers a way to implement the [Dependency Inversion](https://en.wikipedia.org/wiki/Dependency_inversion_principle) principle from SOLID design principles.

"Under the hood" `apscheduler-di`
implements [Decorator](https://en.wikipedia.org/wiki/Decorator_pattern) pattern and wraps up the
work of native `BaseScheduler` using [rodi](https://github.com/Neoteroi/rodi) lib

### Quick example:

```python
import os
from typing import Dict

from apscheduler.jobstores.redis import RedisJobStore
from apscheduler.schedulers.blocking import BlockingScheduler

from apscheduler_di import ContextSchedulerDecorator

# pip install redis
job_stores: Dict[str, RedisJobStore] = {
    "default": RedisJobStore(
        jobs_key="dispatched_trips_jobs", run_times_key="dispatched_trips_running"
    )
}


class Tack:

    def tack(self):
        print("Tack!")


def tick(tack: Tack, some_argument: int):
    print(tack)


def main():
    scheduler = ContextSchedulerDecorator(BlockingScheduler(jobstores=job_stores))
    scheduler.ctx.add_instance(Tack(), Tack)
    scheduler.add_executor('processpool')
    scheduler.add_job(tick, 'interval', seconds=3, kwargs={"some_argument": 5})
    print('Press Ctrl+{0} to exit'.format('Break' if os.name == 'nt' else 'C'))

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        pass


if __name__ == '__main__':
    main()

```
