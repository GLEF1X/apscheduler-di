from apscheduler.executors.base import BaseExecutor
from apscheduler.schedulers.base import BaseScheduler
from apscheduler.schedulers.blocking import BlockingScheduler


class MockExecutor(BaseExecutor):

    def _do_submit_job(self, job, run_times):
        pass


class MockScheduler(BaseScheduler):

    def __init__(self, **options):
        super().__init__(**options)
        self.executor = MockExecutor()

    def shutdown(self, wait=True):
        pass

    def wakeup(self):
        pass


class MockBlockingScheduler(BlockingScheduler):
    def start(self, *args, **kwargs):
        pass

    def _main_loop(self):
        pass
