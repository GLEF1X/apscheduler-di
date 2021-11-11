from typing import Tuple

from apscheduler_di.helper import get_missing_arguments


def some_function(arg1: int, arg2: str, arg3: Tuple):
    pass


def test_get_missing_arguments():
    missing_arguments = get_missing_arguments(some_function, args=(), kwargs={})
    assert missing_arguments == {"arg1": None, "arg2": None, "arg3": None}
