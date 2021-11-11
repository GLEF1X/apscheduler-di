from typing import Tuple, Any, Dict, Callable

from apscheduler_di.binding.util import get_method_annotations_base


def get_missing_arguments(func: Callable[..., Any], args: Tuple[Any, ...],
                          kwargs: Dict[Any, Any]) -> Dict[str, None]:
    """
    Get arguments to skip ValueError with traceback "The following arguments have not been supplied"
    It raises, because we injecting our dependencies using functools.wraps and do not change
    signature, so we still need to deceive Job __init__ method

    :return:
    """
    missing_keyword_arguments: Dict[str, None] = {}
    if len(kwargs) + len(args) < len(get_method_annotations_base(func).keys()):
        for key in get_method_annotations_base(func).keys():
            missing_keyword_arguments[key] = None

    return missing_keyword_arguments
