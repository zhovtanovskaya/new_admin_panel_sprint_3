from functools import wraps


class MaxRetriesExceeded(Exception):
    pass


def keep_alive(connect, exceptions, max_retries=None):
    def func_wrapper(func):
        @wraps(func)
        def inner(*args, **kwargs):
            retries_count = 0
            while True:
                try:
                    return func(connect())
                except exceptions as e:
                    if max_retries is not None:
                        retries_count += 1
                        if retries_count > max_retries:
                            raise MaxRetriesExceeded()
        return inner
    return func_wrapper