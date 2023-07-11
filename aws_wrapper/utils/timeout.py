from concurrent.futures import ThreadPoolExecutor
import functools


# Timeout decorator, timeout in seconds
def timeout(timeout):
    def timeout_decorator(func):
        @functools.wraps(func)
        def func_wrapper(*args, **kwargs):
            with ThreadPoolExecutor() as executor:
                future = executor.submit(func, *args, **kwargs)

                # raises TimeoutError on timeout
                return future.result(timeout=timeout)
        return func_wrapper
    return timeout_decorator
