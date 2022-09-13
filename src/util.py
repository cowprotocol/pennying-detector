

import asyncio
import functools
import time
from contextlib import contextmanager

@contextmanager
def traced_context(logger, description):
    start_time = time.perf_counter()
    logger.debug(f'{description} ...')
    yield
    end_time = time.perf_counter()
    run_time = end_time - start_time
    logger.debug(f'{description} ... done ({run_time:.4f} secs)')


def traced(logger, description=None):
    """Logs calls to the decorated function."""
    def traced_decorator(func):
        nonlocal description
        if description is None:
            description = f'{func.__name__!r}'

        @functools.wraps(func)
        def sync_wrapper(*args, **kwargs):
            with traced_context(logger, description):
                try:
                    return func(*args, **kwargs)
                except Exception as err:
                    logger.error(f'Caught exception in {func.__name__!r}: {err}')
                    raise

        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs):
            with traced_context(logger, description):
                try:
                    return await func(*args, **kwargs)
                except Exception as err:
                    logger.error(f'Caught exception in {func.__name__!r}: {err}')
                    raise

        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper

    return traced_decorator

