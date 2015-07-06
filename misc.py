import functools
import asyncio

def catch_errors(logger):
    def func_wrapper(f):
        @functools.wraps(f)
        @asyncio.coroutine
        def async_wrapper(*args, **kwargs):
            try:
                return (yield from f(*args, **kwargs))
            except Exception as e:
                logger.warning('Unhandled exception caught', exc_info=True)
        
        @functools.wraps(f)
        def blocking_wrapper(*args, **kwargs):
            try:
                return f(*args, **kwargs)
            except Exception as e:
                logger.warning('Unhandled exception caught', exc_info=True)    
        if asyncio.iscoroutinefunction(f):
            return async_wrapper
        else:
            return blocking_wrapper
    return func_wrapper
