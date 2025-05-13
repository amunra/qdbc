import asyncio
import functools


def syncify(func):
    """
    Decorator to convert an asynchronous function to a synchronous one.
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        return asyncio.run(func(*args, **kwargs))
    
    return wrapper
