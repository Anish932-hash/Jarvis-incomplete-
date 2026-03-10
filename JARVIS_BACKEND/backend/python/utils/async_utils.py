import asyncio
from functools import wraps

class AsyncUtils:

    @staticmethod
    async def run_parallel(tasks):
        return await asyncio.gather(*tasks, return_exceptions=False)

    @staticmethod
    def debounce(wait):
        """Prevent function from being called too frequently."""
        def decorator(func):
            task = None

            @wraps(func)
            async def wrapper(*args, **kwargs):
                nonlocal task
                if task:
                    task.cancel()
                task = asyncio.create_task(AsyncUtils._debounce_task(func, wait, *args, **kwargs))
            return wrapper
        return decorator

    @staticmethod
    async def _debounce_task(func, wait, *args, **kwargs):
        await asyncio.sleep(wait)
        return await func(*args, **kwargs)

    @staticmethod
    def throttle(wait):
        """Ensure function cannot run more often than wait seconds."""
        def decorator(func):
            is_waiting = False

            @wraps(func)
            async def wrapper(*args, **kwargs):
                nonlocal is_waiting
                if is_waiting:
                    return
                is_waiting = True
                res = await func(*args, **kwargs)
                await asyncio.sleep(wait)
                is_waiting = False
                return res
            return wrapper
        return decorator

    @staticmethod
    async def run_with_timeout(coro, timeout):
        return await asyncio.wait_for(coro, timeout)