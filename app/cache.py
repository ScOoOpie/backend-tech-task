# app/cache.py
import functools
from typing import Any, Callable, Optional
import hashlib
import inspect
from .redis_client import redis_client

def cache_key_generator(*args, **kwargs) -> str:
    """Генератор ключей кэша на основе аргументов функции"""
    func_name = kwargs.pop('__func_name', 'unknown')
    
    # Создаем уникальную строку из аргументов
    args_str = str(args)
    kwargs_str = str(sorted(kwargs.items()))
    key_string = f"{func_name}:{args_str}:{kwargs_str}"
    
    # Хешируем для короткого ключа
    return f"cache:{hashlib.md5(key_string.encode()).hexdigest()}"

def cached(ttl: int = 300, key_prefix: str = None):
    """
    Декоратор для кэширования результатов функций
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs):
            # Генерируем ключ кэша
            cache_key = cache_key_generator(
                *args, 
                __func_name=key_prefix or func.__name__,
                **kwargs
            )
            
            # Пробуем получить из кэша
            cached_result = await redis_client.get(cache_key)
            if cached_result is not None:
                return cached_result
            
            # Выполняем функцию если кэш пустой
            result = await func(*args, **kwargs)
            
            # Сохраняем в кэш
            if result is not None:
                await redis_client.set(cache_key, result, ttl)
            
            return result
        
        @functools.wraps(func)
        def sync_wrapper(*args, **kwargs):
            # Синхронная версия (для sync функций)
            import asyncio
            return asyncio.run(async_wrapper(*args, **kwargs))
        
        return async_wrapper if inspect.iscoroutinefunction(func) else sync_wrapper
    
    return decorator

def invalidate_cache(pattern: str):
    """
    Декоратор для инвалидации кэша
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs):
            result = await func(*args, **kwargs)
            
            # Инвалидируем кэш после выполнения функции
            if redis_client.is_connected:
                await redis_client.delete_pattern(pattern)
            
            return result
        
        @functools.wraps(func)
        def sync_wrapper(*args, **kwargs):
            import asyncio
            return asyncio.run(async_wrapper(*args, **kwargs))
        
        return async_wrapper if inspect.iscoroutinefunction(func) else sync_wrapper
    
    return decorator