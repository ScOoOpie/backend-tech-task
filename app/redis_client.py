# app/redis_client.py
import redis.asyncio as redis
import json
import logging
from typing import Optional, Any, Union
from datetime import timedelta
import os

logger = logging.getLogger(__name__)

class RedisClient:
    def __init__(self):
        self.redis_url = os.getenv("REDIS_URL", "redis://localhost:6379")
        self.client: Optional[redis.Redis] = None
        self.is_connected = False
    
    async def connect(self):
        """Подключение к Redis"""
        try:
            self.client = redis.from_url(
                self.redis_url,
                encoding="utf-8",
                decode_responses=True,
                socket_connect_timeout=5,
                socket_keepalive=True
            )
            
            # Проверяем подключение
            await self.client.ping()
            self.is_connected = True
            logger.info("✅ Redis connected successfully")
            
        except Exception as e:
            logger.error(f"❌ Redis connection failed: {e}")
            self.is_connected = False
    
    async def close(self):
        """Закрытие подключения"""
        if self.client and self.is_connected:
            await self.client.close()
            self.is_connected = False
            logger.info("🔌 Redis disconnected")
    
    async def get(self, key: str) -> Optional[Any]:
        """Получение значения по ключу"""
        if not self.is_connected:
            return None
            
        try:
            value = await self.client.get(key)
            if value:
                return json.loads(value)
            return None
        except Exception as e:
            logger.error(f"Redis get error for key {key}: {e}")
            return None
    
    async def set(self, key: str, value: Any, expire_seconds: int = 3600) -> bool:
        """Установка значения с TTL"""
        if not self.is_connected:
            return False
            
        try:
            serialized_value = json.dumps(value, default=str)
            await self.client.setex(key, expire_seconds, serialized_value)
            return True
        except Exception as e:
            logger.error(f"Redis set error for key {key}: {e}")
            return False
    
    async def delete(self, key: str) -> bool:
        """Удаление ключа"""
        if not self.is_connected:
            return False
            
        try:
            await self.client.delete(key)
            return True
        except Exception as e:
            logger.error(f"Redis delete error for key {key}: {e}")
            return False
    
    async def delete_pattern(self, pattern: str) -> bool:
        """Удаление ключей по паттерну"""
        if not self.is_connected:
            return False
            
        try:
            keys = await self.client.keys(pattern)
            if keys:
                await self.client.delete(*keys)
                logger.info(f"🗑️ Deleted {len(keys)} keys with pattern: {pattern}")
            return True
        except Exception as e:
            logger.error(f"Redis delete pattern error for {pattern}: {e}")
            return False
    
    async def exists(self, key: str) -> bool:
        """Проверка существования ключа"""
        if not self.is_connected:
            return False
            
        try:
            return await self.client.exists(key) > 0
        except Exception as e:
            logger.error(f"Redis exists error for key {key}: {e}")
            return False

# Глобальный клиент Redis
redis_client = RedisClient()