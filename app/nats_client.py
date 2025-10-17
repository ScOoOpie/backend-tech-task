import asyncio
import json
import logging
from nats.aio.client import Client as NATS
from nats.errors import ConnectionClosedError, TimeoutError, NoServersError
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)

class NATSClient:
    def __init__(self, nats_url: str = "nats://nats:4222"):
        self.nats_url = nats_url
        self.nc = NATS()
        self.is_connected = False
    
    async def connect(self):
        """Просте підключення до NATS"""
        try:
            logger.info(f"🔗 Attempting to connect to NATS at {self.nats_url}")
            
            # Просте підключення без додаткових параметрів
            await self.nc.connect(servers=[self.nats_url])
            
            self.is_connected = True
            logger.info(f"✅ Successfully connected to NATS at {self.nats_url}")
            
        except Exception as e:
            logger.error(f"❌ NATS connection failed: {e}")
            logger.info("This is normal if NATS is not running")
            self.is_connected = False
            # Не кидаємо помилку - дозволяємо продовжити без NATS
    
    async def close(self):
        """Закриття з'єднання"""
        if self.is_connected:
            await self.nc.close()
            self.is_connected = False
            logger.info("🔌 Disconnected from NATS")
    
    async def publish_event(self, subject: str, event_data: Dict[str, Any]) -> bool:
        """Публікація події в NATS"""
        if not self.is_connected:
            logger.debug("NATS not connected, skipping publish")
            return False
            
        try:
            message = json.dumps(event_data, default=str)
            await self.nc.publish(subject, message.encode())
            
            logger.debug(f"📤 Published event to {subject}: {event_data.get('event_id', 'unknown')}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to publish event to {subject}: {e}")
            return False

# Глобальний клієнт NATS
nats_client = NATSClient()