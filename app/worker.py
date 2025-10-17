import asyncio
import json
import logging
import time
from nats.aio.client import Client as NATS
from sqlalchemy.orm import Session
from app.database import SessionLocal
from app.models import Event

logger = logging.getLogger(__name__)

class EventWorker:
    def __init__(self, worker_id: str = "worker_1"):
        self.worker_id = worker_id
        self.db = SessionLocal()
        self.is_running = False
        self.nc = NATS()
    
    async def process_event(self, msg):
        """Обробка події з NATS"""
        try:
            data = json.loads(msg.data.decode())
            event_id = data.get('event_id', 'unknown')
            
            logger.info(f"🔄 [{self.worker_id}] Processing: {event_id}")
            
            # Перевірка дублікатів
            existing = self.db.query(Event).filter(Event.event_id == event_id).first()
            if existing:
                logger.debug(f"⚡ [{self.worker_id}] Duplicate: {event_id}")
                await msg.ack()
                return
            
            # Збереження в БД
            db_event = Event(
                event_id=data['event_id'],
                occurred_at=data['occurred_at'],
                user_id=data['user_id'],
                event_type=data['event_type'],
                properties=data.get('properties'),
                event_date=data['occurred_at'].split('T')[0]  # Проста дата
            )
            
            self.db.add(db_event)
            self.db.commit()
            
            logger.info(f"✅ [{self.worker_id}] Processed: {event_id}")
            await msg.ack()
            
        except Exception as e:
            logger.error(f"❌ [{self.worker_id}] Error: {e}")
            await msg.ack()
    
    async def start_consuming(self):
        """Запуск споживання подій"""
        try:
            # Чекаємо поки NATS буде готовий
            logger.info(f"⏳ [{self.worker_id}] Waiting for NATS...")
            await asyncio.sleep(10)
            
            # Підключення до NATS
            await self.nc.connect("nats://nats:4222")
            logger.info(f"🚀 [{self.worker_id}] Connected to NATS")
            
            # Підписка
            await self.nc.subscribe(
                "events.ingest",
                queue="events_workers",
                cb=self.process_event
            )
            
            logger.info(f"📥 [{self.worker_id}] Subscribed to events.ingest")
            self.is_running = True
            
            # Основний цикл
            while self.is_running:
                await asyncio.sleep(1)
                
        except Exception as e:
            logger.error(f"❌ [{self.worker_id}] Worker error: {e}")
            # Перезапуск через 30 секунд
            await asyncio.sleep(30)
            await self.start_consuming()
    
    async def shutdown(self):
        """Завершення роботи"""
        self.is_running = False
        await self.nc.close()
        self.db.close()
        logger.info(f"🛑 [{self.worker_id}] Stopped")

async def main():
    worker = EventWorker()
    await worker.start_consuming()

if __name__ == "__main__":
    asyncio.run(main())