# app/migrate.py
import os
import sys
import asyncio
import logging
from alembic.config import Config
from alembic import command

# Настройка логгера
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def run_migrations():
    """Применяет миграции Alembic"""
    try:
        logger.info("🚀 Starting database migrations...")
        
        # Получаем URL БД из переменных окружения
        database_url = os.getenv("DATABASE_URL")
        if not database_url:
            logger.error("❌ DATABASE_URL environment variable is not set")
            return False
        
        # Создаем конфиг Alembic
        alembic_cfg = Config("alembic.ini")
        alembic_cfg.set_main_option("sqlalchemy.url", database_url)
        
        # Применяем миграции
        command.upgrade(alembic_cfg, "head")
        
        logger.info("✅ Database migrations applied successfully")
        return True
        
    except Exception as e:
        logger.error(f"❌ Database migrations failed: {e}")
        return False

if __name__ == "__main__":
    success = run_migrations()
    sys.exit(0 if success else 1)