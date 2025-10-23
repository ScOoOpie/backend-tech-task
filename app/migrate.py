# app/migrate.py
import os
import sys
import logging
from alembic.config import Config
from alembic import command
from alembic.script import ScriptDirectory
from sqlalchemy import create_engine, text

# Настройка логгера
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_pending_migrations():
    """Проверяет есть ли непримененные миграции"""
    try:
        database_url = os.getenv("DATABASE_URL")
        alembic_cfg = Config("alembic.ini")
        alembic_cfg.set_main_option("sqlalchemy.url", database_url)
        
        # Получаем head версию из файлов миграций
        script = ScriptDirectory.from_config(alembic_cfg)
        head_rev = script.get_current_head()
        
        # Получаем текущую версию из БД
        engine = create_engine(database_url)
        with engine.connect() as conn:
            # Проверяем существует ли таблица alembic_version
            result = conn.execute(text("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = 'alembic_version'
                );
            """))
            alembic_table_exists = result.scalar()
            
            if not alembic_table_exists:
                logger.info("📝 First time setup - alembic_version table doesn't exist")
                return True
            
            # Получаем текущую версию
            result = conn.execute(text("SELECT version_num FROM alembic_version"))
            current_rev = result.scalar()
            
            logger.info(f"📊 Migration status: current={current_rev}, head={head_rev}")
            
            # Если ревизии разные - есть pending миграции
            return current_rev != head_rev
            
    except Exception as e:
        logger.warning(f"⚠️ Could not check migration status: {e}")
        return True  # В случае ошибки предполагаем что миграции нужны

def run_migrations():
    """Применяет миграции Alembic только если они есть"""
    try:
        logger.info("🔍 Checking database migrations...")
        
        # Получаем URL БД из переменных окружения
        database_url = os.getenv("DATABASE_URL")
        if not database_url:
            logger.error("❌ DATABASE_URL environment variable is not set")
            return False
        
        # Проверяем есть ли pending миграции
        if not get_pending_migrations():
            logger.info("✅ Database is already up to date - no migrations needed")
            return True
        
        logger.info("📦 Applying pending migrations...")
        
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