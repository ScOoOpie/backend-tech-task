from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import os
from sqlalchemy.pool import QueuePool

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://user:password@localhost:5433/events")

print("🔄 Creating database engine with new settings...")


# Создаем новый engine с правильными настройками
engine = create_engine(
    DATABASE_URL,
    poolclass=QueuePool,
    pool_size=50,         
    max_overflow=100,      
    pool_pre_ping=True,
    pool_recycle=1800,
    pool_timeout=30,
    echo=False
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()