from sqlalchemy import Column, String, DateTime, Text, Integer, Date
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.ext.declarative import declarative_base
import uuid
from datetime import datetime, timezone
from sqlalchemy import Index, Boolean, JSON

Base = declarative_base()

def get_utc_now():
    """Возвращает текущее UTC время без временной зоны"""
    return datetime.now(timezone.utc).replace(tzinfo=None)

class Event(Base):
    __tablename__ = "events"
    
    id = Column(Integer, primary_key=True, index=True)
    event_id = Column(UUID(as_uuid=True), unique=True, index=True, default=uuid.uuid4)
    occurred_at = Column(DateTime, nullable=False, index=True)
    user_id = Column(String(255), nullable=False, index=True)
    event_type = Column(String(255), nullable=False, index=True)
    properties = Column(JSONB)
    event_date = Column(Date, index=True)
    
    # Денормалізовані поля для швидкої аналітики
    event_date = Column(Date, index=True)
    def __repr__(self):
        return f"<Event(id={self.id}, type='{self.event_type}', user='{self.user_id}')>"
    
    def to_dict(self):
        return {
            'id': self.id,
            'event_id': str(self.event_id),
            'occurred_at': self.occurred_at.isoformat(),
            'user_id': self.user_id,
            'event_type': self.event_type,
            'properties': self.properties,
            'event_date': self.event_date.isoformat() if self.event_date else None
        }

class UserRetention(Base):
    __tablename__ = "user_retention"
    
    id = Column(Integer, primary_key=True)
    user_id = Column(String(255), nullable=False, index=True)
    cohort_date = Column(Date, nullable=False, index=True)  # Дата первого события
    activity_date = Column(Date, nullable=False, index=True)  # Дата активности
    retention_day = Column(Integer, nullable=False)  # День относительно когорты
    
    __table_args__ = (
        Index('ix_user_cohort_activity', 'user_id', 'cohort_date', 'activity_date', unique=True),
    )

class User(Base):
    __tablename__ = "users"
    
    id = Column(Integer, primary_key=True)
    user_id = Column(String(255), unique=True, index=True)
    name = Column(String(255))
    email = Column(String(255))
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=get_utc_now)
    updated_at = Column(DateTime, default=get_utc_now, onupdate=get_utc_now)

class APIKey(Base):
    __tablename__ = "api_keys"
    
    id = Column(Integer, primary_key=True)
    key_hash = Column(String(255), unique=True, index=True)  # Хеш ключа
    user_id = Column(String(255), index=True)
    name = Column(String(255))  # Назва ключа
    permissions = Column(JSON)  # Список дозволів ["read", "write", "admin"]
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=get_utc_now)
    expires_at = Column(DateTime, nullable=True)
    last_used = Column(DateTime, nullable=True)
    
    __table_args__ = (
        Index('ix_api_keys_user_active', 'user_id', 'is_active'),
    )