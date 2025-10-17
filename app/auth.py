import os
import secrets
import hashlib
from datetime import datetime, timedelta, timezone
from fastapi import HTTPException, Depends, Header
from sqlalchemy.orm import Session
from .database import get_db
from .models import APIKey, User
import logging

logger = logging.getLogger(__name__)

class APIKeyManager:
    def __init__(self):
        pass
    
    def _hash_key(self, api_key: str) -> str:
        """Хешує API key для безпечного зберігання"""
        return hashlib.sha256(api_key.encode()).hexdigest()
    
    def _get_current_utc_time(self):
        """Возвращает текущее время в UTC с временной зоной"""
        return datetime.now(timezone.utc)
    
    def _make_naive_if_needed(self, dt):
        """Преобразует datetime в наивный, если нужно для сравнения"""
        if dt.tzinfo is not None:
            return dt.replace(tzinfo=None)
        return dt
    
    def validate_key(self, db: Session, api_key: str, required_permission: str = None) -> dict:
        """Перевіряє API key в БД і повертає інформацію про користувача"""
        if not api_key:
            raise HTTPException(
                status_code=401, 
                detail="API Key is required"
            )
        
        # Хешуємо ключ для пошуку в БД
        key_hash = self._hash_key(api_key)
        
        # Шукаємо активний ключ в БД
        db_key = db.query(APIKey).filter(
            APIKey.key_hash == key_hash,
            APIKey.is_active == True
        ).first()
        
        if not db_key:
            logger.warning(f"Invalid API key attempt: {key_hash[:16]}...")
            raise HTTPException(
                status_code=401, 
                detail="Invalid API Key"
            )
        
        # 🔧 ИСПРАВЛЕНИЕ: Правильное сравнение времени
        current_time = self._get_current_utc_time()
        
        if db_key.expires_at:
            # Приводим оба времени к одному формату для сравнения
            expires_at_naive = self._make_naive_if_needed(db_key.expires_at)
            current_time_naive = self._make_naive_if_needed(current_time)
            
            if expires_at_naive < current_time_naive:
                logger.warning(f"Expired API key used: {db_key.name}")
                raise HTTPException(
                    status_code=401,
                    detail="API Key has expired"
                )
        
        # 🔧 ИСПРАВЛЕНИЕ: Обновляем last_used
        db_key.last_used = self._make_naive_if_needed(current_time)
        db.commit()
        
        # Получаем информацию о пользователе
        db_user = db.query(User).filter(User.user_id == db_key.user_id).first()
        
        user_info = {
            "user_id": db_key.user_id,
            "user_name": db_user.name if db_user else db_key.user_id,
            "permissions": db_key.permissions or [],
            "key_name": db_key.name,
            "key_id": db_key.id
        }
        
        # Перевіряємо права доступу
        if required_permission and required_permission not in user_info["permissions"]:
            logger.warning(
                f"Permission denied for user {db_key.user_id}. "
                f"Required: {required_permission}, Has: {user_info['permissions']}"
            )
            raise HTTPException(
                status_code=403,
                detail=f"Permission denied. Required: {required_permission}"
            )
        
        logger.info(f"Authenticated user: {db_key.user_id} with key: {db_key.name}")
        return user_info
    
    def generate_key(self, db: Session, user_id: str, name: str, permissions: list, expires_days: int = None) -> str:
        """Генерує новий API key і зберігає в БД"""
        # Генеруємо випадковий ключ
        new_key = secrets.token_urlsafe(32)
        key_hash = self._hash_key(new_key)
        
        # 🔧 ИСПРАВЛЕНИЕ: Правильное установление времени
        current_time = self._get_current_utc_time()
        
        # Встановлюємо термін дії
        expires_at = None
        if expires_days:
            expires_at = self._make_naive_if_needed(current_time + timedelta(days=expires_days))
        
        # Створюємо запис в БД
        db_key = APIKey(
            key_hash=key_hash,
            user_id=user_id,
            name=name,
            permissions=permissions,
            expires_at=expires_at
        )
        
        db.add(db_key)
        db.commit()
        
        logger.info(f"Generated new API key for user: {user_id}, name: {name}")
        
        return new_key
    
    def revoke_key(self, db: Session, key_id: int) -> bool:
        """Відкликає API key по ID"""
        db_key = db.query(APIKey).filter(APIKey.id == key_id).first()
        if db_key:
            db_key.is_active = False
            db.commit()
            logger.info(f"Revoked API key: {db_key.name} for user: {db_key.user_id}")
            return True
        return False
    
    def get_user_keys(self, db: Session, user_id: str) -> list:
        """Отримує всі ключі користувача (без хешів)"""
        keys = db.query(APIKey).filter(
            APIKey.user_id == user_id
        ).order_by(APIKey.created_at.desc()).all()
        
        return [
            {
                "id": key.id,
                "user_id": key.user_id,  # 🔧 ДОБАВЛЕНО
                "name": key.name,
                "permissions": key.permissions,
                "is_active": key.is_active,
                "created_at": key.created_at,
                "expires_at": key.expires_at,
                "last_used": key.last_used,
                "key_preview": f"{key.key_hash[:8]}...{key.key_hash[-4:]}" if key.key_hash else "N/A"  # 🔧 ДОБАВЛЕНО
            }
            for key in keys
        ]

    def get_all_keys(self, db: Session, user_id: str = None, active_only: bool = True) -> list:
        """Отримує всі ключі з фільтрацією"""
        query = db.query(APIKey)
        
        if user_id:
            query = query.filter(APIKey.user_id == user_id)
        
        if active_only:
            query = query.filter(APIKey.is_active == True)
        
        keys = query.order_by(APIKey.created_at.desc()).all()
        
        return [
            {
                "id": key.id,
                "user_id": key.user_id,
                "name": key.name,
                "permissions": key.permissions,
                "is_active": key.is_active,
                "created_at": key.created_at,
                "expires_at": key.expires_at,
                "last_used": key.last_used,
                "key_preview": f"{key.key_hash[:8]}...{key.key_hash[-4:]}" if key.key_hash else "N/A"
            }
            for key in keys
        ]
# Глобальний менеджер ключів
api_key_manager = APIKeyManager()

# Dependency для автентифікації
async def get_current_user(
    x_api_key: str = Header(..., alias="X-API-Key"),
    db: Session = Depends(get_db)
) -> dict:
    """
    Dependency для отримання поточного користувача з API Key
    """
    try:
        user_info = api_key_manager.validate_key(db, x_api_key)
        return user_info
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Authentication error: {str(e)}")
        raise HTTPException(status_code=500, detail="Authentication service error")

# Допоміжні функції для перевірки прав
async def require_permission(required_permission: str, user: dict = Depends(get_current_user)):
    """Перевіряє, чи має користувач необхідні права"""
    if required_permission not in user["permissions"]:
        raise HTTPException(
            status_code=403,
            detail=f"Permission denied. Required: {required_permission}"
        )
    return user

# Спеціалізовані dependencies
async def require_write_access(user: dict = Depends(get_current_user)):
    return await require_permission("write", user)

async def require_read_access(user: dict = Depends(get_current_user)):
    return await require_permission("read", user)

async def require_admin_access(user: dict = Depends(get_current_user)):
    return await require_permission("admin", user)