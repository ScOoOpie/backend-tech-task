import time
from datetime import datetime, timezone
import logging
from contextlib import asynccontextmanager
from typing import List, Optional, Dict, Any
from fastapi import FastAPI, HTTPException, Depends, Query
from sqlalchemy.orm import Session
from prometheus_client import Counter, Histogram, Gauge, generate_latest
from .auth import APIKeyManager, get_current_user, require_admin_access, require_write_access, require_read_access
from .database import get_db, engine
from .models import Base, APIKey, User
from .schemas import APIKeyListResponse, EventBatch, AnalyticsResponse, GenerateAPIKeyRequest, GenerateAPIKeyResponse, UserCreateRequest, UserResponse, UsersListResponse
from .crud import ingest_events, get_user_stats, get_ingestion_metrics, clear_user_cache, get_cache_stats
from .analytics import *
from .middleware import RateLimiter
from .models import Event 
from .redis_client import redis_client
import asyncio
import uuid
from logging.handlers import RotatingFileHandler, TimedRotatingFileHandler
import os
from .migrate import run_migrations
# Моделі БД
# Base.metadata.create_all(bind=engine)

# Налаштування логування
def setup_logging():
    """Настройка логгера с ротацией"""
    
    # Создаем папку для логов
    log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)
    
    # Основной handler с ротацией по размеру
    file_handler = RotatingFileHandler(
        filename=os.path.join(log_dir, 'app.log'),
        maxBytes=10 * 1024 * 1024,  # 10 MB
        backupCount=5,  # Хранить 5 backup файлов
        encoding='utf-8'
    )
    
    # Handler для ошибок с ротацией по времени
    error_handler = TimedRotatingFileHandler(
        filename=os.path.join(log_dir, 'errors.log'),
        when='W0',  # Каждую неделю (понедельник)
        backupCount=4,  # 4 недели
        encoding='utf-8'
    )
    error_handler.setLevel(logging.ERROR)
    
    # Форматтер
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    file_handler.setFormatter(formatter)
    error_handler.setFormatter(formatter)
    
    # Настройка корневого логгера
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            file_handler,
            error_handler,
            logging.StreamHandler()  # Console output
        ]
    )

# Инициализируем логгинг
setup_logging()
logger = logging.getLogger(__name__)

# Метрики
EVENTS_INGESTED_COUNTER = Counter('events_ingested_total', 'Total ingested events')
EVENTS_PUBLISHED_COUNTER = Counter('events_published_nats_total', 'Total events published to NATS')
REQUEST_DURATION = Histogram('request_duration_seconds', 'Request duration')
CACHE_HITS = Counter('cache_hits_total', 'Total cache hits', ['endpoint'])
CACHE_MISSES = Counter('cache_misses_total', 'Total cache misses', ['endpoint'])
REDIS_CONNECTION_GAUGE = Gauge('redis_connected', 'Redis connection status')

# Инициализация менеджера ключей (ОДИН РАЗ)
api_key_manager = APIKeyManager()

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    logger.info("🚀 Starting Event Analytics Service")
    
    # 🔧 ПРИМЕНЯЕМ МИГРАЦИИ ПРИ СТАРТЕ
    logger.info("📦 Checking database migrations...")
    
    # Запускаем миграции в отдельном потоке чтобы не блокировать старт
    migration_success = await asyncio.get_event_loop().run_in_executor(
        None, 
        run_migrations
    )
    
    if not migration_success:
        logger.error("❌ Database migrations failed - application may not work correctly")
        # В production можно выйти, в development продолжаем
        if os.getenv("ENVIRONMENT") == "production":
            raise RuntimeError("Database migrations failed")
    else:
        logger.info("✅ Database migrations completed")
    # Подключаем Redis
    await redis_client.connect()
    REDIS_CONNECTION_GAUGE.set(1 if redis_client.is_connected else 0)
    
    if redis_client.is_connected:
        logger.info("✅ Redis connected successfully")
    else:
        logger.warning("⚠️ Redis connection failed - caching disabled")
    
    # Спробувати імпортувати NATS (опціонально)
    try:
        from .nats_client import nats_client
        await nats_client.connect()
        app.state.nats_enabled = nats_client.is_connected
        if nats_client.is_connected:
            logger.info("✅ NATS integration enabled")
        else:
            logger.info("ℹ️  NATS integration disabled")
    except Exception as e:
        logger.warning(f"⚠️ NATS setup failed: {e}")
        app.state.nats_enabled = False
    
    # Добавляем Redis в state приложения
    app.state.redis_enabled = redis_client.is_connected
    
    logger.info("✅ All services initialized")
    yield
    
    # Shutdown
    logger.info("🛑 Shutting down Event Analytics Service")
    
    # Закрываем Redis
    await redis_client.close()
    REDIS_CONNECTION_GAUGE.set(0)
    
    # Закрываем NATS
    try:
        from .nats_client import nats_client
        await nats_client.close()
    except:
        pass

app = FastAPI(
    title="Event Analytics Service",
    description="API для збору та аналітики подій з системою аутентифікації и Redis кэшированием",
    version="1.1.0",  # Обновили версию из-за добавления Redis
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc"
)

# Rate Limiter
rate_limiter = RateLimiter(capacity=1000, refill_rate=100)

async def get_rate_limiter():
    return rate_limiter

# ==================== REDIS UTILS ====================

async def get_cache_info() -> Dict[str, Any]:
    """Получение информации о кэше"""
    try:
        cache_stats = await get_cache_stats()
        return {
            "redis_enabled": getattr(app.state, 'redis_enabled', False),
            "cache_stats": cache_stats
        }
    except Exception as e:
        logger.error(f"Error getting cache info: {str(e)}")
        return {"redis_enabled": False, "error": str(e)}

# ==================== NATS UTILS ====================

async def publish_events_to_nats(events: List[Dict[str, Any]]) -> int:
    """Публикация событий в NATS"""
    try:
        from .nats_client import nats_client
        
        if not nats_client.is_connected:
            return 0
            
        published_count = 0
        for event in events:
            # Создаем сообщение для NATS
            nats_message = {
                "event_id": str(event.event_id),
                "occurred_at": event.occurred_at.isoformat(),
                "user_id": event.user_id,
                "event_type": event.event_type,
                "properties": event.properties,
                "published_at": datetime.now(timezone.utc).isoformat(),
                "source": "analytics_api"
            }
            
            # Публикуем в разные топики в зависимости от типа события
            subject = f"events.{event.event_type}"
            success = await nats_client.publish_event(subject, nats_message)
            
            if success:
                published_count += 1
                EVENTS_PUBLISHED_COUNTER.inc()
        
        logger.info(f"📤 Published {published_count}/{len(events)} events to NATS")
        return published_count
        
    except Exception as e:
        logger.error(f"Error publishing events to NATS: {str(e)}")
        return 0

# ==================== EVENT ENDPOINTS ====================

@app.post("/events", status_code=201)
async def post_events(
    events: EventBatch,
    db: Session = Depends(get_db),
    limiter: RateLimiter = Depends(get_rate_limiter),
    user: dict = Depends(require_write_access)
):
    """
    Надсилання подій для аналітики
    
    - Вимагає права **write**
    - Обмеження кількості запитів
    - Зберігає в БД та публікує в NATS
    - Автоматически инвалидирует Redis кэш
    """
    start_time = time.time()
    
    # Rate limiting
    if not limiter.consume(1):
        raise HTTPException(status_code=429, detail="Rate limit exceeded")
    
    try:
        # Сохраняем события в БД
        ingested_count = await ingest_events(db, events.events)
        duration = time.time() - start_time
        REQUEST_DURATION.observe(duration)
        EVENTS_INGESTED_COUNTER.inc(ingested_count)
        
        # Публикуем события в NATS (асинхронно, не блокируем ответ)
        if ingested_count > 0:
            asyncio.create_task(publish_events_to_nats(events.events))
        
        # Логируем информацию о кэше
        cache_info = await get_cache_info()
        
        logger.info(f"📝 User {user['user_id']} ingested {ingested_count} events in {duration:.3f}s")
        
        return {
            "status": "success", 
            "ingested": ingested_count,
            "total_received": len(events.events),
            "processing_time": f"{duration:.3f}s",
            "nats_enabled": getattr(app.state, 'nats_enabled', False),
            "cache_enabled": cache_info["redis_enabled"],
            "cache_invalidated": True
        }
            
    except Exception as e:
        logger.error(f"Error ingesting events: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")

# ==================== USER ENDPOINTS ====================

@app.post("/users", status_code=201)
async def create_user(
    user_id: str = Query(..., description="User ID"),
    name: str = Query(..., description="User name"),
    email: str = Query(None, description="User email"),
    db: Session = Depends(get_db),
    user: dict = Depends(require_admin_access)
):
    """Создание нового пользователя"""
    try:
        # Проверяем, существует ли уже пользователь
        existing_user = db.query(User).filter(User.user_id == user_id).first()
        if existing_user:
            raise HTTPException(status_code=400, detail="User already exists")
        
        # Создаем пользователя
        new_user = User(
            user_id=user_id,
            name=name,
            email=email,
            is_active=True
        )
        
        db.add(new_user)
        db.commit()
        
        # Инвалидируем кэш пользователей
        if redis_client.is_connected:
            await redis_client.delete_pattern("user_stats:*")
            await redis_client.delete_pattern("ingestion_metrics:*")
        
        # Публикуем событие в NATS
        try:
            from .nats_client import nats_client
            if nats_client.is_connected:
                user_event = {
                    "event_id": str(uuid.uuid4()),
                    "occurred_at": datetime.now(timezone.utc).isoformat(),
                    "user_id": user_id,
                    "event_type": "user_created",
                    "properties": {
                        "name": name,
                        "email": email,
                        "created_by": user['user_id']
                    },
                    "published_at": datetime.now(timezone.utc).isoformat()
                }
                await nats_client.publish_event("users.created", user_event)
        except Exception as e:
            logger.warning(f"Failed to publish user creation event: {e}")
        
        logger.info(f"Admin {user['user_id']} created user: {user_id}")
        
        return {
            "message": "✅ User created successfully",
            "user_id": user_id,
            "name": name,
            "email": email,
            "cache_invalidated": redis_client.is_connected
        }
        
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        logger.error(f"Error creating user: {str(e)}")
        raise HTTPException(status_code=500, detail="Error creating user")

@app.get("/users")
async def list_users(
    active_only: bool = Query(True, description="Show only active users"),
    db: Session = Depends(get_db),
    user: dict = Depends(require_admin_access)
):
    """Список всех пользователей"""
    try:
        query = db.query(User)
        if active_only:
            query = query.filter(User.is_active == True)
            
        users = query.order_by(User.created_at.desc()).all()
        
        users_info = [
            {
                "user_id": u.user_id,
                "name": u.name,
                "email": u.email,
                "is_active": u.is_active,
                "created_at": u.created_at,
                "updated_at": u.updated_at
            }
            for u in users
        ]
        
        return {
            "total_users": len(users_info),
            "users": users_info,
            "cache_info": await get_cache_info()
        }
        
    except Exception as e:
        logger.error(f"Error listing users: {str(e)}")
        raise HTTPException(status_code=500, detail="Error listing users")

@app.get("/users/{user_id}")
async def get_user(
    user_id: str,
    db: Session = Depends(get_db),
    user: dict = Depends(require_read_access)
):
    """Информация о конкретном пользователе с расширенной статистикой"""
    try:
        db_user = db.query(User).filter(User.user_id == user_id).first()
        if not db_user:
            raise HTTPException(status_code=404, detail="User not found")
        
        # Используем новую функцию для статистики (уже с кэшированием)
        user_stats = get_user_stats(db, user_id)
        
        # Логируем информацию о кэше
        cache_info = await get_cache_info()
        
        return {
            "user_id": db_user.user_id,
            "name": db_user.name,
            "email": db_user.email,
            "is_active": db_user.is_active,
            "created_at": db_user.created_at,
            "updated_at": db_user.updated_at,
            "stats": user_stats,
            "cache_info": {
                "redis_connected": cache_info["redis_enabled"],
                "from_cache": cache_info["redis_enabled"]  # Предполагаем, что если Redis подключен, то данные из кэша
            }
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting user: {str(e)}")
        raise HTTPException(status_code=500, detail="Error getting user")

# ==================== ANALYTICS ENDPOINTS ====================

@app.get("/stats/dau")
async def get_dau(
    from_date: str,
    to_date: str,
    db: Session = Depends(get_db),
    user: dict = Depends(require_read_access)
) -> AnalyticsResponse:
    """
    Отримання DAU (Daily Active Users) статистики
    
    - Вимагає права **read**
    - Параметри: from_date, to_date (формат: YYYY-MM-DD)
    - Данные кэшируются в Redis
    """
    try:
        result = await get_dau_stats(db, from_date, to_date)
        
        # Добавляем информацию о кэше в ответ
        cache_info = await get_cache_info()
        result["cache_info"] = {
            "redis_connected": cache_info["redis_enabled"],
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        
        return AnalyticsResponse(data=result)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error calculating DAU: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/stats/top-events")
async def get_top_events_stats(
    from_date: str,
    to_date: str,
    limit: int = 10,
    db: Session = Depends(get_db),
    user: dict = Depends(require_read_access)
) -> AnalyticsResponse:
    """
    Отримання топу подій за частотою
    
    - Вимагає права **read**
    - Параметри: from_date, to_date, limit
    - Данные кэшируются в Redis
    """
    try:
        result = await get_top_events(db, from_date, to_date, limit)
        
        # Добавляем информацию о кэше
        cache_info = await get_cache_info()
        result["cache_info"] = {
            "redis_connected": cache_info["redis_enabled"],
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        
        return AnalyticsResponse(data=result)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error calculating top events: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/stats/retention")
async def get_retention(
    start_date: str,
    windows: int = 7,
    db: Session = Depends(get_db),
    user: dict = Depends(require_read_access)
) -> AnalyticsResponse:
    """
    Отримання статистики ретеншена
    
    - Вимагає права **read**
    - Параметри: start_date (дата когорты), windows (количество дней)
    - Данные кэшируются в Redis
    """
    try:
        result = await get_retention_stats(db, start_date, windows)
        
        # Добавляем информацию о кэше
        cache_info = await get_cache_info()
        result["cache_info"] = {
            "redis_connected": cache_info["redis_enabled"],
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        
        return AnalyticsResponse(data=result)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error calculating retention: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/cohorts/active")
async def get_active_cohorts(
    limit: int = Query(10, ge=1, le=50),
    db: Session = Depends(get_db),
    user: dict = Depends(require_read_access)
):
    """Список активных когорт с кэшированием"""
    try:
        cohorts = await get_cohorts_list(db, limit)
        
        return {
            "cohorts": cohorts,
            "cache_info": await get_cache_info()
        }
    except Exception as e:
        logger.error(f"Error getting active cohorts: {str(e)}")
        raise HTTPException(status_code=500, detail="Error getting cohorts")

@app.get("/users/{user_id}/retention")
async def get_user_retention(
    user_id: str,
    db: Session = Depends(get_db),
    user: dict = Depends(require_read_access)
):
    """Статистика ретеншена для конкретного пользователя с кэшированием"""
    try:
        stats = await get_user_retention_data(db, user_id)
        if "error" in stats:
            raise HTTPException(status_code=404, detail=stats["error"])
        
        # Добавляем информацию о кэше
        cache_info = await get_cache_info()
        stats["cache_info"] = {
            "redis_connected": cache_info["redis_enabled"],
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        
        return stats
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting user retention: {str(e)}")
        raise HTTPException(status_code=500, detail="Error getting user retention")

# ==================== CACHE MANAGEMENT ENDPOINTS ====================

@app.get("/cache/status")
async def get_cache_status(user: dict = Depends(require_admin_access)):
    """Статус Redis кэша"""
    try:
        cache_info = await get_cache_info()
        return cache_info
    except Exception as e:
        logger.error(f"Error getting cache status: {str(e)}")
        raise HTTPException(status_code=500, detail="Error getting cache status")

@app.post("/cache/clear")
async def clear_cache(
    pattern: str = Query("cache:*", description="Pattern to clear"),
    user: dict = Depends(require_admin_access)
):
    """Очистка кэша"""
    try:
        if not redis_client.is_connected:
            return {
                "message": "Redis not connected",
                "pattern": pattern,
                "keys_deleted": 0
            }
        
        keys = await redis_client.client.keys(pattern)
        if keys:
            await redis_client.client.delete(*keys)
        
        logger.info(f"🗑️ Admin {user['user_id']} cleared cache pattern: {pattern}")
        
        return {
            "message": "✅ Cache cleared successfully",
            "pattern": pattern,
            "keys_deleted": len(keys)
        }
    except Exception as e:
        logger.error(f"Error clearing cache: {str(e)}")
        raise HTTPException(status_code=500, detail="Error clearing cache")

@app.post("/cache/users/{user_id}/clear")
async def clear_user_cache_endpoint(
    user_id: str,
    db: Session = Depends(get_db),
    user: dict = Depends(require_admin_access)
):
    """Очистка кэша для конкретного пользователя"""
    try:
        success = await clear_user_cache(user_id)
        
        return {
            "message": "✅ User cache cleared successfully" if success else "❌ User cache clear failed",
            "user_id": user_id,
            "success": success
        }
    except Exception as e:
        logger.error(f"Error clearing user cache: {str(e)}")
        raise HTTPException(status_code=500, detail="Error clearing user cache")

@app.get("/cache/keys")
async def list_cache_keys(
    pattern: str = Query("cache:*", description="Key pattern"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum keys to return"),
    user: dict = Depends(require_admin_access)
):
    """Список ключей в кэше"""
    try:
        if not redis_client.is_connected:
            return {
                "pattern": pattern,
                "keys_count": 0,
                "keys": [],
                "message": "Redis not connected"
            }
        
        keys = await redis_client.client.keys(pattern)
        
        return {
            "pattern": pattern,
            "keys_count": len(keys),
            "keys": keys[:limit]
        }
    except Exception as e:
        logger.error(f"Error listing cache keys: {str(e)}")
        raise HTTPException(status_code=500, detail="Error listing cache keys")

# ==================== SYSTEM ENDPOINTS ====================

@app.get("/system/metrics")
async def get_system_metrics(user: dict = Depends(require_admin_access)):
    """Системные метрики включая Redis"""
    try:
        cache_info = await get_cache_info()
        ingestion_metrics = get_ingestion_metrics(db=next(get_db()))
        
        return {
            "cache": cache_info,
            "ingestion": ingestion_metrics,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
    except Exception as e:
        logger.error(f"Error getting system metrics: {str(e)}")
        raise HTTPException(status_code=500, detail="Error getting system metrics")

@app.get("/metrics")
async def metrics(
    user: dict = Depends(require_admin_access)
):
    """Prometheus метрики"""
    return generate_latest()

@app.get("/health")
async def health_check():
    """Перевірка здоров'я системи с информацией о Redis"""
    nats_enabled = getattr(app.state, 'nats_enabled', False)
    redis_enabled = getattr(app.state, 'redis_enabled', False)
    
    # Проверяем соединение с Redis
    redis_healthy = False
    if redis_enabled and redis_client.is_connected:
        try:
            await redis_client.client.ping()
            redis_healthy = True
        except:
            redis_healthy = False
    
    return {
        "status": "healthy", 
        "nats_enabled": nats_enabled,
        "redis_enabled": redis_enabled,
        "redis_healthy": redis_healthy,
        "services": ["web", "db", "nats", "redis"],
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

@app.get("/debug/pool-status")
async def pool_status():
    """Статус пула соединений БД"""
    pool = engine.pool
    status = {
        "pool_config": {
            "size": pool.size(),
            "max_overflow": pool._max_overflow,
            "timeout": pool.timeout,
            "recycle": pool._recycle
        },
        "current_usage": {
            "checkedout": pool.checkedout(),  # Занятые соединения
            "checkedin": pool.checkedin(),    # Свободные соединения  
            "overflow": pool.overflow(),      # Сверх лимита
            "total": pool.checkedout() + pool.checkedin()
        },
        "status": "OK" if pool.checkedout() <= (pool.size() + pool._max_overflow) else "OVERLOAD",
        "redis_connected": getattr(app.state, 'redis_enabled', False)
    }
    return status

@app.get("/")
async def root():
    """Кореневий ендпоінт"""
    cache_info = await get_cache_info()
    
    return {
        "message": "Event Analytics Service with Redis Caching", 
        "version": "1.1.0",
        "docs": "/docs",
        "health": "/health",
        "cache_enabled": cache_info["redis_enabled"],
        "features": [
            "Event ingestion",
            "User analytics", 
            "Cohort analysis",
            "Redis caching",
            "NATS integration",
            "API key authentication"
        ]
    }

# ==================== NATS ENDPOINTS ====================

@app.get("/nats/status")
async def get_nats_status():
    """Статус подключения к NATS"""
    nats_enabled = getattr(app.state, 'nats_enabled', False)
    return {
        "nats_enabled": nats_enabled,
        "status": "connected" if nats_enabled else "disconnected"
    }

@app.post("/nats/publish-test")
async def publish_test_message(
    message: str = Query("Test message from API"),
    user: dict = Depends(require_admin_access)
):
    """Тестовая публикация сообщения в NATS"""
    try:
        from .nats_client import nats_client
        
        if not nats_client.is_connected:
            raise HTTPException(status_code=503, detail="NATS not connected")
        
        test_event = {
            "event_id": str(uuid.uuid4()),
            "occurred_at": datetime.now(timezone.utc).isoformat(),
            "user_id": user['user_id'],
            "event_type": "test_message",
            "message": message,
            "published_at": datetime.now(timezone.utc).isoformat()
        }
        
        success = await nats_client.publish_event("test.messages", test_event)
        
        if success:
            return {
                "message": "✅ Test message published to NATS",
                "subject": "test.messages",
                "event_id": test_event["event_id"]
            }
        else:
            raise HTTPException(status_code=500, detail="Failed to publish message")
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error publishing test message: {str(e)}")
        raise HTTPException(status_code=500, detail="Error publishing test message")

# ==================== AUTH ENDPOINTS ====================

@app.post("/auth/generate-key", status_code=201, response_model=GenerateAPIKeyResponse)
async def generate_api_key(
    request: GenerateAPIKeyRequest,
    db: Session = Depends(get_db),
    user: dict = Depends(require_admin_access)
) -> GenerateAPIKeyResponse:
    """
    Генерація нового API ключа
    
    - Вимагає права **admin**
    - Повертає новий API ключ (показується тільки один раз!)
    """
    try:
        new_key = api_key_manager.generate_key(
            db=db,
            user_id=request.user_id,
            name=request.name,
            permissions=request.permissions,
            expires_days=request.expires_days
        )
        
        logger.info(f"Admin {user['user_id']} generated key for {request.user_id}")
        
        return GenerateAPIKeyResponse(
            api_key=new_key,
            user_id=request.user_id,
            permissions=request.permissions,
            name=request.name,
            expires_days=request.expires_days
        )
    except Exception as e:
        logger.error(f"Error generating API key: {str(e)}")
        raise HTTPException(status_code=500, detail="Error generating API key")

@app.get("/auth/keys", response_model=APIKeyListResponse)
async def list_api_keys(
    user_id: Optional[str] = Query(None, description="Filter by user ID"),
    active_only: bool = Query(True, description="Show only active keys"),
    db: Session = Depends(get_db),
    user: dict = Depends(require_admin_access)
) -> APIKeyListResponse:
    """
    Список всіх API ключів
    
    - Вимагає права **admin**
    - Можлива фільтрація по user_id та активності
    """
    try:
        keys_info = api_key_manager.get_all_keys(db, user_id, active_only)
        
        return APIKeyListResponse(
            total_keys=len(keys_info),
            keys=keys_info
        )
    except Exception as e:
        logger.error(f"Error listing API keys: {str(e)}")
        raise HTTPException(status_code=500, detail="Error listing API keys")

@app.post("/auth/create-admin-key")
async def create_admin_key(
    db: Session = Depends(get_db)
):
    """Створення першого адмін ключа (не потребує автентифікації)"""
    try:
        logger.info("Starting admin key creation process...")
        
        # 🔧 НАДЕЖНЫЙ ВАРИАНТ - фильтруем в Python
        all_active_keys = db.query(APIKey).filter(APIKey.is_active == True).all()
        existing_admin_keys = 0
        
        for key in all_active_keys:
            # Безопасная проверка JSON поля
            if key.permissions and isinstance(key.permissions, list):
                if 'admin' in key.permissions:
                    existing_admin_keys += 1
                    logger.info(f"Found admin key: {key.name} for user {key.user_id}")
            else:
                logger.warning(f"Key {key.id} has invalid permissions format: {key.permissions}")
        
        logger.info(f"Total found {existing_admin_keys} existing admin keys")
        
        if existing_admin_keys > 0:
            logger.warning("Admin keys already exist, rejecting creation request")
            raise HTTPException(
                status_code=400, 
                detail="Admin keys already exist. Use regular key generation with admin access."
            )
        
        logger.info("No existing admin keys found, proceeding with creation...")
        
        # Создаем админ ключ
        admin_key = api_key_manager.generate_key(
            db=db,
            user_id="system_admin",
            name="Initial System Admin Key",
            permissions=["read", "write", "admin"],
            expires_days=365
        )
        
        logger.info("✅ Initial admin key created successfully")
        
        return {
            "message": "✅ Initial admin key created successfully",
            "api_key": admin_key,
            "warning": "⚠️ Save this key securely! It will not be shown again.",
            "user_id": "system_admin",
            "permissions": ["read", "write", "admin"],
            "expires_in_days": 365
        }
        
    except Exception as e:
        logger.error(f"❌ Error creating admin key: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Error creating admin key")

@app.post("/auth/create-user-key")
async def create_user_key(
    user_id: str = Query(..., description="User ID for the new key"),
    key_name: str = Query(..., description="Name for the new key"),
    permissions: List[str] = Query(..., description="List of permissions"),
    expires_days: int = Query(30, description="Key expiration in days"),
    db: Session = Depends(get_db),
    admin_user: dict = Depends(require_admin_access)
):
    """
    Швидке створення користувацького ключа через query parameters
    
    - Вимагає права **admin**
    - Зручний формат для швидкого створення ключів
    """
    try:
        # Валидация permissions
        valid_permissions = ["read", "write", "admin"]
        for perm in permissions:
            if perm not in valid_permissions:
                raise HTTPException(
                    status_code=400,
                    detail=f"Invalid permission: {perm}. Valid permissions are: {valid_permissions}"
                )
        
        user_key = api_key_manager.generate_key(
            db=db,
            user_id=user_id,
            name=key_name,
            permissions=permissions,
            expires_days=expires_days
        )

        logger.info(f"Admin {admin_user['user_id']} created key for user {user_id}")

        return {
            "message": "✅ User API key created successfully",
            "api_key": user_key,
            "warning": "⚠️ Save this key securely! It will not be shown again.",
            "user_id": user_id,
            "key_name": key_name,
            "permissions": permissions,
            "expires_in_days": expires_days
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error creating user key: {str(e)}")
        raise HTTPException(status_code=500, detail="Error creating user key")

@app.delete("/auth/keys/{key_id}")
async def revoke_api_key(
    key_id: int,
    db: Session = Depends(get_db),
    user: dict = Depends(require_admin_access)
):
    """
    Відкликання API ключа
    
    - Вимагає права **admin**
    - Ключ позначається як неактивний
    """
    try:
        success = api_key_manager.revoke_key(db, key_id)
        
        if not success:
            raise HTTPException(status_code=404, detail="API key not found")
        
        logger.info(f"Admin {user['user_id']} revoked key ID {key_id}")
        
        return {
            "message": "✅ API key revoked successfully",
            "key_id": key_id
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error revoking API key: {str(e)}")
        raise HTTPException(status_code=500, detail="Error revoking API key")

@app.get("/auth/my-keys", response_model=APIKeyListResponse)
async def get_my_api_keys(
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user)
) -> APIKeyListResponse:
    """
    Отримання ключів поточного користувача
    
    - Вимагає будь-який дійсний API ключ
    - Повертає тільки ключі поточного користувача
    """
    try:
        keys_info = api_key_manager.get_user_keys(db, current_user["user_id"])
        
        return APIKeyListResponse(
            total_keys=len(keys_info),
            keys=keys_info
        )
        
    except Exception as e:
        logger.error(f"Error getting user keys: {str(e)}")
        raise HTTPException(status_code=500, detail="Error getting user keys")

# ==================== SYSTEM ENDPOINTS ====================

@app.get("/metrics")
async def metrics(
    user: dict = Depends(require_admin_access)
):
    """Prometheus метрики"""
    return generate_latest()

@app.get("/health")
async def health_check():
    """Перевірка здоров'я системи"""
    nats_enabled = getattr(app.state, 'nats_enabled', False)
    return {
        "status": "healthy", 
        "nats_enabled": nats_enabled,
        "services": ["web", "db", "nats"],
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

@app.get("/debug/pool-status")
async def pool_status():
    """Статус пула соединений БД"""
    pool = engine.pool
    status = {
        "pool_config": {
            "size": pool.size(),
            "max_overflow": pool._max_overflow,
            "timeout": pool.timeout,
            "recycle": pool._recycle
        },
        "current_usage": {
            "checkedout": pool.checkedout(),  # Занятые соединения
            "checkedin": pool.checkedin(),    # Свободные соединения  
            "overflow": pool.overflow(),      # Сверх лимита
            "total": pool.checkedout() + pool.checkedin()
        },
        "status": "OK" if pool.checkedout() <= (pool.size() + pool._max_overflow) else "OVERLOAD"
    }
    return status

@app.get("/")
async def root():
    """Кореневий ендпоінт"""
    return {
        "message": "Event Analytics Service", 
        "version": "1.0",
        "docs": "/docs",
        "health": "/health"
    }



