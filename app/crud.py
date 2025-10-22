from sqlalchemy.orm import Session
from sqlalchemy import text, insert
from sqlalchemy.dialects.postgresql import insert as pg_insert
from typing import List, Set, Dict, Any
import logging
from sqlalchemy.exc import IntegrityError
from datetime import date, datetime
import uuid
from .cache import invalidate_cache, cached
from .redis_client import redis_client
from .models import Event, UserRetention, User, Base
from .schemas import Event as EventSchema

logger = logging.getLogger(__name__)

# ==================== ОСНОВНАЯ ФУНКЦИЯ ИНГЕСТИРОВАНИЯ ====================

@invalidate_cache(pattern="cohorts:list:*")
@invalidate_cache(pattern="retention:*")
@invalidate_cache(pattern="dau:*") 
@invalidate_cache(pattern="top_events:*")
@invalidate_cache(pattern="user_stats:*")
@invalidate_cache(pattern="ingestion_metrics:*")
async def ingest_events(db: Session, events: List[EventSchema]) -> int:
    """ПРОСТАЯ и ЭФФЕКТИВНАЯ оптимизированная версия для ваших моделей"""
    
    if not events:
        return 0
    
    try:
        # 1. Пакетная вставка событий
        events_data = []
        user_ids = set()
        
        for event in events:
            # Преобразуем event_id в UUID если нужно
            event_id = event.event_id
            if isinstance(event_id, str):
                try:
                    event_id = uuid.UUID(event_id)
                except (ValueError, AttributeError):
                    # Если невалидный UUID, генерируем новый
                    event_id = uuid.uuid4()
                    logger.warning(f"Invalid event_id format, generated new: {event_id}")
            
            events_data.append({
                'event_id': event_id,
                'occurred_at': event.occurred_at,
                'user_id': event.user_id,
                'event_type': event.event_type,
                'properties': event.properties,
                'event_date': event.occurred_at.date() if event.occurred_at else date.today()
            })
            user_ids.add(event.user_id)
        
        # 2. Пакетное создание пользователей
        await _ensure_users_batch(db, user_ids)
        
        # 3. Вставка событий
        if events_data:
            db.execute(insert(Event).values(events_data))
        
        # 4. Пакетное обновление ретеншена
        await _update_retention_batch(db, events)
        
        # ✅ ОДИН КОММИТ после всех операций
        db.commit()
        
        # Дополнительная инвалидация пользовательских кэшей
        await _invalidate_user_caches(user_ids)
        
        logger.info(f"✅ Optimized batch: {len(events_data)} events")
        return len(events_data)
        
    except IntegrityError as e:
        # Fallback для дубликатов
        db.rollback()
        logger.warning(f"⚠️ Batch insert failed due to duplicates, falling back: {str(e)}")
        return await _insert_events_individual_simple(db, events)
        
    except Exception as e:
        db.rollback()
        logger.error(f"❌ Batch insert failed: {str(e)}")
        return 0

async def _invalidate_user_caches(user_ids: Set[str]):
    """Инвалидация кэшей для конкретных пользователей"""
    if not redis_client.is_connected:
        return
        
    try:
        for user_id in user_ids:
            # Инвалидируем кэши статистики пользователей
            await redis_client.delete_pattern(f"user_stats:{user_id}:*")
            await redis_client.delete_pattern(f"user_retention:{user_id}:*")
        
        logger.info(f"🔄 Invalidated caches for {len(user_ids)} users")
    except Exception as e:
        logger.error(f"Error invalidating user caches: {str(e)}")

# ==================== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ====================

async def _ensure_users_batch(db: Session, user_ids: Set[str]):
    """Пакетное создание пользователей для вашей модели User"""
    if not user_ids:
        return
    
    # Используем INSERT ... ON CONFLICT для атомарности (лучший подход)
    users_data = []
    for user_id in user_ids:
        users_data.append({
            'user_id': user_id,
            'name': f"User_{user_id}",
            'email': f"{user_id}@example.com",
            'is_active': True,
            'created_at': datetime.utcnow(),
            'updated_at': datetime.utcnow()
        })
    
    # Вставляем с обработкой конфликтов
    stmt = pg_insert(User).values(users_data)
    stmt = stmt.on_conflict_do_update(
        index_elements=['user_id'],
        set_={
            'is_active': True,
            'updated_at': datetime.utcnow()
        }
    )
    db.execute(stmt)

async def _update_retention_batch(db: Session, events: List[EventSchema]):
    """Пакетное обновление ретеншена для вашей модели UserRetention"""
    if not events:
        return
    
    user_ids = list({event.user_id for event in events})
    
    # Находим первые события для пользователей
    first_events_query = text("""
        SELECT DISTINCT ON (e.user_id) 
            e.user_id, 
            e.occurred_at::date as first_date
        FROM events e
        WHERE e.user_id = ANY(:user_ids)
        ORDER BY e.user_id, e.occurred_at
    """)
    first_events = db.execute(first_events_query, {'user_ids': user_ids}).fetchall()
    first_event_map = {fe.user_id: fe.first_date for fe in first_events}
    
    # Генерируем данные для ретеншена
    retention_data = []
    for event in events:
        user_id = event.user_id
        event_date = event.occurred_at.date() if event.occurred_at else date.today()
        cohort_date = first_event_map.get(user_id, event_date)
        retention_day = (event_date - cohort_date).days
        
        retention_data.append({
            'user_id': user_id,
            'cohort_date': cohort_date,
            'activity_date': event_date,
            'retention_day': retention_day
        })
    
    # Вставляем с обработкой конфликтов
    if retention_data:
        stmt = pg_insert(UserRetention).values(retention_data)
        stmt = stmt.on_conflict_do_nothing(
            index_elements=['user_id', 'cohort_date', 'activity_date']
        )
        db.execute(stmt)

async def _insert_events_individual_simple(db: Session, events: List[EventSchema]) -> int:
    """Простой fallback для дубликатов с вашими моделями"""
    successful = 0
    user_ids = set()
    
    for event in events:
        try:
            event_date = event.occurred_at.date() if event.occurred_at else date.today()
            user_ids.add(event.user_id)
            
            # Преобразуем event_id в UUID если нужно
            event_id = event.event_id
            if isinstance(event_id, str):
                try:
                    event_id = uuid.UUID(event_id)
                except (ValueError, AttributeError):
                    event_id = uuid.uuid4()
                    logger.warning(f"Invalid event_id format in fallback, generated new: {event_id}")
            
            # Создаем пользователя если нужно (с использованием ON CONFLICT)
            user_stmt = pg_insert(User).values(
                user_id=event.user_id,
                name=f"User_{event.user_id}",
                email=f"{event.user_id}@example.com",
                is_active=True,
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow()
            )
            user_stmt = user_stmt.on_conflict_do_update(
                index_elements=['user_id'],
                set_={
                    'is_active': True,
                    'updated_at': datetime.utcnow()
                }
            )
            db.execute(user_stmt)
            
            # Создаем событие
            db_event = Event(
                event_id=event_id,
                occurred_at=event.occurred_at,
                user_id=event.user_id,
                event_type=event.event_type,
                properties=event.properties,
                event_date=event_date
            )
            db.add(db_event)
            db.flush()  # Сохраняем чтобы проверить IntegrityError
            successful += 1
            
        except IntegrityError:
            db.rollback()
            logger.debug(f"Skipped duplicate event: {event.event_id}")
            continue  # Пропускаем дубликат
        except Exception as e:
            db.rollback()
            logger.error(f"Error inserting event individually: {str(e)}")
            continue
    
    # ✅ ОДИН КОММИТ после всех успешных операций
    if successful > 0:
        db.commit()
        # Инвалидируем кэши для затронутых пользователей
        await _invalidate_user_caches(user_ids)
    
    logger.info(f"🔄 Fallback inserted: {successful} events")
    return successful

# ==================== ДОПОЛНИТЕЛЬНЫЕ CRUD ФУНКЦИИ С КЭШИРОВАНИЕМ ====================

@cached(ttl=300, key_prefix="user_stats")  # 5 минут TTL
def get_user_stats(db: Session, user_id: str) -> Dict[str, Any]:
    """Получение статистики пользователя с кэшированием"""
    from sqlalchemy import func
    
    logger.info(f"📊 Getting stats for user: {user_id}")
    
    # Если Redis недоступен, логируем но продолжаем
    if not redis_client.is_connected:
        logger.warning("Redis not connected, executing direct DB query for user stats")
    
    try:
        # Количество событий по типам
        events_by_type = db.query(
            Event.event_type,
            func.count(Event.id).label('count')
        ).filter(Event.user_id == user_id).group_by(Event.event_type).all()
        
        # Первое и последнее событие
        first_event = db.query(Event).filter(
            Event.user_id == user_id
        ).order_by(Event.occurred_at.asc()).first()
        
        last_event = db.query(Event).filter(
            Event.user_id == user_id
        ).order_by(Event.occurred_at.desc()).first()
        
        # Статистика ретеншена
        retention_stats = db.query(
            func.min(UserRetention.cohort_date).label('first_cohort'),
            func.max(UserRetention.activity_date).label('last_activity'),
            func.count(UserRetention.id).label('active_days')
        ).filter(UserRetention.user_id == user_id).first()
        
        result = {
            'events_by_type': {e.event_type: e.count for e in events_by_type},
            'total_events': sum(e.count for e in events_by_type),
            'first_event_at': first_event.occurred_at.isoformat() if first_event else None,
            'last_event_at': last_event.occurred_at.isoformat() if last_event else None,
            'retention_stats': {
                'first_cohort': retention_stats.first_cohort.isoformat() if retention_stats and retention_stats.first_cohort else None,
                'last_activity': retention_stats.last_activity.isoformat() if retention_stats and retention_stats.last_activity else None,
                'active_days_count': retention_stats.active_days if retention_stats else 0
            }
        }
        
        logger.info(f"✅ User stats retrieved: {result['total_events']} events")
        return result
        
    except Exception as e:
        logger.error(f"❌ Error getting user stats: {str(e)}")
        return {
            'events_by_type': {},
            'total_events': 0,
            'first_event_at': None,
            'last_event_at': None,
            'retention_stats': {
                'first_cohort': None,
                'last_activity': None,
                'active_days_count': 0
            }
        }

@cached(ttl=600, key_prefix="ingestion_metrics")  # 10 минут TTL
def get_ingestion_metrics(db: Session) -> Dict[str, Any]:
    """Метрики ингестирования для мониторинга с кэшированием"""
    from sqlalchemy import func
    
    logger.info("📈 Getting ingestion metrics")
    
    if not redis_client.is_connected:
        logger.warning("Redis not connected, executing direct DB query for metrics")
    
    try:
        # Статистика по событиям
        total_events = db.query(func.count(Event.id)).scalar()
        events_today = db.query(func.count(Event.id)).filter(
            Event.event_date == date.today()
        ).scalar()
        
        events_by_type = db.query(
            Event.event_type,
            func.count(Event.id).label('count')
        ).group_by(Event.event_type).order_by(func.count(Event.id).desc()).limit(10).all()
        
        # Статистика по пользователям
        total_users = db.query(func.count(User.id)).scalar()
        active_users_today = db.query(func.count(func.distinct(Event.user_id))).filter(
            Event.event_date == date.today()
        ).scalar()
        
        result = {
            'events': {
                'total': total_events or 0,
                'today': events_today or 0,
                'by_type': {e.event_type: e.count for e in events_by_type}
            },
            'users': {
                'total': total_users or 0,
                'active_today': active_users_today or 0
            },
            'retention': {
                'cohorts_count': db.query(func.count(func.distinct(UserRetention.cohort_date))).scalar() or 0,
                'retention_entries': db.query(func.count(UserRetention.id)).scalar() or 0
            },
            'cache_status': {
                'redis_connected': redis_client.is_connected,
                'timestamp': datetime.utcnow().isoformat()
            }
        }
        
        logger.info(f"✅ Metrics retrieved: {result['events']['total']} total events")
        return result
        
    except Exception as e:
        logger.error(f"❌ Error getting ingestion metrics: {str(e)}")
        return {
            'events': {'total': 0, 'today': 0, 'by_type': {}},
            'users': {'total': 0, 'active_today': 0},
            'retention': {'cohorts_count': 0, 'retention_entries': 0},
            'cache_status': {'redis_connected': False, 'error': str(e)}
        }

# ==================== ДОПОЛНИТЕЛЬНЫЕ ФУНКЦИИ ДЛЯ УПРАВЛЕНИЯ КЭШЕМ ====================

async def clear_user_cache(user_id: str) -> bool:
    """Очистка кэша для конкретного пользователя"""
    try:
        if redis_client.is_connected:
            await redis_client.delete_pattern(f"user_stats:{user_id}:*")
            await redis_client.delete_pattern(f"user_retention:{user_id}:*")
            logger.info(f"✅ Cleared cache for user: {user_id}")
            return True
        return False
    except Exception as e:
        logger.error(f"Error clearing user cache: {str(e)}")
        return False

async def get_cache_stats() -> Dict[str, Any]:
    """Статистика кэша"""
    try:
        if not redis_client.is_connected:
            return {"redis_connected": False}
        
        # Получаем информацию о ключах
        cache_keys = await redis_client.client.keys("cache:*")
        user_stats_keys = await redis_client.client.keys("user_stats:*")
        metrics_keys = await redis_client.client.keys("ingestion_metrics:*")
        
        return {
            "redis_connected": True,
            "total_cache_keys": len(cache_keys),
            "user_stats_keys": len(user_stats_keys),
            "metrics_keys": len(metrics_keys),
            "memory_info": await redis_client.client.info('memory')
        }
    except Exception as e:
        logger.error(f"Error getting cache stats: {str(e)}")
        return {"redis_connected": False, "error": str(e)}