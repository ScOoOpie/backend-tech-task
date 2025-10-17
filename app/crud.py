from sqlalchemy.orm import Session
from sqlalchemy import text, insert
from sqlalchemy.dialects.postgresql import insert as pg_insert
from typing import List, Set, Dict, Any
import logging
from sqlalchemy.exc import IntegrityError
from datetime import date, datetime
import uuid

from .models import Event, UserRetention, User, Base
from .schemas import Event as EventSchema

logger = logging.getLogger(__name__)

# ==================== ОСНОВНАЯ ФУНКЦИЯ ИНГЕСТИРОВАНИЯ ====================


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
    # ❌ НЕТ КОММИТА - коммит будет в основной функции

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
    # ❌ НЕТ КОММИТА - коммит будет в основной функции

async def _insert_events_individual_simple(db: Session, events: List[EventSchema]) -> int:
    """Простой fallback для дубликатов с вашими моделями"""
    successful = 0
    
    for event in events:
        try:
            event_date = event.occurred_at.date() if event.occurred_at else date.today()
            
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
    
    logger.info(f"🔄 Fallback inserted: {successful} events")
    return successful

# ==================== ДОПОЛНИТЕЛЬНЫЕ CRUD ФУНКЦИИ ====================

# ❌ УДАЛЯЕМ ЭТУ ФУНКЦИЮ - она создает лишние коммиты и дублирует логику
# async def ensure_user_exists(db: Session, user_id: str) -> User:
#     """Обеспечивает существование пользователя (для обратной совместимости)"""
#     # Используем оптимизированный подход
#     await _ensure_users_batch(db, {user_id})
#     db.commit()  # ⚠️ ЛИШНИЙ КОММИТ
#     
#     user = db.query(User).filter(User.user_id == user_id).first()
#     return user

def get_user_stats(db: Session, user_id: str) -> Dict[str, Any]:
    """Получение статистики пользователя"""
    from sqlalchemy import func
    
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
    
    return {
        'events_by_type': {e.event_type: e.count for e in events_by_type},
        'total_events': sum(e.count for e in events_by_type),
        'first_event_at': first_event.occurred_at if first_event else None,
        'last_event_at': last_event.occurred_at if last_event else None,
        'retention_stats': {
            'first_cohort': retention_stats.first_cohort if retention_stats else None,
            'last_activity': retention_stats.last_activity if retention_stats else None,
            'active_days_count': retention_stats.active_days if retention_stats else 0
        }
    }

def get_ingestion_metrics(db: Session) -> Dict[str, Any]:
    """Метрики ингестирования для мониторинга"""
    from sqlalchemy import func
    
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
    
    return {
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
        }
    }