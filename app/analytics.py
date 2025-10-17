from sqlalchemy.orm import Session
from sqlalchemy import func, text
from datetime import datetime, timedelta
from typing import Dict, Any, List
import logging

logger = logging.getLogger(__name__)

async def get_cohorts_list(db: Session, limit: int = 10) -> List[Dict[str, Any]]:
    """Список активных когорт"""
    try:
        logger.info(f"Getting cohorts list with limit: {limit}")
        
        # Сначала проверим, существует ли таблица user_retention
        try:
            check_table_query = text("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = 'user_retention'
                );
            """)
            table_exists = db.execute(check_table_query).scalar()
            
            if not table_exists:
                logger.warning("Table 'user_retention' does not exist yet")
                return []
        except Exception as e:
            logger.error(f"Error checking table existence: {str(e)}")
            return []
        
        # Основной запрос для получения когорт
        query = text("""
            SELECT 
                cohort_date,
                COUNT(DISTINCT user_id) as total_users
            FROM user_retention
            WHERE retention_day = 0
            GROUP BY cohort_date
            ORDER BY cohort_date DESC
            LIMIT :limit
        """)
        
        result = db.execute(query, {"limit": limit})
        rows = result.fetchall()
        
        cohorts = [
            {
                "cohort_date": row[0].strftime("%Y-%m-%d"),
                "total_users": row[1],
                "days_since_start": (datetime.now().date() - row[0]).days
            }
            for row in rows
        ]
        
        logger.info(f"Found {len(cohorts)} cohorts")
        return cohorts
        
    except Exception as e:
        logger.error(f"Error getting cohorts list: {str(e)}", exc_info=True)
        return []

async def get_retention_stats(db: Session, start_date: str, windows: int = 7) -> Dict[str, Any]:
    """Ретеншен на основе таблицы user_retention"""
    try:
        logger.info(f"Calculating retention for cohort: {start_date}, windows: {windows}")
        
        cohort_date = datetime.strptime(start_date, '%Y-%m-%d').date()
        
        # Сначала проверим существование таблицы
        try:
            check_table_query = text("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = 'user_retention'
                );
            """)
            table_exists = db.execute(check_table_query).scalar()
            
            if not table_exists:
                logger.warning("Table 'user_retention' does not exist yet")
                return {
                    "metric": "retention",
                    "cohort_start": start_date,
                    "total_users": 0,
                    "windows": windows,
                    "data": [],
                    "message": "Retention data not available yet - table doesn't exist"
                }
        except Exception as e:
            logger.error(f"Error checking table: {str(e)}")
            return {
                "metric": "retention", 
                "cohort_start": start_date,
                "total_users": 0,
                "windows": windows,
                "data": [],
                "message": f"Error: {str(e)}"
            }
        
        # Общее количество пользователей в когорте
        total_users_query = text("""
            SELECT COUNT(DISTINCT user_id)
            FROM user_retention 
            WHERE cohort_date = :cohort_date 
                AND retention_day = 0
        """)
        
        total_result = db.execute(total_users_query, {"cohort_date": cohort_date})
        total_users = total_result.scalar() or 0
        
        if total_users == 0:
            return {
                "metric": "retention",
                "cohort_start": start_date,
                "total_users": 0,
                "windows": windows,
                "data": [],
                "message": "No users found for this cohort"
            }
        
        # Используем новую таблицу user_retention для расчета
        retention_query = text("""
            SELECT 
                retention_day,
                COUNT(DISTINCT user_id) as retained_users
            FROM user_retention 
            WHERE cohort_date = :cohort_date 
                AND retention_day <= :windows
                AND retention_day >= 0
            GROUP BY retention_day
            ORDER BY retention_day
        """)
        
        result = db.execute(retention_query, {
            "cohort_date": cohort_date,
            "windows": windows
        })
        retention_rows = result.fetchall()
        
        # Форматируем результат
        retention_data = []
        for day in range(windows + 1):
            # Находим данные для этого дня
            retained = next((row.retained_users for row in retention_rows if row.retention_day == day), 0)
            retention_rate = (retained / total_users) * 100 if total_users > 0 else 0
            
            retention_data.append({
                "window": day,
                "window_date": (cohort_date + timedelta(days=day)).strftime("%Y-%m-%d"),
                "retained_users": retained,
                "total_cohort_users": total_users,
                "retention_rate": round(retention_rate, 2)
            })
        
        logger.info(f"Retention calculated for {start_date}: {len(retention_data)} days")
        return {
            "metric": "retention",
            "cohort_start": start_date,
            "total_users": total_users,
            "windows": windows,
            "data": retention_data
        }
        
    except Exception as e:
        logger.error(f"Error calculating retention: {str(e)}", exc_info=True)
        return {
            "metric": "retention",
            "cohort_start": start_date,
            "total_users": 0,
            "windows": windows,
            "data": [],
            "error": str(e)
        }

# Остальные функции остаются без изменений
async def get_dau_stats(db: Session, from_date: str, to_date: str) -> Dict[str, Any]:
    """Кількість унікальних user_id по днях"""
    try:
        query = text("""
            SELECT event_date, COUNT(DISTINCT user_id) as unique_users
            FROM events 
            WHERE event_date BETWEEN :from_date AND :to_date
            GROUP BY event_date
            ORDER BY event_date
        """)
        
        result = db.execute(query, {"from_date": from_date, "to_date": to_date})
        rows = result.fetchall()
        
        return {
            "metric": "dau",
            "period": {"from": from_date, "to": to_date},
            "data": [{"date": str(row[0]), "unique_users": row[1]} for row in rows]
        }
    except Exception as e:
        logger.error(f"Error calculating DAU: {str(e)}")
        raise

async def get_top_events(db: Session, from_date: str, to_date: str, limit: int = 10) -> Dict[str, Any]:
    """Топ event_type за кількістю"""
    try:
        query = text("""
            SELECT event_type, COUNT(*) as event_count
            FROM events 
            WHERE event_date BETWEEN :from_date AND :to_date
            GROUP BY event_type
            ORDER BY event_count DESC
            LIMIT :limit
        """)
        
        result = db.execute(query, {
            "from_date": from_date, 
            "to_date": to_date, 
            "limit": limit
        })
        rows = result.fetchall()
        
        return {
            "metric": "top_events",
            "period": {"from": from_date, "to": to_date},
            "data": [{"event_type": row[0], "count": row[1]} for row in rows]
        }
    except Exception as e:
        logger.error(f"Error calculating top events: {str(e)}")
        raise

async def get_user_retention_data(db: Session, user_id: str) -> Dict[str, Any]:
    """Статистика ретеншена для конкретного пользователя"""
    try:
        query = text("""
            SELECT 
                cohort_date,
                retention_day,
                activity_date
            FROM user_retention 
            WHERE user_id = :user_id
            ORDER BY retention_day
        """)
        
        result = db.execute(query, {"user_id": user_id})
        rows = result.fetchall()
        
        if not rows:
            return {"error": "User not found in retention data"}
        
        cohort_date = rows[0][0]
        total_days = max(row[1] for row in rows) + 1 if rows else 0
        active_days = len(rows)
        
        daily_activity = [
            {
                "day": row[1],
                "date": row[2].strftime("%Y-%m-%d"),
                "active": True
            }
            for row in rows
        ]
        
        return {
            "user_id": user_id,
            "cohort_date": cohort_date.strftime("%Y-%m-%d"),
            "total_days_tracked": total_days,
            "active_days": active_days,
            "retention_rate": round((active_days / total_days) * 100, 2) if total_days > 0 else 0,
            "daily_activity": daily_activity
        }
        
    except Exception as e:
        logger.error(f"Error getting user retention data: {str(e)}")
        return {"error": str(e)}