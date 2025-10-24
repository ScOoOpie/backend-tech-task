# tests/test_idempotency.py
import pytest
from unittest.mock import Mock, patch, AsyncMock, MagicMock
from datetime import datetime, timezone
import uuid
from sqlalchemy.exc import IntegrityError

@pytest.mark.asyncio
async def test_event_idempotency():
    """Тест что повторная отправка того же event_id не создает дубликатов"""
    from app.crud import ingest_events, _insert_events_individual_simple
    from app.schemas import Event as EventSchema
    
    # Мокаем базу данных
    mock_db = Mock()
    mock_db.commit = Mock()
    mock_db.rollback = Mock()
    mock_db.execute = Mock()
    
    # Создаем тестовые события
    event_id = str(uuid.uuid4())
    events = [
        EventSchema(
            event_id=event_id,
            occurred_at=datetime.now(timezone.utc),
            user_id="test_user",
            event_type="page_view",
            properties={"page": "/home"}
        )
    ]
    
    # Первый вызов - должен пройти успешно через пакетную вставку
    with patch('app.crud._ensure_users_batch', new_callable=AsyncMock) as mock_users:
        with patch('app.crud._update_retention_batch', new_callable=AsyncMock) as mock_retention:
            with patch('app.crud._invalidate_user_caches', new_callable=AsyncMock) as mock_cache:
                mock_users.return_value = None
                mock_retention.return_value = None
                mock_cache.return_value = None
                
                # Мокаем успешное выполнение пакетной вставки
                mock_db.execute.return_value = None
                
                result1 = await ingest_events(mock_db, events)
                assert result1 == 1
                assert mock_db.commit.called
    
    # Второй вызов - должен обработать дубликат через fallback
    with patch('app.crud._ensure_users_batch', new_callable=AsyncMock) as mock_users:
        with patch('app.crud._update_retention_batch', new_callable=AsyncMock) as mock_retention:
            with patch('app.crud._insert_events_individual_simple', new_callable=AsyncMock) as mock_fallback:
                mock_users.return_value = None
                mock_retention.return_value = None
                mock_fallback.return_value = 0  # Дубликат пропущен
                
                # Эмулируем IntegrityError при пакетной вставке
                mock_db.execute.side_effect = IntegrityError("Duplicate", None, None)
                
                result2 = await ingest_events(mock_db, events)
                assert result2 == 0  # Дубликат должен быть пропущен
                assert mock_fallback.called
    
    print("✅ Идемпотентность событий: дубликаты корректно обрабатываются")

@pytest.mark.asyncio
async def test_user_creation_idempotency():
    """Тест что ensure_users_batch идемпотентный"""
    from app.crud import _ensure_users_batch
    from app.models import User
    
    mock_db = Mock()
    mock_db.execute = Mock()
    
    user_ids = {"user1", "user2"}
    
    # Вызываем функцию - должна использовать ON CONFLICT DO UPDATE
    await _ensure_users_batch(mock_db, user_ids)
    
    # Проверяем что был вызван execute с правильным запросом
    assert mock_db.execute.called
    call_args = mock_db.execute.call_args[0][0]
    
    # Проверяем что используется on_conflict_do_update
    assert hasattr(call_args, '_post_values_clause') or 'ON CONFLICT' in str(call_args)
    
    print("✅ Идемпотентность пользователей: ON CONFLICT корректно настроен")

@pytest.mark.asyncio
async def test_individual_fallback_idempotency():
    """Тест что fallback механизм корректно обрабатывает дубликаты"""
    from app.crud import _insert_events_individual_simple
    from app.schemas import Event as EventSchema
    
    mock_db = Mock()
    mock_db.commit = Mock()
    mock_db.rollback = Mock()
    mock_db.add = Mock()
    mock_db.flush = Mock()
    mock_db.execute = Mock()
    
    # Создаем события с одинаковым event_id
    event_id = str(uuid.uuid4())
    events = [
        EventSchema(
            event_id=event_id,
            occurred_at=datetime.now(timezone.utc),
            user_id="test_user",
            event_type="page_view",
            properties={"page": "/home"}
        ),
        EventSchema(
            event_id=event_id,  # Тот же event_id - дубликат
            occurred_at=datetime.now(timezone.utc),
            user_id="test_user", 
            event_type="page_view",
            properties={"page": "/about"}
        )
    ]
    
    # Настраиваем моки для обработки дубликатов
    flush_call_count = 0
    def flush_side_effect():
        nonlocal flush_call_count
        flush_call_count += 1
        if flush_call_count == 2:  # Второй вызов flush - дубликат
            raise IntegrityError("Duplicate", None, None)
    
    mock_db.flush.side_effect = flush_side_effect
    
    with patch('app.crud._invalidate_user_caches', new_callable=AsyncMock) as mock_cache:
        mock_cache.return_value = None
        
        result = await _insert_events_individual_simple(mock_db, events)
        
        # Должен быть вставлен только один event, второй - пропущен
        assert result == 1
        assert mock_db.commit.call_count == 1  # Исправлено: call_count вместо called_once
        assert mock_db.rollback.called  # Rollback для дубликата
    
    print("✅ Fallback идемпотентность: дубликаты корректно пропускаются")

@pytest.mark.asyncio
async def test_retention_update_idempotency():
    """Тест что обновление ретеншена идемпотентно"""
    from app.crud import _update_retention_batch
    from app.schemas import Event as EventSchema
    
    mock_db = Mock()
    mock_db.execute = Mock()
    
    # Создаем события
    events = [
        EventSchema(
            event_id=str(uuid.uuid4()),
            occurred_at=datetime.now(timezone.utc),
            user_id="user1",
            event_type="page_view",
            properties={"page": "/home"}
        )
    ]
    
    # Мокаем запрос для нахождения первых событий
    mock_first_event = Mock()
    mock_first_event.user_id = "user1"
    mock_first_event.first_date = datetime.now(timezone.utc).date()
    
    with patch('app.crud.text') as mock_text:
        mock_execute = Mock()
        mock_execute.fetchall.return_value = [mock_first_event]
        mock_db.execute.return_value = mock_execute
        
        await _update_retention_batch(mock_db, events)
        
        # Проверяем что используется on_conflict_do_nothing
        assert mock_db.execute.called
        # Последний вызов должен быть с INSERT ... ON CONFLICT
        last_call_args = mock_db.execute.call_args_list[-1][0][0]
        assert 'ON CONFLICT' in str(last_call_args)
    
    print("✅ Ретеншн идемпотентность: ON CONFLICT DO NOTHING корректно настроен")


@pytest.mark.asyncio
async def test_empty_events_handling():
    """Тест обработки пустого списка событий"""
    from app.crud import ingest_events
    
    mock_db = Mock()
    
    result = await ingest_events(mock_db, [])
    
    assert result == 0
    # Не должно быть вызовов к БД при пустом списке
    assert not mock_db.execute.called
    assert not mock_db.commit.called
    
    print("✅ Обработка пустых событий: корректно возвращает 0")

if __name__ == "__main__":
    pytest.main([__file__, "-v"])