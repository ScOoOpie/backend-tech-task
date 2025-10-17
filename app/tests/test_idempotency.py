# tests/test_idempotency.py
import pytest
from unittest.mock import Mock, patch, AsyncMock
from datetime import datetime, timezone
import uuid

@pytest.mark.asyncio
async def test_event_idempotency():
    """Тест что повторная отправка того же event_id не создает дубликатов"""
    from app.crud import ingest_events
    from app.schemas import Event as EventSchema
    
    # Мокаем базу данных
    mock_db = Mock()
    
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
    
    # Первый вызов - должен пройти успешно
    with patch('app.crud._insert_events_batch', new_callable=AsyncMock) as mock_batch:
        mock_batch.return_value = 1
        result1 = await ingest_events(mock_db, events)
        assert result1 == 1
    
    # Второй вызов - должен обработать дубликат
    with patch('app.crud._insert_events_batch', new_callable=AsyncMock) as mock_batch:
        from sqlalchemy.exc import IntegrityError
        mock_batch.side_effect = IntegrityError("Duplicate", None, None)
        
        with patch('app.crud._insert_events_individual', new_callable=AsyncMock) as mock_individual:
            mock_individual.return_value = 0  # Дубликат пропущен
            result2 = await ingest_events(mock_db, events)
            assert result2 == 0
    
    print("✅ Идемпотентность: дубликаты корректно обрабатываются")

@pytest.mark.asyncio
async def test_user_creation_idempotency():
    """Тест что ensure_user_exists идемпотентный"""
    from app.crud import ensure_user_exists
    from app.models import User
    
    mock_db = Mock()
    
    # Мокаем запрос к базе
    mock_db.query.return_value.filter.return_value.first.return_value = None
    
    # Первый вызов - создает пользователя
    user1 = await ensure_user_exists(mock_db, "test_user", "Test User")
    assert user1 is not None
    assert mock_db.add.called
    
    # Сбрасываем мок
    mock_db.add.reset_mock()
    
    # Второй вызов - не должен создавать пользователя
    existing_user = User(user_id="test_user", name="Test User")
    mock_db.query.return_value.filter.return_value.first.return_value = existing_user
    
    user2 = await ensure_user_exists(mock_db, "test_user", "Test User")
    assert user2 is not None
    assert not mock_db.add.called
    
    print("✅ Идемпотентность пользователей: пользователь не создается повторно")