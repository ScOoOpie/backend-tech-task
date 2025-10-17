# tests/conftest.py
import pytest
import os
import logging
from unittest.mock import patch, MagicMock
from fastapi.testclient import TestClient

# Создаем папку для логов если её нет
os.makedirs('logs', exist_ok=True)

# Настраиваем логгирование для тестов
logging.basicConfig(
    level=logging.WARNING,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)

# Мокаем ВСЕ что связано с БД перед импортом приложения
mock_engine = MagicMock()
mock_session = MagicMock()

with patch('app.database.engine', mock_engine):
    with patch('app.database.SessionLocal', mock_session):
        with patch('app.models.Base.metadata.create_all'):
            with patch('app.main.logging.basicConfig'):
                # Теперь безопасно импортируем приложение
                from app.main import app

@pytest.fixture
def client():
    """Тестовый клиент"""
    return TestClient(app)

@pytest.fixture
def mock_db():
    """Мок сессии БД"""
    return MagicMock()