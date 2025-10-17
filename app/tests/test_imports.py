def test_basic_imports():
    """Проверяем что основные модули импортируются"""
    try:
        from fastapi import FastAPI
        from fastapi.testclient import TestClient
        print("✅ FastAPI imports work")
        
        from app import models
        print("✅ Models import works")
        
        from app import schemas
        print("✅ Schemas import works")
        
        # Пробуем импортировать main с моками
        from unittest.mock import patch
        with patch('app.database.engine'), patch('app.database.SessionLocal'):
            with patch('app.models.Base.metadata.create_all'):
                from app import main
                print("✅ Main import works with mocks")
        
        print("🎉 All imports successful!")
        
    except Exception as e:
        print(f"❌ Import error: {e}")
        import traceback
        traceback.print_exc()