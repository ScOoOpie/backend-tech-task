# debug_alembic.py
import os
import sys
from sqlalchemy import create_engine, MetaData, inspect

# Добавляем пути как в env.py
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
app_dir = os.path.join(project_root, 'app')

sys.path.insert(0, project_root)
sys.path.insert(0, app_dir)

print("🔍 DIAGNOSTIC SCRIPT")
print(f"Current dir: {current_dir}")
print(f"Project root: {project_root}")
print(f"App dir: {app_dir}")
print(f"Files in app dir: {os.listdir(app_dir) if os.path.exists(app_dir) else 'NOT FOUND'}")

# Проверяем импорт моделей
try:
    from app.models import Base
    print("✅ SUCCESS: from app.models import Base")
    print(f"Tables in Base.metadata: {list(Base.metadata.tables.keys())}")
    print(f"Number of tables: {len(Base.metadata.tables)}")
except ImportError as e:
    print(f"❌ FAILED: from app.models import Base - {e}")


# Проверяем что вообще в sys.path
print(f"\n📁 sys.path:")
for path in sys.path:
    print(f"  - {path}")

# Проверяем подключение к БД
DATABASE_URL = "postgresql://event_user:secure_password_123@localhost:5433/events_db"
print(f"\n🔗 Testing DB connection: {DATABASE_URL}")
try:
    engine = create_engine(DATABASE_URL)
    with engine.connect() as conn:
        # Проверяем существующие таблицы в БД
        inspector = inspect(engine)
        existing_tables = inspector.get_table_names()
        print(f"📊 Existing tables in DB: {existing_tables}")
        
        # Проверяем metadata отражение
        metadata = MetaData()
        metadata.reflect(bind=engine)
        print(f"📊 Tables in reflected metadata: {list(metadata.tables.keys())}")
        
except Exception as e:
    print(f"❌ DB connection failed: {e}")