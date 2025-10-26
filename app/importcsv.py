#!/usr/bin/env python3
"""
CLI-скрипт для імпорту історичних даних з CSV-файлу
Використання: python import_events.py <path-to-csv> [--database <connection-string>]
"""

import argparse
import csv
import os
import sys
from datetime import datetime, date
import json
import logging
import uuid
from typing import List, Dict, Optional

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError, IntegrityError

from models import Base, Event
from sqlalchemy.orm import sessionmaker
# Налаштування логування
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('import_events.log')
    ]
)
logger = logging.getLogger(__name__)

class CSVImportError(Exception):
    """Користувацький виняток для помилок імпорту"""
    pass

class EventDataValidator:
    """Валідатор даних подій"""
    
    REQUIRED_COLUMNS = ['event_id', 'occurred_at', 'user_id', 'event_type', 'properties_json']
    
    @staticmethod
    def validate_csv_structure(file_path: str) -> bool:
        """Перевіряє структуру CSV-файлу"""
        try:
            with open(file_path, 'r', encoding='utf-8') as file:
                reader = csv.DictReader(file)
                if not reader.fieldnames:
                    raise CSVImportError("CSV файл порожній або не має заголовків")
                
                missing_columns = set(EventDataValidator.REQUIRED_COLUMNS) - set(reader.fieldnames)
                if missing_columns:
                    raise CSVImportError(f"Відсутні обов'язкові стовпці: {missing_columns}")
                
                return True
        except UnicodeDecodeError:
            # Спробуємо з іншою кодуванням
            try:
                with open(file_path, 'r', encoding='cp1251') as file:
                    reader = csv.DictReader(file)
                    if not reader.fieldnames:
                        raise CSVImportError("CSV файл порожній або не має заголовків")
                    
                    missing_columns = set(EventDataValidator.REQUIRED_COLUMNS) - set(reader.fieldnames)
                    if missing_columns:
                        raise CSVImportError(f"Відсутні обов'язкові стовпці: {missing_columns}")
                    
                    return True
            except UnicodeDecodeError:
                raise CSVImportError("Не вдається декодувати CSV файл. Спробуйте інше кодування.")
    
    @staticmethod
    def validate_event_data(row: Dict) -> Optional[str]:
        """Валідує дані окремої події"""
        try:
            # Перевірка event_id (має бути валідним UUID)
            event_id_str = row.get('event_id', '').strip()
            if not event_id_str:
                return "event_id не може бути порожнім"
            
            try:
                uuid.UUID(event_id_str)
            except ValueError:
                return f"event_id має бути валідним UUID: {event_id_str}"
            
            # Перевірка occurred_at
            occurred_at = row.get('occurred_at')
            if not occurred_at:
                return "occurred_at не може бути порожнім"
            
            # Перевірка user_id
            user_id = row.get('user_id', '').strip()
            if not user_id:
                return "user_id не може бути порожнім"
            if len(user_id) > 255:
                return f"user_id занадто довгий (макс. 255 символів): {user_id}"
            
            # Перевірка event_type
            event_type = row.get('event_type', '').strip()
            if not event_type:
                return "event_type не може бути порожнім"
            if len(event_type) > 255:
                return f"event_type занадто довгий (макс. 255 символів): {event_type}"
            
            # Перевірка properties_json
            properties_json = row.get('properties_json', '{}').strip()
            if properties_json and properties_json.strip():
                try:
                    json.loads(properties_json)
                except json.JSONDecodeError:
                    return "properties_json містить некоректний JSON"
            
            return None
            
        except Exception as e:
            return f"Помилка валідації: {str(e)}"

class EventDataProcessor:
    """Обробник даних подій"""
    
    @staticmethod
    def parse_datetime(datetime_str: str) -> datetime:
        """Парсить datetime з різних форматів, включаючи ISO 8601 з часовим поясом"""
        formats = [
            '%Y-%m-%d %H:%M:%S',           # 2023-01-15 10:30:00
            '%Y-%m-%dT%H:%M:%S',           # 2023-01-15T10:30:00
            '%Y-%m-%dT%H:%M:%S.%f',        # 2023-01-15T10:30:00.123456
            '%Y-%m-%dT%H:%M:%S%z',         # 2025-08-21T06:52:34+0300
            '%Y-%m-%dT%H:%M:%S.%f%z',      # 2025-08-21T06:52:34.123+0300
            '%Y-%m-%dT%H:%M:%S%Z',         # 2025-08-21T06:52:34+03:00
            '%Y-%m-%dT%H:%M:%S.%f%Z',      # 2025-08-21T06:52:34.123+03:00
            '%d.%m.%Y %H:%M:%S',           # 15.01.2023 10:30:00
            '%d/%m/%Y %H:%M:%S',           # 15/01/2023 10:30:00
            '%Y-%m-%d',                    # 2023-01-15
            '%d.%m.%Y'                     # 15.01.2023
        ]
        
        # Спрощуємо рядок дати - видаляємо : в часовому поясі для Python 3.6-3.8
        datetime_str_clean = datetime_str.replace('+03:00', '+0300').replace('-03:00', '-0300')
        
        for fmt in formats:
            try:
                parsed_dt = datetime.strptime(datetime_str_clean.strip(), fmt)
                return parsed_dt
            except ValueError:
                continue
        
        # Якщо жоден формат не підійшов, спробуємо dateutil (більш гнучкий)
        try:
            from dateutil import parser
            return parser.parse(datetime_str)
        except ImportError:
            pass
        except Exception:
            pass
        
        raise ValueError(f"Невідомий формат дати: {datetime_str}")
    
    @staticmethod
    def process_row(row: Dict) -> Event:
        """Обробляє рядок CSV і створює об'єкт Event"""
        # Парсинг event_id (UUID)
        event_id = uuid.UUID(row['event_id'].strip())
        
        # Парсинг дати
        occurred_at = EventDataProcessor.parse_datetime(row['occurred_at'])
        
        # Обробка JSON властивостей
        properties_json = row.get('properties_json', '{}').strip()
        if not properties_json:
            properties = None
        else:
            properties = json.loads(properties_json)
        
        # Створення денормалізованого поля event_date
        event_date = occurred_at.date()
        
        # Створення об'єкта події
        event = Event(
            event_id=event_id,
            occurred_at=occurred_at,
            user_id=row['user_id'].strip(),
            event_type=row['event_type'].strip(),
            properties=properties,
            event_date=event_date
        )
        
        return event

class DatabaseManager:
    """Менеджер роботи з базою даних"""
    
    def __init__(self, connection_string: str):
        self.connection_string = connection_string
        self.engine = None
        self.Session = None
    
    def connect(self):
        """Встановлює з'єднання з базою даних"""
        try:
            self.engine = create_engine(self.connection_string)
            self.Session = sessionmaker(bind=self.engine)
            
            # Тестове з'єднання
            with self.engine.connect() as conn:
                logger.info("Успішне підключення до бази даних")
                
        except SQLAlchemyError as e:
            raise CSVImportError(f"Помилка підключення до бази даних: {str(e)}")
    
    def create_tables(self):
        """Створює таблиці, якщо вони не існують"""
        try:
            Base.metadata.create_all(self.engine)
            logger.info("Таблиці перевірені/створені успішно")
        except SQLAlchemyError as e:
            raise CSVImportError(f"Помилка створення таблиць: {str(e)}")
    
    def import_events(self, events: List[Event], batch_size: int = 1000) -> Dict:
        """Імпортує події в базу даних"""
        session = self.Session()
        stats = {
            'total_processed': 0,
            'successful': 0,
            'failed': 0,
            'duplicates': 0,
            'errors': []
        }
        
        try:
            for i in range(0, len(events), batch_size):
                batch = events[i:i + batch_size]
                stats['total_processed'] += len(batch)
                
                try:
                    session.bulk_save_objects(batch)
                    session.commit()
                    stats['successful'] += len(batch)
                    logger.info(f"Оброблено батч {i//batch_size + 1}: {len(batch)} подій")
                    
                except IntegrityError as e:
                    session.rollback()
                    # Спроба індивідуального додавання для виявлення дублікатів
                    batch_successful = 0
                    batch_duplicates = 0
                    
                    for event in batch:
                        try:
                            individual_session = self.Session()
                            individual_session.add(event)
                            individual_session.commit()
                            batch_successful += 1
                            individual_session.close()
                        except IntegrityError:
                            batch_duplicates += 1
                            individual_session.rollback()
                        except Exception as e:
                            stats['errors'].append(f"Помилка для event_id {event.event_id}: {str(e)}")
                            individual_session.rollback()
                        finally:
                            if individual_session:
                                individual_session.close()
                    
                    stats['successful'] += batch_successful
                    stats['duplicates'] += batch_duplicates
                    stats['failed'] += (len(batch) - batch_successful - batch_duplicates)
                    
                    if batch_duplicates > 0:
                        logger.warning(f"Батч {i//batch_size + 1}: знайдено {batch_duplicates} дублікатів")
                    
                except SQLAlchemyError as e:
                    session.rollback()
                    stats['failed'] += len(batch)
                    stats['errors'].append(f"Батч {i//batch_size + 1}: {str(e)}")
                    logger.error(f"Помилка при імпорті батчу {i//batch_size + 1}: {str(e)}")
                    
        except Exception as e:
            session.rollback()
            raise CSVImportError(f"Критична помилка при імпорті: {str(e)}")
        finally:
            session.close()
        
        return stats

def get_database_connection() -> str:
    """Отримує рядок підключення до бази даних"""
    # Спроба отримати з змінних оточення
    db_url = os.getenv('DATABASE_URL')
    if db_url:
        return db_url
    
    # Спроба отримати з .env файлу
    try:
        from dotenv import load_dotenv
        load_dotenv()
        db_url = os.getenv('DATABASE_URL')
        if db_url:
            return db_url
    except ImportError:
        pass
    
    # Стандартний рядок підключення для SQLite (обмежена підтримка JSONB)
    return 'sqlite:///events.db'

def main():
    """Основна функція"""
    parser = argparse.ArgumentParser(description='Імпорт історичних даних з CSV-файлу')
    parser.add_argument('csv_file', help='Шлях до CSV-файлу з даними')
    parser.add_argument('--database', '-d', help='Рядок підключення до бази даних')
    parser.add_argument('--batch-size', '-b', type=int, default=1000, 
                       help='Розмір батчу для імпорту (за замовчуванням: 1000)')
    parser.add_argument('--skip-duplicates', '-s', action='store_true',
                       help='Пропускати дублікати (за замовчуванням: зупинятися на помилках)')
    parser.add_argument('--verbose', '-v', action='store_true', 
                       help='Детальний вивід')
    
    args = parser.parse_args()
    
    # Налаштування рівня логування
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    try:
        # Перевірка існування файлу
        if not os.path.exists(args.csv_file):
            logger.error(f"Файл не знайдено: {args.csv_file}")
            sys.exit(1)
        
        # Отримання рядка підключення
        db_connection = args.database or get_database_connection()
        
        logger.info(f"Початок імпорту з файлу: {args.csv_file}")
        logger.info(f"Використовується база даних: {db_connection}")
        
        # Валідація структури CSV
        logger.info("Перевірка структури CSV файлу...")
        EventDataValidator.validate_csv_structure(args.csv_file)
        
        # Підготовка бази даних
        db_manager = DatabaseManager(db_connection)
        db_manager.connect()
        db_manager.create_tables()
        
        # Читання та обробка даних
        logger.info("Читання та обробка даних...")
        events = []
        validation_errors = []
        
        with open(args.csv_file, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            
            for row_num, row in enumerate(reader, 2):  # +1 для заголовку
                # Валідація даних
                error = EventDataValidator.validate_event_data(row)
                if error:
                    validation_errors.append(f"Рядок {row_num}: {error}")
                    continue
                
                try:
                    # Обробка рядка
                    event = EventDataProcessor.process_row(row)
                    events.append(event)
                except Exception as e:
                    validation_errors.append(f"Рядок {row_num}: {str(e)}")
        
        # Вивід помилок валідації
        if validation_errors:
            logger.warning(f"Знайдено {len(validation_errors)} помилок валідації:")
            for error in validation_errors[:10]:  # Вивід перших 10 помилок
                logger.warning(f"  {error}")
            if len(validation_errors) > 10:
                logger.warning(f"  ... і ще {len(validation_errors) - 10} помилок")
        
        # Імпорт даних
        if events:
            logger.info(f"Початок імпорту {len(events)} коректних подій...")
            stats = db_manager.import_events(events, args.batch_size)
            
            # Вивід статистики
            logger.info("=" * 50)
            logger.info("СТАТИСТИКА ІМПОРТУ:")
            logger.info(f"Загалом оброблено: {stats['total_processed']}")
            logger.info(f"Успішно імпортовано: {stats['successful']}")
            logger.info(f"Дублікатів: {stats['duplicates']}")
            logger.info(f"Не вдалося імпортувати: {stats['failed']}")
            
            if stats['total_processed'] > 0:
                success_rate = (stats['successful'] / stats['total_processed']) * 100
                logger.info(f"Відсоток успіху: {success_rate:.2f}%")
            
            if stats['errors']:
                logger.warning("Помилки при імпорті:")
                for error in stats['errors'][:5]:  # Вивід перших 5 помилок
                    logger.warning(f"  {error}")
                if len(stats['errors']) > 5:
                    logger.warning(f"  ... і ще {len(stats['errors']) - 5} помилок")
        else:
            logger.warning("Немає коректних даних для імпорту")
        
        logger.info("Імпорт завершено!")
        
    except CSVImportError as e:
        logger.error(f"Помилка імпорту: {str(e)}")
        sys.exit(1)
    except KeyboardInterrupt:
        logger.info("Імпорт перервано користувачем")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Неочікувана помилка: {str(e)}")
        sys.exit(1)

if __name__ == '__main__':
    main()