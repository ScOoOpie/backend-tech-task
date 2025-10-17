import asyncio
import aiohttp
import time
import uuid
import random
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor
import asyncio
import threading

class LoadTester:
    def __init__(self, base_url, api_key, num_workers=10, requests_per_worker=100):
        self.base_url = base_url
        self.api_key = api_key
        self.num_workers = num_workers
        self.requests_per_worker = requests_per_worker
        self.results = {
            'success': 0,
            'errors': 0,
            'rate_limit': 0,
            'total_time': 0
        }
        self.lock = threading.Lock()
    
    def generate_event_batch(self, batch_size=10):
        """Генерация батча событий"""
        events = []
        for i in range(batch_size):
            event_types = ['page_view', 'click', 'purchase', 'signup', 'login']
            events.append({
                "event_id": str(uuid.uuid4()),
                "occurred_at": datetime.now(timezone.utc).isoformat(),
                "user_id": f"user_{random.randint(1, 1000)}",
                "event_type": random.choice(event_types),
                "properties": {
                    "page": f"/page_{random.randint(1, 10)}",
                    "device": random.choice(['mobile', 'desktop', 'tablet']),
                    "value": random.randint(1, 1000)
                }
            })
        return {"events": events}
    
    async def send_requests(self, worker_id):
        """Отправка запросов воркером"""
        headers = {"X-API-Key": self.api_key, "Content-Type": "application/json"}
        
        async with aiohttp.ClientSession() as session:
            for i in range(self.requests_per_worker):
                start_time = time.time()
                
                try:
                    # Чередуем эндпоинты для разнообразия нагрузки
                    if i % 3 == 0:
                        # Отправка событий
                        data = self.generate_event_batch(random.randint(1, 20))
                        async with session.post(
                            f"{self.base_url}/events",
                            headers=headers,
                            json=data,
                            timeout=30
                        ) as response:
                            if response.status == 201:
                                with self.lock:
                                    self.results['success'] += 1
                            elif response.status == 429:
                                with self.lock:
                                    self.results['rate_limit'] += 1
                            else:
                                with self.lock:
                                    self.results['errors'] += 1
                    
                    elif i % 3 == 1:
                        # Запрос DAU статистики
                        async with session.get(
                            f"{self.base_url}/stats/dau?from_date=2024-01-01&to_date=2024-01-15",
                            headers=headers,
                            timeout=10
                        ) as response:
                            if response.status == 200:
                                with self.lock:
                                    self.results['success'] += 1
                            else:
                                with self.lock:
                                    self.results['errors'] += 1
                    
                    else:
                        # Запрос топа событий
                        async with session.get(
                            f"{self.base_url}/stats/top-events?from_date=2024-01-01&to_date=2024-01-15&limit=10",
                            headers=headers,
                            timeout=10
                        ) as response:
                            if response.status == 200:
                                with self.lock:
                                    self.results['success'] += 1
                            else:
                                with self.lock:
                                    self.results['errors'] += 1
                
                except asyncio.TimeoutError:
                    with self.lock:
                        self.results['errors'] += 1
                    print(f"Worker {worker_id}: Request {i} timeout")
                except Exception as e:
                    with self.lock:
                        self.results['errors'] += 1
                    print(f"Worker {worker_id}: Request {i} error: {e}")
                
                duration = time.time() - start_time
                with self.lock:
                    self.results['total_time'] += duration
                
                # Небольшая пауза между запросами
                await asyncio.sleep(random.uniform(0.001, 0.002))
                
    
    async def run_load_test(self):
        """Запуск нагрузочного теста"""
        print(f"🚀 Starting load test with {self.num_workers} workers, {self.requests_per_worker} requests each")
        print(f"📊 Total requests: {self.num_workers * self.requests_per_worker}")
        
        start_time = time.time()
        
        # Создаем и запускаем воркеры
        tasks = []
        for i in range(self.num_workers):
            task = asyncio.create_task(self.send_requests(i))
            tasks.append(task)
        
        # Ждем завершения всех воркеров
        await asyncio.gather(*tasks)
        
        total_time = time.time() - start_time
        total_requests = self.num_workers * self.requests_per_worker
        
        # Вывод результатов
        print("\n" + "="*50)
        print("📊 LOAD TEST RESULTS")
        print("="*50)
        print(f"Total requests: {total_requests}")
        print(f"Total time: {total_time:.2f}s")
        print(f"Requests per second: {total_requests / total_time:.2f}")
        print(f"Success: {self.results['success']}")
        print(f"Errors: {self.results['errors']}")
        print(f"Rate limited: {self.results['rate_limit']}")
        print(f"Success rate: {(self.results['success'] / total_requests) * 100:.1f}%")
        print(f"Average response time: {(self.results['total_time'] / total_requests) * 1000:.2f}ms")

async def main():
    # Настройки теста
    BASE_URL = "http://localhost:8000"
    API_KEY = "M6eyywP3G4ahqRsOlj0oF3bv2t39hh9OABzpa0GXiyM"  # Замените на ваш ключ
    
    # Варианты нагрузки (можно менять)
    test_scenarios = [
        # (workers, requests_per_worker, description)
        (5, 100, "5 x 100"),
        (10, 100, "10 x 500"),
        (20, 100, "20 x 100"),
        (50, 100, "50 x 200"),
    ]
    
    for workers, requests, description in test_scenarios:
        print(f"\n{description}: {workers} workers × {requests} requests")
        tester = LoadTester(BASE_URL, API_KEY, workers, requests)
        await tester.run_load_test()
        
        # Пауза между тестами
        print("⏳ Cooling down...")
        await asyncio.sleep(5)

if __name__ == "__main__":
    asyncio.run(main())