# test_redis.py
import asyncio
import aiohttp
import uuid
from datetime import datetime, timezone, timedelta

async def test_redis():
    api_key = "M6eyywP3G4ahqRsOlj0oF3bv2t39hh9OABzpa0GXiyM"  # Замени на реальный ключ
    base_url = "http://localhost:8000"
    
    headers = {"X-API-Key": api_key}
    
    async with aiohttp.ClientSession() as session:
        print("🧪 Starting Redis integration test...")
        
        # Тест 1: Health check
        async with session.get(f"{base_url}/health", headers=headers) as resp:
            health = await resp.json()
            print("✅ Health Check:", health.get('redis_healthy', False))
        
        # Тест 2: Cache status
        async with session.get(f"{base_url}/cache/status", headers=headers) as resp:
            cache_status = await resp.json()
            print("✅ Cache Stats - Keys:", cache_status.get('cache_stats', {}).get('total_cache_keys', 0))
        
        # Тест 3: Создаем КОРРЕКТНОЕ событие
        user_id = f"test_user_{uuid.uuid4().hex[:8]}"
        event_data = {
            "events": [{
                "event_id": str(uuid.uuid4()),
                "occurred_at": datetime.now(timezone.utc).isoformat(),  
                "user_id": user_id,
                "event_type": "page_view",
                "properties": {"page": "/test", "test": True}
            }]
        }
        
        print(f"📝 Sending test event for user: {user_id}")
        async with session.post(f"{base_url}/events", json=event_data, headers=headers) as resp:
            if resp.status == 201:
                result = await resp.json()
                print("✅ Event Ingested:", result.get('ingested', 0), "events")
            else:
                error = await resp.json()
                print("❌ Event Error:", error)
                return
        
        # Тест 4: Проверяем кэш пользователя
        print("🔍 Checking user cache...")
        async with session.get(f"{base_url}/users/{user_id}", headers=headers) as resp:
            if resp.status == 200:
                user_data = await resp.json()
                cache_info = user_data.get('cache_info', {})
                print("✅ User Data - From Cache:", cache_info.get('from_cache', False))
                print("📊 User Stats - Events:", user_data.get('stats', {}).get('total_events', 0))
            else:
                print("❌ User not found yet, waiting...")
                await asyncio.sleep(1)  # Даем время на обработку
        
        # Тест 5: Проверяем аналитику (должна использовать кэш)
        print("📈 Testing analytics cache...")
        
        # Первый запрос (может быть cache miss)
        start_time = asyncio.get_event_loop().time()
        async with session.get(f"{base_url}/cohorts/active?limit=3", headers=headers) as resp:
            cohorts1 = await resp.json()
        time1 = asyncio.get_event_loop().time() - start_time
        
        # Второй запрос (должен быть cache hit)
        start_time = asyncio.get_event_loop().time()
        async with session.get(f"{base_url}/cohorts/active?limit=3", headers=headers) as resp:
            cohorts2 = await resp.json()
        time2 = asyncio.get_event_loop().time() - start_time
        
        print(f"⏱️  Request times - First: {time1:.3f}s, Second: {time2:.3f}s")
        print(f"🚀 Cache speedup: {time1/time2:.1f}x faster" if time2 < time1 else "⚠️  No cache benefit")
        
        # Тест 6: Очистка тестовых данных
        print("🧹 Cleaning up test data...")
        async with session.post(f"{base_url}/cache/users/{user_id}/clear", headers=headers) as resp:
            cleanup = await resp.json()
            print("✅ Cache cleanup:", cleanup.get('message', 'Done'))

if __name__ == "__main__":
    asyncio.run(test_redis())