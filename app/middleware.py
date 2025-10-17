import time
from typing import Dict

class RateLimiter:
    def __init__(self, capacity: int = 1000, refill_rate: float = 100):
        self.capacity = capacity
        self.refill_rate = refill_rate  # токенів в секунду
        self.tokens = capacity
        self.last_refill = time.time()
    
    def consume(self, tokens: int = 1) -> bool:
        now = time.time()
        time_passed = now - self.last_refill
        self.tokens = min(self.capacity, self.tokens + time_passed * self.refill_rate)
        self.last_refill = now
        
        if self.tokens >= tokens:
            self.tokens -= tokens
            return True
        return False