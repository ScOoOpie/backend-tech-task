from pydantic import BaseModel, validator, Field, model_validator
from typing import List, Optional, Dict, Any
from datetime import datetime, timezone, timedelta
import uuid
import re

class Event(BaseModel):
    event_id: uuid.UUID = Field(default_factory=uuid.uuid4, description="Auto-generated event ID")
    occurred_at: datetime = Field(..., description="When the event occurred")
    user_id: str = Field(..., min_length=1, max_length=255, description="User identifier")
    event_type: str = Field(..., min_length=1, max_length=100, description="Type of event")
    properties: Optional[Dict[Any, Any]] = Field(default=None, description="Additional event properties")
    
    @validator('occurred_at')
    def validate_occurred_at(cls, v):
        """Проверяет что событие не из будущего"""
        if v.tzinfo is not None:
            v_utc = v.astimezone(timezone.utc).replace(tzinfo=None)
        else:
            v_utc = v
            
        current_utc = datetime.utcnow()
        
        if v_utc > current_utc:
            raise ValueError('Event cannot be in the future')
        
        # Проверяем что событие не старше 1 года
        one_year_ago = current_utc - timedelta(days=365)
        if v_utc < one_year_ago:
            raise ValueError('Event cannot be older than 1 year')
            
        return v
    
    @validator('event_type')
    def validate_event_type(cls, v):
        """Проверяет формат типа события"""
        if not re.match(r'^[a-zA-Z0-9_.-]+$', v):
            raise ValueError('Event type can only contain letters, numbers, dots, underscores and hyphens')
        return v

class EventBatch(BaseModel):
    events: List[Event] = Field(..., max_items=1000, description="Batch of events (max 1000)")
    
    @validator('events')
    def validate_batch_size(cls, v):
        """Проверяет размер пачки"""
        if len(v) > 1000:
            raise ValueError('Batch cannot contain more than 1000 events')
        return v

class AnalyticsResponse(BaseModel):
    data: Dict[str, Any] = Field(..., description="Analytics data")
    metadata: Optional[Dict[str, Any]] = Field(default=None, description="Response metadata")

class RetentionWindow(BaseModel):
    window: int = Field(..., description="Retention window number")
    retained_users: int = Field(..., description="Number of retained users")
    retention_rate: float = Field(..., ge=0, le=1, description="Retention rate (0-1)")

class GenerateAPIKeyRequest(BaseModel):
    user_id: str = Field(..., min_length=1, max_length=255, description="User identifier")
    name: str = Field(..., min_length=1, max_length=255, description="Key name")
    permissions: List[str] = Field(..., min_items=1, description="List of permissions")
    expires_days: Optional[int] = Field(30, ge=1, le=365, description="Expiration in days (1-365)")
    
    @validator('permissions', each_item=True)
    def validate_permissions(cls, v):
        """Проверяет что права доступа валидны"""
        valid_permissions = ['read', 'write', 'admin']
        if v not in valid_permissions:
            raise ValueError(f'Invalid permission: {v}. Must be one of: {valid_permissions}')
        return v

class GenerateAPIKeyResponse(BaseModel):
    api_key: str = Field(..., description="Generated API key - save this securely!")
    user_id: str = Field(..., description="User identifier")
    name: str = Field(..., description="Key name")
    permissions: List[str] = Field(..., description="List of permissions")
    expires_days: Optional[int] = Field(None, description="Expiration in days")
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

class APIKeyInfo(BaseModel):
    id: int = Field(..., description="Key ID")
    user_id: str = Field(..., description="User identifier")
    name: str = Field(..., description="Key name")
    permissions: List[str] = Field(..., description="List of permissions")
    is_active: bool = Field(..., description="Whether the key is active")
    created_at: datetime = Field(..., description="Creation timestamp")
    expires_at: Optional[datetime] = Field(None, description="Expiration timestamp")
    last_used: Optional[datetime] = Field(None, description="Last usage timestamp")
    key_preview: Optional[str] = Field(None, description="Key preview for identification")

    class Config:
        from_attributes = True

class APIKeyListResponse(BaseModel):
    total_keys: int = Field(..., description="Total number of keys")
    keys: List[APIKeyInfo] = Field(..., description="List of API keys")

class NATSEventMessage(BaseModel):
    event_id: uuid.UUID = Field(..., description="Event ID")
    occurred_at: datetime = Field(..., description="When event occurred")
    user_id: str = Field(..., description="User ID")
    event_type: str = Field(..., description="Event type")
    properties: Optional[Dict[Any, Any]] = Field(None, description="Event properties")
    published_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc), description="When message was published")
    message_id: Optional[str] = Field(None, description="NATS message ID")

class EventProcessingResult(BaseModel):
    success: bool = Field(..., description="Whether processing was successful")
    event_id: uuid.UUID = Field(..., description="Processed event ID")
    message: str = Field(..., description="Processing message")
    processed_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc), description="When processing occurred")

# 🔧 Дополнительные полезные схемы

class HealthCheckResponse(BaseModel):
    status: str = Field(..., description="Service status")
    nats_enabled: bool = Field(..., description="Whether NATS is enabled")
    services: List[str] = Field(..., description="Available services")
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

class ErrorResponse(BaseModel):
    error: str = Field(..., description="Error message")
    details: Optional[Dict[str, Any]] = Field(None, description="Error details")
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

class PaginationParams(BaseModel):
    page: int = Field(1, ge=1, description="Page number")
    size: int = Field(50, ge=1, le=100, description="Page size")

class DateRangeParams(BaseModel):
    from_date: str = Field(..., description="Start date (YYYY-MM-DD)")
    to_date: str = Field(..., description="End date (YYYY-MM-DD)")
    
    @validator('from_date', 'to_date')
    def validate_date_format(cls, v):
        """Проверяет формат даты"""
        try:
            datetime.strptime(v, '%Y-%m-%d')
        except ValueError:
            raise ValueError('Date must be in YYYY-MM-DD format')
        return v
    
    @model_validator(mode='after')
    def validate_date_range(self):
        """Проверяет что from_date <= to_date"""
        if self.from_date and self.to_date and self.from_date > self.to_date:
            raise ValueError('from_date cannot be after to_date')
        return self

class UserCreateRequest(BaseModel):
    user_id: str = Field(..., min_length=1, max_length=255, description="User ID")
    name: str = Field(..., min_length=1, max_length=255, description="User name")
    email: Optional[str] = Field(None, description="User email")

class UserResponse(BaseModel):
    user_id: str = Field(..., description="User ID")
    name: str = Field(..., description="User name")
    email: Optional[str] = Field(None, description="User email")
    is_active: bool = Field(..., description="Whether user is active")
    created_at: datetime = Field(..., description="Creation timestamp")
    updated_at: datetime = Field(..., description="Last update timestamp")
    events_count: Optional[int] = Field(0, description="Number of user events")

class UsersListResponse(BaseModel):
    total_users: int = Field(..., description="Total number of users")
    users: List[UserResponse] = Field(..., description="List of users")
    