import os

def getenv(key: str, default: str) -> str:
    val = os.getenv(key)
    return val if val is not None and val != "" else default

REDIS_URL = getenv("REDIS_URL", "redis://localhost:6379/0")
HMAC_SECRET = getenv("HMAC_SECRET", "change-me-demo-secret")

SESSION_MANAGER_URL = getenv("SESSION_MANAGER_URL", "http://localhost:8001")
SESSION_MANAGER_URL_2 = getenv("SESSION_MANAGER_URL_2", "")

RESOURCE_MANAGER_URL = getenv("RESOURCE_MANAGER_URL", "http://localhost:8002")
BILLING_SERVICE_URL = getenv("BILLING_SERVICE_URL", "http://localhost:8003")
ANALYTICS_SERVICE_URL = getenv("ANALYTICS_SERVICE_URL", "http://localhost:8004")

SERVICE_NAME = getenv("SERVICE_NAME", "service")
SERVICE_REGION = getenv("SERVICE_REGION", "region")
