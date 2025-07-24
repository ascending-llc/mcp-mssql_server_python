import asyncio
import time
from dataclasses import dataclass
from typing import Any, Optional, Dict, List, Set
from collections import OrderedDict

from mssql_mcp_server.config.settings import settings
from mssql_mcp_server.utils.logger import Logger

logger = Logger.get_logger(__name__)


@dataclass
class CacheEntry:
    """Cache entry with metadata."""
    
    data: Any
    timestamp: float
    ttl: float
    access_count: int = 0
    last_access: float = 0
    
    @property
    def is_expired(self) -> bool:
        """Check if the cache entry has expired."""
        return time.time() - self.timestamp > self.ttl
    
    @property
    def age(self) -> float:
        """Get the age of the cache entry in seconds."""
        return time.time() - self.timestamp
    
    def mark_accessed(self) -> None:
        """Mark the entry as accessed."""
        self.access_count += 1
        self.last_access = time.time()


class SmartCache:
    """Smart cache system with LRU eviction and TTL support."""
    
    def __init__(self, max_entries: Optional[int] = None):
        self._cache: OrderedDict[str, CacheEntry] = OrderedDict()
        self._access_stats: Dict[str, List[float]] = {}
        self._lock = asyncio.Lock()
        self._max_entries = max_entries or settings.cache.max_entries
        self._enabled = settings.cache.enabled
        
    async def get(self, key: str) -> Optional[Any]:
        """Get value from cache."""
        if not self._enabled:
            return None
            
        async with self._lock:
            if key not in self._cache:
                return None
            
            entry = self._cache[key]
            
            # Check if expired
            if entry.is_expired:
                await self._delete_entry(key)
                logger.debug(f"Cache entry '{key}' expired and removed")
                return None
            
            # Mark as accessed and move to end (most recently used)
            entry.mark_accessed()
            self._cache.move_to_end(key)
            
            # Record access for statistics
            self._record_access(key)
            
            logger.debug(f"Cache hit: '{key}' (age: {entry.age:.1f}s, access_count: {entry.access_count})")
            return entry.data
    
    async def set(self, key: str, value: Any, ttl: Optional[float] = None) -> None:
        """Set value in cache."""
        if not self._enabled:
            return
            
        if ttl is None:
            ttl = settings.cache.default_ttl
        
        async with self._lock:
            # Remove existing entry if it exists
            if key in self._cache:
                await self._delete_entry(key)
            
            # Create new entry
            entry = CacheEntry(
                data=value,
                timestamp=time.time(),
                ttl=ttl,
                last_access=time.time()
            )
            
            self._cache[key] = entry
            
            # Enforce max entries limit
            if len(self._cache) > self._max_entries:
                await self._evict_lru()
            
            logger.debug(f"Cache set: '{key}' (ttl: {ttl}s)")
    
    async def delete(self, key: str) -> bool:
        """Delete entry from cache."""
        async with self._lock:
            return await self._delete_entry(key)
    
    async def clear(self) -> None:
        """Clear all cache entries."""
        async with self._lock:
            self._cache.clear()
            self._access_stats.clear()
            logger.info("Cache cleared")
    
    async def clear_pattern(self, pattern: str) -> int:
        """Clear entries matching pattern."""
        async with self._lock:
            keys_to_delete = [key for key in self._cache.keys() if pattern in key]
            count = 0
            for key in keys_to_delete:
                if await self._delete_entry(key):
                    count += 1
            
            logger.info(f"Cleared {count} cache entries matching pattern: '{pattern}'")
            return count
    
    async def cleanup_expired(self) -> int:
        """Remove all expired entries."""
        async with self._lock:
            expired_keys = [
                key for key, entry in self._cache.items() 
                if entry.is_expired
            ]
            
            count = 0
            for key in expired_keys:
                if await self._delete_entry(key):
                    count += 1
            
            if count > 0:
                logger.info(f"Cleaned up {count} expired cache entries")
            
            return count
    
    async def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        async with self._lock:
            total_entries = len(self._cache)
            total_memory = sum(len(str(entry.data)) for entry in self._cache.values())
            
            if total_entries == 0:
                return {
                    "total_entries": 0,
                    "memory_usage_bytes": 0,
                    "hit_rate": 0.0,
                    "average_age": 0.0,
                    "most_accessed": []
                }
            
            # Calculate hit rate
            total_accesses = sum(len(accesses) for accesses in self._access_stats.values())
            cache_hits = sum(entry.access_count for entry in self._cache.values())
            hit_rate = cache_hits / total_accesses if total_accesses > 0 else 0.0
            
            # Calculate average age
            current_time = time.time()
            average_age = sum(current_time - entry.timestamp for entry in self._cache.values()) / total_entries
            
            # Most accessed entries
            most_accessed = sorted(
                [(key, entry.access_count) for key, entry in self._cache.items()],
                key=lambda x: x[1],
                reverse=True
            )[:5]
            
            return {
                "total_entries": total_entries,
                "memory_usage_bytes": total_memory,
                "hit_rate": hit_rate,
                "average_age": average_age,
                "most_accessed": most_accessed,
                "max_entries": self._max_entries,
                "enabled": self._enabled
            }
    
    async def _delete_entry(self, key: str) -> bool:
        """Internal method to delete entry."""
        if key in self._cache:
            del self._cache[key]
            if key in self._access_stats:
                del self._access_stats[key]
            logger.debug(f"Cache entry '{key}' deleted")
            return True
        return False
    
    async def _evict_lru(self) -> None:
        """Evict least recently used entry."""
        if self._cache:
            lru_key = next(iter(self._cache))  # First item is LRU
            await self._delete_entry(lru_key)
            logger.debug(f"Evicted LRU cache entry: '{lru_key}'")
    
    def _record_access(self, key: str) -> None:
        """Record access time for statistics."""
        if key not in self._access_stats:
            self._access_stats[key] = []
        
        self._access_stats[key].append(time.time())
        
        # Keep only recent accesses (last 100)
        if len(self._access_stats[key]) > 100:
            self._access_stats[key] = self._access_stats[key][-100:]


class CacheManager:
    """Global cache manager with different cache types."""
    
    def __init__(self):
        self.table_names_cache = SmartCache()
        self.table_data_cache = SmartCache()
        self.table_schema_cache = SmartCache()
        self.query_cache = SmartCache()
        
        # Background cleanup task
        self._cleanup_task: Optional[asyncio.Task] = None
        self._cleanup_interval = 300  # 5 minutes
    
    async def start_cleanup_task(self) -> None:
        """Start background cleanup task."""
        if self._cleanup_task is None or self._cleanup_task.done():
            self._cleanup_task = asyncio.create_task(self._cleanup_loop())
            logger.info("Cache cleanup task started")
    
    async def stop_cleanup_task(self) -> None:
        """Stop background cleanup task."""
        if self._cleanup_task and not self._cleanup_task.done():
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass
            logger.info("Cache cleanup task stopped")
    
    async def _cleanup_loop(self) -> None:
        """Background cleanup loop."""
        while True:
            try:
                await asyncio.sleep(self._cleanup_interval)
                
                # Cleanup expired entries from all caches
                total_cleaned = 0
                for cache_name, cache in [
                    ("table_names", self.table_names_cache),
                    ("table_data", self.table_data_cache),
                    ("table_schema", self.table_schema_cache),
                    ("query", self.query_cache)
                ]:
                    cleaned = await cache.cleanup_expired()
                    total_cleaned += cleaned
                
                if total_cleaned > 0:
                    logger.info(f"Background cleanup removed {total_cleaned} expired cache entries")
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in cache cleanup task: {e}")
    
    async def get_table_names(self, key: str = "table_names") -> Optional[List[str]]:
        """Get table names from cache."""
        return await self.table_names_cache.get(key)
    
    async def set_table_names(self, value: List[str], key: str = "table_names") -> None:
        """Set table names in cache."""
        await self.table_names_cache.set(key, value, settings.cache.table_names_ttl)
    
    async def get_table_data(self, table_name: str) -> Optional[str]:
        """Get table data from cache."""
        return await self.table_data_cache.get(f"table_data_{table_name}")
    
    async def set_table_data(self, table_name: str, value: str) -> None:
        """Set table data in cache."""
        await self.table_data_cache.set(f"table_data_{table_name}", value, settings.cache.table_data_ttl)
    
    async def get_table_schema(self, table_name: str) -> Optional[str]:
        """Get table schema from cache."""
        return await self.table_schema_cache.get(f"table_schema_{table_name}")
    
    async def set_table_schema(self, table_name: str, value: str) -> None:
        """Set table schema in cache."""
        await self.table_schema_cache.set(f"table_schema_{table_name}", value, settings.cache.table_schema_ttl)
    
    async def invalidate_table_related(self, table_name: Optional[str] = None) -> None:
        """Invalidate table-related cache entries."""
        if table_name:
            # Invalidate specific table
            await self.table_data_cache.delete(f"table_data_{table_name}")
            await self.table_schema_cache.delete(f"table_schema_{table_name}")
            logger.info(f"Invalidated cache for table: {table_name}")
        else:
            # Invalidate all table-related caches
            await self.table_names_cache.clear()
            await self.table_data_cache.clear()
            await self.table_schema_cache.clear()
            logger.info("Invalidated all table-related caches")
    
    async def get_global_stats(self) -> Dict[str, Any]:
        """Get statistics for all caches."""
        return {
            "table_names_cache": await self.table_names_cache.get_stats(),
            "table_data_cache": await self.table_data_cache.get_stats(),
            "table_schema_cache": await self.table_schema_cache.get_stats(),
            "query_cache": await self.query_cache.get_stats()
        }


# Global cache manager instance
cache_manager = CacheManager() 