import json
import hashlib
from datetime import datetime, timedelta
from typing import Dict, Optional, Any, List
from pathlib import Path
import logging
from dataclasses import dataclass, asdict
from models.document import Document

logger = logging.getLogger(__name__)

@dataclass
class CacheEntry:
    """Represents a cached processing result"""
    document_etag: str
    document_type: str
    machine_names: List[str]
    processing_result: Dict[str, Any]
    processor_used: str
    created_at: datetime
    file_size: int
    filename: str
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization"""
        result = asdict(self)
        result['created_at'] = self.created_at.isoformat()
        return result
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'CacheEntry':
        """Create from dictionary"""
        data['created_at'] = datetime.fromisoformat(data['created_at'])
        return cls(**data)

class ResultCache:
    """
    Manages caching of document processing results based on ETag versioning.
    Cache key is generated from document ETag + processing configuration.
    """
    
    def __init__(self, cache_dir: str = "cache", max_age_hours: int = 24 * 7):  # 1 week default
        self.cache_dir = Path(cache_dir)
        self.max_age_hours = max_age_hours
        self.cache_dir.mkdir(exist_ok=True)
        
        # In-memory cache for faster lookups
        self._memory_cache: Dict[str, CacheEntry] = {}
        self._load_cache_index()
    
    def _generate_cache_key(self, document: Document, metadata: Dict[str, Any]) -> str:
        """Generate cache key from document ETag and processing configuration"""
        # Create a hash from ETag + document_type + machine_names + processor config
        cache_input = {
            'etag': document.etag,
            'document_type': metadata.get('document_type', ''),
            'machine_names': sorted(metadata.get('machine_names', [])),
            'basic': metadata.get('basic', False)
        }
        
        cache_string = json.dumps(cache_input, sort_keys=True)
        return hashlib.md5(cache_string.encode()).hexdigest()
    
    def _get_cache_file_path(self, cache_key: str) -> Path:
        """Get file path for cache entry"""
        return self.cache_dir / f"{cache_key}.json"
    
    def _load_cache_index(self) -> None:
        """Load cache index into memory for faster lookups"""
        try:
            for cache_file in self.cache_dir.glob("*.json"):
                try:
                    with open(cache_file, 'r') as f:
                        data = json.load(f)
                        entry = CacheEntry.from_dict(data)
                        
                        # Skip expired entries
                        if self._is_expired(entry):
                            cache_file.unlink()  # Delete expired file
                            continue
                        
                        cache_key = cache_file.stem
                        self._memory_cache[cache_key] = entry
                        
                except Exception as e:
                    logger.warning(f"Error loading cache file {cache_file}: {e}")
                    # Delete corrupted cache files
                    try:
                        cache_file.unlink()
                    except:
                        pass
        except Exception as e:
            logger.error(f"Error loading cache index: {e}")
    
    def _is_expired(self, entry: CacheEntry) -> bool:
        """Check if cache entry is expired"""
        age = datetime.now() - entry.created_at
        return age > timedelta(hours=self.max_age_hours)
    
    def get(self, document: Document, metadata: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Get cached processing result for document.
        Returns None if not cached or cache is invalid.
        """
        cache_key = self._generate_cache_key(document, metadata)
        
        # Check memory cache first
        if cache_key in self._memory_cache:
            entry = self._memory_cache[cache_key]
            
            # Check if expired
            if self._is_expired(entry):
                logger.debug(f"Cache entry expired for {document.filename}")
                self._invalidate_entry(cache_key)
                return None
            
            # Validate ETag hasn't changed
            if entry.document_etag != document.etag:
                logger.debug(f"ETag mismatch for {document.filename}, cache invalid")
                self._invalidate_entry(cache_key)
                return None
            
            logger.info(f"Cache hit for {document.filename}")
            return entry.processing_result
        
        logger.debug(f"Cache miss for {document.filename}")
        return None
    
    def set(self, document: Document, metadata: Dict[str, Any], 
            processing_result: Dict[str, Any], processor_used: str) -> None:
        """
        Cache processing result for document.
        """
        cache_key = self._generate_cache_key(document, metadata)
        
        entry = CacheEntry(
            document_etag=document.etag,
            document_type=metadata.get('document_type', ''),
            machine_names=metadata.get('machine_names', []),
            processing_result=processing_result,
            processor_used=processor_used,
            created_at=datetime.now(),
            file_size=document.file_size,
            filename=document.filename
        )
        
        # Save to memory cache
        self._memory_cache[cache_key] = entry
        
        # Save to disk
        try:
            cache_file = self._get_cache_file_path(cache_key)
            with open(cache_file, 'w') as f:
                json.dump(entry.to_dict(), f, indent=2)
            
            logger.info(f"Cached processing result for {document.filename}")
        except Exception as e:
            logger.error(f"Error saving cache entry for {document.filename}: {e}")
    
    def _invalidate_entry(self, cache_key: str) -> None:
        """Remove cache entry from memory and disk"""
        # Remove from memory
        self._memory_cache.pop(cache_key, None)
        
        # Remove from disk
        cache_file = self._get_cache_file_path(cache_key)
        try:
            if cache_file.exists():
                cache_file.unlink()
        except Exception as e:
            logger.warning(f"Error removing cache file {cache_file}: {e}")
    
    def invalidate_document(self, document: Document) -> None:
        """
        Invalidate all cached entries for a specific document.
        Useful when document is updated or reprocessed.
        """
        to_remove = []
        for cache_key, entry in self._memory_cache.items():
            if entry.document_etag == document.etag:
                to_remove.append(cache_key)
        
        for cache_key in to_remove:
            self._invalidate_entry(cache_key)
        
        if to_remove:
            logger.info(f"Invalidated {len(to_remove)} cache entries for {document.filename}")
    
    def cleanup_expired(self) -> int:
        """
        Remove expired cache entries.
        Returns number of entries removed.
        """
        expired_keys = []
        for cache_key, entry in self._memory_cache.items():
            if self._is_expired(entry):
                expired_keys.append(cache_key)
        
        for cache_key in expired_keys:
            self._invalidate_entry(cache_key)
        
        if expired_keys:
            logger.info(f"Cleaned up {len(expired_keys)} expired cache entries")
        
        return len(expired_keys)
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """Get cache statistics"""
        total_entries = len(self._memory_cache)
        cache_size_mb = 0
        
        try:
            for cache_file in self.cache_dir.glob("*.json"):
                cache_size_mb += cache_file.stat().st_size / (1024 * 1024)
        except Exception as e:
            logger.warning(f"Error calculating cache size: {e}")
        
        # Group by document type
        type_distribution = {}
        for entry in self._memory_cache.values():
            doc_type = entry.document_type
            type_distribution[doc_type] = type_distribution.get(doc_type, 0) + 1
        
        return {
            'total_entries': total_entries,
            'cache_size_mb': round(cache_size_mb, 2),
            'type_distribution': type_distribution,
            'max_age_hours': self.max_age_hours
        }
    
    def clear_all(self) -> None:
        """Clear all cache entries"""
        # Clear memory cache
        self._memory_cache.clear()
        
        # Clear disk cache
        try:
            for cache_file in self.cache_dir.glob("*.json"):
                cache_file.unlink()
            logger.info("All cache entries cleared")
        except Exception as e:
            logger.error(f"Error clearing cache: {e}")