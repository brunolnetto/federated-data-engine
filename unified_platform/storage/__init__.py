"""
Unified Platform Storage Backends
=================================

This module provides storage backend implementations for the unified data platform.
Supports multiple storage systems with consistent interfaces.
"""

from .abstract_backend import StorageBackend, StorageType
from .factory import (
    StorageBackendFactory, 
    create_storage_backend, 
    list_storage_backends,
    compare_storage_backends
)

# Storage backend implementations
from .postgresql import PostgreSQLStorageBackend
from .iceberg import IcebergStorageBackend
from .clickhouse import ClickHouseStorageBackend
from .duckdb import DuckDBStorageBackend
from .bigquery import BigQueryStorageBackend
from .snowflake import SnowflakeStorageBackend
from .delta_lake import DeltaLakeStorageBackend
from .parquet import ParquetStorageBackend

__all__ = [
    # Abstract base classes
    'StorageBackend',
    'StorageType',
    
    # Factory functions
    'StorageBackendFactory',
    'create_storage_backend',
    'list_storage_backends', 
    'compare_storage_backends',
    
    # Backend implementations
    'PostgreSQLStorageBackend',
    'IcebergStorageBackend',
    'ClickHouseStorageBackend',
    'DuckDBStorageBackend',
    'BigQueryStorageBackend',
    'SnowflakeStorageBackend',
    'DeltaLakeStorageBackend',
    'ParquetStorageBackend',
]

# Convenience functions for quick access
def get_backend(storage_type: str, **kwargs):
    """Get a storage backend instance"""
    return create_storage_backend(storage_type, **kwargs)

def list_backends():
    """List all available storage backends"""
    return list_storage_backends()

def compare_backends(*backend_types):
    """Compare multiple storage backends"""
    return compare_storage_backends(list(backend_types))