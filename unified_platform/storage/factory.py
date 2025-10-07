"""
Storage Backend Factory with Dimensional Modeling Intelligence
==============================================================

Factory pattern for creating storage backend instances with payload-agnostic
dimensional modeling capabilities and intelligent workload-based selection.
"""

from typing import Dict, Any, Optional, List
from dataclasses import dataclass
from .abstract_backend import StorageBackend, StorageType

# Import all backend implementations
from .postgresql import PostgreSQLStorageBackend
from .iceberg import IcebergStorageBackend
from .clickhouse import ClickHouseStorageBackend
from .duckdb import DuckDBStorageBackend
from .bigquery import BigQueryStorageBackend
from .snowflake import SnowflakeStorageBackend
from .delta_lake import DeltaLakeStorageBackend
from .parquet import ParquetStorageBackend


@dataclass
class BackendRecommendation:
    """Recommendation for optimal backend selection"""
    storage_type: StorageType
    processing_engine: str
    reason: str
    confidence: float
    backend_specific_features: List[str]
    performance_characteristics: Dict[str, str]


class DimensionalStorageBackendFactory:
    """Factory for creating storage backend instances with dimensional modeling intelligence"""
    
    _backends = {
        StorageType.POSTGRESQL: PostgreSQLStorageBackend,
        StorageType.ICEBERG: IcebergStorageBackend,
        StorageType.CLICKHOUSE: ClickHouseStorageBackend,
        StorageType.DUCKDB: DuckDBStorageBackend,
        StorageType.BIGQUERY: BigQueryStorageBackend,
        StorageType.SNOWFLAKE: SnowflakeStorageBackend,
        StorageType.DELTA_LAKE: DeltaLakeStorageBackend,
        StorageType.PARQUET: ParquetStorageBackend,
    }
    
    # Backend capabilities for dimensional modeling
    _backend_capabilities = {
        StorageType.POSTGRESQL: {
            'acid_compliance': True,
            'referential_integrity': True,
            'complex_constraints': True,
            'scd2_support': 'native',
            'optimal_for': ['compliance', 'oltp', 'small_dimensions'],
            'dimensional_patterns': ['scd1', 'scd2', 'scd3']
        },
        StorageType.CLICKHOUSE: {
            'columnar_storage': True,
            'analytical_performance': True,
            'materialized_views': True,
            'scd2_support': 'replacing_merge_tree',
            'optimal_for': ['analytics', 'aggregation', 'large_facts'],
            'dimensional_patterns': ['scd1', 'scd2_optimized']
        },
        StorageType.ICEBERG: {
            'time_travel': True,
            'schema_evolution': True,
            'acid_transactions': True,
            'scd2_support': 'native_time_travel',
            'optimal_for': ['data_lake', 'large_dimensions', 'schema_evolution'],
            'dimensional_patterns': ['scd1', 'scd2', 'time_travel_scd']
        },
        StorageType.DUCKDB: {
            'in_memory_analytics': True,
            'vectorized_execution': True,
            'parquet_integration': True,
            'scd2_support': 'application_logic',
            'optimal_for': ['medium_analytics', 'development', 'prototyping'],
            'dimensional_patterns': ['scd1', 'scd2']
        },
        StorageType.DELTA_LAKE: {
            'acid_streaming': True,
            'time_travel': True,
            'schema_enforcement': True,
            'scd2_support': 'merge_operations',
            'optimal_for': ['streaming', 'transaction_facts', 'data_lake'],
            'dimensional_patterns': ['scd1', 'scd2', 'streaming_scd']
        },
        StorageType.BIGQUERY: {
            'serverless_analytics': True,
            'ml_integration': True,
            'clustering': True,
            'scd2_support': 'merge_statements',
            'optimal_for': ['cloud_analytics', 'large_scale', 'ml_workloads'],
            'dimensional_patterns': ['scd1', 'scd2', 'clustered_dimensions']
        },
        StorageType.SNOWFLAKE: {
            'cloud_native': True,
            'automatic_scaling': True,
            'time_travel': True,
            'scd2_support': 'merge_statements',
            'optimal_for': ['cloud_dwh', 'variable_workloads', 'data_sharing'],
            'dimensional_patterns': ['scd1', 'scd2', 'zero_copy_cloning']
        },
        StorageType.PARQUET: {
            'columnar_format': True,
            'compression': True,
            'cross_platform': True,
            'scd2_support': 'file_versioning',
            'optimal_for': ['data_exchange', 'archival', 'analytics'],
            'dimensional_patterns': ['scd1', 'file_based_scd']
        }
    }
    
    @classmethod
    def recommend_backend_for_entity(cls, entity_metadata: Dict[str, Any]) -> List[BackendRecommendation]:
        """Recommend optimal backends based on entity characteristics (payload-agnostic)"""
        
        entity_type = entity_metadata.get('entity_type')
        workload = entity_metadata.get('workload_characteristics', {})
        scd_type = entity_metadata.get('scd', 'SCD1')
        
        recommendations = []
        
        if entity_type == 'dimension':
            recommendations.extend(cls._analyze_dimension_workload(entity_metadata, workload, scd_type))
        elif entity_type in ['fact', 'transaction_fact']:
            recommendations.extend(cls._analyze_fact_workload(entity_metadata, workload))
        elif entity_type == 'bridge':
            recommendations.extend(cls._analyze_bridge_workload(entity_metadata, workload))
        
        # Sort by confidence score
        return sorted(recommendations, key=lambda x: x.confidence, reverse=True)
    
    @classmethod
    def _analyze_dimension_workload(cls, entity: Dict[str, Any], workload: Dict[str, Any], scd_type: str) -> List[BackendRecommendation]:
        """Analyze dimension workload for backend selection"""
        recommendations = []
        
        volume = workload.get('volume', 'medium')
        update_freq = workload.get('update_frequency', 'daily')
        compliance = workload.get('compliance_requirements', [])
        query_patterns = workload.get('query_patterns', [])
        
        # PostgreSQL: ACID compliance + complex SCD2
        if compliance or (scd_type == 'SCD2' and update_freq in ['hourly', 'real_time']):
            recommendations.append(BackendRecommendation(
                storage_type=StorageType.POSTGRESQL,
                processing_engine='postgresql',
                reason=f'ACID compliance for {scd_type} with {update_freq} updates and compliance: {compliance}',
                confidence=0.95,
                backend_specific_features=['constraints', 'triggers', 'referential_integrity'],
                performance_characteristics={'consistency': 'strong', 'availability': 'high'}
            ))
        
        # ClickHouse: High-volume analytical dimensions
        if volume in ['large', 'very_large'] and 'aggregation' in query_patterns:
            recommendations.append(BackendRecommendation(
                storage_type=StorageType.CLICKHOUSE,
                processing_engine='clickhouse',
                reason=f'High-volume ({volume}) dimension with analytical queries',
                confidence=0.85,
                backend_specific_features=['columnar_storage', 'replacing_merge_tree', 'materialized_views'],
                performance_characteristics={'read_performance': 'very_high', 'compression': 'excellent'}
            ))
        
        # Iceberg: Large dimensions with time travel
        if volume in ['large', 'very_large'] and workload.get('time_travel_required', False):
            recommendations.append(BackendRecommendation(
                storage_type=StorageType.ICEBERG,
                processing_engine='trino',
                reason=f'Large dimension with time travel and schema evolution needs',
                confidence=0.8,
                backend_specific_features=['time_travel', 'schema_evolution', 'hidden_partitioning'],
                performance_characteristics={'flexibility': 'very_high', 'schema_evolution': 'native'}
            ))
        
        # Default fallback
        if not recommendations:
            recommendations.append(BackendRecommendation(
                storage_type=StorageType.POSTGRESQL,
                processing_engine='postgresql',
                reason=f'Default choice for {scd_type} dimension',
                confidence=0.6,
                backend_specific_features=['reliability', 'sql_compliance'],
                performance_characteristics={'consistency': 'strong', 'maturity': 'high'}
            ))
        
        return recommendations
    
    @classmethod
    def _analyze_fact_workload(cls, entity: Dict[str, Any], workload: Dict[str, Any]) -> List[BackendRecommendation]:
        """Analyze fact table workload for backend selection"""
        recommendations = []
        
        volume = workload.get('volume', 'medium')
        insert_freq = workload.get('insert_frequency', 'batch')
        query_patterns = workload.get('query_patterns', [])
        entity_type = entity.get('entity_type')
        
        # ClickHouse: High-volume analytical facts
        if volume in ['high', 'very_high'] and 'aggregation' in query_patterns:
            recommendations.append(BackendRecommendation(
                storage_type=StorageType.CLICKHOUSE,
                processing_engine='clickhouse',
                reason=f'High-volume fact table optimized for aggregation',
                confidence=0.9,
                backend_specific_features=['columnar_aggregation', 'parallel_processing'],
                performance_characteristics={'aggregation_speed': 'very_high', 'compression': 'excellent'}
            ))
        
        # Delta Lake: Streaming transaction facts
        if entity_type == 'transaction_fact' and insert_freq in ['streaming', 'real_time']:
            recommendations.append(BackendRecommendation(
                storage_type=StorageType.DELTA_LAKE,
                processing_engine='spark',
                reason=f'Streaming transaction facts with ACID guarantees',
                confidence=0.85,
                backend_specific_features=['acid_streaming', 'schema_enforcement'],
                performance_characteristics={'streaming_performance': 'high', 'consistency': 'strong'}
            ))
        
        # Iceberg: Large batch facts
        if volume in ['high', 'very_high'] and insert_freq == 'batch':
            recommendations.append(BackendRecommendation(
                storage_type=StorageType.ICEBERG,
                processing_engine='spark',
                reason=f'Large batch fact processing with efficient file pruning',
                confidence=0.8,
                backend_specific_features=['efficient_pruning', 'parallel_scanning'],
                performance_characteristics={'batch_performance': 'very_high', 'file_pruning': 'excellent'}
            ))
        
        return recommendations
    
    @classmethod
    def _analyze_bridge_workload(cls, entity: Dict[str, Any], workload: Dict[str, Any]) -> List[BackendRecommendation]:
        """Analyze bridge table workload for backend selection"""
        return [BackendRecommendation(
            storage_type=StorageType.POSTGRESQL,
            processing_engine='postgresql',
            reason='Bridge table with referential integrity requirements',
            confidence=0.8,
            backend_specific_features=['foreign_keys', 'complex_joins'],
            performance_characteristics={'referential_integrity': 'strong', 'join_performance': 'good'}
        )]
    
    @classmethod
    def create_backend(cls, storage_type: StorageType, 
                      **kwargs) -> StorageBackend:
        """Create a storage backend instance"""
        
        if storage_type not in cls._backends:
            raise ValueError(f"Unsupported storage type: {storage_type}")
        
        backend_class = cls._backends[storage_type]
        
        # Handle specific configuration for each backend
        if storage_type == StorageType.POSTGRESQL:
            return backend_class(
                host=kwargs.get('host', 'localhost'),
                database=kwargs.get('database', 'analytics'),
                schema=kwargs.get('schema', 'public')
            )
        
        elif storage_type == StorageType.ICEBERG:
            return backend_class(
                warehouse_path=kwargs.get('warehouse_path', '/warehouse/'),
                catalog_name=kwargs.get('catalog_name', 'iceberg_catalog')
            )
        
        elif storage_type == StorageType.CLICKHOUSE:
            return backend_class(
                host=kwargs.get('host', 'localhost'),
                database=kwargs.get('database', 'analytics')
            )
        
        elif storage_type == StorageType.DUCKDB:
            return backend_class(
                database_path=kwargs.get('database_path', 'analytics.duckdb')
            )
        
        elif storage_type == StorageType.BIGQUERY:
            return backend_class(
                project_id=kwargs.get('project_id', 'your-project'),
                dataset_id=kwargs.get('dataset_id', 'analytics')
            )
        
        elif storage_type == StorageType.SNOWFLAKE:
            return backend_class(
                warehouse_name=kwargs.get('warehouse_name', 'ANALYTICS_WH'),
                database_name=kwargs.get('database_name', 'ANALYTICS_DB'),
                schema_name=kwargs.get('schema_name', 'PUBLIC')
            )
        
        elif storage_type == StorageType.DELTA_LAKE:
            return backend_class(
                storage_path=kwargs.get('storage_path', '/delta/tables/'),
                catalog_name=kwargs.get('catalog_name', 'main')
            )
        
        elif storage_type == StorageType.PARQUET:
            return backend_class(
                storage_path=kwargs.get('storage_path', '/data/parquet/'),
                compression=kwargs.get('compression', 'snappy')
            )
        
        else:
            # Fallback for any missing implementations
            return backend_class(**kwargs)
    
    @classmethod
    def list_available_backends(cls) -> Dict[str, str]:
        """List all available storage backends with dimensional capabilities"""
        backends_info = {}
        for storage_type, backend_class in cls._backends.items():
            capabilities = cls._backend_capabilities.get(storage_type, {})
            optimal_for = capabilities.get('optimal_for', [])
            backends_info[storage_type.value] = {
                'description': backend_class.__doc__ or backend_class.__name__,
                'optimal_for': optimal_for,
                'dimensional_patterns': capabilities.get('dimensional_patterns', []),
                'scd2_support': capabilities.get('scd2_support', 'unknown')
            }
        return backends_info
    
    @classmethod
    def get_dimensional_capabilities(cls, storage_type: StorageType) -> Dict[str, Any]:
        """Get dimensional modeling capabilities for a specific backend"""
        return cls._backend_capabilities.get(storage_type, {})
    
    @classmethod
    def compare_backends_for_entity(cls, entity_metadata: Dict[str, Any]) -> Dict[str, Any]:
        """Compare all backends for a specific entity type"""
        entity_type = entity_metadata.get('entity_type')
        scd_type = entity_metadata.get('scd', 'SCD1')
        
        comparison = {
            'entity_type': entity_type,
            'scd_type': scd_type,
            'backend_analysis': {}
        }
        
        for storage_type in cls._backends.keys():
            capabilities = cls._backend_capabilities.get(storage_type, {})
            
            # Score backend for this entity type
            score = cls._score_backend_for_entity(storage_type, entity_metadata)
            
            comparison['backend_analysis'][storage_type.value] = {
                'suitability_score': score,
                'scd2_support': capabilities.get('scd2_support', 'unknown'),
                'optimal_for': capabilities.get('optimal_for', []),
                'dimensional_patterns': capabilities.get('dimensional_patterns', [])
            }
        
        return comparison
    
    @classmethod
    def _score_backend_for_entity(cls, storage_type: StorageType, entity_metadata: Dict[str, Any]) -> float:
        """Score backend suitability for entity (0.0 to 1.0)"""
        capabilities = cls._backend_capabilities.get(storage_type, {})
        entity_type = entity_metadata.get('entity_type')
        workload = entity_metadata.get('workload_characteristics', {})
        
        score = 0.0
        
        # Base score for entity type compatibility
        optimal_for = capabilities.get('optimal_for', [])
        if entity_type == 'dimension':
            if 'small_dimensions' in optimal_for or 'compliance' in optimal_for:
                score += 0.3
        elif entity_type in ['fact', 'transaction_fact']:
            if 'analytics' in optimal_for or 'large_facts' in optimal_for:
                score += 0.3
        
        # Workload characteristics scoring
        volume = workload.get('volume', 'medium')
        if volume in ['large', 'very_large']:
            if 'large_scale' in optimal_for or 'analytics' in optimal_for:
                score += 0.2
        
        compliance = workload.get('compliance_requirements', [])
        if compliance:
            if 'compliance' in optimal_for or capabilities.get('acid_compliance'):
                score += 0.3
        
        query_patterns = workload.get('query_patterns', [])
        if 'aggregation' in query_patterns:
            if 'analytics' in optimal_for or capabilities.get('analytical_performance'):
                score += 0.2
        
        return min(score, 1.0)  # Cap at 1.0
    
    @classmethod
    def get_backend_features(cls, storage_type: StorageType) -> Dict[str, Any]:
        """Get features supported by a specific backend"""
        if storage_type not in cls._backends:
            raise ValueError(f"Unsupported storage type: {storage_type}")
        
        backend_class = cls._backends[storage_type]
        
        # Create a temporary instance to check features
        try:
            # Use minimal configuration for feature checking
            if storage_type == StorageType.BIGQUERY:
                temp_backend = backend_class(project_id="temp")
            elif storage_type == StorageType.POSTGRESQL:
                temp_backend = backend_class(host="temp")
            elif storage_type == StorageType.CLICKHOUSE:
                temp_backend = backend_class(host="temp")
            else:
                temp_backend = backend_class()
            
            # Common features to check
            features_to_check = [
                'partitioning', 'clustering', 'materialized_views', 'time_travel',
                'change_tracking', 'acid_transactions', 'schema_evolution',
                'compression', 'encryption', 'row_level_security'
            ]
            
            supported_features = {}
            for feature in features_to_check:
                try:
                    supported_features[feature] = temp_backend.supports_feature(feature)
                except (AttributeError, NotImplementedError):
                    supported_features[feature] = False
            
            # Add performance optimizations if available
            try:
                optimizations = temp_backend.get_performance_optimizations()
                supported_features['performance_optimizations'] = optimizations
            except (AttributeError, NotImplementedError):
                supported_features['performance_optimizations'] = []
            
            return supported_features
            
        except Exception as e:
            return {'error': f"Could not determine features: {str(e)}"}


# Convenience functions and legacy compatibility
StorageBackendFactory = DimensionalStorageBackendFactory  # Legacy compatibility


def create_storage_backend(storage_type_str: str, **kwargs) -> StorageBackend:
    """Convenience function to create backend from string"""
    try:
        storage_type = StorageType(storage_type_str.lower())
        return DimensionalStorageBackendFactory.create_backend(storage_type, **kwargs)
    except ValueError:
        available = list(StorageType)
        raise ValueError(f"Invalid storage type '{storage_type_str}'. "
                        f"Available types: {[t.value for t in available]}")


def recommend_backend_for_entity(entity_metadata: Dict[str, Any]) -> List[BackendRecommendation]:
    """Convenience function to get backend recommendations for any entity"""
    return DimensionalStorageBackendFactory.recommend_backend_for_entity(entity_metadata)


def list_storage_backends() -> Dict[str, Any]:
    """Convenience function to list available backends with dimensional capabilities"""
    return DimensionalStorageBackendFactory.list_available_backends()


def compare_storage_backends_for_entity(entity_metadata: Dict[str, Any]) -> Dict[str, Any]:
    """Compare all backends for a specific entity"""
    return DimensionalStorageBackendFactory.compare_backends_for_entity(entity_metadata)


def get_backend_dimensional_capabilities(storage_type_str: str) -> Dict[str, Any]:
    """Get dimensional modeling capabilities for a backend"""
    try:
        storage_type = StorageType(storage_type_str.lower())
        return DimensionalStorageBackendFactory.get_dimensional_capabilities(storage_type)
    except ValueError:
        available = list(StorageType)
        raise ValueError(f"Invalid storage type '{storage_type_str}'. "
                        f"Available types: {[t.value for t in available]}")