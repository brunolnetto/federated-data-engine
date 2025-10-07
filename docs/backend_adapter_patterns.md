# Backend Adapter Implementation Patterns

## Overview

This document provides implementation patterns for creating backend-specific adapters that optimize dimensional modeling patterns for each storage technology while maintaining business logic consistency.

## Adapter Architecture

```python
from abc import ABC, abstractmethod
from typing import Dict, Any, List
from dataclasses import dataclass

@dataclass
class AdaptationResult:
    """Result of adapting an entity to a specific backend"""
    adapted_entity: Dict[str, Any]
    backend_specific_features: List[str]
    performance_optimizations: List[str]
    implementation_notes: str

class DimensionalPatternAdapter(ABC):
    """Base adapter for dimensional patterns to specific backends"""
    
    @abstractmethod
    def adapt_dimension(self, entity: Dict[str, Any]) -> AdaptationResult:
        """Adapt dimension pattern to backend capabilities"""
        pass
    
    @abstractmethod
    def adapt_fact(self, entity: Dict[str, Any]) -> AdaptationResult:
        """Adapt fact pattern to backend capabilities"""
        pass
    
    @abstractmethod
    def adapt_bridge(self, entity: Dict[str, Any]) -> AdaptationResult:
        """Adapt bridge pattern to backend capabilities"""
        pass
    
    @abstractmethod
    def get_backend_capabilities(self) -> Dict[str, Any]:
        """Return backend-specific capabilities and limitations"""
        pass
```

## PostgreSQL Adapter Implementation

```python
from typing import Dict, Any, List

class PostgreSQLDimensionalAdapter(DimensionalPatternAdapter):
    """Adapts dimensional patterns for PostgreSQL strengths"""
    
    def adapt_dimension(self, entity: Dict[str, Any]) -> AdaptationResult:
        """Optimize dimension for PostgreSQL ACID and referential integrity"""
        
        adapted = entity.copy()
        scd_type = entity.get('scd', 'SCD1')
        business_keys = entity.get('business_keys', [])
        
        # PostgreSQL-specific optimizations
        backend_features = []
        optimizations = []
        
        if scd_type == 'SCD2':
            # Add PostgreSQL-specific SCD2 constraints
            adapted['constraints'] = self._generate_scd2_constraints(entity, business_keys)
            adapted['indexes'] = self._generate_scd2_indexes(entity, business_keys)
            adapted['merge_strategy'] = 'postgresql_merge_statement'
            
            backend_features.extend(['unique_constraints', 'check_constraints', 'partial_indexes'])
            optimizations.extend(['constraint_enforcement', 'index_optimization', 'merge_performance'])
        
        # Add audit triggers for compliance
        if entity.get('workload_characteristics', {}).get('compliance_requirements'):
            adapted['triggers'] = self._generate_audit_triggers(entity)
            backend_features.append('audit_triggers')
            optimizations.append('compliance_automation')
        
        # Partitioning for large dimensions
        workload = entity.get('workload_characteristics', {})
        if workload.get('volume') in ['large', 'very_large']:
            adapted['partitioning'] = self._generate_dimension_partitioning(entity)
            backend_features.append('native_partitioning')
            optimizations.append('partition_pruning')
        
        return AdaptationResult(
            adapted_entity=adapted,
            backend_specific_features=backend_features,
            performance_optimizations=optimizations,
            implementation_notes="PostgreSQL dimension optimized for ACID compliance and referential integrity"
        )
    
    def adapt_fact(self, entity: Dict[str, Any]) -> AdaptationResult:
        """Optimize fact table for PostgreSQL analytical capabilities"""
        
        adapted = entity.copy()
        entity_type = entity.get('entity_type')
        workload = entity.get('workload_characteristics', {})
        
        backend_features = []
        optimizations = []
        
        # Foreign key constraints for referential integrity
        dim_refs = entity.get('dimension_references', [])
        if dim_refs:
            adapted['foreign_keys'] = self._generate_foreign_keys(entity, dim_refs)
            backend_features.append('referential_integrity')
            optimizations.append('constraint_validation')
        
        # Partitioning strategy for facts
        if entity_type == 'transaction_fact':
            adapted['partitioning'] = self._generate_fact_partitioning(entity)
            backend_features.append('range_partitioning')
            optimizations.append('partition_elimination')
        
        # Indexes for analytical queries
        adapted['indexes'] = self._generate_fact_indexes(entity)
        backend_features.append('btree_indexes')
        optimizations.append('query_performance')
        
        # Materialized views for common aggregations
        if 'aggregation' in workload.get('query_patterns', []):
            adapted['materialized_views'] = self._generate_aggregation_views(entity)
            backend_features.append('materialized_views')
            optimizations.append('aggregation_performance')
        
        return AdaptationResult(
            adapted_entity=adapted,
            backend_specific_features=backend_features,
            performance_optimizations=optimizations,
            implementation_notes="PostgreSQL fact table with referential integrity and analytical optimization"
        )
    
    def _generate_scd2_constraints(self, entity: Dict[str, Any], business_keys: List[str]) -> List[str]:
        """Generate PostgreSQL-specific SCD2 constraints"""
        constraints = []
        table_name = entity['name']
        
        if business_keys:
            # Unique constraint for current records
            bk_cols = ', '.join(business_keys)
            constraints.append(
                f"CONSTRAINT uk_{table_name}_current UNIQUE ({bk_cols}, is_current) "
                f"WHERE is_current = true"
            )
        
        # Date range validation
        constraints.append(
            "CONSTRAINT ck_valid_date_range CHECK (effective_date <= expiration_date)"
        )
        
        # Current flag validation
        constraints.append(
            "CONSTRAINT ck_current_flag CHECK (is_current IN (true, false))"
        )
        
        return constraints
    
    def _generate_scd2_indexes(self, entity: Dict[str, Any], business_keys: List[str]) -> List[str]:
        """Generate PostgreSQL-specific SCD2 indexes"""
        indexes = []
        table_name = entity['name']
        
        if business_keys:
            bk_cols = ', '.join(business_keys)
            indexes.append(f"CREATE INDEX idx_{table_name}_business_key ON {table_name} ({bk_cols})")
        
        # Partial index for current records
        indexes.append(f"CREATE INDEX idx_{table_name}_current ON {table_name} (is_current) WHERE is_current = true")
        
        # Index for temporal queries
        indexes.append(f"CREATE INDEX idx_{table_name}_temporal ON {table_name} (effective_date, expiration_date)")
        
        return indexes
    
    def get_backend_capabilities(self) -> Dict[str, Any]:
        """PostgreSQL capabilities for dimensional modeling"""
        return {
            "acid_compliance": True,
            "referential_integrity": True,
            "complex_constraints": True,
            "partial_indexes": True,
            "materialized_views": True,
            "native_partitioning": True,
            "window_functions": True,
            "cte_support": True,
            "merge_statements": True,
            "triggers": True,
            "custom_functions": True,
            "json_support": True,
            "full_text_search": True
        }
```

## ClickHouse Adapter Implementation

```python
class ClickHouseDimensionalAdapter(DimensionalPatternAdapter):
    """Adapts dimensional patterns for ClickHouse analytical strengths"""
    
    def adapt_dimension(self, entity: Dict[str, Any]) -> AdaptationResult:
        """Optimize dimension for ClickHouse columnar analytics"""
        
        adapted = entity.copy()
        scd_type = entity.get('scd', 'SCD1')
        business_keys = entity.get('business_keys', [])
        
        backend_features = []
        optimizations = []
        
        if scd_type == 'SCD2':
            # Use ReplacingMergeTree for SCD2
            adapted['engine'] = 'ReplacingMergeTree(row_version)'
            adapted['order_by'] = self._determine_order_by(business_keys)
            adapted['partition_by'] = 'toYYYYMM(effective_date)'
            
            # Materialized view for current records
            adapted['materialized_views'] = [
                f"""CREATE MATERIALIZED VIEW {entity['name']}_current AS
                   SELECT * FROM {entity['name']} FINAL WHERE is_current = 1"""
            ]
            
            backend_features.extend(['replacing_merge_tree', 'materialized_views', 'final_queries'])
            optimizations.extend(['deduplication', 'current_view_performance', 'partition_pruning'])
        
        else:
            adapted['engine'] = 'ReplacingMergeTree()'
            adapted['order_by'] = self._determine_order_by(business_keys)
            
            backend_features.append('replacing_merge_tree')
            optimizations.append('deduplication')
        
        # Compression for large dimensions
        workload = entity.get('workload_characteristics', {})
        if workload.get('volume') in ['large', 'very_large']:
            adapted['compression'] = 'ZSTD(1)'
            backend_features.append('compression')
            optimizations.append('storage_efficiency')
        
        return AdaptationResult(
            adapted_entity=adapted,
            backend_specific_features=backend_features,
            performance_optimizations=optimizations,
            implementation_notes="ClickHouse dimension optimized for analytical queries and columnar storage"
        )
    
    def adapt_fact(self, entity: Dict[str, Any]) -> AdaptationResult:
        """Optimize fact table for ClickHouse analytical performance"""
        
        adapted = entity.copy()
        workload = entity.get('workload_characteristics', {})
        measures = entity.get('measures', [])
        
        backend_features = []
        optimizations = []
        
        # MergeTree engine for facts
        adapted['engine'] = 'MergeTree()'
        
        # Optimal sorting key for analytics
        sort_keys = self._determine_fact_sort_order(entity)
        adapted['order_by'] = sort_keys
        
        # Partitioning strategy
        if entity.get('entity_type') == 'transaction_fact':
            event_col = entity.get('event_timestamp_column', 'event_timestamp')
            adapted['partition_by'] = f'toYYYYMM({event_col})'
            backend_features.append('time_partitioning')
            optimizations.append('temporal_queries')
        
        # Aggregating MergeTree for pre-aggregated facts
        additive_measures = [m for m in measures if m.get('additivity') == 'additive']
        if additive_measures and 'aggregation' in workload.get('query_patterns', []):
            adapted['aggregating_views'] = self._generate_aggregating_views(entity, additive_measures)
            backend_features.append('aggregating_merge_tree')
            optimizations.append('pre_aggregation')
        
        # Projection for analytical queries
        if workload.get('query_patterns'):
            adapted['projections'] = self._generate_projections(entity, workload['query_patterns'])
            backend_features.append('projections')
            optimizations.append('query_acceleration')
        
        backend_features.extend(['merge_tree', 'columnar_storage'])
        optimizations.extend(['parallel_processing', 'vectorized_execution'])
        
        return AdaptationResult(
            adapted_entity=adapted,
            backend_specific_features=backend_features,
            performance_optimizations=optimizations,
            implementation_notes="ClickHouse fact table optimized for high-performance analytics"
        )
    
    def _determine_order_by(self, business_keys: List[str]) -> str:
        """Determine optimal sort order for ClickHouse"""
        if business_keys:
            return ', '.join(business_keys)
        return 'tuple()'
    
    def _determine_fact_sort_order(self, entity: Dict[str, Any]) -> str:
        """Determine optimal sort order for fact tables"""
        sort_keys = []
        
        # Add timestamp for transaction facts
        if entity.get('entity_type') == 'transaction_fact':
            event_col = entity.get('event_timestamp_column', 'event_timestamp')
            sort_keys.append(event_col)
        
        # Add dimension foreign keys
        dim_refs = entity.get('dimension_references', [])
        for dim_ref in dim_refs[:3]:  # Limit to 3 for performance
            fk_col = dim_ref.get('fk_column')
            if fk_col:
                sort_keys.append(fk_col)
        
        return ', '.join(sort_keys) if sort_keys else 'tuple()'
    
    def get_backend_capabilities(self) -> Dict[str, Any]:
        """ClickHouse capabilities for dimensional modeling"""
        return {
            "columnar_storage": True,
            "parallel_processing": True,
            "vectorized_execution": True,
            "compression": True,
            "materialized_views": True,
            "aggregating_merge_tree": True,
            "replacing_merge_tree": True,
            "projections": True,
            "partition_pruning": True,
            "final_queries": True,
            "window_functions": True,
            "array_functions": True,
            "approximate_functions": True,
            "time_functions": True
        }
```

## Iceberg Adapter Implementation

```python
class IcebergDimensionalAdapter(DimensionalPatternAdapter):
    """Adapts dimensional patterns for Iceberg data lake capabilities"""
    
    def adapt_dimension(self, entity: Dict[str, Any]) -> AdaptationResult:
        """Optimize dimension for Iceberg time travel and schema evolution"""
        
        adapted = entity.copy()
        scd_type = entity.get('scd', 'SCD1')
        business_keys = entity.get('business_keys', [])
        
        backend_features = []
        optimizations = []
        
        # Leverage Iceberg's time travel for SCD2
        if scd_type == 'SCD2':
            adapted['iceberg_features'] = {
                'time_travel': True,
                'schema_evolution': True,
                'partition_evolution': True
            }
            
            # Hidden partitioning
            adapted['partitioning'] = [
                'months(effective_date)',
                f'bucket(10, {business_keys[0]})' if business_keys else 'bucket(10, row_id)'
            ]
            
            backend_features.extend(['time_travel', 'hidden_partitioning', 'schema_evolution'])
            optimizations.extend(['file_pruning', 'partition_evolution', 'snapshot_isolation'])
        
        # Metadata optimization
        adapted['table_properties'] = {
            'write.format.default': 'parquet',
            'write.parquet.compression-codec': 'zstd',
            'write.target-file-size-bytes': '134217728'  # 128MB
        }
        
        backend_features.extend(['metadata_tables', 'file_format_flexibility'])
        optimizations.extend(['compression', 'file_sizing'])
        
        return AdaptationResult(
            adapted_entity=adapted,
            backend_specific_features=backend_features,
            performance_optimizations=optimizations,
            implementation_notes="Iceberg dimension with time travel and schema evolution support"
        )
    
    def adapt_fact(self, entity: Dict[str, Any]) -> AdaptationResult:
        """Optimize fact table for Iceberg data lake analytics"""
        
        adapted = entity.copy()
        workload = entity.get('workload_characteristics', {})
        
        backend_features = []
        optimizations = []
        
        # Time-based partitioning for facts
        if entity.get('entity_type') == 'transaction_fact':
            event_col = entity.get('event_timestamp_column', 'event_timestamp')
            adapted['partitioning'] = [
                f'days({event_col})',
                f'bucket(50, customer_sk)'  # Assuming customer dimension
            ]
            backend_features.append('temporal_partitioning')
            optimizations.append('time_travel_queries')
        
        # Sort order for query performance
        dim_refs = entity.get('dimension_references', [])
        if dim_refs:
            sort_columns = [ref.get('fk_column') for ref in dim_refs if ref.get('fk_column')]
            if entity.get('entity_type') == 'transaction_fact':
                event_col = entity.get('event_timestamp_column', 'event_timestamp')
                sort_columns.insert(0, event_col)
            
            adapted['sort_order'] = sort_columns
            backend_features.append('sort_optimization')
            optimizations.append('scan_performance')
        
        # File format optimization
        adapted['table_properties'] = {
            'write.format.default': 'parquet',
            'write.parquet.compression-codec': 'zstd',
            'write.target-file-size-bytes': '268435456'  # 256MB for facts
        }
        
        # Metadata optimization for large tables
        if workload.get('volume') in ['high', 'very_high']:
            adapted['table_properties'].update({
                'write.metadata.metrics.default': 'truncate(16)',
                'write.metadata.metrics.column.event_timestamp': 'full'
            })
            backend_features.append('metadata_optimization')
            optimizations.append('query_planning')
        
        backend_features.extend(['acid_transactions', 'snapshot_isolation', 'file_pruning'])
        optimizations.extend(['parallel_scanning', 'predicate_pushdown'])
        
        return AdaptationResult(
            adapted_entity=adapted,
            backend_specific_features=backend_features,
            performance_optimizations=optimizations,
            implementation_notes="Iceberg fact table optimized for data lake analytics and time travel"
        )
    
    def get_backend_capabilities(self) -> Dict[str, Any]:
        """Iceberg capabilities for dimensional modeling"""
        return {
            "time_travel": True,
            "schema_evolution": True,
            "partition_evolution": True,
            "hidden_partitioning": True,
            "acid_transactions": True,
            "snapshot_isolation": True,
            "file_pruning": True,
            "metadata_tables": True,
            "column_statistics": True,
            "predicate_pushdown": True,
            "file_format_flexibility": True,
            "concurrent_writes": True,
            "rollback_support": True
        }
```

## Adapter Factory and Selection

```python
from typing import Dict, Type

class DimensionalAdapterFactory:
    """Factory for creating backend-specific dimensional adapters"""
    
    _adapters: Dict[str, Type[DimensionalPatternAdapter]] = {
        'postgresql': PostgreSQLDimensionalAdapter,
        'clickhouse': ClickHouseDimensionalAdapter,
        'iceberg': IcebergDimensionalAdapter,
        'delta_lake': DeltaLakeDimensionalAdapter,
        'duckdb': DuckDBDimensionalAdapter,
        'bigquery': BigQueryDimensionalAdapter,
        'snowflake': SnowflakeDimensionalAdapter,
        'parquet': ParquetDimensionalAdapter
    }
    
    @classmethod
    def create_adapter(cls, backend_type: str) -> DimensionalPatternAdapter:
        """Create adapter for specified backend type"""
        if backend_type not in cls._adapters:
            raise ValueError(f"Unsupported backend type: {backend_type}")
        
        return cls._adapters[backend_type]()
    
    @classmethod
    def get_supported_backends(cls) -> List[str]:
        """Get list of supported backend types"""
        return list(cls._adapters.keys())
    
    @classmethod
    def register_adapter(cls, backend_type: str, adapter_class: Type[DimensionalPatternAdapter]):
        """Register new backend adapter"""
        cls._adapters[backend_type] = adapter_class

class AdapterSelector:
    """Selects optimal adapter based on entity workload characteristics"""
    
    def select_adapter(self, entity: Dict[str, Any], available_backends: List[str]) -> str:
        """Select best adapter based on workload analysis"""
        
        workload = entity.get('workload_characteristics', {})
        entity_type = entity.get('entity_type')
        
        # Score each available backend
        backend_scores = {}
        
        for backend in available_backends:
            score = self._score_backend_for_workload(backend, entity_type, workload)
            backend_scores[backend] = score
        
        # Return highest scoring backend
        return max(backend_scores, key=backend_scores.get)
    
    def _score_backend_for_workload(self, backend: str, entity_type: str, workload: Dict[str, Any]) -> float:
        """Score backend appropriateness for specific workload"""
        
        score = 0.0
        volume = workload.get('volume', 'medium')
        query_patterns = workload.get('query_patterns', [])
        compliance = workload.get('compliance_requirements', [])
        
        if backend == 'postgresql':
            score += 0.9 if compliance else 0.3
            score += 0.8 if entity_type == 'dimension' else 0.4
            score += 0.6 if volume in ['small', 'medium'] else 0.2
        
        elif backend == 'clickhouse':
            score += 0.9 if 'aggregation' in query_patterns else 0.3
            score += 0.8 if volume in ['large', 'very_large'] else 0.4
            score += 0.7 if entity_type in ['fact', 'transaction_fact'] else 0.3
        
        elif backend == 'iceberg':
            score += 0.9 if workload.get('time_travel_required') else 0.4
            score += 0.8 if volume in ['large', 'very_large'] else 0.5
            score += 0.7 if workload.get('schema_evolution_required') else 0.4
        
        return score
```

## Testing Adapter Implementations

```python
import pytest
from typing import Dict, Any

class TestDimensionalAdapters:
    """Test suite for dimensional pattern adapters"""
    
    @pytest.fixture
    def sample_dimension(self) -> Dict[str, Any]:
        return {
            "name": "dim_customer",
            "entity_type": "dimension",
            "grain": "One row per customer per version",
            "scd": "SCD2",
            "business_keys": ["customer_number"],
            "workload_characteristics": {
                "volume": "large",
                "update_frequency": "daily",
                "compliance_requirements": ["GDPR"],
                "query_patterns": ["lookup", "drill_down"]
            }
        }
    
    @pytest.fixture
    def sample_fact(self) -> Dict[str, Any]:
        return {
            "name": "fact_sales",
            "entity_type": "transaction_fact",
            "grain": "One row per sales transaction",
            "event_timestamp_column": "transaction_time",
            "measures": [
                {"name": "sales_amount", "additivity": "additive"},
                {"name": "quantity", "additivity": "additive"}
            ],
            "workload_characteristics": {
                "volume": "very_high",
                "insert_frequency": "streaming",
                "query_patterns": ["aggregation", "time_series"]
            }
        }
    
    def test_postgresql_dimension_adaptation(self, sample_dimension):
        """Test PostgreSQL adapter for dimensions"""
        adapter = PostgreSQLDimensionalAdapter()
        result = adapter.adapt_dimension(sample_dimension)
        
        # Check ACID compliance features
        assert 'constraints' in result.adapted_entity
        assert 'indexes' in result.adapted_entity
        assert 'merge_strategy' in result.adapted_entity
        
        # Check backend-specific features
        assert 'unique_constraints' in result.backend_specific_features
        assert 'audit_triggers' in result.backend_specific_features
        assert 'constraint_enforcement' in result.performance_optimizations
    
    def test_clickhouse_fact_adaptation(self, sample_fact):
        """Test ClickHouse adapter for facts"""
        adapter = ClickHouseDimensionalAdapter()
        result = adapter.adapt_fact(sample_fact)
        
        # Check analytical optimization
        assert result.adapted_entity['engine'] == 'MergeTree()'
        assert 'order_by' in result.adapted_entity
        assert 'partition_by' in result.adapted_entity
        
        # Check columnar features
        assert 'columnar_storage' in result.backend_specific_features
        assert 'parallel_processing' in result.performance_optimizations
    
    def test_iceberg_time_travel_features(self, sample_dimension):
        """Test Iceberg time travel capabilities"""
        adapter = IcebergDimensionalAdapter()
        result = adapter.adapt_dimension(sample_dimension)
        
        # Check time travel features
        iceberg_features = result.adapted_entity.get('iceberg_features', {})
        assert iceberg_features.get('time_travel') is True
        assert iceberg_features.get('schema_evolution') is True
        
        # Check partitioning
        assert 'partitioning' in result.adapted_entity
        assert 'time_travel' in result.backend_specific_features
    
    def test_adapter_factory_creation(self):
        """Test adapter factory functionality"""
        # Test supported adapters
        supported = DimensionalAdapterFactory.get_supported_backends()
        assert 'postgresql' in supported
        assert 'clickhouse' in supported
        assert 'iceberg' in supported
        
        # Test adapter creation
        pg_adapter = DimensionalAdapterFactory.create_adapter('postgresql')
        assert isinstance(pg_adapter, PostgreSQLDimensionalAdapter)
        
        # Test unsupported backend
        with pytest.raises(ValueError):
            DimensionalAdapterFactory.create_adapter('unsupported_backend')
    
    def test_adapter_selector(self, sample_dimension, sample_fact):
        """Test adapter selection logic"""
        selector = AdapterSelector()
        
        # Test dimension with compliance requirements
        available = ['postgresql', 'clickhouse', 'iceberg']
        selected = selector.select_adapter(sample_dimension, available)
        assert selected == 'postgresql'  # Should select PostgreSQL for compliance
        
        # Test high-volume analytical fact
        selected = selector.select_adapter(sample_fact, available)
        assert selected == 'clickhouse'  # Should select ClickHouse for analytics
```

## Best Practices for Adapter Implementation

### 1. Maintain Business Logic Consistency
- Never change dimensional modeling rules in adapters
- Only optimize implementation for backend capabilities
- Preserve entity grain and relationships

### 2. Leverage Backend Strengths
- Use each backend's optimal features (constraints, engines, partitioning)
- Consider performance characteristics of storage technology
- Optimize for expected query patterns

### 3. Handle Backend Limitations Gracefully
- Provide fallback implementations for missing features
- Document backend-specific constraints
- Ensure adapter robustness across all scenarios

### 4. Comprehensive Testing
- Test adapters with various entity configurations
- Validate that business rules are preserved
- Performance test optimizations

### 5. Documentation and Monitoring
- Document backend-specific optimizations
- Monitor performance impact of adaptations
- Track backend selection effectiveness

---

**Last Updated**: October 6, 2025  
**Version**: 1.0  
**Status**: Implementation Guide