# Payload-Agnostic Implementation Guide

## Overview

This guide ensures all platform components maintain **payload agnosticism** - the ability to work with any business domain or data structure while preserving dimensional modeling rigor.

## Core Principles

### 1. Business Logic First, Technology Second
- Define entity types (dimension/fact/bridge) based on business purpose
- Generate technology-specific implementations from business metadata
- Never hardcode specific business domains or column structures

### 2. Dynamic Pattern Generation
- Build dimensional patterns from metadata, not templates
- Support any column structure within dimensional constraints
- Adapt patterns to leverage each backend's capabilities

### 3. Universal Validation with Backend Optimization
- Apply dimensional modeling rules regardless of payload
- Optimize implementation per storage technology
- Maintain business consistency across all backends

## Implementation Patterns

### Pattern 1: Dynamic Entity Column Generation

```python
from typing import Dict, Any, List
from dataclasses import dataclass

@dataclass
class ColumnDefinition:
    name: str
    type: str
    nullable: bool = True
    default: Any = None
    constraints: List[str] = None

class PayloadAgnosticEntityBuilder:
    """Builds dimensional entities from any business metadata"""
    
    def generate_entity_columns(self, entity: Dict[str, Any]) -> List[ColumnDefinition]:
        """Generate columns based on entity metadata, not hardcoded schemas"""
        
        # Start with user-defined business columns
        base_columns = self._parse_business_columns(entity.get('physical_columns', []))
        
        # Add dimensional pattern columns based on entity type
        entity_type = entity.get('entity_type')
        
        if entity_type == 'dimension':
            base_columns.extend(self._generate_dimension_columns(entity))
        elif entity_type in ['fact', 'transaction_fact']:
            base_columns.extend(self._generate_fact_columns(entity))
        elif entity_type == 'bridge':
            base_columns.extend(self._generate_bridge_columns(entity))
        
        return base_columns
    
    def _parse_business_columns(self, columns: List[Dict[str, Any]]) -> List[ColumnDefinition]:
        """Parse any business column structure into standard format"""
        parsed_columns = []
        
        for col in columns:
            parsed_columns.append(ColumnDefinition(
                name=col.get('name'),
                type=col.get('type', 'VARCHAR(255)'),
                nullable=col.get('nullable', True),
                default=col.get('default'),
                constraints=col.get('constraints', [])
            ))
        
        return parsed_columns
    
    def _generate_dimension_columns(self, entity: Dict[str, Any]) -> List[ColumnDefinition]:
        """Generate dimension-specific columns for any business entity"""
        dim_columns = []
        
        # Standard surrogate key (always generated)
        entity_name = entity['name'].replace('dim_', '').replace('_dim', '')
        dim_columns.append(ColumnDefinition(
            name=f"{entity_name}_sk",
            type="BIGINT",
            nullable=False,
            constraints=["PRIMARY KEY", "IDENTITY(1,1)"]
        ))
        
        # SCD pattern columns
        scd_type = entity.get('scd', 'SCD1')
        if scd_type == 'SCD2':
            dim_columns.extend([
                ColumnDefinition("effective_date", "TIMESTAMP", False),
                ColumnDefinition("expiration_date", "TIMESTAMP", True, "'9999-12-31 23:59:59'"),
                ColumnDefinition("is_current", "BOOLEAN", False, "TRUE"),
                ColumnDefinition("row_version", "INTEGER", False, "1")
            ])
        
        # Audit columns (always added)
        dim_columns.extend([
            ColumnDefinition("created_at", "TIMESTAMP", False, "CURRENT_TIMESTAMP"),
            ColumnDefinition("updated_at", "TIMESTAMP", False, "CURRENT_TIMESTAMP"),
            ColumnDefinition("source_system", "VARCHAR(100)", True)
        ])
        
        return dim_columns
    
    def _generate_fact_columns(self, entity: Dict[str, Any]) -> List[ColumnDefinition]:
        """Generate fact-specific columns for any business process"""
        fact_columns = []
        
        # Standard fact key
        entity_name = entity['name'].replace('fact_', '').replace('_fact', '')
        fact_columns.append(ColumnDefinition(
            name=f"{entity_name}_key",
            type="BIGINT",
            nullable=False,
            constraints=["PRIMARY KEY", "IDENTITY(1,1)"]
        ))
        
        # Transaction fact specific columns
        if entity.get('entity_type') == 'transaction_fact':
            event_ts_col = entity.get('event_timestamp_column', 'event_timestamp')
            fact_columns.append(ColumnDefinition(
                name=event_ts_col,
                type="TIMESTAMP",
                nullable=False
            ))
            
            # Add derived time dimensions for partitioning
            fact_columns.extend([
                ColumnDefinition("event_date", "DATE", False, f"DATE({event_ts_col})"),
                ColumnDefinition("event_hour", "INTEGER", False, f"HOUR({event_ts_col})")
            ])
        
        # Audit columns
        fact_columns.extend([
            ColumnDefinition("created_at", "TIMESTAMP", False, "CURRENT_TIMESTAMP"),
            ColumnDefinition("batch_id", "VARCHAR(100)", True),
            ColumnDefinition("source_system", "VARCHAR(100)", True)
        ])
        
        return fact_columns
```

### Pattern 2: Backend-Agnostic DDL Generation

```python
from abc import ABC, abstractmethod
from typing import Dict, Any, List

class PayloadAgnosticDDLGenerator(ABC):
    """Base class for generating DDL from any business entity"""
    
    def generate_ddl(self, entity: Dict[str, Any]) -> str:
        """Generate DDL for any business entity"""
        
        # Validate entity follows dimensional patterns
        validation_result = self._validate_entity(entity)
        if not validation_result.is_valid:
            raise ValueError(f"Invalid entity: {validation_result.errors}")
        
        # Generate columns dynamically
        columns = self.entity_builder.generate_entity_columns(entity)
        
        # Build backend-specific DDL
        return self._generate_backend_ddl(entity, columns)
    
    @abstractmethod
    def _generate_backend_ddl(self, entity: Dict[str, Any], columns: List[ColumnDefinition]) -> str:
        """Generate backend-specific DDL from universal column definitions"""
        pass
    
    def _validate_entity(self, entity: Dict[str, Any]) -> ValidationResult:
        """Universal validation rules for any business entity"""
        errors = []
        
        # Required fields for all entities
        if not entity.get('name'):
            errors.append("Entity must have a name")
        
        if not entity.get('entity_type'):
            errors.append("Entity must specify type (dimension/fact/transaction_fact/bridge)")
        
        if not entity.get('grain'):
            errors.append("Entity must define grain")
        
        # Entity-specific validation
        entity_type = entity.get('entity_type')
        
        if entity_type == 'dimension':
            if not entity.get('business_keys'):
                errors.append("Dimension must have business keys defined")
        
        elif entity_type in ['fact', 'transaction_fact']:
            measures = entity.get('measures', [])
            if not measures:
                errors.append("Fact table must have measures defined")
            
            # Validate measure additivity
            for measure in measures:
                if measure.get('additivity') not in ['additive', 'semi_additive', 'non_additive']:
                    errors.append(f"Measure {measure.get('name')} must specify additivity")
        
        return ValidationResult(is_valid=len(errors) == 0, errors=errors)

class PostgreSQLPayloadAgnosticDDL(PayloadAgnosticDDLGenerator):
    """PostgreSQL DDL generation for any business entity"""
    
    def _generate_backend_ddl(self, entity: Dict[str, Any], columns: List[ColumnDefinition]) -> str:
        """Generate PostgreSQL DDL from any entity structure"""
        
        table_name = entity['name']
        entity_type = entity.get('entity_type')
        
        # Build column definitions
        column_ddl = []
        for col in columns:
            col_def = f"    {col.name} {col.type}"
            if not col.nullable:
                col_def += " NOT NULL"
            if col.default:
                col_def += f" DEFAULT {col.default}"
            column_ddl.append(col_def)
        
        # Add constraints
        constraints = self._generate_constraints(entity, columns)
        if constraints:
            column_ddl.extend([f"    {constraint}" for constraint in constraints])
        
        ddl = f"""
-- PostgreSQL DDL for {table_name} ({entity_type})
-- Generated from payload-agnostic metadata
-- Grain: {entity.get('grain', 'Not specified')}

CREATE TABLE IF NOT EXISTS {table_name} (
{',\\n'.join(column_ddl)}
);
"""
        
        # Add indexes
        indexes = self._generate_indexes(entity, columns)
        for index in indexes:
            ddl += f"\n{index};"
        
        # Add comments
        ddl += f"\n\nCOMMENT ON TABLE {table_name} IS '{entity.get('grain', 'Business entity table')}';"
        
        return ddl
    
    def _generate_constraints(self, entity: Dict[str, Any], columns: List[ColumnDefinition]) -> List[str]:
        """Generate constraints based on entity type and patterns"""
        constraints = []
        
        entity_type = entity.get('entity_type')
        
        if entity_type == 'dimension':
            scd_type = entity.get('scd', 'SCD1')
            business_keys = entity.get('business_keys', [])
            
            if scd_type == 'SCD2' and business_keys:
                # Unique constraint for current records
                bk_cols = ', '.join(business_keys)
                constraints.append(
                    f"CONSTRAINT uk_{entity['name']}_current UNIQUE ({bk_cols}, is_current) "
                    f"WHERE is_current = true"
                )
                
                # Valid date range constraint
                constraints.append(
                    "CONSTRAINT ck_valid_date_range CHECK (effective_date <= expiration_date)"
                )
        
        elif entity_type in ['fact', 'transaction_fact']:
            # Foreign key constraints for dimension references
            dim_refs = entity.get('dimension_references', [])
            for dim_ref in dim_refs:
                fk_col = dim_ref.get('fk_column')
                dim_table = dim_ref.get('dimension')
                if fk_col and dim_table:
                    constraints.append(
                        f"CONSTRAINT fk_{entity['name']}_{fk_col} "
                        f"FOREIGN KEY ({fk_col}) REFERENCES {dim_table}({dim_table.replace('dim_', '')}_sk)"
                    )
        
        return constraints
    
    def _generate_indexes(self, entity: Dict[str, Any], columns: List[ColumnDefinition]) -> List[str]:
        """Generate indexes based on entity type and query patterns"""
        indexes = []
        table_name = entity['name']
        entity_type = entity.get('entity_type')
        
        if entity_type == 'dimension':
            business_keys = entity.get('business_keys', [])
            scd_type = entity.get('scd', 'SCD1')
            
            # Index on business keys
            if business_keys:
                bk_cols = ', '.join(business_keys)
                indexes.append(f"CREATE INDEX idx_{table_name}_business_key ON {table_name} ({bk_cols})")
            
            # SCD2 specific indexes
            if scd_type == 'SCD2':
                indexes.append(f"CREATE INDEX idx_{table_name}_current ON {table_name} (is_current) WHERE is_current = true")
                indexes.append(f"CREATE INDEX idx_{table_name}_effective ON {table_name} (effective_date)")
        
        elif entity_type in ['fact', 'transaction_fact']:
            # Indexes on foreign keys
            dim_refs = entity.get('dimension_references', [])
            for dim_ref in dim_refs:
                fk_col = dim_ref.get('fk_column')
                if fk_col:
                    indexes.append(f"CREATE INDEX idx_{table_name}_{fk_col} ON {table_name} ({fk_col})")
            
            # Transaction fact specific indexes
            if entity_type == 'transaction_fact':
                event_ts_col = entity.get('event_timestamp_column', 'event_timestamp')
                indexes.append(f"CREATE INDEX idx_{table_name}_event_time ON {table_name} ({event_ts_col})")
        
        return indexes

class ClickHousePayloadAgnosticDDL(PayloadAgnosticDDLGenerator):
    """ClickHouse DDL generation for any business entity"""
    
    def _generate_backend_ddl(self, entity: Dict[str, Any], columns: List[ColumnDefinition]) -> str:
        """Generate ClickHouse DDL optimized for analytical workloads"""
        
        table_name = entity['name']
        entity_type = entity.get('entity_type')
        
        # Map column types to ClickHouse equivalents
        ch_columns = []
        for col in columns:
            ch_type = self._map_to_clickhouse_type(col.type)
            ch_columns.append(f"    {col.name} {ch_type}")
        
        # Generate engine based on entity type
        engine = self._determine_engine(entity)
        order_by = self._determine_order_by(entity, columns)
        partition_by = self._determine_partition_by(entity)
        
        ddl = f"""
-- ClickHouse DDL for {table_name} ({entity_type})
-- Optimized for analytical workloads
-- Grain: {entity.get('grain', 'Not specified')}

CREATE TABLE IF NOT EXISTS {table_name} (
{',\\n'.join(ch_columns)}
)
ENGINE = {engine}
ORDER BY ({order_by})
{f"PARTITION BY {partition_by}" if partition_by else ""}
SETTINGS index_granularity = 8192;
"""
        
        # Add materialized views for dimensions
        if entity_type == 'dimension' and entity.get('scd') == 'SCD2':
            ddl += f"""

-- Materialized view for current dimension records
CREATE MATERIALIZED VIEW IF NOT EXISTS {table_name}_current AS
SELECT * FROM {table_name}
FINAL
WHERE is_current = 1;
"""
        
        return ddl
    
    def _map_to_clickhouse_type(self, generic_type: str) -> str:
        """Map generic SQL types to ClickHouse types"""
        type_mapping = {
            'BIGINT': 'Int64',
            'INTEGER': 'Int32',
            'VARCHAR(255)': 'String',
            'VARCHAR(100)': 'String',
            'VARCHAR(50)': 'String',
            'TEXT': 'String',
            'DECIMAL(10,2)': 'Decimal(10,2)',
            'DECIMAL(12,2)': 'Decimal(12,2)',
            'BOOLEAN': 'UInt8',
            'TIMESTAMP': 'DateTime',
            'DATE': 'Date'
        }
        
        return type_mapping.get(generic_type.upper(), 'String')
    
    def _determine_engine(self, entity: Dict[str, Any]) -> str:
        """Determine optimal ClickHouse engine for entity type"""
        entity_type = entity.get('entity_type')
        
        if entity_type == 'dimension':
            scd_type = entity.get('scd', 'SCD1')
            if scd_type == 'SCD2':
                return 'ReplacingMergeTree(row_version)'
            else:
                return 'ReplacingMergeTree()'
        
        elif entity_type in ['fact', 'transaction_fact']:
            return 'MergeTree()'
        
        return 'MergeTree()'
    
    def _determine_order_by(self, entity: Dict[str, Any], columns: List[ColumnDefinition]) -> str:
        """Determine optimal ordering for query performance"""
        entity_type = entity.get('entity_type')
        
        if entity_type == 'dimension':
            business_keys = entity.get('business_keys', [])
            if business_keys:
                return ', '.join(business_keys)
            else:
                # Use first column as fallback
                return columns[0].name if columns else 'tuple()'
        
        elif entity_type in ['fact', 'transaction_fact']:
            # Order by timestamp for facts
            event_ts_col = entity.get('event_timestamp_column', 'event_timestamp')
            if any(col.name == event_ts_col for col in columns):
                return event_ts_col
            
            # Fallback to foreign keys
            dim_refs = entity.get('dimension_references', [])
            if dim_refs:
                return ', '.join([ref.get('fk_column') for ref in dim_refs if ref.get('fk_column')])
        
        return 'tuple()'
    
    def _determine_partition_by(self, entity: Dict[str, Any]) -> str:
        """Determine partitioning strategy for performance"""
        entity_type = entity.get('entity_type')
        
        if entity_type in ['fact', 'transaction_fact']:
            event_ts_col = entity.get('event_timestamp_column', 'event_timestamp')
            return f"toYYYYMM({event_ts_col})"
        
        elif entity_type == 'dimension':
            scd_type = entity.get('scd', 'SCD1')
            if scd_type == 'SCD2':
                return "toYYYYMM(effective_date)"
        
        return None
```

### Pattern 3: Workload-Based Backend Selection

```python
from typing import Dict, Any, List
from dataclasses import dataclass
from enum import Enum

@dataclass
class BackendRecommendation:
    storage: str
    processing: str
    reason: str
    confidence: float
    backend_specific_features: List[str]

class WorkloadCharacteristics(Enum):
    VOLUME_SMALL = "small"
    VOLUME_MEDIUM = "medium" 
    VOLUME_LARGE = "large"
    VOLUME_VERY_LARGE = "very_large"
    
    FREQUENCY_BATCH = "batch"
    FREQUENCY_DAILY = "daily"
    FREQUENCY_HOURLY = "hourly"
    FREQUENCY_REAL_TIME = "real_time"
    
    PATTERN_LOOKUP = "lookup"
    PATTERN_AGGREGATION = "aggregation"
    PATTERN_TIME_SERIES = "time_series"
    PATTERN_OLAP = "olap"

class PayloadAgnosticWorkloadAnalyzer:
    """Analyzes any business entity to recommend optimal backends"""
    
    def recommend_backends(self, entity: Dict[str, Any]) -> List[BackendRecommendation]:
        """Recommend backends based on entity characteristics, not hardcoded rules"""
        
        entity_type = entity.get('entity_type')
        workload = entity.get('workload_characteristics', {})
        
        if entity_type == 'dimension':
            return self._analyze_dimension_workload(entity, workload)
        elif entity_type in ['fact', 'transaction_fact']:
            return self._analyze_fact_workload(entity, workload)
        elif entity_type == 'bridge':
            return self._analyze_bridge_workload(entity, workload)
        
        return []
    
    def _analyze_dimension_workload(self, entity: Dict[str, Any], workload: Dict[str, Any]) -> List[BackendRecommendation]:
        """Dimension-specific workload analysis"""
        recommendations = []
        
        scd_type = entity.get('scd', 'SCD1')
        update_freq = workload.get('update_frequency', 'daily')
        compliance = workload.get('compliance_requirements', [])
        volume = workload.get('volume', 'medium')
        query_patterns = workload.get('query_patterns', [])
        
        # PostgreSQL: ACID compliance + complex SCD logic
        if compliance or (scd_type == 'SCD2' and update_freq in ['hourly', 'real_time']):
            recommendations.append(BackendRecommendation(
                storage='postgresql',
                processing='postgresql',
                reason=f'ACID compliance for {scd_type} with {update_freq} updates and compliance requirements: {compliance}',
                confidence=0.95,
                backend_specific_features=['constraints', 'triggers', 'merge_upsert', 'referential_integrity']
            ))
        
        # ClickHouse: High-volume with analytical queries
        elif volume in ['large', 'very_large'] and 'aggregation' in query_patterns:
            recommendations.append(BackendRecommendation(
                storage='clickhouse',
                processing='clickhouse',
                reason=f'High-volume ({volume}) dimension with analytical query patterns: {query_patterns}',
                confidence=0.85,
                backend_specific_features=['columnar_storage', 'replacing_merge_tree', 'materialized_views']
            ))
        
        # Iceberg: Large dimensions with time travel needs
        elif volume in ['large', 'very_large'] and workload.get('time_travel_required', False):
            recommendations.append(BackendRecommendation(
                storage='iceberg',
                processing='trino',
                reason=f'Large dimension ({volume}) with time travel requirements',
                confidence=0.8,
                backend_specific_features=['time_travel', 'schema_evolution', 'hidden_partitioning']
            ))
        
        # DuckDB: Medium dimensions with analytical workloads
        elif volume in ['small', 'medium'] and 'olap' in query_patterns:
            recommendations.append(BackendRecommendation(
                storage='duckdb',
                processing='duckdb',
                reason=f'Medium-volume dimension with OLAP query patterns',
                confidence=0.75,
                backend_specific_features=['in_memory_analytics', 'vectorized_execution', 'parquet_integration']
            ))
        
        # Default fallback
        else:
            recommendations.append(BackendRecommendation(
                storage='postgresql',
                processing='postgresql',
                reason=f'Default choice for {scd_type} dimension with {update_freq} updates',
                confidence=0.6,
                backend_specific_features=['reliability', 'sql_compliance', 'mature_ecosystem']
            ))
        
        return recommendations
    
    def _analyze_fact_workload(self, entity: Dict[str, Any], workload: Dict[str, Any]) -> List[BackendRecommendation]:
        """Fact table workload analysis"""
        recommendations = []
        
        fact_type = entity.get('fact_type', 'transactional')
        volume = workload.get('volume', 'medium')
        insert_freq = workload.get('insert_frequency', 'batch')
        query_patterns = workload.get('query_patterns', [])
        retention = workload.get('retention_period', 'years')
        
        # ClickHouse: High-volume analytical facts
        if volume in ['high', 'very_high'] and 'aggregation' in query_patterns:
            recommendations.append(BackendRecommendation(
                storage='clickhouse',
                processing='clickhouse',
                reason=f'High-volume fact table ({volume}) optimized for aggregation queries',
                confidence=0.9,
                backend_specific_features=['columnar_aggregation', 'parallel_processing', 'compression']
            ))
        
        # Delta Lake: Streaming transaction facts
        elif entity.get('entity_type') == 'transaction_fact' and insert_freq in ['streaming', 'real_time']:
            recommendations.append(BackendRecommendation(
                storage='delta_lake',
                processing='spark',
                reason=f'Streaming transaction facts with {insert_freq} ingestion',
                confidence=0.85,
                backend_specific_features=['acid_streaming', 'schema_enforcement', 'time_travel']
            ))
        
        # Iceberg: Large batch ETL facts
        elif volume in ['high', 'very_high'] and insert_freq == 'batch':
            recommendations.append(BackendRecommendation(
                storage='iceberg',
                processing='spark',
                reason=f'Large batch fact processing with efficient file pruning',
                confidence=0.8,
                backend_specific_features=['efficient_pruning', 'parallel_scanning', 'schema_evolution']
            ))
        
        # PostgreSQL: Transactional facts with referential integrity
        elif fact_type == 'transactional' and workload.get('compliance_requirements'):
            recommendations.append(BackendRecommendation(
                storage='postgresql',
                processing='postgresql',
                reason=f'Transactional facts with compliance and referential integrity requirements',
                confidence=0.75,
                backend_specific_features=['foreign_keys', 'constraints', 'acid_compliance']
            ))
        
        return recommendations
    
    def _analyze_bridge_workload(self, entity: Dict[str, Any], workload: Dict[str, Any]) -> List[BackendRecommendation]:
        """Bridge table workload analysis"""
        
        volume = workload.get('volume', 'medium')
        query_patterns = workload.get('query_patterns', [])
        
        # PostgreSQL for complex many-to-many relationships
        if 'allocation' in query_patterns or volume in ['small', 'medium']:
            return [BackendRecommendation(
                storage='postgresql',
                processing='postgresql',
                reason='Bridge table with complex allocation logic and referential integrity',
                confidence=0.8,
                backend_specific_features=['foreign_keys', 'complex_joins', 'allocation_functions']
            )]
        
        return []
```

## Anti-Patterns to Avoid

### ❌ Don't Hardcode Business Domains
```python
# WRONG: Hardcoded for specific business domain
def create_customer_table():
    return """
    CREATE TABLE customers (
        customer_id INT,
        customer_name VARCHAR(100),
        -- Hardcoded customer-specific logic
    )"""

# ✅ RIGHT: Generic entity-driven approach
def create_dimension_table(entity_metadata: Dict[str, Any]):
    columns = generate_entity_columns(entity_metadata)
    return build_ddl_from_columns(entity_metadata['name'], columns)
```

### ❌ Don't Use Technology-Specific Patterns in Business Logic
```python
# WRONG: ClickHouse-specific logic in business layer
entity = {
    "name": "dim_customer",
    "engine": "ReplacingMergeTree",  # Technology leak!
    "order_by": "customer_id"        # Technology leak!
}

# ✅ RIGHT: Business-focused metadata
entity = {
    "name": "dim_customer",
    "entity_type": "dimension",
    "scd": "SCD2",
    "business_keys": ["customer_number"],
    "workload_characteristics": {
        "volume": "large",
        "query_patterns": ["lookup", "drill_down"]
    }
}
```

### ❌ Don't Skip Universal Validation
```python
# WRONG: Backend-specific validation only
if backend == 'postgresql':
    validate_postgresql_constraints(entity)
elif backend == 'clickhouse':
    validate_clickhouse_engine(entity)

# ✅ RIGHT: Universal validation + backend adaptation
validation_result = validate_dimensional_entity(entity)
if validation_result.is_valid:
    adapted_entity = backend_adapter.adapt_entity(entity)
    return backend.generate_ddl(adapted_entity)
```

## Testing Strategy

### Universal Entity Validation Tests
```python
def test_dimension_validation_any_domain():
    """Test dimension validation works for any business domain"""
    
    # Test with customer dimension
    customer_dim = {
        "name": "dim_customer",
        "entity_type": "dimension",
        "grain": "One row per customer per version",
        "scd": "SCD2",
        "business_keys": ["customer_number"]
    }
    
    # Test with product dimension  
    product_dim = {
        "name": "dim_product",
        "entity_type": "dimension", 
        "grain": "One row per product per version",
        "scd": "SCD2",
        "business_keys": ["product_code", "sku"]
    }
    
    # Same validation logic should work for both
    assert validate_dimensional_entity(customer_dim).is_valid
    assert validate_dimensional_entity(product_dim).is_valid

def test_fact_validation_any_business_process():
    """Test fact validation works for any business process"""
    
    # Test with sales facts
    sales_fact = {
        "name": "fact_sales",
        "entity_type": "fact",
        "grain": "One row per sales transaction",
        "measures": [
            {"name": "sales_amount", "additivity": "additive"},
            {"name": "quantity", "additivity": "additive"}
        ]
    }
    
    # Test with inventory facts
    inventory_fact = {
        "name": "fact_inventory",
        "entity_type": "fact", 
        "grain": "One row per product per day",
        "measures": [
            {"name": "quantity_on_hand", "additivity": "semi_additive"},
            {"name": "unit_cost", "additivity": "non_additive"}
        ]
    }
    
    # Same validation logic should work for both
    assert validate_dimensional_entity(sales_fact).is_valid
    assert validate_dimensional_entity(inventory_fact).is_valid
```

### Backend Adapter Tests
```python
def test_postgresql_adapter_any_dimension():
    """Test PostgreSQL adapter works with any dimension structure"""
    
    test_dimensions = [
        {"name": "dim_customer", "scd": "SCD2", "business_keys": ["customer_id"]},
        {"name": "dim_product", "scd": "SCD1", "business_keys": ["product_code"]},
        {"name": "dim_location", "scd": "SCD2", "business_keys": ["location_code", "region"]}
    ]
    
    adapter = PostgreSQLDimensionalAdapter()
    
    for dim in test_dimensions:
        adapted = adapter.adapt_scd2_pattern(dim)
        
        # Should always generate proper constraints
        assert "CONSTRAINT uk_" in str(adapted.get('constraints', []))
        assert "CREATE INDEX idx_" in str(adapted.get('indexes', []))
```

## Checklist for Payload-Agnostic Implementation

### ✅ Business Logic Layer
- [ ] Entity metadata drives all decisions, not hardcoded domains
- [ ] Universal validation rules apply regardless of business content
- [ ] Dimensional patterns generated dynamically from metadata
- [ ] Workload analysis considers characteristics, not specific use cases

### ✅ Backend Adaptation Layer  
- [ ] Adapters optimize for backend capabilities, not business domains
- [ ] Column generation works with any business column structure
- [ ] DDL generation handles arbitrary entity metadata
- [ ] Performance optimizations based on workload characteristics

### ✅ Testing Coverage
- [ ] Tests use multiple business domains to verify generality
- [ ] Validation tests work with arbitrary entity structures
- [ ] Backend adapters tested with various metadata configurations
- [ ] End-to-end tests cover different business scenarios

### ✅ Documentation
- [ ] Examples show multiple business domains
- [ ] Patterns focus on dimensional modeling, not specific business cases
- [ ] Implementation guides avoid domain-specific assumptions
- [ ] Backend selection criteria based on workload characteristics

---

**Last Updated**: October 6, 2025  
**Version**: 1.0  
**Status**: Implementation Guide