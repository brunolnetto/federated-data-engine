# Dimensional Modeling Standards for Unified Platform

## Executive Summary

This document establishes the dimensional modeling standards for our unified data platform, ensuring consistent implementation of dimensional patterns across all storage backends while maintaining payload-agnostic flexibility.

**Key Principle**: Business logic drives dimensional patterns; technology selection optimizes implementation.

## Core Entity Types & Patterns

### 1. Dimension Tables

**Purpose**: Descriptive context for business processes  
**Grain**: One row per business entity (or per version in SCD2)  
**Key Characteristics**:
- Contain descriptive attributes (who, what, where, when context)
- Relatively small in size (thousands to millions of rows)
- Change slowly compared to facts
- Support drill-down and filtering in analytics

```python
dimension_entity = {
    "entity_type": "dimension",
    "grain": "One row per [business_entity] [per version if SCD2]",
    "scd": "SCD1|SCD2|SCD3|SCD6",  # Slowly Changing Dimension type
    "business_keys": ["natural_key_1", "natural_key_2"],  # Source system keys
    "change_tracking": "full|delta|timestamp",  # How changes are detected
    "workload_characteristics": {
        "update_frequency": "daily|hourly|real_time",
        "compliance_requirements": ["GDPR", "SOX", "HIPAA"],
        "volume": "small|medium|large|very_large",
        "query_patterns": ["lookup", "drill_down", "real_time_lookup"]
    },
    "physical_columns": [
        # Business descriptive attributes (payload-agnostic)
        {"name": "business_key", "type": "varchar(50)", "nullable": False},
        {"name": "description", "type": "varchar(200)", "nullable": True},
        # SCD2 tracking columns (auto-added if scd=SCD2)
        # effective_date, expiration_date, is_current, row_version
    ]
}
```

### 2. Fact Tables (General Facts)

**Purpose**: Store measurements at a specific grain  
**Grain**: Business process at specific dimensional intersection  
**Key Characteristics**:
- Large tables (millions to billions of rows)
- Contain measures (numeric facts)
- Foreign keys to dimensions
- Additive measures can be summed across any dimension

```python
fact_entity = {
    "entity_type": "fact",
    "grain": "One row per [business_process] at [dimensional_intersection]",
    "fact_type": "transactional|periodic_snapshot|accumulating_snapshot",
    "dimension_references": [
        {"dimension": "dim_customer", "fk_column": "customer_sk"},
        {"dimension": "dim_product", "fk_column": "product_sk"},
        {"dimension": "dim_time", "fk_column": "time_sk"}
    ],
    "measures": [
        {"name": "quantity", "type": "integer", "additivity": "additive"},
        {"name": "unit_price", "type": "decimal(10,2)", "additivity": "non_additive"},
        {"name": "extended_amount", "type": "decimal(12,2)", "additivity": "additive"}
    ],
    "workload_characteristics": {
        "volume": "high|very_high",
        "insert_frequency": "batch|streaming|real_time",
        "query_patterns": ["aggregation", "drill_down", "olap"],
        "retention_period": "months|years|indefinite"
    }
}
```

### 3. Transaction Facts (Event-Driven Facts)

**Purpose**: Capture individual business events as they occur  
**Grain**: One row per business event/transaction  
**Key Characteristics**:
- Finest level of detail
- Time-stamped events
- Often used as source for aggregated facts
- High insert volume, minimal updates

```python
transaction_fact_entity = {
    "entity_type": "transaction_fact", 
    "grain": "One row per [business_event]",
    "event_timestamp_column": "event_time",  # Required for transaction facts
    "event_type": "order_placed|payment_processed|shipment_sent",
    "dimension_references": [
        {"dimension": "dim_customer", "fk_column": "customer_sk"},
        {"dimension": "dim_product", "fk_column": "product_sk"}
    ],
    "measures": [
        {"name": "transaction_amount", "type": "decimal(10,2)", "additivity": "additive"}
    ],
    "workload_characteristics": {
        "volume": "very_high",
        "insert_frequency": "streaming|real_time",
        "query_patterns": ["time_series", "event_analysis", "stream_processing"],
        "partitioning_strategy": "time_based"
    },
    "derived_facts": [  # Optional: Aggregated facts built from this
        "fact_daily_sales", "fact_monthly_customer_summary"
    ]
}
```

### 4. Bridge Tables (Many-to-Many Resolution)

**Purpose**: Resolve many-to-many relationships between facts and dimensions  
**Grain**: One row per combination in the many-to-many relationship

```python
bridge_entity = {
    "entity_type": "bridge",
    "grain": "One row per [entity1] to [entity2] relationship",
    "bridge_type": "multi_valued_dimension|many_to_many_fact",
    "primary_dimension": "dim_account",
    "secondary_dimension": "dim_customer", 
    "allocation_factors": [  # For weighted allocations
        {"name": "allocation_percentage", "type": "decimal(5,2)"}
    ],
    "workload_characteristics": {
        "volume": "medium",
        "update_frequency": "daily|weekly",
        "query_patterns": ["allocation", "many_to_many_join"]
    }
}
```

## Measure Additivity Classification

### Additive Measures
- **Definition**: Can be summed across ALL dimensions
- **Examples**: sales_amount, quantity_sold, transaction_count
- **Implementation**: Safe for any aggregation operation
- **Backend Optimization**: Ideal for columnar stores (ClickHouse, Parquet)

### Semi-Additive Measures  
- **Definition**: Can be summed across SOME dimensions but not others (typically not time)
- **Examples**: account_balance, inventory_quantity
- **Implementation**: Require special handling for time-based aggregations
- **Backend Optimization**: Need window functions (PostgreSQL) or materialized views (ClickHouse)

### Non-Additive Measures
- **Definition**: Cannot be meaningfully summed across any dimension
- **Examples**: unit_price, percentage, ratio
- **Implementation**: Require derived calculations (weighted averages, etc.)
- **Backend Optimization**: Complex aggregation engines (PostgreSQL, Spark)

## SCD (Slowly Changing Dimension) Patterns

### SCD1 - Overwrite
```python
scd1_pattern = {
    "scd": "SCD1",
    "change_behavior": "overwrite_previous_value",
    "history_preservation": False,
    "required_columns": ["updated_at"],  # Audit trail
    "optimal_backends": ["postgresql", "clickhouse"],  # Simple updates
    "implementation_notes": "Use for dimensions where history is not important"
}
```

### SCD2 - Add New Record (Most Common)
```python
scd2_pattern = {
    "scd": "SCD2", 
    "change_behavior": "create_new_version",
    "history_preservation": True,
    "required_columns": [
        "effective_date",   # When this version became effective
        "expiration_date",  # When this version expired (or 9999-12-31)
        "is_current",       # Boolean flag for current version
        "row_version"       # Optional: version number
    ],
    "optimal_backends": {
        "postgresql": "Complex merge logic with constraints",
        "iceberg": "Native time travel capabilities", 
        "clickhouse": "ReplacingMergeTree with materialized views"
    }
}
```

### SCD3 - Add New Attribute
```python
scd3_pattern = {
    "scd": "SCD3",
    "change_behavior": "store_previous_value", 
    "history_preservation": "limited",
    "required_columns": ["current_value", "previous_value", "change_date"],
    "optimal_backends": ["postgresql"],  # Simple column additions
    "use_cases": "When only previous value matters"
}
```

## Unified Metadata with Backend-Specific Adaptation

### Core Principle: Same Business Logic, Optimized Implementation

The platform maintains **unified metadata** for business consistency while providing **backend-specific adapters** for optimal implementation.

```python
# Universal business entity definition
customer_dimension = {
    "entity_type": "dimension",
    "grain": "One row per customer per version",
    "scd": "SCD2",
    "business_keys": ["customer_number"],
    "workload_characteristics": {
        "update_frequency": "daily",
        "compliance_requirements": ["GDPR"],
        "volume": "large",
        "query_patterns": ["lookup", "drill_down"]
    }
}

# Backend-specific adaptations:
# PostgreSQL → ACID compliance with merge statements
# ClickHouse → ReplacingMergeTree with materialized views  
# Iceberg → Native time travel with hidden partitioning
```

### Backend-Specific Pattern Adaptations

#### PostgreSQL Dimensional Adapter
```python
class PostgreSQLDimensionalAdapter:
    """Optimizes dimensional patterns for PostgreSQL capabilities"""
    
    def adapt_scd2_pattern(self, entity):
        return {
            **entity,
            "constraints": [
                "CONSTRAINT uk_business_key_current UNIQUE (customer_number, is_current) WHERE is_current = true",
                "CONSTRAINT ck_valid_date_range CHECK (effective_date <= expiration_date)"
            ],
            "indexes": [
                "CREATE INDEX idx_customer_business_key ON dim_customers (customer_number)",
                "CREATE INDEX idx_customer_current ON dim_customers (is_current) WHERE is_current = true"
            ],
            "merge_strategy": "postgresql_merge_upsert"
        }
```

#### ClickHouse Dimensional Adapter
```python
class ClickHouseDimensionalAdapter:
    """Optimizes dimensional patterns for ClickHouse capabilities"""
    
    def adapt_scd2_pattern(self, entity):
        return {
            **entity,
            "engine": "ReplacingMergeTree(record_version)",
            "order_by": "customer_number, effective_date",
            "materialized_views": [
                """CREATE MATERIALIZED VIEW dim_customers_current AS
                   SELECT * FROM dim_customers FINAL WHERE is_current = 1"""
            ]
        }
```

#### Iceberg Dimensional Adapter
```python
class IcebergDimensionalAdapter:
    """Optimizes dimensional patterns for Iceberg capabilities"""
    
    def adapt_scd2_pattern(self, entity):
        return {
            **entity,
            "iceberg_features": {
                "time_travel": True,
                "schema_evolution": True,
                "partition_evolution": True
            },
            "partitioning": [
                "months(effective_date)",
                "bucket(10, customer_number)"
            ]
        }
```

## Workload-Based Backend Selection Matrix

### Dimension Workload Patterns

| Workload Characteristics | Recommended Backend | Reasoning |
|-------------------------|-------------------|-----------|
| **Compliance + SCD2 + Frequent Updates** | PostgreSQL + PostgreSQL | ACID transactions, complex merge logic, audit trails |
| **High Volume + Real-time Lookups** | ClickHouse + ClickHouse | Columnar storage, ReplacingMergeTree, materialized views |
| **Large Dimensions + Time Travel** | Iceberg + Trino | Native time travel, schema evolution, federated queries |
| **Simple SCD1 + High Throughput** | DuckDB + DuckDB | In-memory performance, simple updates |

### Fact Workload Patterns

| Workload Characteristics | Recommended Backend | Reasoning |
|-------------------------|-------------------|-----------|
| **High-Volume Analytics + Aggregations** | ClickHouse + ClickHouse | Columnar aggregations, parallel processing |
| **Streaming Transaction Facts** | Delta Lake + Spark | ACID streaming, schema enforcement |
| **Batch ETL + Large Scans** | Iceberg + Spark | Efficient file pruning, parallel scanning |
| **Cross-System Analytics** | Iceberg + Trino | Federated queries, no data movement |
| **Real-time OLAP** | ClickHouse + ClickHouse | Sub-second aggregations, materialized views |

## Payload-Agnostic Implementation Patterns

### Dynamic Column Generation
```python
def generate_entity_columns(entity: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Generate columns based on entity metadata, not hardcoded schemas"""
    
    base_columns = entity.get('physical_columns', [])
    entity_type = entity.get('entity_type')
    
    # Add dimensional pattern columns based on entity type
    if entity_type == 'dimension':
        scd_type = entity.get('scd', 'SCD1')
        if scd_type == 'SCD2':
            base_columns.extend(get_scd2_columns())
    
    elif entity_type in ['fact', 'transaction_fact']:
        base_columns.extend(get_fact_audit_columns())
        
        if entity_type == 'transaction_fact':
            event_ts_col = entity.get('event_timestamp_column', 'event_timestamp')
            base_columns.append({
                "name": event_ts_col, 
                "type": "timestamp", 
                "nullable": False
            })
    
    return base_columns
```

### Universal Validation Rules
```python
def validate_dimensional_model(entities: List[Dict[str, Any]]) -> ValidationResult:
    """Validate entire dimensional model for consistency"""
    
    issues = []
    
    # Ensure all fact foreign keys reference existing dimensions
    dimensions = {e['name'] for e in entities if e.get('entity_type') == 'dimension'}
    facts = [e for e in entities if e.get('entity_type') in ['fact', 'transaction_fact']]
    
    for fact in facts:
        for dim_ref in fact.get('dimension_references', []):
            if dim_ref['dimension'] not in dimensions:
                issues.append(f"Fact {fact['name']} references undefined dimension {dim_ref['dimension']}")
    
    # Validate grain consistency 
    for entity in entities:
        if not entity.get('grain'):
            issues.append(f"Entity {entity['name']} missing grain definition")
    
    return ValidationResult(issues)
```

## Implementation Guidelines

### 1. Always Start with Business Requirements
- Define grain before technology
- Identify entity type (dimension/fact/bridge) based on business purpose
- Determine SCD requirements based on business needs

### 2. Use Workload Characteristics for Backend Selection
- Don't hardcode backend choices
- Consider compliance, volume, update frequency, query patterns
- Allow multiple backend recommendations with confidence scores

### 3. Maintain Universal Validation
- Business rules apply regardless of storage technology
- Grain definition is mandatory
- Foreign key relationships must be valid
- SCD patterns must be complete

### 4. Optimize per Backend Capabilities
- Leverage each storage technology's strengths
- Adapt dimensional patterns to backend features
- Consider performance implications of implementation choices

### 5. Preserve Payload Agnosticism
- Don't hardcode specific business domains
- Generate patterns dynamically from metadata
- Support any column structure within dimensional constraints

## Future Considerations

### Schema Evolution Support
- Plan for business rule changes over time
- Support backward compatibility
- Handle dimensional model refactoring

### Cross-Backend Consistency
- Maintain referential integrity across backends
- Support federated queries when entities are split
- Handle data synchronization patterns

### Performance Optimization
- Monitor query patterns to validate backend selections
- Support backend migration as workloads evolve
- Implement adaptive optimization based on usage

---

**Last Updated**: October 6, 2025  
**Version**: 1.0  
**Status**: Living Document - Updated as patterns evolve