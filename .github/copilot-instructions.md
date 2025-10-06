# Unified Data Platform - AI Coding Agent Instructions

## Architecture Overview

This is a revolutionary **federated data platform** that separates storage backends from processing engines, enabling 48 possible combinations (8 storage × 6 processing). The platform combines dimensional modeling expertise with intelligent multi-backend technology selection.

### Core Components
- **Storage Layer**: `unified_platform/storage/` - 8 backends (PostgreSQL, ClickHouse, Iceberg, DuckDB, BigQuery, Snowflake, Delta Lake, Parquet)
- **Processing Layer**: `unified_platform/processing/` - 6 engines (Trino, Spark, Polars, DuckDB, PostgreSQL, ClickHouse)  
- **Orchestrator**: `unified_platform/orchestrator/platform.py` - Intelligent workload placement engine
- **SQL Foundation**: `examples/sql/` - PostgreSQL functions for DDL/DML generation with dimensional patterns

## Key Architectural Patterns

### 1. Abstract Backend Pattern
All storage/processing implementations inherit from abstract bases:
- `StorageBackend` in `storage/abstract_backend.py` 
- `ProcessingEngineBackend` in `processing/abstract_engine.py`

Each backend must implement: `generate_ddl()`, `generate_dml()`, `get_optimal_workloads()`, `supports_feature()`

### 2. Factory Pattern for Backend Selection
- `StorageBackendFactory` and `ProcessingEngineFactory` in respective `factory.py` files
- Includes intelligent recommendation methods like `recommend_engine_for_workload()`

### 3. Dimensional Modeling Intelligence
The platform enforces dimensional patterns (SCD2, fact tables, grain validation) across ALL backends:
- SCD2 dimensions automatically get: `valid_from`, `valid_to`, `is_current` columns
- Fact tables validate measure additivity: `additive`, `semi_additive`, `non_additive`
- Grain validation ensures consistent entity definitions

### 4. Workload-Based Intelligent Placement
Key decision logic in `examples/unified_platform_demo.py` class `DimensionalWorkloadAnalyzer`:
- **OLTP + Compliance**: PostgreSQL + PostgreSQL
- **High-volume Analytics**: ClickHouse + ClickHouse  
- **Batch ETL**: Iceberg + Spark
- **Real-time Streaming**: Delta Lake + Spark
- **Cross-system Analytics**: Iceberg + Trino

## Development Workflows

### Adding New Storage Backend
1. Create `unified_platform/storage/{backend_name}_backend.py`
2. Inherit from `StorageBackend` and implement required methods
3. Add to `StorageType` enum in `abstract_backend.py`
4. Register in `backend_factory.py`
5. Add dimensional pattern support (SCD2, fact tables)

### Adding New Processing Engine
1. Create `unified_platform/processing/{engine_name}_engine.py`
2. Inherit from `ProcessingEngineBackend`
3. Add to `ProcessingEngine` enum in `abstract_engine.py`
4. Register in `factory.py` with workload recommendations

### Testing Strategy
- **SQL Tests**: `examples/sql/tests/run_all_tests.sql` - PostgreSQL functions
- **Python Tests**: Use pytest with markers `@pytest.mark.unit`, `@pytest.mark.integration`, `@pytest.mark.slow`
- **Example Validation**: Run demo scripts in `examples/` to validate end-to-end functionality

## Project-Specific Conventions

### Metadata Structure
```python
# Standard entity metadata format
entity = {
    "name": "dim_customers",
    "entity_type": "dimension",  # dimension|fact|transaction_fact
    "grain": "One row per customer per version",
    "scd": "SCD2",
    "business_keys": ["customer_number"],
    "physical_columns": [...],
    "workload_characteristics": {
        "update_frequency": "daily",
        "compliance_requirements": ["GDPR"],
        "volume": "high",
        "query_patterns": ["aggregation", "drill_down"]
    }
}
```

### Backend Recommendation Pattern
```python
@dataclass
class BackendRecommendation:
    storage: str
    processing: str  
    reason: str
    confidence: float = 0.8
```

### Configuration Management
- Use `uv` for dependency management (pyproject.toml)
- Environment configs in `StorageConfig` and `ProcessingConfig` dataclasses
- Docker configs in `docker/` for each backend

## Critical Integration Points

### SQL Foundation Integration
- `unified_platform/schema_generator_client.py` bridges Python platform with PostgreSQL SQL functions
- SQL functions in `examples/sql/ddl_generator.sql` handle complex dimensional patterns
- Use `SELECT generate_ddl(jsonb_pipeline, table_name)` for production DDL

### Cross-Backend Query Translation
Each processing engine must handle queries across different storage backends:
- **Trino**: Federated queries using catalog.schema.table syntax
- **Spark**: DataFrameReader with format-specific options
- **Polars**: LazyFrame with scan_* methods for different formats

### Performance Optimization Patterns
- **Columnar Optimization**: ClickHouse and Parquet for analytical workloads
- **ACID Compliance**: PostgreSQL and Delta Lake for transactional requirements  
- **Schema Evolution**: Iceberg and Delta Lake for evolving data structures
- **Federated Analytics**: Trino for cross-system queries without data movement

## Common Pitfalls to Avoid

1. **Don't hardcode backend selection** - Always use workload analysis
2. **Don't ignore dimensional patterns** - SCD2 and fact validation is mandatory
3. **Don't mix storage concerns with processing** - Keep layers independent
4. **Don't skip grain validation** - Critical for dimensional model integrity
5. **Don't forget performance characteristics** - Each backend has optimal use cases

## Quick Commands

```bash
# Run all examples to validate platform
python examples/unified_platform_demo.py
python examples/architecture_demo.py
python examples/implementation_guide.py

# SQL testing (requires PostgreSQL)
psql -f examples/sql/tests/run_all_tests.sql

# Python testing
pytest tests/ -m "not slow"  # Unit tests only
pytest tests/ -m integration  # Integration tests
```

When implementing new features, always consider: dimensional modeling compliance, cross-backend compatibility, workload optimization, and the 48 possible storage/processing combinations.