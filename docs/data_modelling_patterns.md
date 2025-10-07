# Dimensional Modeling Standards for Unified Platform

## Core Entity Types

### 1. **Dimension Tables**
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
    "physical_columns": [
        # Business descriptive attributes
        {"name": "business_key", "type": "varchar(50)", "nullable": False},
        {"name": "description", "type": "varchar(200)", "nullable": True},
        # SCD2 tracking (auto-added if scd=SCD2)
        # effective_date, expiration_date, is_current, row_version
    ]
}
```

### 2. **Fact Tables** (General Facts)
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
    ]
}
```

### 3. **Transaction Facts** (Event-Driven Facts)
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
    "derived_facts": [  # Optional: Aggregated facts built from this
        "fact_daily_sales", "fact_monthly_customer_summary"
    ]
}
```

### 4. **Bridge Tables** (Many-to-Many Resolution)
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
    ]
}
```

## Measure Additivity Types

### **Additive Measures**
- Can be summed across ALL dimensions
- Examples: sales_amount, quantity_sold, transaction_count
- Safe for any aggregation operation

### **Semi-Additive Measures**  
- Can be summed across SOME dimensions but not others (typically not time)
- Examples: account_balance, inventory_quantity
- Require special handling for time-based aggregations

### **Non-Additive Measures**
- Cannot be meaningfully summed across any dimension
- Examples: unit_price, percentage, ratio
- Require derived calculations (weighted averages, etc.)

## SCD (Slowly Changing Dimension) Types

### **SCD1 - Overwrite**
```python
scd1_pattern = {
    "scd": "SCD1",
    "change_behavior": "overwrite_previous_value",
    "history_preservation": False,
    "required_columns": ["updated_at"]  # Audit trail
}
```

### **SCD2 - Add New Record**
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
    ]
}
```

### **SCD3 - Add New Attribute**
```python
scd3_pattern = {
    "scd": "SCD3",
    "change_behavior": "store_previous_value", 
    "history_preservation": "limited",
    "required_columns": ["current_value", "previous_value", "change_date"]
}
```

## Payload-Agnostic Implementation Strategy

### 1. **Dynamic Column Generation**
```python
def generate_entity_columns(entity: Dict[str, Any]) -> List[str]:
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

def get_scd2_columns() -> List[Dict[str, Any]]:
    """Standard SCD2 tracking columns - works for any business entity"""
    return [
        {"name": "effective_date", "type": "timestamp", "nullable": False},
        {"name": "expiration_date", "type": "timestamp", "nullable": True, "default": "'9999-12-31'"},
        {"name": "is_current", "type": "boolean", "nullable": False, "default": "true"},
        {"name": "row_version", "type": "integer", "nullable": False, "default": "1"}
    ]
```

### 2. **Generic Dimensional Patterns**
```python
class DimensionalPatternGenerator:
    """Generate dimensional SQL for any business entity"""
    
    def create_dimension_table(self, entity: Dict[str, Any], backend: StorageBackend) -> str:
        """Create dimension table for any business entity"""
        
        table_name = entity['name']
        business_keys = entity.get('business_keys', [])
        scd_type = entity.get('scd', 'SCD1')
        columns = self.generate_entity_columns(entity)
        
        # Backend-specific DDL generation
        if backend.storage_type == StorageType.POSTGRESQL:
            return self._generate_postgresql_dimension(table_name, columns, business_keys, scd_type)
        elif backend.storage_type == StorageType.CLICKHOUSE:
            return self._generate_clickhouse_dimension(table_name, columns, business_keys, scd_type)
        # ... other backends
    
    def create_fact_table(self, entity: Dict[str, Any], backend: StorageBackend) -> str:
        """Create fact table for any business process"""
        
        table_name = entity['name']
        measures = entity.get('measures', [])
        dim_refs = entity.get('dimension_references', [])
        fact_type = entity.get('fact_type', 'transactional')
        
        # Generate appropriate fact table structure
        return self._generate_fact_ddl(table_name, measures, dim_refs, fact_type, backend)
```

### 3. **Workload-Based Backend Selection**
```python
class PayloadAgnosticWorkloadAnalyzer:
    """Select backends based on entity characteristics, not hardcoded rules"""
    
    def recommend_backend(self, entity: Dict[str, Any]) -> BackendRecommendation:
        """Recommend backend based on entity metadata"""
        
        entity_type = entity.get('entity_type')
        workload = entity.get('workload_characteristics', {})
        
        # Dimension backend selection
        if entity_type == 'dimension':
            return self._analyze_dimension_workload(entity, workload)
        
        # Fact backend selection  
        elif entity_type in ['fact', 'transaction_fact']:
            return self._analyze_fact_workload(entity, workload)
        
        # Bridge backend selection
        elif entity_type == 'bridge':
            return self._analyze_bridge_workload(entity, workload)
    
    def _analyze_dimension_workload(self, entity: Dict[str, Any], workload: Dict[str, Any]) -> BackendRecommendation:
        """Dimension-specific analysis based on actual metadata"""
        
        scd_type = entity.get('scd', 'SCD1')
        update_freq = workload.get('update_frequency', 'daily')
        compliance_reqs = workload.get('compliance_requirements', [])
        volume = workload.get('volume', 'medium')
        
        # ACID compliance + frequent updates = PostgreSQL
        if compliance_reqs and update_freq in ['real_time', 'hourly']:
            return BackendRecommendation(
                storage='postgresql',
                processing='postgresql',
                reason=f'ACID compliance for {scd_type} with {update_freq} updates'
            )
        
        # Large dimensions with time travel = Iceberg
        elif volume == 'large' and scd_type == 'SCD2':
            return BackendRecommendation(
                storage='iceberg', 
                processing='trino',
                reason=f'Time travel and scale for large {scd_type} dimension'
            )
        
        # Default for most dimensions
        else:
            return BackendRecommendation(
                storage='postgresql',
                processing='postgresql', 
                reason=f'Standard dimensional processing for {entity_type}'
            )
```

## Key Distinctions

### **Fact vs Transaction_Fact**
- **Fact**: Can be any aggregation level (daily, monthly summaries)
- **Transaction_Fact**: Always atomic business events with timestamps
- **Transaction_Fact** often feeds into aggregate **Fact** tables

### **Dimension Change Patterns**
- **SCD1**: Simple overwrites (no history)
- **SCD2**: Full history preservation (most common)
- **SCD3**: Limited history (previous value only)

### **Measure Types Drive Backend Selection**
- **High-volume additive measures** → ClickHouse (columnar aggregation)
- **Complex semi-additive measures** → PostgreSQL (advanced window functions)
- **Real-time non-additive ratios** → ClickHouse (materialized views)

## Validation Rules

### **Cross-Entity Consistency**
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

This documentation ensures we maintain dimensional modeling excellence while building truly payload-agnostic backends that work with any business entity structure.