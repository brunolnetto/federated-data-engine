# Architectural Decision Record: Unified Metadata with Backend-Specific Adaptation

## Status: Accepted

**Date**: October 6, 2025  
**Decision Makers**: Core Architecture Team  
**Stakeholders**: Data Engineering, Analytics, Platform Engineering

## Context

We faced a fundamental architectural question: Should we maintain the same metadata modeling across all storage backends in our unified data platform, or should each backend have its own metadata schema?

This decision directly impacts:
- Developer experience and consistency
- Platform maintainability and evolution
- Performance optimization capabilities
- Business logic consistency across technologies

## Decision

**We will implement unified metadata with backend-specific adaptation layers.**

The platform will maintain a single, universal metadata schema for business entities (dimensions, facts, bridges) while providing intelligent backend-specific adapters that optimize implementation for each storage technology.

## Rationale

### Arguments For Unified Metadata

1. **Business Logic is Technology-Agnostic**
   - A customer dimension has the same grain regardless of storage in PostgreSQL vs ClickHouse
   - SCD2 requirements (history tracking, current flags) are business rules, not technical concerns
   - Dimensional modeling patterns are proven methodologies independent of technology

2. **Consistent Developer Experience**
   - Single metadata schema to learn and maintain
   - Business analysts can define entities once, deploy anywhere
   - Reduces cognitive overhead and training requirements

3. **Workload-Driven Technology Selection**
   - Backend selection should be based on workload characteristics, not arbitrary choices
   - Same entity might need different backends for different use cases
   - Enables intelligent platform optimization over time

4. **Future-Proof Architecture**
   - New storage backends can be added without changing business logic
   - Migration between backends becomes metadata-driven
   - Platform evolution doesn't break existing business definitions

### Arguments Against (Considered but Rejected)

1. **Backend-Specific Optimization Concerns**
   - *Counter-argument*: Optimization handled by adaptation layers, not different schemas
   - *Solution*: Backend-specific adapters optimize implementation while preserving business consistency

2. **Performance Trade-offs**
   - *Counter-argument*: Proper adaptation can leverage each backend's strengths better than separate schemas
   - *Solution*: Workload analysis drives backend selection for optimal performance

3. **Complexity of Abstraction**
   - *Counter-argument*: Complexity is contained in adapter layer, business logic remains simple
   - *Solution*: Well-defined interfaces between universal metadata and backend adapters

## Implementation Strategy

### 1. Universal Metadata Schema
```python
# Single source of truth for business entities
entity_metadata = {
    "entity_type": "dimension|fact|transaction_fact|bridge",
    "grain": "Business grain definition",
    "business_keys": ["natural_keys"],
    "scd": "SCD1|SCD2|SCD3",
    "workload_characteristics": {
        "update_frequency": "daily|hourly|real_time",
        "compliance_requirements": ["GDPR", "SOX"],
        "volume": "small|medium|large|very_large",
        "query_patterns": ["lookup", "aggregation", "time_series"]
    },
    "physical_columns": [
        # Payload-agnostic column definitions
    ]
}
```

### 2. Backend-Specific Adaptation
```python
class DimensionalPatternAdapter(ABC):
    @abstractmethod
    def adapt_scd2_pattern(self, entity: Dict[str, Any]) -> Dict[str, Any]:
        """Adapt SCD2 pattern to backend capabilities"""
        pass
    
    @abstractmethod  
    def adapt_fact_partitioning(self, entity: Dict[str, Any]) -> Dict[str, Any]:
        """Adapt fact table partitioning to backend strengths"""
        pass

# PostgreSQL: ACID compliance + complex merge logic
# ClickHouse: ReplacingMergeTree + materialized views
# Iceberg: Time travel + schema evolution
```

### 3. Intelligent Workload Analysis
```python
class WorkloadAnalyzer:
    def recommend_backend(self, entity: Dict[str, Any]) -> BackendRecommendation:
        """Select optimal backend based on workload characteristics"""
        
        # Compliance + SCD2 + Frequent Updates → PostgreSQL
        # High Volume + Analytics → ClickHouse  
        # Large Dimensions + Time Travel → Iceberg
        # Batch ETL + Cross-system → Iceberg + Trino
```

## Consequences

### Positive Consequences

1. **Unified Developer Experience**
   - Single metadata schema to learn
   - Consistent business logic across all backends
   - Simplified training and onboarding

2. **Optimal Performance**
   - Backend selection driven by workload analysis
   - Each storage technology used for its strengths
   - Performance optimization without business logic changes

3. **Platform Evolution**
   - New backends can be added seamlessly
   - Existing entities can be migrated between backends
   - Technology decisions can evolve with business needs

4. **Maintainability**
   - Business logic centralized in metadata
   - Backend-specific code isolated in adapters
   - Clear separation of concerns

### Negative Consequences

1. **Initial Complexity**
   - Requires sophisticated adapter layer
   - More complex than simple backend-specific schemas
   - Higher upfront development investment

2. **Abstraction Overhead**
   - Additional layer between business logic and storage
   - Potential for leaky abstractions
   - Debugging complexity across adaptation layers

### Mitigation Strategies

1. **Comprehensive Testing**
   - Universal validation rules tested across all backends
   - Backend-specific adapter tests for each storage technology
   - End-to-end integration tests for complete workflows

2. **Clear Documentation**
   - Dimensional modeling standards (documented separately)
   - Backend adapter implementation guides
   - Workload analysis decision trees

3. **Incremental Implementation**
   - Start with core backends (PostgreSQL, ClickHouse)
   - Add additional backends iteratively
   - Validate approach before full platform expansion

## Alternative Approaches Considered

### Alternative 1: Backend-Specific Metadata Schemas
- **Pros**: Direct optimization, simpler implementation per backend
- **Cons**: Inconsistent business logic, developer confusion, maintenance overhead
- **Verdict**: Rejected due to business logic fragmentation

### Alternative 2: Lowest Common Denominator Approach
- **Pros**: Simple unified schema, easy implementation
- **Cons**: Cannot leverage backend strengths, poor performance
- **Verdict**: Rejected due to performance limitations

### Alternative 3: Configuration-Driven Backend Selection
- **Pros**: Explicit control, predictable behavior
- **Cons**: Manual optimization, no intelligence, static selections
- **Verdict**: Rejected in favor of intelligent workload analysis

## Success Metrics

### Technical Metrics
- **Backend Selection Accuracy**: >90% of workloads deployed to optimal backend
- **Performance Consistency**: <10% performance degradation vs native implementation
- **Schema Evolution**: Support for backward-compatible changes

### Developer Experience Metrics
- **Learning Curve**: New developers productive within 1 week
- **Metadata Consistency**: Zero business logic discrepancies across backends
- **Development Velocity**: 50% faster entity deployment vs backend-specific approach

### Operational Metrics
- **Workload Migration**: Support for zero-downtime backend migration
- **Platform Reliability**: 99.9% uptime across all backend adapters
- **Maintenance Overhead**: <20% additional complexity vs single-backend systems

## Related Decisions

- [ADR-001: Multi-Backend Storage Architecture](/docs/adr/001-multi-backend-storage.md)
- [ADR-003: Dimensional Modeling Patterns](/docs/adr/003-dimensional-modeling.md)
- [ADR-005: Workload-Based Optimization](/docs/adr/005-workload-optimization.md)

## References

- [Dimensional Modeling Standards](/docs/dimensional_modeling_standards.md)
- [Backend Adapter Implementation Guide](/docs/backend_adapter_guide.md)
- [Workload Analysis Framework](/docs/workload_analysis.md)

---

**Status**: Accepted  
**Last Review**: October 6, 2025  
**Next Review**: January 6, 2026