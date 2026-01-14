---
description: Intelligent database access workflow that routes requests to appropriate MCP servers with optimal tool selection
---

## Intelligent Database Access Workflow

### 1. Analyze user request and identify database type

**Database Type Detection:**
- **PostgreSQL**: Keywords like "supabase", "postgres", "postgresql", "pg"
- **YashanDB**: Keywords like "yashandb", "yasdb", "oracle-compatible"
- **Oracle**: Keywords like "oracle", "oci", "tns"
- **MySQL**: Keywords like "mysql", "mariadb"
- **Generic SQL**: When no specific database mentioned

**Request Intent Analysis:**
- **Query**: "select", "show", "list", "get", "find"
- **Analysis**: "explain", "analyze", "plan", "performance"
- **Schema**: "describe", "schema", "structure", "tables"
- **Health**: "health", "status", "metrics", "monitoring"
- **DDL**: "create", "alter", "drop", "migration"
- **DML**: "insert", "update", "delete"

### 2. Select optimal MCP server and tool

**PostgreSQL Database (prefer Supabase MCP server for PostgreSQL-like database access):**
```yaml
Read Operations:
  - list_tables: List all tables
  - list_extensions: List database extensions
  - execute_sql: Custom SELECT queries
  - get_logs: Database logs (api, auth, storage)
  - get_project_url: Get project URL

Write Operations (explicit user request required):
  - apply_migration: DDL operations
  - execute_sql: INSERT/UPDATE/DELETE
```

**YashanDB/Oracle (prefer YashanDB MCP server for YashanDB access):**
```yaml
Read Operations:
  - list_schemas: List database schemas
  - list_objects: List tables, views, sequences
  - get_object_details: Get table/view structure
  - get_top_queries: Get performance metrics
  - analyze_db_health: Comprehensive health check
  - explain_query: Execution plan analysis
  - analyze_query_indexes: Index recommendations
  - execute_sql: SELECT queries

Write Operations (explicit user request required):
  - execute_sql: DML/DDL statements
```

### 3. Execute command with safety checks

**Safety Validation:**
```yaml
Read-Only Intent: ✅ Auto-execute
  - SELECT queries
  - Schema queries
  - Performance analysis
  - Health checks
  - Explain plans

Write Intent: ⚠️ Require explicit confirmation
  - INSERT/UPDATE/DELETE
  - CREATE/ALTER/DROP
  - Migration execution
  - Index modifications
```

**Error Handling:**
```yaml
Connection Issues:
  - Check MCP server availability
  - Verify database connectivity
  - Validate credentials/permissions

Query Issues:
  - Validate SQL syntax
  - Check table/object existence
  - Handle permission errors
  - Manage timeout scenarios

Data Issues:
  - Handle large result sets
  - Format output appropriately
  - Respect privacy/sensitivity
```

### 4. Format and present results

**Result Formatting:**
```yaml
Tabular Data:
  - Use markdown tables for structured data
  - Limit rows to prevent overwhelming output
  - Include row count and query summary

Schema Information:
  - Show object hierarchy
  - Include data types and constraints
  - Format with proper indentation

Performance Metrics:
  - Present key metrics clearly
  - Include recommendations
  - Highlight critical issues

Error Messages:
  - Provide clear, actionable error descriptions
  - Suggest troubleshooting steps
  - Include relevant context
```

### 5. Usage Examples

**Simple Query:**
```
"Show me all users in Supabase"
→ Supabase: execute_sql("SELECT * FROM users LIMIT 10")
```

**Performance Analysis:**
```
"Analyze query performance in YashanDB"
→ YashanDB: analyze_query_indexes(["SELECT * FROM large_table WHERE id = 1"])
```

**Schema Information:**
```
"List all tables in PostgreSQL"
→ Supabase: list_tables()
```

**Health Check:**
```
"Check database health"
→ YashanDB: analyze_db_health("all")
```

**Complex Query:**
```
"Get top 5 customers by order value"
→ Supabase: execute_sql("SELECT c.*, SUM(o.total) as total_spent FROM customers c JOIN orders o ON c.id = o.customer_id GROUP BY c.id ORDER BY total_spent DESC LIMIT 5")
```

### 6. Advanced Features

**Query Optimization:**
```yaml
Index Analysis:
  - Identify missing indexes
  - Suggest index improvements
  - Analyze index usage patterns

Performance Tuning:
  - Execution plan analysis
  - Bottleneck identification
  - Resource usage optimization
```

**Schema Management:**
```yaml
Documentation:
  - Generate schema documentation
  - Create entity relationship diagrams
  - Export schema definitions

Validation:
  - Check schema consistency
  - Validate constraint integrity
  - Identify orphaned objects
```

**Monitoring Integration:**
```yaml
Metrics Collection:
  - Query performance metrics
  - Resource usage tracking
  - Error rate monitoring

Alerting:
  - Performance threshold alerts
  - Error condition notifications
  - Resource utilization warnings
```

### 7. Best Practices

**Security:**
- Never expose sensitive credentials
- Validate all user inputs
- Use parameterized queries when possible
- Respect data privacy regulations

**Performance:**
- Use appropriate query limits
- Optimize for result set size
- Cache frequently accessed data
- Monitor query execution time

**Usability:**
- Provide clear, concise results
- Include relevant context
- Suggest next actions
- Handle edge cases gracefully

### 8. Troubleshooting Guide

**Common Issues:**
```yaml
Connection Failed:
  - Check MCP server status
  - Verify database credentials
  - Confirm network connectivity
  - Validate database URL

Query Errors:
  - Check SQL syntax
  - Verify table/column names
  - Confirm user permissions
  - Review data type compatibility

Performance Issues:
  - Analyze execution plans
  - Check for missing indexes
  - Monitor resource usage
  - Optimize query structure
```

**Error Recovery:**
```yaml
Automatic Recovery:
  - Retry failed connections
  - Fallback to alternative queries
  - Use cached results when available

Manual Intervention:
  - Provide specific error messages
  - Suggest corrective actions
  - Offer alternative approaches
```

## Implementation Notes

- Always prefer specialized tools over generic execute_sql
- Implement proper error handling and user feedback
- Maintain security best practices for all operations
- Provide clear documentation for complex operations
- Support both read-only and write operations with appropriate safeguards