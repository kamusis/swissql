---
description: Database testing workflow that validates database connectivity, queries, and schema compatibility
---

## Database Testing Workflow

### 1. Test database connectivity
// turbo
# Test connection to different database types
cd swissql-cli && ./swissql.exe connect --profile oracle_test
cd swissql-cli && ./swissql.exe connect --profile postgres_test
cd swissql-cli && ./swissql.exe connect --profile yashandb_test

# Verify backend status
curl -s "http://localhost:8080/v1/status" | jq .

# Test session management
curl -X POST "http://localhost:8080/v1/connect" -H "Content-Type: application/json" -d '{"dsn":"oracle://test:test@localhost:1521/XE"}'
```

### 2. Validate database schemas and metadata
// turbo
# Test metadata endpoints
curl -s "http://localhost:8080/v1/meta/list?session_id=test" | jq .
curl -s "http://localhost:8080/v1/meta/describe?session_id=test&table=USERS" | jq .
curl -s "http://localhost:8080/v1/meta/conninfo?session_id=test" | jq .

# Test schema compatibility across databases
cd swissql-cli && ./swissql.exe query "SELECT table_name FROM user_tables LIMIT 5"
cd swissql-cli && ./swissql.exe query "SELECT table_name FROM information_schema.tables LIMIT 5"
```

### 3. Test SQL execution and results
// turbo
# Basic SQL queries
curl -X POST "http://localhost:8080/v1/execute_sql" -H "Content-Type: application/json" -d '{"session_id":"test","sql":"SELECT 1 as test_column"}'

# Complex queries with joins
curl -X POST "http://localhost:8080/v1/execute_sql" -H "Content-Type: application/json" -d '{"session_id":"test","sql":"SELECT t.table_name, c.column_name FROM user_tables t JOIN user_tab_columns c ON t.table_name = c.table_name WHERE ROWNUM < 10"}'

# Parameterized queries
curl -X POST "http://localhost:8080/v1/execute_sql" -H "Content-Type: application/json" -d '{"session_id":"test","sql":"SELECT * FROM users WHERE id = :id","params":{"id":1}}'
```

### 4. Test collector functionality
// turbo
# List available collectors
curl -s "http://localhost:8080/v1/collectors/list?session_id=test" | jq .

# List collector queries
curl -s "http://localhost:8080/v1/collectors/queries?session_id=test&collector_id=oracle_top" | jq .

# Run collector
curl -X POST "http://localhost:8080/v1/collectors/run" -H "Content-Type: application/json" -d '{"session_id":"test","collector_id":"oracle_top"}'
```

### 5. Test sampler functionality
// turbo
# Create sampler
curl -X PUT "http://localhost:8080/v1/sessions/test/samplers/top_sampler" -H "Content-Type: application/json" -d '{"interval_seconds":10,"collector_id":"oracle_top"}'

# Get sampler status
curl -s "http://localhost:8080/v1/sessions/test/samplers/top_sampler" | jq .

# Get sampler snapshot
curl -s "http://localhost:8080/v1/sessions/test/samplers/top_sampler/snapshot" | jq .

# Stop sampler
curl -X DELETE "http://localhost:8080/v1/sessions/test/samplers/top_sampler"
```

### 6. Database compatibility testing

**Test Matrix:**
- **Oracle**: 12c, 19c, 21c, 23ai
- **PostgreSQL**: 12, 13, 14, 15
- **YashanDB**: Latest version
- **MySQL**: 8.0+
- **SQL Server**: 2019+

**Compatibility Tests:**
```sql
-- Basic data types
SELECT 
  1 as integer_col,
  'test' as varchar_col,
  SYSDATE as date_col,
  123.45 as decimal_col
FROM dual;

-- Database-specific features
-- Oracle: ROWNUM, DECODE, NVL
-- PostgreSQL: LIMIT, COALESCE, NULLIF
-- MySQL: LIMIT, IFNULL, REPLACE

-- Metadata queries
SELECT table_name, column_name, data_type
FROM user_tables t
JOIN user_tab_columns c ON t.table_name = c.table_name;
```

### 7. Performance and stress testing

**Connection Pool Testing:**
```bash
# Test concurrent connections
for i in {1..20}; do
  curl -X POST "http://localhost:8080/v1/connect" -d '{"dsn":"oracle://test:test@localhost:1521/XE"}" &
done
wait

# Test connection pool exhaustion
curl -X POST "http://localhost:8080/v1/connect" -d '{"dsn":"oracle://test:test@localhost:1521/XE"}'
```

**Query Performance:**
```bash
# Time query execution
time curl -X POST "http://localhost:8080/v1/execute_sql" -d '{"session_id":"test","sql":"SELECT COUNT(*) FROM large_table"}'

# Test large result sets
curl -X POST "http://localhost:8080/v1/execute_sql" -d '{"session_id":"test","sql":"SELECT * FROM large_table WHERE ROWNUM < 10000"}'
```

### 8. Error handling and edge cases

**Error Scenarios:**
- Invalid DSN/connection strings
- Network connectivity issues
- Database server down
- Invalid SQL syntax
- Permission denied errors
- Timeout scenarios

**Test Commands:**
```bash
# Invalid DSN
curl -X POST "http://localhost:8080/v1/connect" -d '{"dsn":"invalid://connection/string"}'

# Invalid SQL
curl -X POST "http://localhost:8080/v1/execute_sql" -d '{"session_id":"test","sql":"INVALID SQL"}'

# Non-existent table
curl -X POST "http://localhost:8080/v1/execute_sql" -d '{"session_id":"test","sql":"SELECT * FROM non_existent_table"}'
```

### 9. Generate test report

**Test Report Format:**
```
## Database Test Report

### Test Environment
- Oracle Database: 19c Enterprise Edition
- PostgreSQL: 15.3
- YashanDB: 23.2
- Java Version: 21
- Go Version: 1.21

### Connectivity Tests
- Oracle Connection: ✅ PASS
- PostgreSQL Connection: ✅ PASS  
- YashanDB Connection: ✅ PASS
- Connection Pool: ✅ PASS

### Functionality Tests
- Basic Queries: ✅ PASS
- Parameterized Queries: ✅ PASS
- Metadata Queries: ✅ PASS
- Collectors: ✅ PASS
- Samplers: ✅ PASS

### Performance Tests
- Query Response Time: < 100ms average
- Concurrent Connections: 20 successful
- Large Result Sets: 10,000 rows in < 5s

### Error Handling Tests
- Invalid DSN: ✅ Proper error message
- Invalid SQL: ✅ Proper error message
- Connection Timeout: ✅ Handled gracefully

### Issues Found
- [Issue 1]: Description and severity
- [Issue 2]: Description and severity

### Recommendations
- [Recommendation 1]: Action required
- [Recommendation 2]: Improvement suggestion
```

### 10. Usage Examples

**Full database test suite:**
```
/database-test
```

**Specific database only:**
```
/database-test oracle
/database-test postgres
/database-test yashandb
```

**Connectivity test only:**
```
/database-test connectivity
```

**Performance test:**
```
/database-test performance
```

**Error handling test:**
```
/database-test errors
```

## Test Data Setup

**Required Test Objects:**
```sql
-- Create test tables (manual execution required)
-- CREATE TABLE swissql_test (
--   id NUMBER PRIMARY KEY,
--   name VARCHAR2(100),
--   created_date DATE,
--   amount DECIMAL(10,2)
-- );

-- Insert test data (manual execution required)
-- INSERT INTO swissql_test VALUES (1, 'Test User 1', SYSDATE, 100.50);
-- INSERT INTO swissql_test VALUES (2, 'Test User 2', SYSDATE-1, 200.75);
```

**Cleanup:**
```sql
-- Cleanup (manual execution required)
-- DROP TABLE swissql_test;
```

## Continuous Integration

**CI Pipeline Integration:**
```yaml
database_test:
  script:
    - mvn spring-boot:run &
    - sleep 30
    - /database-test connectivity
    - /database-test functionality
    - /database-test performance
  artifacts:
    - database_test_report.html
```
