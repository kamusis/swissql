---
description: Performance testing workflow that benchmarks application performance and identifies bottlenecks
---

## Performance Testing Workflow

### 1. Baseline performance measurement
// turbo
# Backend startup time
time mvn -f swissql-backend/pom.xml spring-boot:run &
sleep 10
curl -w "@curl-format.txt" -o /dev/null -s "http://localhost:8080/v1/status"

# CLI startup time
cd swissql-cli && time ./swissql.exe --help

# Database connection test
cd swissql-cli && time ./swissql.exe connect --profile test_profile
```

### 2. Load testing setup
// turbo
# Install/load testing tools
curl -s 'http://localhost:8080/v1/execute_sql' -X POST -H "Content-Type: application/json" -d '{"session_id":"test","sql":"SELECT 1"}' > /dev/null

# Concurrent connection test
for i in {1..10}; do
  curl -s "http://localhost:8080/v1/status" &
done
wait

### 3. Memory and resource profiling
// turbo
# JVM memory analysis
jstat -gc $(jps | grep -i swissql | cut -d' ' -f1) 5s 10

# Go memory profiling
cd swissql-cli && go build -race -o swissql-profile.exe .
cd swissql-cli && GODEBUG=gctrace=1 ./swissql-profile.exe --help

# System resource usage
top -p $(pgrep -f swissql) -b -n 1
```

### 4. Database performance testing
// turbo
# Connection pool testing
curl -X POST "http://localhost:8080/v1/connect" -H "Content-Type: application/json" -d '{"dsn":"oracle://test:test@localhost:1521/XE"}'

# Query performance test
curl -X POST "http://localhost:8080/v1/execute_sql" -H "Content-Type: application/json" -d '{"session_id":"test","sql":"SELECT COUNT(*) FROM all_tables"}'

# Explain plan analysis
curl -X POST "http://localhost:8080/v1/meta/explain" -H "Content-Type: application/json" -d '{"session_id":"test","sql":"SELECT * FROM large_table WHERE id = 1"}'
```

### 5. Performance metrics collection

**Key Metrics:**
- **Response Time**: API endpoint latency (p50, p95, p99)
- **Throughput**: Requests per second, concurrent connections
- **Resource Usage**: CPU, memory, disk I/O, network I/O
- **Database**: Connection pool usage, query execution time
- **Error Rate**: Failed requests, timeouts, exceptions

**Benchmark Categories:**
```bash
# API Benchmarks
curl -w "@curl-format.txt" -o /dev/null -s "http://localhost:8080/v1/status"
curl -w "@curl-format.txt" -o /dev/null -s -X POST "http://localhost:8080/v1/connect" -d '{"dsn":"..."}'

# Database Benchmarks  
time curl -X POST "http://localhost:8080/v1/execute_sql" -d '{"session_id":"...","sql":"..."}'

# CLI Benchmarks
time ./swissql.exe connect --profile test
time ./swissql.exe query "SELECT 1"
```

### 6. Performance analysis and optimization

**Backend Optimization:**
```java
// Check for:
- Connection pool configuration (HikariCP settings)
- SQL query optimization (indexes, execution plans)
- Memory usage (object creation, garbage collection)
- Thread pool configuration
- Cache usage patterns
```

**CLI Optimization:**
```go
// Check for:
- HTTP client timeout settings
- JSON parsing efficiency
- Memory allocation patterns
- Concurrent goroutine usage
- I/O buffer sizes
```

### 7. Generate performance report

**Report Format:**
```
## Performance Test Report

### System Configuration
- CPU: [cores, architecture]
- Memory: [total, available]
- Java Version: [version]
- Go Version: [version]

### Baseline Metrics
- API Response Time: p50=50ms, p95=200ms, p99=500ms
- Throughput: 1000 req/s
- Memory Usage: 512MB heap, 256MB non-heap

### Load Test Results
- Concurrent Connections: 100
- Response Time Under Load: p50=100ms, p95=800ms, p99=2000ms
- Error Rate: 0.1%
- Resource Usage: CPU 80%, Memory 1GB

### Performance Issues Identified
- [Issue 1]: Description and impact
- [Issue 2]: Description and impact

### Recommendations
- [Optimization 1]: Expected improvement
- [Optimization 2]: Expected improvement
```

### 8. Usage Examples

**Full performance test:**
```
/performance-test
```

**API performance only:**
```
/performance-test api
```

**Database performance:**
```
/performance-test database
```

**CLI performance:**
```
/performance-test cli
```

**Load testing:**
```
/performance-test load
```

## Performance Testing Tools

**Required Tools:**
- `curl` - HTTP request testing
- `ab` (Apache Benchmark) - Load testing
- `jstat` - JVM statistics
- `go test -bench` - Go benchmarking
- `pprof` - Go profiling

**Optional Tools:**
- `wrk` - Modern HTTP benchmarking
- `gatling` - Advanced load testing
- `visualvm` - Java profiling
- `prometheus/grafana` - Monitoring

## Performance Targets

**SLA Targets:**
- API Response Time: < 100ms (p95)
- CLI Startup Time: < 2 seconds
- Database Connection: < 5 seconds
- Memory Usage: < 1GB for backend
- Error Rate: < 0.01%

**Alert Thresholds:**
- Response Time: > 500ms (p95)
- CPU Usage: > 90%
- Memory Usage: > 2GB
- Error Rate: > 1%
