---
description: Security audit workflow that analyzes code for security vulnerabilities and compliance issues
---

## Security Audit Workflow

### 1. Scan for hardcoded secrets and credentials
// turbo
# Scan for potential secrets in code
git grep -i "password\|secret\|key\|token" -- '*.go' '*.java' '*.properties' '*.yml' '*.yaml'
git grep -i "api[_-]key\|access[_-]key\|private[_-]key" -- '*'

# Check for common secret patterns
grep -r "sk-[a-zA-Z0-9]\{48\}" .
grep -r "ghp_[a-zA-Z0-9]\{36\}" .
grep -r "AKIA[0-9A-Z]{16}" .

# Check configuration files
find . -name "*.properties" -o -name "*.yml" -o -name "*.yaml" | xargs grep -l "password\|secret"
```

### 2. Dependency vulnerability scanning
// turbo
# Backend Maven dependency check
mvn -f swissql-backend/pom.xml org.owasp:dependency-check-maven:check

# Go dependency security scan
cd swissql-cli && go list -json -m all | nancy sleuth
cd swissql-cli && gosec ./...

# Check for known vulnerable versions
mvn -f swissql-backend/pom.xml versions:display-dependency-updates
cd swissql-cli && go list -u -m all
```

### 3. Code security analysis
// turbo
# Static code analysis
mvn -f swissql-backend/pom.xml spotbugs:check
mvn -f swissql-backend/pom.xml checkstyle:check

# Go security checks
cd swissql-cli && go vet ./...
cd swissql-cli && staticcheck ./...

# SQL injection vulnerability check
grep -r "fmt\.Sprintf.*SELECT\|strings\.Replace.*SELECT" -- '*.go'
grep -r "Statement\.execute\|createStatement" -- '*.java'
```

### 4. Authentication and authorization review

**Security Checklist:**
- [ ] No hardcoded credentials in source code
- [ ] Proper password encryption/hashing
- [ ] Secure credential storage (AES encryption)
- [ ] Session management and timeout
- [ ] Input validation and sanitization
- [ ] SQL injection protection
- [ ] HTTPS/TLS usage
- [ ] API rate limiting
- [ ] Error message sanitization
- [ ] Logging of security events

**Code Review Patterns:**
```java
// Check for proper parameter binding
PreparedStatement stmt = conn.prepareStatement("SELECT * FROM users WHERE id = ?");
stmt.setInt(1, userId);

// Check for proper error handling
try {
    // Database operations
} catch (SQLException e) {
    log.error("Database error occurred", e);
    // Don't expose internal details to users
}
```

```go
// Check for proper error handling
resp, err := http.Post(url, "application/json", body)
if err != nil {
    log.Printf("Request failed: %v", err)
    return
}
defer resp.Body.Close()
```

### 5. Network and communication security

**Security Checks:**
```bash
# Check for insecure HTTP usage
grep -r "http://" -- '*.go' '*.java' '*.properties' '*.yml'

# Check for TLS/SSL configuration
grep -r "tls\|ssl\|https" -- '*.go' '*.java' '*.properties' '*.yml'

# Check for hardcoded IPs and ports
grep -r "localhost\|127\.0\.0\.1\|0\.0\.0\.0" -- '*.go' '*.java' '*.properties'
```

**Configuration Security:**
```yaml
# Check for secure defaults
server:
  port: 8080
  ssl:
    enabled: true
    key-store: classpath:keystore.p12
```

### 6. Input validation and sanitization

**Validation Checks:**
```java
// Check for proper input validation
@Valid
@NotNull
private String sessionId;

@PostMapping("/connect")
public ResponseEntity<?> connect(@Valid @RequestBody ConnectRequest request) {
    // Validate input
}
```

```go
// Check for input sanitization
func validateSQL(sql string) error {
    // Basic SQL injection prevention
    if strings.Contains(strings.ToUpper(sql), "DROP") ||
       strings.Contains(strings.ToUpper(sql), "DELETE") {
        return errors.New("potentially dangerous SQL")
    }
    return nil
}
```

### 7. Logging and monitoring security

**Security Logging:**
```java
// Check for security event logging
log.warn("Failed login attempt for user: {}", username);
log.error("Security violation: SQL injection attempt detected");
log.info("User {} accessed sensitive data", userId);
```

```go
// Check for security monitoring
log.Printf("Security event: %s from IP %s", eventType, clientIP)
log.Printf("Database connection failed: %v", err)
```

### 8. Generate security audit report

**Report Format:**
```
## Security Audit Report

### Executive Summary
- Critical Issues: 2
- High Risk Issues: 5
- Medium Risk Issues: 8
- Low Risk Issues: 12
- Overall Security Score: 7.2/10

### Critical Security Issues
- [Issue 1]: Hardcoded database password in config file
  - File: `swissql-cli/config/connections.json`
  - Impact: Full database access compromise
  - Recommendation: Use encrypted credential storage

- [Issue 2]: SQL injection vulnerability in query builder
  - File: `swissql-backend/src/main/java/com/swissql/service/QueryService.java`
  - Impact: Arbitrary SQL execution
  - Recommendation: Use parameterized queries

### High Risk Issues
- [Issue 1]: Missing input validation on API endpoints
- [Issue 2]: Insecure HTTP usage in development mode
- [Issue 3]: Outdated dependencies with known vulnerabilities

### Medium Risk Issues
- [Issue 1]: Insufficient error message sanitization
- [Issue 2]: Missing security headers in HTTP responses
- [Issue 3]: Weak session management configuration

### Low Risk Issues
- [Issue 1]: Verbose error messages exposing internal details
- [Issue 2]: Missing rate limiting on API endpoints

### Dependency Vulnerabilities
- Spring Boot 3.1.5: 2 medium severity CVEs
- PostgreSQL Driver 42.6.0: 1 low severity CVE
- Go Module github.com/spf13/cobra: No vulnerabilities

### Recommendations
1. Implement encrypted credential storage
2. Add comprehensive input validation
3. Update all vulnerable dependencies
4. Enable HTTPS in all environments
5. Add security headers and rate limiting
6. Implement security event logging
7. Conduct regular security audits

### Compliance Status
- OWASP Top 10: 8/10 addressed
- GDPR Compliance: Partially compliant
- SOC 2: Not applicable
```

### 9. Usage Examples

**Full security audit:**
```
/security-audit
```

**Secrets scanning only:**
```
/security-audit secrets
```

**Dependency vulnerabilities:**
```
/security-audit dependencies
```

**Code security analysis:**
```
/security-audit code
```

**Network security:**
```
/security-audit network
```

### 10. Security best practices

**Development Guidelines:**
- Never commit credentials to version control
- Use environment variables for sensitive configuration
- Implement proper input validation and sanitization
- Use parameterized queries for all database operations
- Enable security headers and rate limiting
- Log security events appropriately
- Regularly update dependencies
- Conduct security testing before releases

**Configuration Security:**
```yaml
# Secure configuration example
spring:
  datasource:
    password: ${DB_PASSWORD:encrypted_value}
  security:
    require-ssl: true
    
server:
  ssl:
    enabled: true
    key-store: ${SSL_KEYSTORE_PATH}
    key-store-password: ${SSL_KEYSTORE_PASSWORD}
```

**CI/CD Integration:**
```yaml
security_scan:
  script:
    - /security-audit secrets
    - /security-audit dependencies
    - /security-audit code
  artifacts:
    - security_audit_report.html
  only:
    - master
    - develop
```
