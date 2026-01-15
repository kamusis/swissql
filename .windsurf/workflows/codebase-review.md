---
description: Comprehensive codebase review workflow for analyzing entire project structure, architecture, and quality
---

## Codebase Review Workflow

### 1. Analyze codebase structure
// turbo
find . -type f -name "*.go" -o -name "*.java" -o -name "*.py" -o -name "*.js" -o -name "*.ts" | head -50
find . -type d -maxdepth 3 | grep -v node_modules | grep -v vendor | grep -v target | head -30
ls -la
cat package.json 2>/dev/null || cat pom.xml 2>/dev/null || cat go.mod 2>/dev/null || echo "No package manager file found"

### 2. Check codebase health metrics
// turbo
# Language-specific checks
if [ -f "go.mod" ]; then
  go vet ./...
  go fmt ./...
  go test ./... -v 2>&1 | head -50
fi

if [ -f "pom.xml" ]; then
  mvn spotbugs:check 2>&1 | head -50
  mvn checkstyle:check 2>&1 | head -50
fi

if [ -f "package.json" ]; then
  npm run lint 2>&1 | head -50 || echo "No lint script found"
  npm run test 2>&1 | head -50 || echo "No test script found"
fi

### 3. Analyze code patterns and duplication
// turbo
# Check for code duplication
find . -name "*.go" -o -name "*.java" | xargs wc -l | tail -10
grep -r "TODO\|FIXME\|XXX" --include="*.go" --include="*.java" --include="*.py" 2>/dev/null | head -20

# Check for large files
find . -type f \( -name "*.go" -o -name "*.java" -o -name "*.py" \) -exec wc -l {} + | sort -rn | head -10

### 4. Generate review checklist

**Security Review:**
- [ ] No hardcoded credentials, API keys, or secrets in code
- [ ] Proper input validation and sanitization throughout codebase
- [ ] SQL injection protection (parameterized queries)
- [ ] Authentication/authorization checks in place
- [ ] Sensitive data handling (encryption, masking, logging)
- [ ] Dependency security vulnerabilities checked
- [ ] Environment variables used for configuration
- [ ] No debug code or backdoors in production

**Code Quality:**
- [ ] Consistent naming conventions across codebase
- [ ] Proper error handling and logging patterns
- [ ] No unused imports or variables
- [ ] Adequate code comments and documentation
- [ ] Test coverage for critical functionality
- [ ] **No code duplication (use existing helper functions)**
- [ ] **Reuse existing functions from common packages**
- [ ] **Extract common logic into shared helpers**
- [ ] Code follows language-specific best practices
- [ ] No dead code or commented-out code blocks

**Architecture Review:**
- [ ] Clear separation of concerns (layers, modules)
- [ ] Follows established patterns and conventions
- [ ] No circular dependencies
- [ ] Proper dependency management
- [ ] API design consistency
- [ ] Database schema changes are backward compatible
- [ ] Configuration management is centralized
- [ ] Logging and monitoring strategy defined

**Performance Review:**
- [ ] No obvious performance bottlenecks
- [ ] Efficient database queries and indexing
- [ ] Proper resource cleanup (connections, files, memory)
- [ ] Caching strategies where appropriate
- [ ] Async/parallel processing for long-running tasks
- [ ] Pagination for large datasets
- [ ] No memory leaks or excessive allocations

**Documentation Review:**
- [ ] README.md is comprehensive and up-to-date
- [ ] API documentation exists and is accurate
- [ ] Installation/setup instructions are clear
- [ ] Contributing guidelines provided
- [ ] Code comments explain complex logic
- [ ] Architecture diagrams or design docs exist
- [ ] CHANGELOG or release notes maintained

**Testing Review:**
- [ ] Unit tests exist for core functionality
- [ ] Integration tests cover critical paths
- [ ] Test data is properly managed
- [ ] Tests are fast and reliable
- [ ] CI/CD pipeline runs tests automatically
- [ ] Test coverage is acceptable (>70% recommended)

**Dependency Review:**
- [ ] Dependencies are up-to-date and secure
- [ ] No unused dependencies
- [ ] License compatibility checked
- [ ] Dependency versions are pinned
- [ ] Vendor/lock files committed
- [ ] Dependency updates are tracked

### 5. Review by project type

**Backend Projects (Java/Go/Python/Node):**
- [ ] API endpoints follow RESTful conventions
- [ ] Request/response validation in place
- [ ] Proper HTTP status codes used
- [ ] Database transactions managed correctly
- [ ] Connection pooling configured
- [ ] Error handling is consistent
- [ ] Logging includes request IDs for tracing
- [ ] Health check endpoints implemented

**Frontend Projects (React/Vue/Angular):**
- [ ] Component structure is modular
- [ ] State management is appropriate
- [ ] API calls are centralized
- [ ] Error boundaries implemented
- [ ] Accessibility standards met (WCAG)
- [ ] Responsive design implemented
- [ ] Performance optimization (lazy loading, code splitting)
- [ ] Environment variables for configuration

**CLI Projects:**
- [ ] Command structure follows conventions
- [ ] Help text is comprehensive
- [ ] Error messages are actionable
- [ ] Input validation is robust
- [ ] Exit codes are appropriate
- [ ] Configuration file support
- [ ] Shell completion scripts provided

**Library Projects:**
- [ ] Public API is well-documented
- [ ] Semantic versioning followed
- [ ] Breaking changes documented
- [ ] Examples provided for common use cases
- [ ] Type definitions available (TypeScript/Go)
- [ ] No internal APIs exposed

### 6. Identify technical debt

**High Priority:**
- [ ] Security vulnerabilities
- [ ] Performance issues affecting users
- [ ] Broken tests or failing builds
- [ ] Outdated dependencies with known issues

**Medium Priority:**
- [ ] Code duplication
- [ ] Complex code that needs refactoring
- [ ] Missing tests for critical paths
- [ ] Inconsistent patterns

**Low Priority:**
- [ ] Code style inconsistencies
- [ ] Outdated comments
- [ ] Minor optimizations
- [ ] Nice-to-have features

### 7. Generate recommendations

**Immediate Actions (This Sprint):**
1.
2.
3.

**Short-term Actions (Next 2-4 Sprints):**
1.
2.
3.

**Long-term Actions (Next Quarter):**
1.
2.
3.

### 8. Summary metrics

- Total lines of code: ___
- Number of files: ___
- Test coverage: ___%
- Critical issues found: ___
- High priority technical debt: ___
- Medium priority technical debt: ___
