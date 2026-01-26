---
description: Code review workflow that analyzes recent changes and provides comprehensive review checklist
---

## Intelligent Code Review Workflow

### 1. Analyze changes for review

// turbo git log --oneline -5 git diff HEAD~5..HEAD --stat git diff HEAD~5..HEAD
--name-only

### 2. Check code quality metrics

// turbo

# Backend (Java) - uses built-in Maven plugins

cd swissql-backend && mvn compile -q cd swissql-backend && mvn test -q

# CLI (Go) - uses built-in Go tools

cd swissql-cli && go vet ./... cd swissql-cli && golangci-lint run ./...
2>/dev/null || go vet ./... cd swissql-cli && go test ./... -short -count=1

### 3. Analyze change patterns

// turbo git log --oneline -20 --no-merges git log --since="1 week ago"
--pretty=format:"%h %s" --no-merges

### 4. Generate review checklist

**Security Review:**

- [ ] No hardcoded credentials or API keys
- [ ] Proper input validation and sanitization
- [ ] SQL injection protection (parameterized queries)
- [ ] Authentication/authorization checks
- [ ] Sensitive data handling (encryption, masking)

**Code Quality:**

- [ ] Consistent naming conventions
- [ ] Proper error handling and logging
- [ ] No unused imports or variables
- [ ] Adequate code comments and documentation
- [ ] Test coverage for new functionality
- [ ] No code duplication (use existing helper functions)
- [ ] Reuse existing functions from config package (LoadProfiles, SaveProfiles,
      GetProfile, AddProfile)
- [ ] Extract common logic into shared helpers

**Architecture Review:**

- [ ] Follows established patterns and conventions
- [ ] No circular dependencies
- [ ] Proper separation of concerns
- [ ] API design consistency
- [ ] Database schema changes are backward compatible

**Performance Review:**

- [ ] No obvious performance bottlenecks
- [ ] Efficient database queries
- [ ] Proper resource cleanup (connections, files)
- [ ] Memory usage considerations
- [ ] Caching strategies where appropriate

### 5. Review specific file types

**Java Backend Changes:**

```java
// Check for:
- @Service, @RestController, @Component annotations
- Proper dependency injection
- Transaction management (@Transactional)
- Exception handling with @ControllerAdvice
- DTO vs Entity usage
- JDBC connection management (HikariCP)
```

**Go CLI Changes:**

```go
// Check for:
- Error handling patterns (if err != nil)
- Proper resource cleanup (defer)
- Interface usage for testability
- Logging consistency
- CLI command structure (cobra)
- Flag parsing with parseReplFlags helper
```

**Documentation Changes:**

```markdown
// Check for:

- README accuracy
- API documentation updates
- Example code correctness
- Installation instructions
- Troubleshooting guides
```

### 6. Generate review summary

**Review Categories:**

- **Critical**: Security issues, breaking changes, major bugs
- **Major**: Architecture violations, performance issues, missing tests
- **Minor**: Code style, documentation, naming conventions
- **Suggestions**: Improvements, optimizations, best practices

**Review Format:**

```
## Code Review Summary

### Critical Issues
- [Issue 1]: Description and recommendation

### Major Issues  
- [Issue 1]: Description and recommendation

### Minor Issues
- [Issue 1]: Description and recommendation

### Suggestions
- [Suggestion 1]: Description and benefit

### Positive Notes
- [Good practice 1]: Recognition of good implementation
```

### 7. Usage Examples

**Full review:**

```
/code-review-swissql
```

**Review last N commits:**

```
/code-review-swissql HEAD~3
```

## Review Best Practices

**For Reviewers:**

- Be constructive and specific in feedback
- Explain the "why" behind suggestions
- Recognize good implementation patterns
- Focus on maintainability and scalability
- Consider the broader codebase impact

**For Authors:**

- Provide clear commit messages
- Include tests for new functionality
- Update documentation as needed
- Address all review comments before merging
- Consider future maintenance implications

## Integration with CI/CD

**Pre-commit hooks:**

```bash
#!/bin/sh
cd swissql-cli && go fmt ./... && go vet ./...
cd swissql-backend && mvn compile -q
```

**PR templates:**

```markdown
## Code Review Checklist

- [ ] Security review completed
- [ ] Performance review completed
- [ ] Tests added/updated
- [ ] Documentation updated
- [ ] Breaking changes documented
```
