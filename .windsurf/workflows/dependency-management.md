---
description: Intelligent dependency management workflow that analyzes project dependencies and provides upgrade recommendations
---

## Intelligent Dependency Management Workflow

### 1. Detect project type and dependency files
// turbo
# Detect Java/Maven projects
if [ -f "pom.xml" ]; then
  echo "Java/Maven project detected"
  PROJECT_TYPE="maven"
fi

# Detect Go modules  
if [ -f "go.mod" ]; then
  echo "Go module project detected"
  PROJECT_TYPE="go"
fi

# Ensure required tools are available
if [ "$PROJECT_TYPE" = "maven" ]; then
  command -v mvn >/dev/null 2>&1 || { echo "Maven not found"; exit 1; }
fi
if [ "$PROJECT_TYPE" = "go" ]; then
  command -v go >/dev/null 2>&1 || { echo "Go not found"; exit 1; }
fi

### 2. Analyze current dependencies
// turbo
# Backend (Java/Maven)
if [ "$PROJECT_TYPE" = "maven" ]; then
  mvn dependency:tree
  mvn versions:display-dependency-updates
fi

# CLI (Go)
if [ "$PROJECT_TYPE" = "go" ]; then
  go list -m -u all
  go mod tidy
fi

### 3. Check for security vulnerabilities
// turbo
# Backend
if [ "$PROJECT_TYPE" = "maven" ]; then
  mvn org.owasp:dependency-check-maven:check
fi

# CLI (if gosec is available)
if [ "$PROJECT_TYPE" = "go" ]; then
  command -v gosec >/dev/null 2>&1 && gosec ./...
fi

### 4. Analyze dependency age and maintenance
// turbo
# Check Go module ages
if [ "$PROJECT_TYPE" = "go" ]; then
  go list -m -json all | grep -E "(ModulePath|Version|Time)"
fi

# Check Maven dependency versions
if [ "$PROJECT_TYPE" = "maven" ]; then
  mvn help:effective-pom | grep -A 5 -B 5 "<version>"
fi

### 4. Generate upgrade recommendations

**Criteria for Upgrades:**
- **Major versions**: Breaking changes, require testing
- **Minor versions**: New features, generally safe
- **Patch versions**: Bug fixes, always recommended
- **Security updates**: Critical, immediate action required

**Priority Levels:**
- **High**: Security vulnerabilities, deprecated dependencies
- **Medium**: Outdated major versions, performance improvements
- **Low**: Minor version bumps, documentation updates

### 5. Create upgrade plan

**Backend (Maven) Upgrades:**
```xml
<!-- Example: Update Spring Boot -->
<dependency>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter</artifactId>
    <version>3.2.0</version> <!-- from 3.1.5 -->
</dependency>
```

**CLI (Go) Upgrades:**
```bash
# Example: Review upgrade options (manual execution required)
# go get github.com/spf13/cobra@v1.8.0
# go mod tidy
```

### 5. Generate specific upgrade commands

**Based on the analysis above, here are the exact commands to execute:**

**Critical Security Updates (execute immediately):**
```bash
# Go module security updates
if [ "$PROJECT_TYPE" = "go" ]; then
  go get github.com/spf13/cobra@v1.8.0  # Replace with actual vulnerable dependency
  go mod tidy
fi

# Maven dependency security updates
if [ "$PROJECT_TYPE" = "maven" ]; then
  mvn versions:use-latest-releases
  mvn versions:commit
fi
```

**Recommended Upgrades (execute after testing):**
```bash
# Go module updates
if [ "$PROJECT_TYPE" = "go" ]; then
  go get -u ./...
  go mod tidy
fi

# Maven dependency updates
if [ "$PROJECT_TYPE" = "maven" ]; then
  mvn versions:use-latest-versions
  mvn versions:commit
fi
```

**Verification Commands (run after updates):**
```bash
# Verify Go modules
if [ "$PROJECT_TYPE" = "go" ]; then
  go mod verify
  go build -o [binary-name] .
fi

# Verify Maven dependencies
if [ "$PROJECT_TYPE" = "maven" ]; then
  mvn dependency:analyze
  mvn compile
fi
```

**Review the commands above, then execute them in your preferred order.**

### 6. Verify dependency integrity
// turbo
# Check Go module consistency
if [ "$PROJECT_TYPE" = "go" ]; then
  go mod verify
fi

# Check Maven dependency tree
if [ "$PROJECT_TYPE" = "maven" ]; then
  mvn dependency:analyze
fi

# Quick health checks
if [ "$PROJECT_TYPE" = "go" ]; then
  go vet ./...
fi

if [ "$PROJECT_TYPE" = "maven" ]; then
  mvn compile
fi

### 7. Generate execution report

**Report Format:**
```
## Dependency Management Report

### Project Analysis
- Project Type: [Java/Maven or Go Module]
- Tools Used: [mvn, go]
- Lock Files: [go.sum, Maven dependencies]

### Security Scan Results
- Vulnerabilities Found: [number]
- High Priority: [number]
- Medium Priority: [number]
- Low Priority: [number]

### Generated Commands
#### Critical Security Updates
[Copy-paste ready commands for immediate execution]

#### Recommended Upgrades  
[Copy-paste ready commands for planned execution]

#### Verification Commands
[Copy-paste ready commands for post-update verification]

### Upgrade Recommendations
- Critical Security Updates: [list]
- Recommended Upgrades: [list]
- Optional Updates: [list]

### Verification Status
- Go Modules: ✅ Verified (if applicable)
- Maven Dependencies: ✅ Verified (if applicable)
- Build Status: ✅ Successful

### Next Steps
1. Copy and execute critical security updates
2. Test in development environment
3. Apply recommended upgrades
4. Run verification commands
5. Commit changes in separate branch
```

### 8. Usage Examples

**Full analysis:**
```
/dependency-management
```

**Security focused:**
```
/dependency-management security
```

**Upgrade planning:**
```
/dependency-management upgrade
```

## Implementation Notes

- Always test upgrades in a separate branch
- Check for breaking changes in major version updates
- Run full test suite after dependency upgrades
- Document any required code changes due to API updates
- Consider using dependency management tools like Renovate or Dependabot for automated updates
