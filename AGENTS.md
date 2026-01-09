# AGENTS.md - Guide for Agentic Coding Agents

This file provides essential information for agentic coding agents working in this repository.

## Architecture Overview

SwissQL is a monorepo with two main components:

- **swissql-cli/**: Go-based CLI (Go 1.23.x) using Cobra for commands
- **swissql-backend/**: Java 21 Spring Boot backend with JDBC/HikariCP

**Critical Architecture Rule**: DO NOT implement business logic in the CLI. All business logic must be designed and implemented as backend APIs first. The CLI is a thin client that calls backend APIs and renders results in the terminal.

---

## Build Commands

### Backend (Java/Maven)

```bash
# Build (skip tests)
mvn -f swissql-backend/pom.xml -DskipTests package

# Run backend locally
mvn -f swissql-backend/pom.xml spring-boot:run

# Run all tests
mvn -f swissql-backend/pom.xml test

# Run single test
mvn -f swissql-backend/pom.xml test -Dtest=ClassName#methodName
# Example: mvn test -Dtest=DatabaseServiceTest#testConnection
```

### CLI (Go)

```bash
# Build CLI
cd swissql-cli
go build -o swissql.exe .

# Run tests
go test ./...

# Run single test
go test -run TestName
# Example: go test -run TestCLI_HelpSmoke
```

### Verify Backend Status
```bash
curl http://localhost:8080/v1/status
```

---

## Java Code Style (Backend)

### Packages
- Package structure: `com.swissql.{controller|service|model|api|util|driver|sampler|web}`
- Directory names: kebab-case (e.g., `swissql-backend/src/main/java/com/swissql/service/`)
- Imports order: java.*, jakarta.*, org.springframework.*, third-party, com.swissql.*

### Naming Conventions
- Classes: PascalCase (`DatabaseService`, `SwissQLController`)
- Methods: camelCase (`initializeSession`, `metaDescribe`)
- Fields: camelCase (private fields often prefixed with `this.`)
- Constants: UPPER_SNAKE_CASE (`MAX_LOB_CHARS`, `MAX_STRING_CHARS`)
- Interfaces: PascalCase, often with `Interface` suffix or functional

### Formatting
- Indentation: 4 spaces (editorconfig may show tabs)
- Line length: typically under 120 characters
- Braces: K&R style (opening brace on same line)

### Lombok Usage
```java
@Slf4j                    // SLF4J logger
@Data                     // Getters, setters, equals, hashCode, toString
@Builder                  // Builder pattern
@AllArgsConstructor       // All-args constructor
@NoArgsConstructor        // No-args constructor
@Valid                    // Jakarta validation
```

### Error Handling
- Methods that interact with databases throw `SQLException`
- Use try-with-resources for Connection, Statement, ResultSet
- Log errors using `log.error()` or `log.info()` with relevant context
- Return structured error responses via `ErrorResponse.builder()`

### Dependencies
- Constructor injection for Spring beans
- Private final fields for injected dependencies
- Use `@Service`, `@RestController`, `@Component` annotations

### API Response Structure
Controllers return `ResponseEntity<?>` with consistent error handling:
- Success: `ResponseEntity.ok(response)`
- Errors: `ResponseEntity.status(HttpStatus.BAD_REQUEST).body(ErrorResponse.builder()...)`

---

## Go Code Style (CLI)

### Packages
- Package names: lowercase, single word (`cmd`, `client`, `config`)
- Directory structure mirrors package structure
- Import grouping: stdlib, third-party (github.com/*), local (github.com/kamusis/swissql/*)

### Naming Conventions
- Exported functions/types: PascalCase (`NewClient`, `ConnectRequest`)
- Unexported functions/types: camelCase (`connectCmd`, `buildHikariConfig`)
- Struct fields: PascalCase for exported, camelCase for unexported
- JSON tags: lowercase with underscores (`json:"session_id"`)

### Formatting
- Indentation: tabs (gofmt standard)
- Use `go fmt` to format files before committing
- Line length: typically under 100-120 characters

### Error Handling
- Functions return `error` as last return value
- Check errors immediately: `if err != nil { return err }`
- Use `defer` for cleanup (closing connections, bodies, etc.)
- Wrap errors with context: `fmt.Errorf("failed to connect: %w", err)`

### CLI Commands (Cobra)
- Commands defined in `swissql-cli/cmd/`
- Use `var cmdName = &cobra.Command{...}` pattern
- Register with `rootCmd.AddCommand(cmdName)`
- Use `init()` for flag registration

### HTTP Client
- Use `client.NewClient()` for backend communication
- Response bodies must be closed: `defer body.Close()`
- Parse JSON with `json.NewDecoder(body).Decode(&resp)`

### Testing
- Tests use `t.Helper()` for helper functions
- Test names: `Test{FunctionName}` or `Test{Feature}_{Scenario}`
- Use table-driven tests for multiple cases

---

## Database Connections

### Supported Databases
- Oracle (ojdbc11) - supports TNS_ADMIN for wallet-based connections
- PostgreSQL (postgresql)

### DSN Format
```
oracle://user:password@host:port/serviceName?TNS_ADMIN=/path/to/wallet
postgres://user:password@host:5432/database
```

### Backend Connection Pooling
- HikariCP for connection pooling
- Default pool size: 5 max, 1 min idle
- Sessions tracked via `SessionManager`

---

## API Endpoints (Reference)

### Core Endpoints
- `GET /v1/status` - Health check
- `POST /v1/connect` - Create database session
- `POST /v1/disconnect?session_id=...` - Terminate session
- `POST /v1/execute_sql` - Execute SQL

### Metadata
- `GET /v1/meta/describe` - Describe table/view
- `GET /v1/meta/list` - List tables/views
- `GET /v1/meta/conninfo` - Connection info
- `POST /v1/meta/explain` - Execution plan
- `GET /v1/meta/completions` - Autocomplete

### AI Features
- `POST /v1/ai/generate` - Generate SQL from natural language
- `GET /v1/ai/context` - Get recent SQL context
- `POST /v1/ai/context/clear` - Clear AI context

### Drivers
- `GET /v1/meta/drivers` - List loaded JDBC drivers
- `POST /v1/meta/drivers/reload` - Reload drivers from directory

---

## Session Management

- Sessions are created via `/v1/connect` and return a `session_id`
- Sessions expire after a configurable timeout
- CLI maintains a local registry of named sessions (tmux-like)
- Use `attach`, `ls`, `kill` commands to manage sessions

---

## Testing Notes

- Backend tests are in `swissql-backend/src/test/` (if present)
- CLI tests use standard Go testing package
- Smoke test: `swissql --help` to verify CLI wiring

---

## Environment Variables

- `SPRING_PROFILES_ACTIVE` - Spring profile (e.g., `local`)
- `PORTKEY_API_KEY`, `PORTKEY_VIRTUAL_KEY`, `PORTKEY_MODEL` - AI gateway configuration
- JDBC connection params via DSN or HikariCP config properties
