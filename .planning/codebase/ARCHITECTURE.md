# Architecture

**Analysis Date:** 2024-03-21

## Pattern Overview

**Overall:** Client-Server with REPL Frontend

**Key Characteristics:**
- **Decoupled Frontend/Backend:** The CLI (`swissql-cli`) and the backend (`swissql-backend`) communicate via a REST API.
- **Session-Based State:** Backend maintains database connection state across requests using a `sessionId`.
- **Dynamic Resource Loading:** JDBC drivers and Sampler configurations are loaded dynamically at runtime.

## Layers

**CLI Frontend (Go):**
- Purpose: Provides a user-friendly REPL and command-line interface.
- Location: `swissql-cli/`
- Contains: Cobra commands, REPL loop, API client, configuration management.
- Depends on: Backend REST API.
- Used by: End users.

**Backend API Layer (Java/Spring Boot):**
- Purpose: Exposes database operations and AI capabilities over HTTP.
- Location: `swissql-backend/src/main/java/com/swissql/controller/`
- Contains: Spring RestControllers, Request/Response DTOs.
- Depends on: Service layer.
- Used by: CLI Frontend.

**Service Layer (Business Logic):**
- Purpose: Implements core logic for session management, database execution, and AI integration.
- Location: `swissql-backend/src/main/java/com/swissql/service/`
- Contains: `DatabaseService`, `SessionManager`, `AiSqlGenerateService`.
- Depends on: Driver registry, Connection pooling (HikariCP).

**Infrastructure/Driver Layer:**
- Purpose: Manages JDBC connections and dynamic driver loading.
- Location: `swissql-backend/src/main/java/com/swissql/driver/`
- Contains: `DriverRegistry`, `JdbcDriverAutoLoader`, `DriverShim`.
- Depends on: External JDBC JARs.

## Data Flow

**Query Execution Flow:**

1. User enters SQL or meta-command in `swissql-cli` REPL.
2. CLI sends a POST request to `/v1/execute_sql` (or `/v1/meta/*`) on the backend with the current `sessionId`.
3. Backend's `SwissQLController` receives the request and calls `DatabaseService`.
4. `DatabaseService` retrieves the `HikariDataSource` for the session (creating it if necessary).
5. SQL is executed against the database via JDBC.
6. Results are processed into a JSON-serializable `ExecuteResponse` (tabular or text).
7. `AiContextService` records the execution for future AI context.
8. CLI receives the response and renders it to the terminal.

**AI SQL Generation Flow:**

1. User enters `/ai <prompt>` in the CLI.
2. CLI calls `/v1/ai/generate` with the prompt, `dbType`, and `sessionId`.
3. Backend calls `AiSqlGenerateService` which uses `AiContextService` to gather recent query history.
4. Prompt and context are sent to an LLM (via Portkey).
5. Backend returns the generated SQL statements.
6. CLI displays the SQL and asks for confirmation.
7. Upon confirmation, CLI calls `/v1/execute_sql` for each statement.

## Key Abstractions

**SessionInfo:**
- Purpose: Tracks database connection details, options, and expiration for a user session.
- Examples: `swissql-backend/src/main/java/com/swissql/model/SessionInfo.java`
- Pattern: State Object.

**DriverRegistry:**
- Purpose: Central registry for all available JDBC drivers, both built-in and dynamically loaded.
- Examples: `swissql-backend/src/main/java/com/swissql/driver/DriverRegistry.java`
- Pattern: Registry.

**Sampler/Collector:**
- Purpose: Periodic background data collection from databases.
- Examples: `swissql-backend/src/main/java/com/swissql/sampler/SamplerManager.java`
- Pattern: Observer/Scheduled Task.

## Entry Points

**CLI Entry Point:**
- Location: `swissql-cli/main.go`
- Triggers: User execution of `swissql` command.
- Responsibilities: Initializes Cobra root command and subcommands.

**Backend Entry Point:**
- Location: `swissql-backend/src/main/java/com/swissql/SwissqlBackendApplication.java`
- Triggers: Execution of the JAR or Docker container.
- Responsibilities: Starts the Spring Boot application and initializes services.

## Error Handling

**Strategy:** Centralized error handling with specific API error codes.

**Patterns:**
- **Global Exception Handler:** `swissql-backend/src/main/java/com/swissql/web/GlobalExceptionHandler.java` catches service-level exceptions and returns `ErrorResponse`.
- **Trace IDs:** Every response (success or error) includes a `traceId` for log correlation.
- **Client-Side Wrapping:** `swissql-cli/internal/client/client.go` wraps API errors into Go `error` types with detailed information.

## Cross-Cutting Concerns

**Logging:** Slf4j with Logback in the backend; standard `fmt` and `log` in the CLI. Uses MDC for trace ID propagation.
**Validation:** Jakarta Bean Validation in the backend for API requests.
**Authentication:** Currently session-based via `sessionId`. Credentials for databases are managed by the CLI and sent to the backend during `/connect`.

---

*Architecture analysis: 2024-03-21*
