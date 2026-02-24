# Codebase Structure

**Analysis Date:** 2024-03-21

## Directory Layout

```
swissql/
├── swissql-backend/     # Java/Spring Boot backend application
│   ├── src/
│   │   ├── main/
│   │   │   ├── java/com/swissql/
│   │   │   │   ├── api/          # Request/Response DTOs
│   │   │   │   ├── controller/   # REST API controllers
│   │   │   │   ├── driver/       # JDBC driver management
│   │   │   │   ├── model/        # Domain models
│   │   │   │   ├── sampler/      # Periodic data collection logic
│   │   │   │   ├── service/      # Business logic services
│   │   │   │   ├── util/         # Utility classes
│   │   │   │   └── web/          # Web-related components (filters, error handling)
│   │   │   └── resources/        # Configuration and application properties
│   │   └── test/                 # Backend tests
│   ├── jdbc_drivers/             # External JDBC JAR directory
│   ├── Dockerfile                # Container build configuration
│   └── pom.xml                   # Maven project configuration
├── swissql-cli/         # Go-based command line interface
│   ├── cmd/                      # Cobra command definitions and REPL logic
│   ├── internal/
│   │   ├── client/               # Backend API client
│   │   ├── config/               # CLI configuration (profiles, sessions)
│   │   ├── dbeaver/              # DBeaver project import logic
│   │   └── ui/                   # Terminal UI and display helpers
│   ├── go.mod                    # Go module definition
│   └── main.go                   # CLI entry point
├── design/              # Design documents and issue-related notes
├── .planning/           # GSD planning and codebase mapping (this directory)
├── README.md            # Project overview
└── AGENTS.md            # Agent-specific instructions
```

## Directory Purposes

**swissql-backend/src/main/java/com/swissql/service:**
- Purpose: Core business logic.
- Contains: `DatabaseService` (SQL execution), `SessionManager` (lifecycle), `AiSqlGenerateService` (AI logic).
- Key files: `DatabaseService.java`, `SessionManager.java`.

**swissql-backend/src/main/java/com/swissql/driver:**
- Purpose: Dynamic JDBC driver loading.
- Contains: `DriverRegistry`, `JdbcDriverAutoLoader`, `DriverShim`.
- Key files: `DriverRegistry.java`, `JdbcDriverAutoLoader.java`.

**swissql-cli/cmd:**
- Purpose: CLI command and REPL implementation.
- Contains: Command definitions, REPL loop, multi-line SQL handling, AI-specific REPL commands.
- Key files: `repl_loop.go`, `root.go`, `repl_commands_ai.go`, `connect.go`.

**swissql-cli/internal/client:**
- Purpose: API client for backend communication.
- Contains: `Client` struct, DTOs for the API, HTTP helper methods.
- Key files: `client.go`.

## Key File Locations

**Entry Points:**
- `swissql-backend/src/main/java/com/swissql/SwissqlBackendApplication.java`: Backend Spring Boot application entry.
- `swissql-cli/main.go`: CLI entry point.

**Configuration:**
- `swissql-backend/src/main/resources/application.properties`: Backend configuration.
- `swissql-cli/internal/config/config.go`: CLI configuration logic (uses `~/.swissql.yaml`).

**Core Logic:**
- `swissql-backend/src/main/java/com/swissql/service/DatabaseService.java`: Primary SQL execution logic.
- `swissql-cli/cmd/repl_loop.go`: Main REPL interactive loop.

**Testing:**
- `swissql-backend/src/test/java/com/swissql/`: Backend unit and integration tests.
- `swissql-cli/cmd/smoke_test.go`: CLI smoke tests.

## Naming Conventions

**Files:**
- Java: PascalCase (e.g., `DatabaseService.java`).
- Go: snake_case (e.g., `repl_loop.go`).

**Directories:**
- Java: package-style (lowercase).
- Go: package-style (lowercase, typically single word).

## Where to Add New Code

**New Backend API Endpoint:**
1. Define DTOs in `swissql-backend/src/main/java/com/swissql/api/`.
2. Add service methods in `swissql-backend/src/main/java/com/swissql/service/`.
3. Add endpoint to `SwissQLController.java` in `swissql-backend/src/main/java/com/swissql/controller/`.

**New CLI Command:**
1. Create new command file in `swissql-cli/cmd/`.
2. Register command in `root.go`.
3. Update `client.go` in `swissql-cli/internal/client/` if a new backend API is needed.

**New REPL Command:**
1. Add dispatch logic in `swissql-cli/cmd/repl_registry.go`.
2. Implement command handler in a new or existing `repl_commands_*.go` file.

**New Utility Helper:**
- Backend: `swissql-backend/src/main/java/com/swissql/util/`.
- CLI: `swissql-cli/internal/` (appropriate subpackage).

## Special Directories

**swissql-backend/jdbc_drivers:**
- Purpose: Contains external JDBC driver JAR files.
- Generated: No (manually populated).
- Committed: No (typically ignored, but can be mounted).

**swissql-backend/target:**
- Purpose: Build output for Maven.
- Generated: Yes.
- Committed: No.

---

*Structure analysis: 2024-03-21*
