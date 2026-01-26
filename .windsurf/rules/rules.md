# Workspace Rules and Memories

This file consolidates the rules/memories that were visible in the current workspace.

## Workspace Rules
- User preference: new CLI commands must not hand-roll output with `fmt.Printf`/tab formatting. Route output through the unified CLI rendering pipeline so output format switching remains consistent.
- User preference: after every code change, run a compile/build step to verify. Backend: Maven; CLI: `go test ./...` and optionally `go build`.
- On this machine, if commands/instructions would use `pip`, prefer using `uv pip`.
- Mermaid diagrams: prefer minimal syntax `graph LR`/`graph TD`; avoid subgraphs, comments, HTML, parentheses in labels, multiline labels, complex shapes.
- Project naming convention: use snake_case consistently; avoid camelCase in flags/API fields.
- Design docs under `design/` are not tracked by git for now.
- After modifying SQL in collector YAML, validate SQL via appropriate DB MCP server when available.
- Top-mode design note: TUI mode, polling interval, `x` key fetches `/top/sqltext` then `/v1/meta/explain`, backend default sampling interval 10s, snapshot includes context/cpu/sessions/waits/topSql/io, no auth header, session_id required.

## Repository Agent Guidance (AGENTS.md)

### Architecture Overview

SwissQL is a monorepo with two main components:

- `swissql-cli/`: Go-based CLI (Go 1.23.x) using Cobra for commands
- `swissql-backend/`: Java 21 Spring Boot backend with JDBC/HikariCP

Critical Architecture Rule:

- DO NOT implement business logic in the CLI. All business logic must be designed and implemented as backend APIs first. The CLI is a thin client that calls backend APIs and renders results in the terminal.

### Build Commands

#### Backend (Java/Maven)

- Build (skip tests): `mvn -f swissql-backend/pom.xml -DskipTests package`
- Run backend locally: `mvn -f swissql-backend/pom.xml spring-boot:run`
- Run all tests: `mvn -f swissql-backend/pom.xml test`
- Run single test: `mvn -f swissql-backend/pom.xml test -Dtest=ClassName#methodName`

#### CLI (Go)

- Build CLI: `cd swissql-cli` then `go build -o swissql.exe .`
- Run tests: `go test ./...`
- Run single test: `go test -run TestName`

#### Verify Backend Status

- `curl http://localhost:8080/v1/status`

### Java Code Style (Backend)

- Package structure: `com.swissql.{controller|service|model|api|util|driver|sampler|web}`
- Directory names: kebab-case
- Imports order: `java.*`, `jakarta.*`, `org.springframework.*`, third-party, `com.swissql.*`
- Indentation: 4 spaces
- Braces: K&R
- Line length: typically under 120

#### Error handling

- Methods that interact with databases throw `SQLException`
- Use try-with-resources for `Connection`, `Statement`, `ResultSet`
- Log errors using `log.error()` / `log.info()`
- Return structured error responses via `ErrorResponse.builder()`

### Go Code Style (CLI)

- Packages: lowercase, single word
- Import grouping: stdlib, third-party, local
- Formatting: tabs (gofmt)

#### Error handling

- Return `error` as last return value
- Check errors immediately
- Wrap errors with context using `fmt.Errorf("...: %w", err)`

#### HTTP client

- Use `client.NewClient()` for backend communication
- Always close response bodies
- Parse JSON with `json.NewDecoder(body).Decode(&resp)`

### API Endpoints (Reference)

- `GET /v1/status`
- `POST /v1/connect`
- `POST /v1/disconnect?session_id=...`
- `POST /v1/execute_sql`

Collectors:

- `GET /v1/collectors/list?session_id=...`
- `GET /v1/collectors/queries?session_id=...&collector_id=...`
- `POST /v1/collectors/run`

Samplers:

- `PUT /v1/sessions/{session_id}/samplers/{sampler_id}`
- `DELETE /v1/sessions/{session_id}/samplers/{sampler_id}`
- `GET /v1/sessions/{session_id}/samplers`
- `GET /v1/sessions/{session_id}/samplers/{sampler_id}`
- `GET /v1/sessions/{session_id}/samplers/{sampler_id}/snapshot`

Metadata:

- `GET /v1/meta/describe`
- `GET /v1/meta/list`
- `GET /v1/meta/conninfo`
- `POST /v1/meta/explain`
- `GET /v1/meta/completions`

AI Features:

- `POST /v1/ai/generate`
- `GET /v1/ai/context`
- `POST /v1/ai/context/clear`

Drivers:

- `GET /v1/meta/drivers`
- `POST /v1/meta/drivers/reload`

### Session management

- Sessions are created via `/v1/connect` and return a `session_id`
- Sessions expire after a configurable timeout
- CLI maintains a local registry of named sessions







