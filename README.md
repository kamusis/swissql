# SwissQL 🛠️

SwissQL is a developer-focused, modern “database Swiss Army knife” that provides a unified CLI experience for connecting to and querying different databases through a lightweight backend service.

The MVP uses an **HTTP/JSON (REST)** protocol between the CLI and backend to keep local development and debugging simple, with a planned evolution path toward streaming and higher-performance protocols (e.g., gRPC).

## What this project does

- A **Go-based CLI** that provides a convenient command interface for database workflows.
- A **Java 21 backend API** that manages connections and executes SQL against target databases via **JDBC**.
- A forward-looking design for **AI-assisted SQL generation** and **MCP-based extensibility** (plugin-style routing), while keeping JDBC as the core execution path.

## High-level architecture

- **CLI (Go)**
  - Command engine built with Cobra.
  - Communicates with the backend via HTTP/JSON.
- **Backend service (Java 21, Spring Boot)**
  - REST API layer.
  - Connection/session management and JDBC execution.
  - Uses HikariCP for JDBC connection pooling.
  - Packs common database drivers in the service (Oracle and PostgreSQL are included in the Maven dependencies).

```text
User -> swissql-cli (Go) -> HTTP/JSON -> swissql-backend (Java/Spring Boot) -> JDBC -> Database
```

## Quick Start
```
# ./swissql connect "postgres://postgres:postgres@localhost:5433/postgres"
Connecting to postgres://postgres:postgres@localhost:5433/postgres via backend http://localhost:8080...
Connected successfully! Session ID: d76c7b90-8dd9-40e9-9ea0-28c2e615afe1
SwissQL REPL (Session: d76c7b90-8dd9-40e9-9ea0-28c2e615afe1)
Type 'help' for commands. Use 'detach' to leave without disconnecting.
Type 'exit' or 'quit' to disconnect and remove this session.
Use '/ai <prompt>' to generate SQL via backend and confirm before execution.
swissql> help
Commands:

[CLI]
  help                          Show this help
  detach                        Leave REPL without disconnecting (like tmux detach)
  exit | quit                   Disconnect backend session and remove it from registry
  set display wide|narrow       Toggle truncation mode for tabular output
  set display expanded on|off   Expanded display mode
  set display width <n>         Set max column width for tabular output
  set output table|csv|tsv|json Set output format

[psql-compat (\)]
  \conninfo                    Show current session and backend information
  \d <name> (alias: desc)       Describe a table/view
  \d+ <name> (alias: desc+)     Describe with more details
  \dt | \dv                     List tables/views
  \explain <sql> (alias: explain, explain plan for)
                               Show execution plan
  \explain analyze <sql> (alias: explain analyze)
                               Show actual execution plan (executes the statement)
  \i <file> (alias: @<file>)    Execute statements from a file
  \x [on|off]                   Expanded display mode (like psql \\x)
  \o <file>                     Redirect query output to a file
  \o                            Restore output to stdout

[AI (/)]
  /ai <prompt>                  Generate SQL via AI and confirm before execution
  /context show                 Show recent executed SQL context used by AI
  /context clear                Clear AI context

Notes:
  - End a statement with ';' to execute
swissql>
```

## Current MVP capabilities

The backend currently implements the following REST endpoints:

- **Health**
  - `GET /v1/status`
- **Sessions**
  - `POST /v1/connect` (returns `session_id`)
  - `POST /v1/disconnect?session_id=...`
  - `GET /v1/sessions/validate?session_id=...`
- **SQL execution**
  - `POST /v1/execute`
- **Metadata helpers (psql-like commands)**
  - `GET /v1/meta/list?session_id=...&kind=table|view&schema=...`
  - `GET /v1/meta/describe?session_id=...&name=...&detail=full`
  - `GET /v1/meta/conninfo?session_id=...`
  - `POST /v1/meta/explain` (supports `analyze`)
- **Autocomplete / completions**
  - `GET /v1/meta/completions?session_id=...&kind=schema|table|column&schema=...&table=...&prefix=...`
- **AI assistance**
  - `POST /v1/ai/generate` (generates SQL JSON; does not execute)
  - `GET /v1/ai/context?session_id=...&limit=...`
  - `POST /v1/ai/context/clear`

The CLI currently provides an interactive REPL with the following commands:

- **CLI**
  - `help` (show help)
  - `detach` (leave REPL without disconnecting)
  - `exit | quit` (disconnect backend session and remove it from registry)
  - `set display wide|narrow` (toggle truncation mode)
  - `set display expanded on|off` (expanded display mode)
  - `set display width <n>` (set max column width)
  - `set output table|csv|tsv|json` (set output format)
- **psql-compat (\\)**
  - `\conninfo` (show current session and backend information)
  - `\d <name>` (alias: `desc`) (describe a table/view)
  - `\d+ <name>` (alias: `desc+`) (describe with more details)
  - `\dt | \dv` (list tables/views)
  - `\explain <sql>` (aliases: `explain`, `explain plan for`) (show execution plan)
  - `\explain analyze <sql>` (alias: `explain analyze`) (show actual execution plan; executes the statement)
  - `\i <file>` (alias: `@<file>`) (execute statements from a file)
  - `\x [on|off]` (expanded display mode)
  - `\o <file>` (redirect query output to a file)
  - `\o` (restore output to stdout)
- **AI (/)**
  - `/ai <prompt>` (generate SQL via AI and confirm before execution)
  - `/context show` (show recent executed SQL context used by AI)
  - `/context clear` (clear AI context)

## **Architecture Principles**

This repository is intentionally structured as a multi-client architecture (today: CLI; future: GUI).

The most important rule:

- **DO NOT IMPLEMENT BUSINESS LOGIC IN THE CLI !!** All business logic must be designed and implemented as backend APIs first. The CLI is only a thin client responsible for calling backend APIs and presenting results in the terminal.

This keeps the domain logic centralized and makes it straightforward to add additional clients (for example a GUI) without re-implementing logic.

## Developer setup (local)

### Prerequisites

- **Go**: 1.23.x (see `swissql-cli/go.mod`)
- **Java**: 21 (see `swissql-backend/pom.xml`)
- **Maven**: 3.8+ recommended
- Access to a target database (e.g., Oracle or PostgreSQL) and credentials.

### Clone the repository

```bash
git clone <your-github-repo-url>
```

### Run the backend (Spring Boot)

From the repository root, build and run:

```bash
mvn -f swissql-backend/pom.xml -DskipTests package
mvn -f swissql-backend/pom.xml spring-boot:run
```

The backend should start on localhost (see Spring Boot defaults / project configuration).

You can verify it via:

```bash
curl http://localhost:8080/v1/status
```

### Build and run the CLI

Build the CLI:

```bash
cd swissql-cli

go build -o swissql.exe .
```

Run:

```bash
./swissql.exe --help
```

### Typical local workflow

- Start the backend service.
- Use the CLI commands to:
  - Connect to a DB (backend creates a session).
  - Execute SQL using the returned session.

### AI setup (optional)

The backend can generate SQL from natural language via an OpenAI-compatible gateway (Portkey by default). If AI is not configured, the endpoint still exists but returns an “AI generation is disabled” response.

Where to store configuration locally:

- Option A (recommended for local dev): use `swissql-backend/src/main/resources/application-local.properties` (gitignored) and keep secrets out of Git.
  - A committed template is available at `swissql-backend/src/main/resources/application-example.properties`.
- Option B: export environment variables in your shell before starting the backend.

Enable the `local` Spring profile (PowerShell) before starting the backend:

```powershell
$env:SPRING_PROFILES_ACTIVE="local"
```

Required environment variables:

- `PORTKEY_API_KEY`
- `PORTKEY_VIRTUAL_KEY` (or `PORTKEY_VIRTUAL_KEY_<PROFILE>`)
- `PORTKEY_MODEL` (or `PORTKEY_MODEL_<PROFILE>`)

Optional environment variables:

- `PORTKEY_PROFILE` (e.g. `DEV`, `PROD`)
- `PORTKEY_BASE_URL` (or `PORTKEY_BASE_URL_<PROFILE>`, defaults to `https://api.portkey.ai`)
- `PORTKEY_TIMEOUT_MS` (request timeout in milliseconds, defaults to `30000`)

Profile example (`PORTKEY_PROFILE=DEV`):

- `PORTKEY_VIRTUAL_KEY_DEV=...`
- `PORTKEY_MODEL_DEV=...`
- (optional) `PORTKEY_BASE_URL_DEV=...`

### DSN format (Oracle)

The design document defines the Oracle DSN semantics as:

- Service name:
  - `oracle://user:password@host:port/serviceName`
- SID (via query parameter):
  - `oracle://user:password@host:port/?sid=ORCL`

For Oracle Cloud / Autonomous Database connections using a Wallet (TNS alias), the backend also supports passing `TNS_ADMIN` as a query parameter. The host part is treated as the TNS alias (for example `ora23ai_high`).

Example:

`oracle://user:password@ora23ai_high?TNS_ADMIN=/path/to/Wallet_ORA23AI`

Note: if your wallet path contains spaces or special characters, URL-encode the value (e.g., replace spaces with `%20`). The backend URL-decodes DSN query parameters such as `TNS_ADMIN`.

If your username/password contains URL-reserved characters (for example `@`, `:`), URL-encode them as well.

## Repository structure

```text
swissql-cli/        # Go CLI
swissql-backend/    # Java 21 Spring Boot backend
```

## Notes

- The protocol and endpoints are designed for an MVP-friendly REST API and may evolve.
- Do not commit credentials. Use environment variables, stdin-based password input, or your OS keychain (planned).
