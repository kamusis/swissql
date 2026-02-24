# Concerns Focus

**Analysis Date:** 2025-03-05

## Technical Debt
- **Backend Test Coverage**: The `src/test/java` directory in the backend is currently minimal. Increasing unit and integration test coverage for the Spring Boot service is a high priority.
- **Protocol Limitations**: The current HTTP/JSON (REST) protocol between the CLI and backend is suitable for the MVP but may face performance bottlenecks with very large result sets. Plans for gRPC or streaming protocols are noted but not yet implemented.
- **JDBC Driver Management**: While Oracle and PostgreSQL drivers are managed via Maven, other drivers (MySQL, SQL Server, etc.) are manually managed in the `jdbc_drivers/` directory. This requires manual updates and consistent manifest maintenance.

## Known Issues / Risks
- **MVP Status**: As an MVP, many features (like AI-assisted SQL generation) are in early stages and may require further refinement for edge cases.
- **AI Setup Complexity**: Configuring AI features requires specific environment variables or Docker secrets, which can be error-prone for new users.
- **Credential Storage**: Credentials are encrypted using AES-CBC in `~/.swissql/credentials.json`, but reliance on a local key/file for security is a common risk factor that may need a more robust solution (e.g., OS Keychain integration) in the future.

## Architecture Compliance
- **Logic Placement**: A core principle is to keep all business logic in the backend. Constant vigilance is required to ensure the Go CLI remains a "thin client" and does not accumulate domain-specific logic.

## Future Considerations
- **CLI/Backend Versioning**: As the project evolves, ensuring compatibility between different versions of the CLI and backend will become increasingly important.
- **Extensibility**: The MCP-based extensibility is a forward-looking design that will need clear patterns as more plugins/routes are added.
