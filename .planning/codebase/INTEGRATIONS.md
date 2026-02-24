# Integrations Focus

**Analysis Date:** 2025-03-05

## AI Services
- **Portkey Gateway**: Integration with OpenAI-compatible APIs via `https://api.portkey.ai`. Uses `PORTKEY_API_KEY` and `PORTKEY_VIRTUAL_KEY`.

## Databases
- **Native JDBC Support**: Support for Oracle, PostgreSQL, MySQL, SQL Server, DB2, Informix, MogDB, and YashanDB via JDBC drivers located in `swissql-backend/jdbc_drivers/`.

## Internal Integration
- **REST API**: CLI interacts with the Backend service via a REST API defined in `swissql-cli/internal/client/client.go`.

## Protocols
- **Model Context Protocol (MCP)**: Support indicated in connection options (`useMcp` flag).
