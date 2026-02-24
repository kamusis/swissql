# Conventions Focus

**Analysis Date:** 2025-03-05

## Naming
- **Classes/Structs**: PascalCase (e.g., `JdbcDriverAutoLoader.java`).
- **Methods/Functions/Variables**: camelCase.

## Java Style (Backend)
- **Standard Spring Boot 3 structure**.
- **Lombok**: Extensive use of `@Data`, `@Builder`, and `@Slf4j` to reduce boilerplate.
- **Dependency Injection**: Use constructor injection for Spring Services.
- **Logging**: Use SLF4J (via Lombok's `@Slf4j`).
- **Documentation**: Use Javadoc for public methods and classes.

## Go Style (CLI)
- **Standard Go layout**.
- **Naming**: PascalCase for exported symbols, camelCase for internal ones.
- **Framework**: Use **Cobra** for all command-line interface commands.
- **Formatting**: Strictly follow `gofmt`.

## Error Handling
- **Java**: Standard exception handling with custom exceptions where appropriate.
- **Go**: Idiomatic `if err != nil` pattern.
