# Testing Focus

**Analysis Date:** 2025-03-05

## CLI Testing (Go)
- **Framework**: Standard Go `testing` package.
- **Isolation**: Use `setupTestConfigDir` to redirect `HOME` to a temporary directory.
- **Output Verification**: Use `captureOutput` and `executeRootCmd` to verify CLI output and behavior.
- **Patterns**: Table-driven tests using `t.Run`.
- **Helpers**: Use `t.Helper()` in setup and assertion helper functions.

## Backend Testing (Java)
- **Framework**: `spring-boot-starter-test` (JUnit 5, AssertJ, Mockito).
- **Status**: Currently, `src/test/java` is minimal, indicating a need for increased coverage.
- **Mocking**: Use JUnit 5 and Mockito. For Spring context tests, use `@MockBean`.

## Mocking & Isolation
- **CLI**: Redirect global writers or use interfaces to facilitate testing.
- **Backend**: Focus on unit tests with Mockito and integration tests with `@SpringBootTest`.
