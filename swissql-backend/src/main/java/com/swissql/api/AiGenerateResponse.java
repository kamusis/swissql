package com.swissql.api;

import lombok.Builder;
import lombok.Data;
import java.util.List;

/**
 * Response DTO for AI-powered SQL generation.
 * Contains generated SQL, risk assessment, and configuration warnings.
 *
 * JSON fields (snake_case):
 * - sql: Generated SQL statement (empty if LLM not configured)
 * - risk: Risk level (LOW/MEDIUM/HIGH/UNKNOWN)
 * - explanation: Why this risk level was assigned
 * - warnings: Configuration warnings for client
 * - trace_id: Request correlation ID
 */
@Data
@Builder
public class AiGenerateResponse {

    /**
     * Generated SQL statement.
     * Empty if LLM is not configured.
     */
    private String sql;

    /**
     * Risk level of the generated SQL.
     * Values: LOW, MEDIUM, HIGH, UNKNOWN
     */
    private String risk;

    /**
     * Explanation of why this risk level was assigned.
     */
    private String explanation;

    /**
     * List of warnings (e.g., LLM not configured).
     * Used to communicate non-blocking issues to the client.
     */
    private List<String> warnings;

    /**
     * Trace ID for request correlation and debugging.
     * Matches X-Request-Id header value.
     */
    private String traceId;
}
