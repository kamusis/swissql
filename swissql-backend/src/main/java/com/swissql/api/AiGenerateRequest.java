package com.swissql.api;

import jakarta.validation.constraints.NotBlank;
import lombok.Data;

/**
 * Request DTO for AI-powered SQL generation.
 * Converts natural language prompts to SQL statements.
 *
 * JSON fields (snake_case):
 * - prompt: Natural language description
 * - db_type: Target database type
 * - schema_context: Optional schema information
 */
@Data
public class AiGenerateRequest {

    /**
     * Natural language description of desired data/operation.
     * Example: "Show me all users created in last week, ordered by name"
     */
    @NotBlank(message = "Natural language prompt is required")
    private String prompt;

    /**
     * Target database type for SQL generation.
     * Values: "oracle", "postgres", "mysql"
     */
    @NotBlank(message = "Database type is required")
    private String dbType;

    /**
     * Optional backend session id.
     * If provided, backend can attach recent executed SQL context to the LLM prompt.
     */
    private String sessionId;

    /**
     * Context mode for LLM prompt enrichment.
     * Values: off | sql_only | schema_and_samples
     */
    private String contextMode;

    /**
     * Maximum number of recent context items to include.
     */
    private Integer contextLimit;

    /**
     * Optional database schema context to improve SQL accuracy.
     * Can contain table definitions, column names, relationships.
     */
    private String schemaContext;
}
