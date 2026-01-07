package com.swissql.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.swissql.api.AiGenerateRequest;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Service;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Service for generating SQL from natural-language prompts via an OpenAI-compatible API.
 *
 * This implementation targets the Portkey OpenAI-compatible gateway by default and relies solely
 * on HTTP requests (no vendor SDK) to avoid locking into any specific LLM provider library.
 */
@Service
public class AiSqlGenerateService {

    private static final Logger log = LoggerFactory.getLogger(AiSqlGenerateService.class);

    private static final String DEFAULT_PORTKEY_BASE_URL = "https://api.portkey.ai";
    private static final int DEFAULT_TIMEOUT_MS = 30000;

    private final ObjectMapper objectMapper;
    private final HttpClient httpClient;
    private final Environment environment;
    private final AiContextService aiContextService;

    /**
     * Create a new AI SQL generation service.
     *
     * @param objectMapper Jackson object mapper
     * @param environment Spring environment for configuration
     */
    public AiSqlGenerateService(ObjectMapper objectMapper, Environment environment, AiContextService aiContextService) {
        this.objectMapper = objectMapper;
        this.environment = environment;
        this.aiContextService = aiContextService;
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .build();
    }

    /**
     * Log whether AI generation is enabled and which profile settings are active.
     *
     * This intentionally does not log any secrets (e.g., Portkey API key).
     */
    @PostConstruct
    public void logAiConfigStatus() {
        PortkeyConfig config = PortkeyConfig.fromEnvironment(environment);

        String profile = null;
        if (environment != null) {
            profile = environment.getProperty("swissql.ai.portkey.profile");
            if (profile == null || profile.isBlank()) {
                profile = environment.getProperty("PORTKEY_PROFILE");
            }
        }
        if (profile != null) {
            profile = profile.trim();
        }
        boolean hasApiKey = config.apiKey() != null && !config.apiKey().isBlank();
        boolean hasProvider = config.provider() != null && !config.provider().isBlank();
        boolean hasModel = config.model() != null && !config.model().isBlank();

        if (config.isEnabled()) {
            log.info(
                    "AI SQL generation is ENABLED (gateway_base_url={}, profile={}, model={}, provider_configured={})",
                    config.baseUrl(),
                    profile,
                    config.model(),
                    true
            );
            return;
        }

        log.warn(
                "AI SQL generation is DISABLED (gateway_base_url={}, profile={}, api_key_configured={}, provider_configured={}, model_configured={})",
                config.baseUrl(),
                profile,
                hasApiKey,
                hasProvider,
                hasModel
        );
    }

    /**
     * Generate SQL for a given natural language prompt.
     *
     * @param request request containing prompt and database type
     * @return generated SQL as a string (may be empty if configuration is missing)
     */
    public GeneratedSqlResult generate(AiGenerateRequest request) {
        PortkeyConfig config = PortkeyConfig.fromEnvironment(environment);
        if (!config.isEnabled()) {
            return GeneratedSqlResult.disabled(config.getDisabledWarnings());
        }

        try {
            String sqlDialect = normalizeDbType(request.getDbType());

            String userPrompt = buildUserPrompt(request);

            Map<String, Object> payload = new HashMap<>();
            payload.put("model", config.model());
            payload.put("messages", List.of(
                    Map.of(
                            "role", "system",
                            "content", buildSystemPrompt(sqlDialect)
                    ),
                    Map.of(
                            "role", "user",
                            "content", userPrompt
                    )
            ));

            String json = objectMapper.writeValueAsString(payload);

            URI uri = URI.create(config.baseUrl() + "/v1/chat/completions");
            HttpRequest.Builder httpRequestBuilder = HttpRequest.newBuilder()
                    .uri(uri)
                    .timeout(Duration.ofMillis(config.timeoutMs()))
                    .header("Content-Type", "application/json")
                    .header("x-portkey-api-key", config.apiKey())
                    .header("x-portkey-virtual-key", config.provider())
                    .POST(HttpRequest.BodyPublishers.ofString(json, StandardCharsets.UTF_8));

            HttpResponse<String> response = httpClient.send(httpRequestBuilder.build(), HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() >= 400) {
                log.warn(
                        "AI gateway request failed (status_code={}, gateway_base_url={}, model={}, virtual_key_configured={})",
                        response.statusCode(),
                        config.baseUrl(),
                        config.model(),
                        config.provider() != null && !config.provider().isBlank()
                );
                return GeneratedSqlResult.error("AI gateway error: HTTP " + response.statusCode() + " - " + response.body());
            }

            JsonNode root = objectMapper.readTree(response.body());
            JsonNode contentNode = root.path("choices").path(0).path("message").path("content");
            String content = contentNode.isTextual() ? contentNode.asText() : "";

            String sqlJson = sanitizeStatementsJson(content);
            if (sqlJson.isBlank()) {
                return GeneratedSqlResult.error("AI gateway returned invalid JSON output");
            }

            return GeneratedSqlResult.success(sqlJson);
        } catch (Exception e) {
            return GeneratedSqlResult.error("AI generation failed: " + e.getMessage());
        }
    }

    private String normalizeDbType(String dbType) {
        if (dbType == null) {
            return "oracle";
        }
        String v = dbType.trim().toLowerCase(Locale.ROOT);
        if (v.isBlank()) {
            return "oracle";
        }
        return v;
    }

    private String buildSystemPrompt(String dbType) {
        return "You are a SQL generator. Output ONLY valid JSON and nothing else. " +
                "Do not use markdown fences. Do not add explanations. " +
                "Output schema: {\"statements\": [\"<SQL statement 1>\", \"<SQL statement 2>\", ...]}. " +
                "Each statement must be a complete SQL statement WITHOUT a trailing semicolon. " +
                "Target database dialect: " + dbType + ".";
    }

    private String sanitizeSql(String content) {
        if (content == null) {
            return "";
        }
        String s = content.trim();

        if (s.startsWith("```")) {
            s = s.replaceFirst("^```[a-zA-Z0-9_-]*\\n", "");
            s = s.replaceFirst("\\n```$", "");
            s = s.trim();
        }

        return s;
    }

    private String sanitizeStatementsJson(String content) {
        String s = sanitizeSql(content);
        if (s.isBlank()) {
            return "";
        }

        try {
            JsonNode root = objectMapper.readTree(s);
            JsonNode statementsNode = root.path("statements");
            if (!statementsNode.isArray() || statementsNode.isEmpty()) {
                return "";
            }
            for (JsonNode n : statementsNode) {
                if (!n.isTextual() || n.asText().trim().isEmpty()) {
                    return "";
                }
            }
            return objectMapper.writeValueAsString(root);
        } catch (Exception e) {
            return "";
        }
    }

    private String buildUserPrompt(AiGenerateRequest request) {
        if (request == null) {
            return "";
        }

        StringBuilder sb = new StringBuilder();
        if (request.getPrompt() != null) {
            sb.append(request.getPrompt().trim());
        }

        if (request.getSchemaContext() != null && !request.getSchemaContext().isBlank()) {
            sb.append("\n\nSchema context:\n");
            sb.append(request.getSchemaContext().trim());
        }

        String contextMode = resolveContextMode(request.getContextMode());
        if ("off".equals(contextMode)) {
            return sb.toString();
        }

        String sessionId = request.getSessionId();
        if (sessionId == null || sessionId.isBlank()) {
            return sb.toString();
        }

        int limit = request.getContextLimit() != null ? request.getContextLimit() : 10;
        List<AiContextItem> items = aiContextService.getRecent(sessionId, limit);
        if (items.isEmpty()) {
            return sb.toString();
        }

        sb.append("\n\nRecent executed SQL context (most recent first):\n");
        sb.append(formatContextItems(items, contextMode));
        return sb.toString();
    }

    private String resolveContextMode(String raw) {
        if (raw == null || raw.isBlank()) {
            return "schema_and_samples";
        }
        String v = raw.trim().toLowerCase(Locale.ROOT);
        if ("off".equals(v) || "sql_only".equals(v) || "schema_and_samples".equals(v)) {
            return v;
        }
        return "schema_and_samples";
    }

    private String formatContextItems(List<AiContextItem> items, String contextMode) {
        StringBuilder sb = new StringBuilder();
        int idx = 1;
        for (AiContextItem item : items) {
            if (item == null) {
                continue;
            }

            sb.append("[").append(idx).append("] SQL: ").append(item.getSql()).append("\n");

            if (item.getError() != null && !item.getError().isBlank()) {
                sb.append("    ERROR: ").append(item.getError()).append("\n");
            }

            if ("schema_and_samples".equals(contextMode)) {
                if (item.getColumns() != null && !item.getColumns().isEmpty()) {
                    sb.append("    Result columns:\n");
                    for (AiContextItem.Column c : item.getColumns()) {
                        if (c == null) {
                            continue;
                        }
                        sb.append("      - ").append(c.getName()).append(" ").append(c.getType()).append("\n");
                    }
                }

                if (item.getSampleRows() != null && !item.getSampleRows().isEmpty()) {
                    sb.append("    Sample rows:");
                    sb.append("\n");
                    for (Map<String, Object> row : item.getSampleRows()) {
                        sb.append("      - ").append(row).append("\n");
                    }
                }
            }

            idx++;
        }
        return sb.toString();
    }

    /**
     * Immutable Portkey configuration resolved from environment variables.
     */
    private record PortkeyConfig(
            String baseUrl,
            String apiKey,
            String provider,
            String model,
            int timeoutMs
    ) {
        static PortkeyConfig fromEnvironment(Environment environment) {
            String profile = getTrimmed(environment, "swissql.ai.portkey.profile", "PORTKEY_PROFILE");
            String apiKey = getTrimmed(environment, "swissql.ai.portkey.api-key", "PORTKEY_API_KEY");

            String resolvedBaseUrl = resolveProfileValue(environment, "swissql.ai.portkey.base-url", "PORTKEY_BASE_URL", profile);
            if (resolvedBaseUrl == null || resolvedBaseUrl.isBlank()) {
                resolvedBaseUrl = DEFAULT_PORTKEY_BASE_URL;
            }

            String resolvedProvider = resolveProfileValue(environment, "swissql.ai.portkey.virtual-key", "PORTKEY_VIRTUAL_KEY", profile);
            String resolvedModel = resolveProfileValue(environment, "swissql.ai.portkey.model", "PORTKEY_MODEL", profile);

            int timeoutMs = DEFAULT_TIMEOUT_MS;
            String timeoutRaw = getTrimmed(environment, "swissql.ai.portkey.timeout-ms", "PORTKEY_TIMEOUT_MS");
            if (timeoutRaw != null && !timeoutRaw.isBlank()) {
                try {
                    timeoutMs = Integer.parseInt(timeoutRaw);
                } catch (NumberFormatException ignored) {
                    // Keep default
                }
            }

            return new PortkeyConfig(resolvedBaseUrl, apiKey, resolvedProvider, resolvedModel, timeoutMs);
        }

        boolean isEnabled() {
            return apiKey != null && !apiKey.isBlank()
                    && provider != null && !provider.isBlank()
                    && model != null && !model.isBlank();
        }

        List<String> getDisabledWarnings() {
            return List.of(
                    "AI generation is disabled - missing Portkey configuration",
                    "Required env: PORTKEY_API_KEY, PORTKEY_VIRTUAL_KEY(_<PROFILE>), PORTKEY_MODEL(_<PROFILE>)",
                    "Optional env: PORTKEY_PROFILE, PORTKEY_BASE_URL(_<PROFILE>), PORTKEY_TIMEOUT_MS"
            );
        }

        private static String resolveProfileValue(Environment environment, String propKey, String envKey, String profile) {
            if (profile != null && !profile.isBlank()) {
                String profiledPropKey = propKey + "." + profile.toUpperCase(Locale.ROOT);
                String profiledEnvKey = envKey + "_" + profile.toUpperCase(Locale.ROOT);

                String profiled = getTrimmed(environment, profiledPropKey, profiledEnvKey);
                if (profiled != null && !profiled.isBlank()) {
                    return profiled;
                }
            }

            return getTrimmed(environment, propKey, envKey);
        }

        private static String getTrimmed(Environment environment, String propKey, String envKey) {
            String v = null;
            if (environment != null && propKey != null && !propKey.isBlank()) {
                v = environment.getProperty(propKey);
            }
            if ((v == null || v.isBlank()) && envKey != null && !envKey.isBlank()) {
                v = environment != null ? environment.getProperty(envKey) : null;
            }
            if (v == null) {
                return null;
            }
            return v.trim();
        }
    }

    /**
     * Result wrapper for SQL generation.
     */
    public static class GeneratedSqlResult {
        private final String sql;
        private final boolean enabled;
        private final String error;
        private final List<String> warnings;

        /**
         * Create a result.
         *
         * @param sql generated sql
         * @param enabled whether AI is enabled
         * @param error error message, if any
         * @param warnings warning messages
         */
        public GeneratedSqlResult(String sql, boolean enabled, String error, List<String> warnings) {
            this.sql = sql;
            this.enabled = enabled;
            this.error = error;
            this.warnings = warnings;
        }

        /**
         * Create a successful result.
         *
         * @param sql generated SQL
         * @return result
         */
        public static GeneratedSqlResult success(String sql) {
            return new GeneratedSqlResult(sql, true, null, List.of());
        }

        /**
         * Create a disabled result.
         *
         * @param warnings warning messages
         * @return result
         */
        public static GeneratedSqlResult disabled(List<String> warnings) {
            return new GeneratedSqlResult("", false, null, warnings);
        }

        /**
         * Create an error result.
         *
         * @param error error message
         * @return result
         */
        public static GeneratedSqlResult error(String error) {
            return new GeneratedSqlResult("", true, error, List.of());
        }

        /**
         * Get generated SQL.
         *
         * @return sql
         */
        public String getSql() {
            return sql;
        }

        /**
         * Check if AI is enabled.
         *
         * @return enabled
         */
        public boolean isEnabled() {
            return enabled;
        }

        /**
         * Get error message.
         *
         * @return error
         */
        public String getError() {
            return error;
        }

        /**
         * Get warnings.
         *
         * @return warnings
         */
        public List<String> getWarnings() {
            return warnings;
        }
    }
}
