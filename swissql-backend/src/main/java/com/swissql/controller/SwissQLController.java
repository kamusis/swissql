package com.swissql.controller;

import com.swissql.api.*;
import com.swissql.driver.DriverRegistry;
import com.swissql.driver.JdbcDriverAutoLoader;
import com.swissql.model.TopSnapshot;
import com.swissql.service.AiContextService;
import com.swissql.service.AiSqlGenerateService;
import com.swissql.service.DatabaseService;
import com.swissql.service.SessionManager;
import com.swissql.sampler.SamplerConfig;
import com.swissql.sampler.SamplerManager;
import jakarta.validation.Valid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/v1")
public class SwissQLController {

    private static final Logger log = LoggerFactory.getLogger(SwissQLController.class);

    private final SessionManager sessionManager;
    private final DatabaseService databaseService;
    private final AiSqlGenerateService aiSqlGenerateService;
    private final AiContextService aiContextService;
    private final DriverRegistry driverRegistry;
    private final JdbcDriverAutoLoader jdbcDriverAutoLoader;
    private final SamplerManager samplerManager;

    public SwissQLController(
            SessionManager sessionManager,
            DatabaseService databaseService,
            AiSqlGenerateService aiSqlGenerateService,
            AiContextService aiContextService,
            DriverRegistry driverRegistry,
            JdbcDriverAutoLoader jdbcDriverAutoLoader,
            SamplerManager samplerManager
    ) {
        this.sessionManager = sessionManager;
        this.databaseService = databaseService;
        this.aiSqlGenerateService = aiSqlGenerateService;
        this.aiContextService = aiContextService;
        this.driverRegistry = driverRegistry;
        this.jdbcDriverAutoLoader = jdbcDriverAutoLoader;
        this.samplerManager = samplerManager;
    }

    /**
     * Establish database connection and return session.
     *
     * POST /v1/connect
     *
     * @param request Connection request with DSN, dbType, and options
     * @return Connection response with session_id and expiration
     */
    @PostMapping("/connect")
    public ResponseEntity<?> connect(@Valid @RequestBody ConnectRequest request) {
        com.swissql.model.SessionInfo sessionInfo = null;
        try {
            sessionInfo = sessionManager.createSession(request);
            databaseService.initializeSession(sessionInfo);
            // TODO(P1): Provide an explicit sampler (re)start API so clients can restart sampling without reconnecting.
            samplerManager.startSampler(sessionInfo.getSessionId());
            ConnectResponse response = ConnectResponse.builder()
                    .sessionId(sessionInfo.getSessionId())
                    .traceId(MDC.get("trace_id"))
                    .expiresAt(sessionInfo.getExpiresAt())
                    .build();
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            if (sessionInfo != null && sessionInfo.getSessionId() != null) {
                try {
                    samplerManager.stopSampler(sessionInfo.getSessionId());
                } catch (Exception ignored) {
                }
                try {
                    databaseService.closeSession(sessionInfo.getSessionId());
                } catch (Exception ignored) {
                    // Best effort only
                }
                try {
                    sessionManager.terminateSession(sessionInfo.getSessionId());
                } catch (Exception ignored) {
                    // Best effort only
                }
            }
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(ErrorResponse.builder()
                    .code("CONNECTION_FAILED")
                    .message("Failed to connect to database: " + e.getMessage())
                    .traceId(MDC.get("trace_id"))
                    .build());
        }
    }

    /**
     * Terminate a session and release resources.
     *
     * POST /v1/disconnect
     */
    @PostMapping("/disconnect")
    public ResponseEntity<Void> disconnect(@RequestParam("session_id") String sessionId) {
        samplerManager.stopSampler(sessionId);
        sessionManager.terminateSession(sessionId);
        databaseService.closeSession(sessionId);
        aiContextService.clear(sessionId);
        return ResponseEntity.ok().build();
    }

    /**
     * Execute SQL query or command with optional limit and timeout.
     *
     * POST /v1/execute_sql
     *
     * @param request Execute request with session ID and SQL
     * @return Query results with metadata
     */
    @PostMapping("/execute_sql")
    public ResponseEntity<?> execute(@Valid @RequestBody ExecuteRequest request) {
        var sessionInfoOpt = sessionManager.getSession(request.getSessionId());
        if (sessionInfoOpt.isEmpty()) {
            return ResponseEntity.status(401).body(ErrorResponse.builder()
                    .code("SESSION_EXPIRED")
                    .message("Session missing or expired")
                    .traceId(MDC.get("trace_id"))
                    .build());
        }

        try {
            ExecuteResponse response = databaseService.execute(sessionInfoOpt.get(), request);
            aiContextService.recordExecute(request.getSessionId(), request.getSql(), response);
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            aiContextService.recordExecuteError(request.getSessionId(), request.getSql(), e);
            return ResponseEntity.status(500).body(ErrorResponse.builder()
                    .code("EXECUTION_ERROR")
                    .message(e.getMessage())
                    .traceId(MDC.get("trace_id"))
                    .build());
        }
    }

    /**
     * Generate SQL from natural language using AI.
     *
     * POST /v1/ai/generate
     *
     * @param request AI generation request with prompt and dbType
     * @return Generated SQL with risk assessment and warnings
     */
    @PostMapping("/ai/generate")
    public ResponseEntity<AiGenerateResponse> generateSql(
            @Valid @RequestBody AiGenerateRequest request) {
        var result = aiSqlGenerateService.generate(request);

        AiGenerateResponse.AiGenerateResponseBuilder responseBuilder = AiGenerateResponse.builder()
                .traceId(MDC.get("trace_id"))
                .risk("UNKNOWN")
                .explanation(null);

        if (!result.isEnabled()) {
            return ResponseEntity.ok(responseBuilder
                    .sql("")
                    .warnings(result.getWarnings())
                    .explanation("AI generation is disabled. Configure Portkey environment variables to enable.")
                    .build());
        }

        if (result.getError() != null && !result.getError().isBlank()) {
            return ResponseEntity.ok(responseBuilder
                    .sql("")
                    .warnings(List.of(result.getError()))
                    .explanation("AI generation failed.")
                    .build());
        }

        return ResponseEntity.ok(responseBuilder
                .sql(result.getSql())
                .warnings(List.of())
                .build());
    }

    /**
     * Validate whether a session is still present and not expired.
     *
     * GET /v1/sessions/validate
     *
     * @param sessionId Session ID to validate
     * @return 200 if valid, 401 if missing/expired
     */
    @GetMapping("/sessions/validate")
    public ResponseEntity<?> validateSession(@RequestParam("session_id") String sessionId) {
        var sessionInfoOpt = sessionManager.getSession(sessionId);
        if (sessionInfoOpt.isEmpty()) {
            return ResponseEntity.status(401).body(ErrorResponse.builder()
                    .code("SESSION_EXPIRED")
                    .message("Session missing or expired")
                    .traceId(MDC.get("trace_id"))
                    .build());
        }
        return ResponseEntity.ok(Map.of("valid", "true"));
    }

    @GetMapping("/meta/describe")
    public ResponseEntity<?> metaDescribe(
            @RequestParam("session_id") String sessionId,
            @RequestParam("name") String name,
            @RequestParam(value = "detail", required = false) String detail
    ) {
        var sessionInfoOpt = sessionManager.getSession(sessionId);
        if (sessionInfoOpt.isEmpty()) {
            return ResponseEntity.status(401).body(ErrorResponse.builder()
                    .code("SESSION_EXPIRED")
                    .message("Session missing or expired")
                    .traceId(MDC.get("trace_id"))
                    .build());
        }

        try {
            String pseudoSql = buildMetaDescribePseudoSql(name, detail);
            ExecuteResponse response = databaseService.metaDescribe(sessionInfoOpt.get(), name, detail);
            aiContextService.recordExecute(sessionId, pseudoSql, response);
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            String pseudoSql = buildMetaDescribePseudoSql(name, detail);
            aiContextService.recordExecuteError(sessionId, pseudoSql, e);
            return ResponseEntity.status(500).body(ErrorResponse.builder()
                    .code("EXECUTION_ERROR")
                    .message(e.getMessage())
                    .traceId(MDC.get("trace_id"))
                    .build());
        }
    }

    @GetMapping("/meta/list")
    public ResponseEntity<?> metaList(
            @RequestParam("session_id") String sessionId,
            @RequestParam("kind") String kind,
            @RequestParam(value = "schema", required = false) String schema
    ) {
        var sessionInfoOpt = sessionManager.getSession(sessionId);
        if (sessionInfoOpt.isEmpty()) {
            return ResponseEntity.status(401).body(ErrorResponse.builder()
                    .code("SESSION_EXPIRED")
                    .message("Session missing or expired")
                    .traceId(MDC.get("trace_id"))
                    .build());
        }

        try {
            String pseudoSql = buildMetaListPseudoSql(kind, schema);
            ExecuteResponse response = databaseService.metaList(sessionInfoOpt.get(), kind, schema);
            aiContextService.recordExecute(sessionId, pseudoSql, response);
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            String pseudoSql = buildMetaListPseudoSql(kind, schema);
            aiContextService.recordExecuteError(sessionId, pseudoSql, e);
            return ResponseEntity.status(500).body(ErrorResponse.builder()
                    .code("EXECUTION_ERROR")
                    .message(e.getMessage())
                    .traceId(MDC.get("trace_id"))
                    .build());
        }
    }

    private String buildMetaDescribePseudoSql(String name, String detail) {
        String resolvedName = name != null ? name.trim() : "";
        String detailLower = detail != null ? detail.trim().toLowerCase() : "";
        String cmd = "full".equals(detailLower) ? "\\\\d+" : "\\\\d";
        if (resolvedName.isBlank()) {
            return cmd;
        }
        return cmd + " " + resolvedName;
    }

    private String buildMetaListPseudoSql(String kind, String schema) {
        String kindLower = kind != null ? kind.trim().toLowerCase() : "";
        String cmd = "view".equals(kindLower) ? "\\\\dv" : "\\\\dt";

        String resolvedSchema = schema != null ? schema.trim() : "";
        if (resolvedSchema.isBlank()) {
            return cmd;
        }
        return cmd + " " + resolvedSchema;
    }

    /**
     * List currently available JDBC drivers (built-in + directory-loaded).
     *
     * GET /v1/meta/drivers
     *
     * @return drivers response
     */
    @GetMapping("/meta/drivers")
    public ResponseEntity<DriversResponse> metaDrivers() {
        DriversResponse response = new DriversResponse();
        List<DriversResponse.DriverEntry> entries = new ArrayList<>();

        for (DriverRegistry.Entry e : driverRegistry.list()) {
            DriversResponse.DriverEntry item = new DriversResponse.DriverEntry();
            item.setDbType(e.getDbType());
            item.setSource(e.getSource() != null ? e.getSource().name().toLowerCase() : "unknown");

            if (e.getManifest() != null) {
                item.setDriverClass(e.getManifest().getDriverClass());
                item.setJdbcUrlTemplate(e.getManifest().getJdbcUrlTemplate());
                item.setDefaultPort(e.getManifest().getDefaultPort());
            }

            item.setDriverClasses(e.getDiscoveredDriverClasses());
            item.setJarPaths(e.getJarPaths());
            entries.add(item);
        }

        response.setDrivers(entries);
        return ResponseEntity.ok(response);
    }

    /**
     * Rescan the configured driver directory and register newly discovered JDBC drivers.
     *
     * POST /v1/meta/drivers/reload
     *
     * @return reload response
     */
    @PostMapping("/meta/drivers/reload")
    public ResponseEntity<DriversReloadResponse> metaDriversReload() {
        JdbcDriverAutoLoader.ReloadResult result = jdbcDriverAutoLoader.reload();
        DriversReloadResponse response = new DriversReloadResponse();
        response.setStatus("ok");
        response.setReloaded(result.toMap());
        return ResponseEntity.ok(response);
    }

    @GetMapping("/meta/conninfo")
    public ResponseEntity<?> metaConninfo(
            @RequestParam("session_id") String sessionId
    ) {
        var sessionInfoOpt = sessionManager.getSession(sessionId);
        if (sessionInfoOpt.isEmpty()) {
            return ResponseEntity.status(401).body(ErrorResponse.builder()
                    .code("SESSION_EXPIRED")
                    .message("Session missing or expired")
                    .traceId(MDC.get("trace_id"))
                    .build());
        }

        String pseudoSql = "\\\\conninfo";
        try {
            ExecuteResponse response = databaseService.metaConninfo(sessionInfoOpt.get());
            aiContextService.recordExecute(sessionId, pseudoSql, response);
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            aiContextService.recordExecuteError(sessionId, pseudoSql, e);
            return ResponseEntity.status(500).body(ErrorResponse.builder()
                    .code("EXECUTION_ERROR")
                    .message(e.getMessage())
                    .traceId(MDC.get("trace_id"))
                    .build());
        }
    }

    @PostMapping("/meta/explain")
    public ResponseEntity<?> metaExplain(@Valid @RequestBody MetaExplainRequest request) {
        var sessionInfoOpt = sessionManager.getSession(request.getSessionId());
        if (sessionInfoOpt.isEmpty()) {
            return ResponseEntity.status(401).body(ErrorResponse.builder()
                    .code("SESSION_EXPIRED")
                    .message("Session missing or expired")
                    .traceId(MDC.get("trace_id"))
                    .build());
        }

        try {
            ExecuteResponse response = databaseService.metaExplain(sessionInfoOpt.get(), request.getSql(), request.isAnalyze());
            String pseudoSql = buildMetaExplainPseudoSql(request.getSql(), request.isAnalyze());
            aiContextService.recordExecute(request.getSessionId(), pseudoSql, response);
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            String pseudoSql = buildMetaExplainPseudoSql(request.getSql(), request.isAnalyze());
            aiContextService.recordExecuteError(request.getSessionId(), pseudoSql, e);
            return ResponseEntity.status(500).body(ErrorResponse.builder()
                    .code("EXECUTION_ERROR")
                    .message(e.getMessage())
                    .traceId(MDC.get("trace_id"))
                    .build());
        }
    }

    private String buildMetaExplainPseudoSql(String sql, boolean analyze) {
        String resolvedSql = sql != null ? sql.trim() : "";
        String cmd = analyze ? "\\\\explain analyze" : "\\\\explain";
        if (resolvedSql.isBlank()) {
            return cmd;
        }
        return cmd + " " + resolvedSql;
    }

    @GetMapping("/meta/completions")
    public ResponseEntity<?> metaCompletions(
            @RequestParam("session_id") String sessionId,
            @RequestParam("kind") String kind,
            @RequestParam(value = "schema", required = false) String schema,
            @RequestParam(value = "table", required = false) String table,
            @RequestParam(value = "prefix", required = false) String prefix
    ) {
        var sessionInfoOpt = sessionManager.getSession(sessionId);
        if (sessionInfoOpt.isEmpty()) {
            return ResponseEntity.status(401).body(ErrorResponse.builder()
                    .code("SESSION_EXPIRED")
                    .message("Session missing or expired")
                    .traceId(MDC.get("trace_id"))
                    .build());
        }

        try {
            ExecuteResponse response = databaseService.metaCompletions(sessionInfoOpt.get(), kind, schema, table, prefix);
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            return ResponseEntity.status(500).body(ErrorResponse.builder()
                    .code("EXECUTION_ERROR")
                    .message(e.getMessage())
                    .traceId(MDC.get("trace_id"))
                    .build());
        }
    }

    /**
     * Retrieve recent executed SQL context that may be included in AI prompts.
     *
     * GET /v1/ai/context
     */
    @GetMapping("/ai/context")
    public ResponseEntity<AiContextResponse> getAiContext(
            @RequestParam("session_id") String sessionId,
            @RequestParam(value = "limit", required = false) Integer limit
    ) {
        int resolvedLimit = limit != null ? limit : 10;
        return ResponseEntity.ok(AiContextResponse.builder()
                .sessionId(sessionId)
                .items(aiContextService.getRecent(sessionId, resolvedLimit))
                .build());
    }

    /**
     * Clear stored AI context for a session.
     *
     * POST /v1/ai/context/clear
     */
    @PostMapping("/ai/context/clear")
    public ResponseEntity<Void> clearAiContext(@Valid @RequestBody AiContextClearRequest request) {
        aiContextService.clear(request.getSessionId());
        return ResponseEntity.ok().build();
    }

    /**
     * Health check endpoint.
     *
     * GET /v1/status
     *
     * @return Service status
     */
    @GetMapping("/status")
    public ResponseEntity<Map<String, String>> getStatus() {
        return ResponseEntity.ok(Map.of("status", "UP"));
    }

    /**
     * Get top performance snapshot.
     *
     * GET /v1/top/snapshot
     */
    @GetMapping("/top/snapshot")
    public ResponseEntity<?> getTopSnapshot(@RequestParam("session_id") String sessionId) {
        var sessionInfoOpt = sessionManager.getSession(sessionId);
        if (sessionInfoOpt.isEmpty()) {
            return ResponseEntity.status(401).body(ErrorResponse.builder()
                    .code("SESSION_EXPIRED")
                    .message("Session missing or expired")
                    .traceId(MDC.get("trace_id"))
                    .build());
        }

        TopSnapshot snapshot = samplerManager.getSnapshot(sessionId);
        if (snapshot == null) {
            return ResponseEntity.status(404).body(ErrorResponse.builder()
                    .code("SAMPLER_NOT_FOUND")
                    .message("Sampler not started for this session")
                    .traceId(MDC.get("trace_id"))
                    .build());
        }

        // log.debug("Returning snapshot with topSessions size: {}", snapshot.getTopSessions() != null ? snapshot.getTopSessions().size() : 0);
        return ResponseEntity.ok(snapshot);
    }

    /**
     * Get SQL text by ID.
     *
     * GET /v1/meta/sqltext
     */
    @GetMapping("/meta/sqltext")
    public ResponseEntity<?> getSqlText(
            @RequestParam("session_id") String sessionId,
            @RequestParam("sql_id") String sqlId
    ) {
        var sessionInfoOpt = sessionManager.getSession(sessionId);
        if (sessionInfoOpt.isEmpty()) {
            return ResponseEntity.status(401).body(ErrorResponse.builder()
                    .code("SESSION_EXPIRED")
                    .message("Session missing or expired")
                    .traceId(MDC.get("trace_id"))
                    .build());
        }

        try {
            var sessionInfo = sessionInfoOpt.get();
            var collectorConfig = samplerManager.getCollectorRegistry().getConfig(
                    databaseService.getConnection(sessionInfo), sessionInfo.getDbType());

            if (collectorConfig == null || collectorConfig.getCollectors() == null) {
                return ResponseEntity.status(404).body(ErrorResponse.builder()
                        .code("COLLECTOR_NOT_FOUND")
                        .message("Collector configuration not found")
                        .traceId(MDC.get("trace_id"))
                        .build());
            }

            var sqltextCollector = collectorConfig.getCollectors().get("sqltext");
            if (sqltextCollector == null || sqltextCollector.getQueries() == null) {
                return ResponseEntity.status(404).body(ErrorResponse.builder()
                        .code("SQLTEXT_COLLECTOR_NOT_FOUND")
                        .message("SQL text collector not found")
                        .traceId(MDC.get("trace_id"))
                        .build());
            }

            var query = sqltextCollector.getQueries().get("by_id");
            if (query == null) {
                return ResponseEntity.status(404).body(ErrorResponse.builder()
                        .code("SQLTEXT_QUERY_NOT_FOUND")
                        .message("SQL text query not found")
                        .traceId(MDC.get("trace_id"))
                        .build());
            }

            var sql = query.getSql();
            if (sql.contains(":sql_id")) {
                sql = sql.replace(":sql_id", "?");
            }

            // TODO(P0): Move :param -> ? replacement and parameter binding to GenericCollector/SqlTemplateBinder
            // utility instead of manual string replacement. This avoids code duplication and potential SQL
            // injection. See: GenericCollector.executeQuery() should support parameterized queries with
            // List<Object> parameters.

            try (var conn = databaseService.getConnection(sessionInfo);
                 var stmt = conn.prepareStatement(sql)) {

                stmt.setString(1, sqlId);
                try (var rs = stmt.executeQuery()) {

                    if (rs.next()) {
                        String fullText = rs.getString("sql_fulltext");
                        String shortText = rs.getString("sql_text");
                        String text = fullText != null ? fullText : shortText;
                        boolean truncated = fullText == null;

                        return ResponseEntity.ok(SqlTextResponse.builder()
                                .sqlId(sqlId)
                                .text(text)
                                .truncated(truncated)
                                .build());
                    } else {
                        return ResponseEntity.status(404).body(ErrorResponse.builder()
                                .code("SQL_NOT_FOUND")
                                .message("SQL not found: " + sqlId)
                                .traceId(MDC.get("trace_id"))
                                .build());
                    }
                }
            }
        } catch (Exception e) {
            return ResponseEntity.status(500).body(ErrorResponse.builder()
                    .code("EXECUTION_ERROR")
                    .message(e.getMessage())
                    .traceId(MDC.get("trace_id"))
                    .build());
        }
    }

    /**
     * Update top sampler configuration.
     *
     * POST /v1/top/config
     */
    @PostMapping("/top/config")
    public ResponseEntity<?> updateTopConfig(@Valid @RequestBody TopConfigRequest request) {
        var sessionInfoOpt = sessionManager.getSession(request.getSessionId());
        if (sessionInfoOpt.isEmpty()) {
            return ResponseEntity.status(401).body(ErrorResponse.builder()
                    .code("SESSION_EXPIRED")
                    .message("Session missing or expired")
                    .traceId(MDC.get("trace_id"))
                    .build());
        }

        try {
            SamplerConfig config = SamplerConfig.builder()
                    .intervalSec(request.getIntervalSec())
                    .enableTopSql(request.getEnableTopSql())
                    .enableTopSessions(request.getEnableTopSessions())
                    .maxItems(request.getMaxItems())
                    .build();

            samplerManager.updateConfig(request.getSessionId(), config);

            return ResponseEntity.ok(TopConfigResponse.builder()
                    .message("Configuration updated successfully")
                    .intervalSec(config.getIntervalSec())
                    .enableTopSql(config.getEnableTopSql())
                    .enableTopSessions(config.getEnableTopSessions())
                    .maxItems(config.getMaxItems())
                    .build());
        } catch (Exception e) {
            return ResponseEntity.status(500).body(ErrorResponse.builder()
                    .code("CONFIG_UPDATE_FAILED")
                    .message(e.getMessage())
                    .traceId(MDC.get("trace_id"))
                    .build());
        }
    }
}
