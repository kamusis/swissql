package com.swissql.controller;

import com.swissql.api.*;
import com.swissql.service.AiContextService;
import com.swissql.service.AiSqlGenerateService;
import com.swissql.service.DatabaseService;
import com.swissql.service.SessionManager;
import jakarta.validation.Valid;
import org.slf4j.MDC;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/v1")
public class SwissQLController {

    private final SessionManager sessionManager;
    private final DatabaseService databaseService;
    private final AiSqlGenerateService aiSqlGenerateService;
    private final AiContextService aiContextService;

    public SwissQLController(
            SessionManager sessionManager,
            DatabaseService databaseService,
            AiSqlGenerateService aiSqlGenerateService,
            AiContextService aiContextService
    ) {
        this.sessionManager = sessionManager;
        this.databaseService = databaseService;
        this.aiSqlGenerateService = aiSqlGenerateService;
        this.aiContextService = aiContextService;
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
            ConnectResponse response = ConnectResponse.builder()
                    .sessionId(sessionInfo.getSessionId())
                    .traceId(MDC.get("trace_id"))
                    .expiresAt(sessionInfo.getExpiresAt())
                    .build();
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            if (sessionInfo != null && sessionInfo.getSessionId() != null) {
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
        sessionManager.terminateSession(sessionId);
        databaseService.closeSession(sessionId);
        aiContextService.clear(sessionId);
        return ResponseEntity.ok().build();
    }

    /**
     * Execute SQL query or command with optional limit and timeout.
     *
     * POST /v1/execute
     *
     * @param request Execute request with session ID and SQL
     * @return Query results with metadata
     */
    @PostMapping("/execute")
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
}
