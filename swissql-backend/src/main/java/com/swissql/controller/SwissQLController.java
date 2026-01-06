package com.swissql.controller;

import com.swissql.api.*;
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

    public SwissQLController(SessionManager sessionManager, DatabaseService databaseService) {
        this.sessionManager = sessionManager;
        this.databaseService = databaseService;
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
        // TODO: Implement actual AI integration

        // Stub response - LLM not configured yet
        AiGenerateResponse response = AiGenerateResponse.builder()
                .sql("")  // Empty - AI generation requires LLM configuration
                .risk("UNKNOWN")
                .explanation("AI generation requires LLM configuration. " +
                            "Set OPENAI_API_KEY or DEEPSEEK_API_KEY environment variable to enable.")
                .warnings(List.of(
                    "LLM not configured - AI generation is disabled",
                    "Set environment variable: export OPENAI_API_KEY=your-key",
                    "Or use DEEPSEEK_API_KEY for DeepSeek LLM integration"
                ))
                .traceId(MDC.get("trace_id"))
                .build();

        return ResponseEntity.ok(response);
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
