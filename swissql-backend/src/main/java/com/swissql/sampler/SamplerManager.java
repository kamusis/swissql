package com.swissql.sampler;

import com.swissql.model.TopSnapshot;
import com.swissql.model.SessionInfo;
import com.swissql.service.DatabaseService;
import com.swissql.service.SessionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Manages samplers for database sessions.
 */
@Component
public class SamplerManager {
    private static final Logger log = LoggerFactory.getLogger(SamplerManager.class);

    // TODO(P2): Add sampling metrics (last sample duration, failure count, last success time).
    // This helps with observability and troubleshooting sampling issues in production.

    private final SessionManager sessionManager;
    private final DatabaseService databaseService;
    private final CollectorRegistry collectorRegistry;
    private final GenericCollector genericCollector;
    private final ScheduledExecutorService scheduler;

    private final Map<String, TopSampler> samplers = new ConcurrentHashMap<>();
    private final Map<String, String> samplerStoppedReasons = new ConcurrentHashMap<>();

    /**
     * Represents the current top sampler status for a session.
     */
    public static class SamplerStatus {
        private final String status;
        private final String reason;

        public SamplerStatus(String status, String reason) {
            this.status = status;
            this.reason = reason;
        }

        public String getStatus() {
            return status;
        }

        public String getReason() {
            return reason;
        }
    }

    public SamplerManager(SessionManager sessionManager, DatabaseService databaseService,
                          CollectorRegistry collectorRegistry, GenericCollector genericCollector) {
        this.sessionManager = sessionManager;
        this.databaseService = databaseService;
        this.collectorRegistry = collectorRegistry;
        this.genericCollector = genericCollector;
        this.scheduler = Executors.newScheduledThreadPool(10);
    }

    // TODO(P1): Allow multiple sampler types per session (e.g., TopSampler, PerfSampler) each with its own interval
    // and collector set. Possible design:
    // - Maintain Map<String, Map<SamplerType, Sampler>> where SamplerType enumerates roles (TOP, PERF, …).
    // - Start all required samplers on connect; each sampler schedules its own ScheduledFuture with its own interval.
    // - Add explicit APIs to start/stop/restart a specific sampler type without reconnecting the session.
    // - Ensure cleanup/disconnect stops all sampler instances for that session and cancels their tasks.

    /**
     * Returns the latest snapshot for a session.
     *
     * @param sessionId the session ID
     * @return the latest snapshot, or null if no sampler is found
     */
    public TopSnapshot getSnapshot(String sessionId) {
        TopSampler sampler = samplers.get(sessionId);
        if (sampler == null) {
            log.warn("No sampler found for session: {}", sessionId);
            return null;
        }
        return sampler.getLatestSnapshot();
    }

    public String getStoppedReason(String sessionId) {
        return samplerStoppedReasons.get(sessionId);
    }

    /**
     * Returns current top sampler status for a session.
     *
     * Status values:
     * - RUNNING: sampler exists and is running
     * - STOPPED: sampler was auto-stopped and a reason is recorded
     * - STOPPED: sampler is not running
     *
     * @param sessionId the session ID
     * @return the current top sampler status
     */
    public SamplerStatus getTopSamplerStatus(String sessionId) {
        TopSampler sampler = samplers.get(sessionId);
        if (sampler != null && sampler.isRunning()) {
            return new SamplerStatus("RUNNING", null);
        }

        String reason = samplerStoppedReasons.get(sessionId);
        if (reason != null && !reason.isBlank()) {
            return new SamplerStatus("STOPPED", reason);
        }

        return new SamplerStatus("STOPPED", null);
    }

    /**
     * Restarts top sampler for a session.
     *
     * @param sessionId the session ID
     */
    public void restartSampler(String sessionId) {
        stopSampler(sessionId);
        samplerStoppedReasons.remove(sessionId);
        startSampler(sessionId);
    }

    public void startSampler(String sessionId) {
        if (samplers.containsKey(sessionId)) {
            log.warn("Sampler already exists for session: {}", sessionId);
            return;
        }

        samplerStoppedReasons.remove(sessionId);

        collectorRegistry.reloadConfigs();

        try {
            var sessionInfoOpt = sessionManager.getSession(sessionId);
            if (sessionInfoOpt.isEmpty()) {
                log.error("Session not found: {}", sessionId);
                return;
            }

            SessionInfo sessionInfo = sessionInfoOpt.get();
            Connection connection = databaseService.getConnection(sessionInfo);
            String dbType = sessionInfo.getDbType();

            var collectorConfig = collectorRegistry.getConfig(connection, dbType);
            if (collectorConfig == null) {
                String reason = "No collector config found for dbType: " + dbType;
                samplerStoppedReasons.put(sessionId, reason);
                log.warn("Sampler not started for session: {}. reason={}", sessionId, reason);
                return;
            }

            SamplerConfig defaultConfig = SamplerConfig.builder()
                    .intervalSec(10)
                    .enableTopSql(true)
                    .enableTopSessions(true)
                    .maxItems(20)
                    .build();

            TopSampler sampler = new TopSampler(
                    sessionId, connection, dbType, collectorRegistry,
                    genericCollector, scheduler, defaultConfig,
                    (stoppedSessionId, reason) -> {
                        samplers.remove(stoppedSessionId);
                        samplerStoppedReasons.put(stoppedSessionId, reason);
                        log.warn("Sampler stopped for session: {}. reason={}", stoppedSessionId, reason);
                    }
            );

            samplers.put(sessionId, sampler);
            sampler.start();

            // TODO(P1): Document ownership model: SessionManager owns HikariDataSource lifecycle,
            // TopSampler holds a Connection reference for sampling efficiency, and cleanup happens
            // via databaseService.closeSession() which closes entire pool. This design avoids
            // per-sample connection overhead but requires careful handling of stop/shutdown ordering.

            log.info("Started sampler for session: {}", sessionId);
        } catch (Exception e) {
            log.error("Failed to start sampler for session: {}", sessionId, e);
        }
    }

    public void updateConfig(String sessionId, SamplerConfig newConfig) {
        TopSampler sampler = samplers.get(sessionId);
        if (sampler == null) {
            log.warn("No sampler found for session: {}", sessionId);
            return;
        }

        sampler.updateConfig(newConfig);
        log.info("Updated config for sampler: {}", sessionId);
    }

    public void stopSampler(String sessionId) {
        TopSampler sampler = samplers.remove(sessionId);
        if (sampler != null) {
            sampler.stop();
            log.info("Stopped sampler for session: {}", sessionId);
            return;
        }

        // Stop is idempotent; do not record a reason for manual stop.
    }

    public void cleanup() {
        samplers.values().forEach(TopSampler::stop);
        samplers.clear();
        samplerStoppedReasons.clear();
        scheduler.shutdown();
    }

    public CollectorRegistry getCollectorRegistry() {
        return collectorRegistry;
    }
}
