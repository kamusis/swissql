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

    public SamplerManager(SessionManager sessionManager, DatabaseService databaseService,
                          CollectorRegistry collectorRegistry, GenericCollector genericCollector) {
        this.sessionManager = sessionManager;
        this.databaseService = databaseService;
        this.collectorRegistry = collectorRegistry;
        this.genericCollector = genericCollector;
        this.scheduler = Executors.newScheduledThreadPool(10);
    }

    public TopSnapshot getSnapshot(String sessionId) {
        TopSampler sampler = samplers.get(sessionId);
        if (sampler == null) {
            log.warn("No sampler found for session: {}", sessionId);
            return null;
        }
        return sampler.getLatestSnapshot();
    }

    public void startSampler(String sessionId) {
        if (samplers.containsKey(sessionId)) {
            log.warn("Sampler already exists for session: {}", sessionId);
            return;
        }

        try {
            var sessionInfoOpt = sessionManager.getSession(sessionId);
            if (sessionInfoOpt.isEmpty()) {
                log.error("Session not found: {}", sessionId);
                return;
            }

            SessionInfo sessionInfo = sessionInfoOpt.get();
            Connection connection = databaseService.getConnection(sessionInfo);
            String dbType = sessionInfo.getDbType();

            SamplerConfig defaultConfig = SamplerConfig.builder()
                    .intervalSec(10)
                    .enableTopSql(true)
                    .enableTopSessions(true)
                    .maxItems(20)
                    .build();

            TopSampler sampler = new TopSampler(
                    sessionId, connection, dbType, collectorRegistry,
                    genericCollector, scheduler, defaultConfig
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
        }
    }

    public void cleanup() {
        samplers.values().forEach(TopSampler::stop);
        samplers.clear();
        scheduler.shutdown();
    }

    public CollectorRegistry getCollectorRegistry() {
        return collectorRegistry;
    }
}
