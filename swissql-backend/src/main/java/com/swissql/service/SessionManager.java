package com.swissql.service;

import com.swissql.api.ConnectRequest;
import com.swissql.model.SessionInfo;
import org.springframework.stereotype.Service;

import java.time.OffsetDateTime;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Service
public class SessionManager {
    private final Map<String, SessionInfo> sessions = new ConcurrentHashMap<>();
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    public SessionManager() {
        // Run cleanup task every 5 minutes
        scheduler.scheduleAtFixedRate(this::cleanupExpiredSessions, 5, 5, TimeUnit.MINUTES);
    }

    public SessionInfo createSession(ConnectRequest request) {
        String sessionId = UUID.randomUUID().toString();
        OffsetDateTime now = OffsetDateTime.now();
        SessionInfo sessionInfo = SessionInfo.builder()
                .sessionId(sessionId)
                .dsn(request.getDsn())
                .dbType(request.getDbType())
                .options(request.getOptions())
                .createdAt(now)
                .lastAccessedAt(now)
                .expiresAt(now.plusHours(24)) // maxLifetime=24h
                .build();
        
        sessions.put(sessionId, sessionInfo);
        return sessionInfo;
    }

    public Optional<SessionInfo> getSession(String sessionId) {
        SessionInfo session = sessions.get(sessionId);
        if (session != null) {
            if (isSessionExpired(session)) {
                sessions.remove(sessionId);
                return Optional.empty();
            }
            session.setLastAccessedAt(OffsetDateTime.now());
            return Optional.of(session);
        }
        return Optional.empty();
    }

    public void removeSession(String sessionId) {
        sessions.remove(sessionId);
    }

    public void terminateSession(String sessionId) {
        removeSession(sessionId);
    }

    private boolean isSessionExpired(SessionInfo session) {
        OffsetDateTime now = OffsetDateTime.now();
        // idleTimeout=30m
        boolean idleExpired = session.getLastAccessedAt().plusMinutes(30).isBefore(now);
        // maxLifetime=24h
        boolean lifeExpired = session.getExpiresAt().isBefore(now);
        return idleExpired || lifeExpired;
    }

    private void cleanupExpiredSessions() {
        sessions.entrySet().removeIf(entry -> isSessionExpired(entry.getValue()));
    }
}
