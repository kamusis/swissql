package com.swissql.model;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Map;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TopSnapshot {
    private String dbType;
    private Integer intervalSec;
    private Map<String, Object> context;
    private Map<String, Object> cpu;
    private Map<String, Object> sessions;
    private List<Map<String, Object>> waits;
    private List<Map<String, Object>> topSessions;
    private Map<String, Object> io;

    // TODO(P2): Add LocalDateTime snapshotTime field to record when snapshot was taken.
    // Currently only has intervalSec but not actual capture time, which is useful
    // for trend analysis and debugging.
}
