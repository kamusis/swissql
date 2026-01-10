package com.swissql.sampler;

import com.swissql.model.CollectorConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;
import java.io.FileInputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

@Component
public class CollectorRegistry {
    private static final Logger log = LoggerFactory.getLogger(CollectorRegistry.class);

    @Value("${jdbc.drivers.path:jdbc_drivers}")
    private String jdbcDriversPath;

    private volatile Map<String, List<CollectorConfig>> configsByDbType = new ConcurrentHashMap<>();

    @PostConstruct
    public void loadConfigs() {
        reloadConfigs();
    }

    /**
     * Reloads collector YAML configurations from the configured driver directory.
     *
     * This method builds a new configuration map and swaps it atomically to avoid partial updates.
     */
    public void reloadConfigs() {
        try {
            Map<String, List<CollectorConfig>> newConfigsByDbType = new ConcurrentHashMap<>();
            Path driversDir = Paths.get(jdbcDriversPath);
            if (!Files.exists(driversDir)) {
                log.warn("JDBC drivers directory not found: {}", jdbcDriversPath);
                configsByDbType = newConfigsByDbType;
                return;
            }

            try (Stream<Path> dbTypeDirs = Files.list(driversDir)) {
                dbTypeDirs.filter(Files::isDirectory).forEach(dbTypeDir -> {
                    String dbType = dbTypeDir.getFileName().toString();
                    List<CollectorConfig> configs = loadConfigsForDbType(dbType, dbTypeDir);
                    if (!configs.isEmpty()) {
                        newConfigsByDbType.put(dbType, configs);
                    }
                });
            }

            configsByDbType = newConfigsByDbType;
            log.info("Reloaded collector configs for {} database types", newConfigsByDbType.size());
        } catch (Exception e) {
            log.error("Failed to reload collector configs", e);
        }
    }

    private List<CollectorConfig> loadConfigsForDbType(String dbType, Path dbTypeDir) {
        List<CollectorConfig> configs = new ArrayList<>();

        try (Stream<Path> yamlFiles = Files.list(dbTypeDir)) {
            yamlFiles.filter(p -> p.toString().endsWith(".yaml") || p.toString().endsWith(".yml"))
                    .forEach(yamlFile -> {
                        try {
                            CollectorConfig config = loadYamlConfig(yamlFile);
                            if (config != null && config.getSupportedVersions() != null) {
                                if (yamlFile.getFileName() != null) {
                                    config.setSourceFile(yamlFile.getFileName().toString());
                                }
                                configs.add(config);
                                log.debug("Loaded config: {} for dbType: {}", yamlFile.getFileName(), dbType);
                            }
                        } catch (Exception e) {
                            log.error("Failed to load config: {}", yamlFile, e);
                        }
                    });
        } catch (Exception e) {
            log.error("Failed to list configs for dbType: {}", dbType, e);
        }

        if (!configs.isEmpty()) {
            return configs;
        }

        return List.of();
    }

    private CollectorConfig loadYamlConfig(Path yamlFile) throws Exception {
        try (FileInputStream fis = new FileInputStream(yamlFile.toFile())) {
            return new org.yaml.snakeyaml.Yaml().loadAs(fis, CollectorConfig.class);
        }
    }

    public CollectorConfig getConfig(Connection conn, String dbType) {
        try {
            String dbVersion = conn.getMetaData().getDatabaseProductVersion();
            String numericVersion = extractNumericVersion(dbVersion);
            Map<String, List<CollectorConfig>> snapshot = configsByDbType;
            List<CollectorConfig> configs = snapshot.get(dbType);

            if (configs == null || configs.isEmpty()) {
                log.error(
                        "No collector configs found for dbType: {} (dbVersion='{}', numericVersion='{}', loadedDbTypes={})",
                        dbType,
                        dbVersion,
                        numericVersion,
                        snapshot.keySet()
                );
                return null;
            }

            CollectorConfig matched = findMatchingConfig(configs, numericVersion);
            if (matched == null) {
                String availableRanges = configs.stream()
                        .map(c -> {
                            if (c == null || c.getSupportedVersions() == null) {
                                return "<missing supportedVersions>";
                            }
                            String sourceFile = c.getSourceFile();
                            String range = c.getSupportedVersions().getMin() + "-" + c.getSupportedVersions().getMax();
                            if (sourceFile == null || sourceFile.isBlank()) {
                                return range;
                            }
                            return sourceFile + ":" + range;
                        })
                        .distinct()
                        .sorted()
                        .reduce((a, b) -> a + ", " + b)
                        .orElse("<none>");

                log.error(
                        "No matching collector config for dbType: {} (dbVersion='{}', numericVersion='{}', configsLoaded={}, supportedVersions=[{}])",
                        dbType,
                        dbVersion,
                        numericVersion,
                        configs.size(),
                        availableRanges
                );
                return null;
            }

            return matched;
        } catch (Exception e) {
            log.error("Failed to get config for dbType: {}", dbType, e);
            return null;
        }
    }

    private String extractNumericVersion(String fullVersion) {
        // Extract numeric version from Oracle version string
        // Example: "Oracle AI Database 26ai Enterprise Edition Release 23.26.0.1.0 - for Oracle Cloud and Engineered Systems"
        // Extract: "23.26.0.1.0"
        java.util.regex.Pattern pattern = java.util.regex.Pattern.compile("(\\d+\\.\\d+\\.\\d+\\.\\d+\\.\\d+)");
        java.util.regex.Matcher matcher = pattern.matcher(fullVersion);
        if (matcher.find()) {
            return matcher.group(1);
        }
        // Fallback: try to extract just the first number sequence
        pattern = java.util.regex.Pattern.compile("(\\d+\\.\\d+\\.\\d+)");
        matcher = pattern.matcher(fullVersion);
        if (matcher.find()) {
            return matcher.group(1);
        }
        return fullVersion;
    }

    private CollectorConfig findMatchingConfig(List<CollectorConfig> configs, String dbVersion) {
        return configs.stream()
                .filter(config -> isVersionInRange(dbVersion, config.getSupportedVersions()))
                .max(Comparator.comparing(config -> config.getSupportedVersions().getMax()))
                .orElse(null);
    }

    private boolean isVersionInRange(String dbVersion, com.swissql.model.SupportedVersions range) {
        try {
            String[] dbParts = dbVersion.split("\\.");
            String[] minParts = range.getMin().split("\\.");
            String[] maxParts = range.getMax().split("\\.");

            return compareVersions(dbParts, minParts) >= 0 && compareVersions(dbParts, maxParts) <= 0;
        } catch (Exception e) {
            log.error("Failed to compare version: {} with range: {}-{}", dbVersion, range.getMin(), range.getMax(), e);
            return false;
        }
    }

    private int compareVersions(String[] v1, String[] v2) {
        int maxLength = Math.max(v1.length, v2.length);
        for (int i = 0; i < maxLength; i++) {
            int n1 = i < v1.length ? Integer.parseInt(v1[i]) : 0;
            int n2 = i < v2.length ? Integer.parseInt(v2[i]) : 0;
            if (n1 != n2) {
                return Integer.compare(n1, n2);
            }
        }
        return 0;
    }
}
