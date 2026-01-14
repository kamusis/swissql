package com.swissql.util;

import java.util.Locale;

/**
 * Normalizes incoming dbType (and aliases) into canonical dbType strings.
 *
 * This must run before builtin checks and before {@code DriverRegistry.find(...)}.
 */
public final class DbTypeNormalizer {

    private DbTypeNormalizer() {
    }

    /**
     * Normalize dbType.
     *
     * @param dbType incoming dbType
     * @return normalized dbType (lowercased + alias mapping)
     */
    public static String normalize(String dbType) {
        if (dbType == null) {
            return "";
        }
        String v = dbType.trim().toLowerCase(Locale.ROOT);
        if (v.isBlank()) {
            return "";
        }
        return v;
    }
}
