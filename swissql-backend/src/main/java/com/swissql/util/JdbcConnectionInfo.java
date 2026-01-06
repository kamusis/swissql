package com.swissql.util;

import lombok.Builder;
import lombok.Data;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

@Data
@Builder
public class JdbcConnectionInfo {
    private String url;
    private String username;
    private String password;
    private String dbType;
}
