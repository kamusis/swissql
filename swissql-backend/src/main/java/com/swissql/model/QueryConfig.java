package com.swissql.model;

import lombok.Data;

import java.util.List;

@Data
public class QueryConfig {
    private String description;
    private String sql;
    private Boolean singleRow;
    private List<String> parameters;
}
