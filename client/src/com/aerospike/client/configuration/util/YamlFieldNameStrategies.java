package com.aerospike.client.configuration.util;

public class YamlFieldNameStrategies {
    // Simple regex based camel to snake case conversion
    public static String camelToSnake(String camelCase) {
        return camelCase
        .replaceAll("([a-z])([A-Z])", "$1_$2")
            .toLowerCase();
    }
}
