/*
 * Copyright (c) 2012-2026 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements WHICH ARE COMPATIBLE WITH THE APACHE LICENSE, VERSION 2.0.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.aerospike.client.configuration.serializers.staticconfig;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.aerospike.client.AerospikeException;

/**
 * Typed access to raw YAML values. Every error names the full YAML key.
 */
final class YamlValues {
    static Map<?,?> block(String path, Object value) {
        if (!(value instanceof Map)) {
            throw invalid(path, value, "expected a map");
        }
        return (Map<?,?>)value;
    }

    static Map<String,String> stringMap(String path, Object value) {
        Map<String,String> map = new LinkedHashMap<>();

        for (Map.Entry<?,?> entry : block(path, value).entrySet()) {
            if (!(entry.getKey() instanceof String)) {
                throw new AerospikeException("Invalid key '" + entry.getKey() + "' in " + path + ": expected a string");
            }
            if (!(entry.getValue() instanceof String)) {
                throw invalid(path + '.' + entry.getKey(), entry.getValue(), "expected a string");
            }
            map.put((String)entry.getKey(), (String)entry.getValue());
        }
        return Collections.unmodifiableMap(map);
    }

    static Map<String,Object> options(String path, Map<?,?> block) {
        Map<String,Object> map = new LinkedHashMap<>();

        for (Map.Entry<?,?> entry : block.entrySet()) {
            if (!(entry.getKey() instanceof String)) {
                throw new AerospikeException("Invalid key '" + entry.getKey() + "' in " + path + ": expected a string");
            }
            if (! entry.getKey().equals("provider")) {
                map.put((String)entry.getKey(), entry.getValue());
            }
        }
        return Collections.unmodifiableMap(map);
    }

    static int intValue(String path, Object value) {
        if (!(value instanceof Integer)) {
            throw invalid(path, value, "expected an integer");
        }
        return (Integer)value;
    }

    static boolean boolValue(String path, Object value) {
        if (!(value instanceof Boolean)) {
            throw invalid(path, value, "expected true or false");
        }
        return (Boolean)value;
    }

    static String enumValue(String path, Object value, List<String> values) {
        if (!(value instanceof String) || !values.contains(value)) {
            throw invalid(path, value, "valid values: " + String.join(", ", values));
        }
        return (String)value;
    }

    private static AerospikeException invalid(String path, Object value, String reason) {
        return new AerospikeException("Invalid " + path + " value '" + value + "': " + reason);
    }
}
