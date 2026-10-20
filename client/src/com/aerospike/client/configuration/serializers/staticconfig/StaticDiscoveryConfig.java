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

import java.util.List;
import java.util.Locale;
import java.util.Map;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.configuration.primitiveprops.IntProperty;
import com.aerospike.client.configuration.primitiveprops.StringProperty;
import com.aerospike.client.discovery.SeedMergePolicy;

/**
 * Settings under {@code static.client.discovery}.
 */
public class StaticDiscoveryConfig {
    static final String PATH = "static.client.discovery";

    /**
     * Built-in seed candidate provider names.
     */
    public static final String STATIC = "static";
    public static final String DNS = "dns";
    public static final String AWS_TAGS = "aws-tags";
    public static final List<String> PROVIDERS = List.of(STATIC, DNS, AWS_TAGS);

    /**
     * Built-in provider names accepted from YAML.
     */
    public static final List<String> YAML_PROVIDERS = List.of(STATIC);

    private static final List<String> CODE_ONLY_PROVIDERS = List.of(DNS, AWS_TAGS);
    private static final List<String> MERGE_POLICIES = List.of("merge", "replace", "discovery_only");

    /**
     * {@code provider}: {@code static} or a name registered with
     * {@link com.aerospike.client.policy.ClientPolicy#registerSeedCandidateProvider}.
     */
    public StringProperty provider;

    /**
     * Every key of the block except {@code provider}, passed to the registered factory. Null when
     * {@code provider} is absent or {@code static}.
     */
    public Map<String,Object> options;

    /**
     * {@code merge_policy}.
     */
    public SeedMergePolicy mergePolicy;

    /**
     * {@code refresh_interval_ms}.
     */
    public IntProperty refreshIntervalMs;

    /**
     * {@code tend_refresh_deadline_ms}.
     */
    public IntProperty tendRefreshDeadlineMs;

    /**
     * Parse the raw {@code static.client.discovery} block. Unknown keys are ignored.
     *
     * @throws AerospikeException if a setting is invalid. The message names the key or value.
     */
    StaticDiscoveryConfig(Object block) {
        Map<?,?> map = YamlValues.block(PATH, block);

        for (String key : List.of("execution_mode", "refresh_timeout_ms")) {
            if (map.containsKey(key)) {
                throw new AerospikeException(PATH + '.' + key + " is not supported: the Java client runs " +
                    "discovery on the tend thread only");
            }
        }

        Object value = map.get("provider");

        if (value != null) {
            if (CODE_ONLY_PROVIDERS.contains(value)) {
                throw new AerospikeException(PATH + ".provider '" + value + "' is not yet configurable from YAML. " +
                    "Set ClientPolicy.seedCandidateProvider in code");
            }
            if (value instanceof String && ! YAML_PROVIDERS.contains(value)) {
                provider = new StringProperty((String)value);
                options = YamlValues.options(PATH, map);
            }
            else {
                provider = new StringProperty(YamlValues.enumValue(PATH + ".provider", value, YAML_PROVIDERS));
            }
        }

        value = map.get("merge_policy");

        if (value != null) {
            String name = YamlValues.enumValue(PATH + ".merge_policy", value, MERGE_POLICIES);
            mergePolicy = SeedMergePolicy.valueOf(name.toUpperCase(Locale.ROOT));
        }

        value = map.get("refresh_interval_ms");

        if (value != null) {
            refreshIntervalMs = new IntProperty(YamlValues.intValue(PATH + ".refresh_interval_ms", value));
        }

        value = map.get("tend_refresh_deadline_ms");

        if (value != null) {
            tendRefreshDeadlineMs = new IntProperty(YamlValues.intValue(PATH + ".tend_refresh_deadline_ms", value));
        }
    }

    @Override
    public String toString() {
        return "{ provider=" + ((provider != null)? provider.value : null) +
            ", merge_policy=" + mergePolicy +
            ", refresh_interval_ms=" + ((refreshIntervalMs != null)? refreshIntervalMs.value : null) +
            ", tend_refresh_deadline_ms=" + ((tendRefreshDeadlineMs != null)? tendRefreshDeadlineMs.value : null) + " }";
    }
}
