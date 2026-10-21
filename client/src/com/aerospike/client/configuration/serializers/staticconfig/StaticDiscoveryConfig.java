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
import com.aerospike.client.discovery.DnsSeedCandidateProvider;
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
    public static final List<String> YAML_PROVIDERS = List.of(STATIC, DNS);

    private static final List<String> CODE_ONLY_PROVIDERS = List.of(AWS_TAGS);
    private static final List<String> MERGE_POLICIES = List.of("merge", "replace", "discovery_only");
    private static final List<String> RECORD_TYPES = List.of("a", "srv");

    /**
     * {@code provider}: {@code static}, {@code dns} or a name registered with
     * {@link com.aerospike.client.policy.ClientPolicy#registerSeedCandidateProvider}.
     */
    public StringProperty provider;

    /**
     * Every key of the block except {@code provider}, passed to the registered factory. Null when
     * {@code provider} is absent or {@code static}.
     */
    public Map<String,Object> options;

    /**
     * {@code dns}: settings of provider {@code dns}, validated by
     * {@link DnsSeedCandidateProvider.Builder#build()} when the provider is built. Null unless
     * {@code provider} is {@code dns}.
     */
    public DnsSeedCandidateProvider.Builder dns;

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

        value = map.get(DNS);

        if (provider != null && DNS.equals(provider.value)) {
            if (value == null) {
                throw new AerospikeException(PATH + '.' + DNS + " is required with provider " + DNS);
            }
            dns = dns(value);
        }
        else if (value != null) {
            throw new AerospikeException(PATH + '.' + DNS + " is only valid with provider " + DNS);
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

    private static DnsSeedCandidateProvider.Builder dns(Object block) {
        String path = PATH + '.' + DNS;
        Map<?,?> map = YamlValues.block(path, block);
        DnsSeedCandidateProvider.Builder builder = DnsSeedCandidateProvider.builder();
        Object value = map.get("names");

        if (value == null) {
            throw new AerospikeException(path + ".names is required");
        }
        builder.names(YamlValues.stringList(path + ".names", value));

        value = map.get("record_type");

        if (value != null) {
            String name = YamlValues.enumValue(path + ".record_type", value, RECORD_TYPES);
            builder.recordType(DnsSeedCandidateProvider.RecordType.valueOf(name.toUpperCase(Locale.ROOT)));
        }

        value = map.get("port");

        if (value != null) {
            builder.port(YamlValues.intValue(path + ".port", value, 1, 65535));
        }

        value = map.get("tls_name");

        if (value != null) {
            builder.tlsName(YamlValues.stringValue(path + ".tls_name", value));
        }

        value = map.get("timeout_ms");

        if (value != null) {
            builder.timeout(YamlValues.intValue(path + ".timeout_ms", value, 1, Integer.MAX_VALUE));
        }

        value = map.get("servers");

        if (value != null) {
            builder.servers(YamlValues.stringList(path + ".servers", value));
        }
        return builder;
    }

    @Override
    public String toString() {
        return "{ provider=" + ((provider != null)? provider.value : null) +
            ", merge_policy=" + mergePolicy +
            ", refresh_interval_ms=" + ((refreshIntervalMs != null)? refreshIntervalMs.value : null) +
            ", tend_refresh_deadline_ms=" + ((tendRefreshDeadlineMs != null)? tendRefreshDeadlineMs.value : null) + " }";
    }
}
