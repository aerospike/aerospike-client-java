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
import com.aerospike.client.configuration.primitiveprops.StringProperty;
import com.aerospike.client.discovery.PortMapAddressTranslator;
import com.aerospike.client.discovery.TranslationMissMode;

/**
 * Settings under {@code static.client.translation}.
 */
public class StaticTranslationConfig {
    static final String PATH = "static.client.translation";
    private static final List<String> PROVIDERS = List.of("static-map", "port-map", "pass-through");
    private static final List<String> ON_MISS = List.of("pass_through", "strict");

    /**
     * {@code provider}: {@code static-map}, {@code port-map} or {@code pass-through}. Null when
     * neither {@code provider} nor {@code static_map} is set.
     */
    public StringProperty provider;

    /**
     * {@code on_miss}.
     */
    public TranslationMissMode onMiss;

    /**
     * {@code static_map}: advertised host to host.
     */
    public Map<String,String> staticMap;

    /**
     * {@code port_map}: advertised {@code host:port} or node name to {@code host:port}.
     */
    public Map<String,String> portMap;

    /**
     * Parse the raw {@code static.client.translation} block. Unknown keys are ignored.
     *
     * @throws AerospikeException if a setting is invalid. The message names the key or value.
     */
    StaticTranslationConfig(Object block) {
        Map<?,?> map = YamlValues.block(PATH, block);

        Object value = map.get("on_miss");

        if (value != null) {
            String name = YamlValues.enumValue(PATH + ".on_miss", value, ON_MISS);
            onMiss = TranslationMissMode.valueOf(name.toUpperCase(Locale.ROOT));
        }

        value = map.get("static_map");

        if (value != null) {
            staticMap = YamlValues.stringMap(PATH + ".static_map", value);
        }

        value = map.get("port_map");

        if (value != null) {
            portMap = YamlValues.stringMap(PATH + ".port_map", value);
        }

        value = map.get("provider");

        String name = (value != null)? YamlValues.enumValue(PATH + ".provider", value, PROVIDERS) : "static-map";

        if (staticMap != null && ! name.equals("static-map")) {
            throw new AerospikeException(PATH + ".static_map is only valid with provider static-map, not '" + name + "'");
        }

        if (portMap != null && ! name.equals("port-map")) {
            throw new AerospikeException(PATH + ".port_map is only valid with provider port-map, not '" + name + "'");
        }

        if (name.equals("port-map")) {
            if (portMap == null) {
                throw new AerospikeException(PATH + ".port_map is required with provider port-map");
            }

            try {
                new PortMapAddressTranslator(portMap);
            }
            catch (AerospikeException e) {
                throw new AerospikeException(PATH + ".port_map: " + e.getBaseMessage());
            }
        }

        if (value != null || staticMap != null) {
            provider = new StringProperty(name);
        }
    }

    @Override
    public String toString() {
        return "{ provider=" + ((provider != null)? provider.value : null) +
            ", on_miss=" + onMiss +
            ", static_map=" + staticMap +
            ", port_map=" + portMap + " }";
    }
}
