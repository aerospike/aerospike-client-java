/*
 * Copyright (c) 2012-2025 Aerospike, Inc.
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

import java.util.Map;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.configuration.primitiveprops.BooleanProperty;
import com.aerospike.client.configuration.primitiveprops.IntProperty;
import com.aerospike.client.Log;

public class StaticClientConfig {
    public IntProperty configInterval;
    public IntProperty maxConnectionsPerNode;
    public IntProperty minConnectionsPerNode;
    public IntProperty asyncMaxConnectionsPerNode;
    public IntProperty asyncMinConnectionsPerNode;
    public Map<String,String> ipMap;
    public BooleanProperty useServicesAlternate;
    public StaticDiscoveryConfig discovery;
    public StaticTranslationConfig translation;

    public StaticClientConfig() {
    }

    public void setConfigInterval(IntProperty configInterval) { this.configInterval = configInterval; }

    public void setMaxConnectionsPerNode(IntProperty maxConnectionsPerNode) { this.maxConnectionsPerNode = maxConnectionsPerNode; }

    public void setMinConnectionsPerNode(IntProperty minConnectionsPerNode) { this.minConnectionsPerNode = minConnectionsPerNode; }

    public void setAsyncMaxConnectionsPerNode(IntProperty asyncMaxConnectionsPerNode) { this.asyncMaxConnectionsPerNode = asyncMaxConnectionsPerNode; }

    public void setAsyncMinConnectionsPerNode(IntProperty asyncMinConnectionsPerNode) { this.asyncMinConnectionsPerNode = asyncMinConnectionsPerNode; }

    public IntProperty getConfigInterval() { return configInterval; }

    public IntProperty getMaxConnectionsPerNode() { return maxConnectionsPerNode; }

    public IntProperty getMinConnectionsPerNode() { return minConnectionsPerNode; }

    public IntProperty getAsyncMaxConnectionsPerNode() { return asyncMaxConnectionsPerNode; }

    public IntProperty getAsyncMinConnectionsPerNode() { return asyncMinConnectionsPerNode; }

    private void setIpMap(Object ipMap) {
        this.ipMap = (ipMap != null)? YamlValues.stringMap("static.client.ip_map", ipMap) : null;
        checkStaticMap();
    }

    private void setUseServicesAlternate(Object useServicesAlternate) {
        this.useServicesAlternate = (useServicesAlternate != null)?
            new BooleanProperty(YamlValues.boolValue("static.client.use_services_alternate", useServicesAlternate)) : null;
    }

    private void setDiscovery(Object discovery) {
        this.discovery = (discovery != null)? new StaticDiscoveryConfig(discovery) : null;
    }

    private void setTranslation(Object translation) {
        this.translation = (translation != null)? new StaticTranslationConfig(translation) : null;
        checkStaticMap();
    }

    private void checkStaticMap() {
        if (ipMap != null && translation != null && translation.staticMap != null) {
            throw new AerospikeException("static.client.translation.static_map and static.client.ip_map are both set. " +
                "Set one of them");
        }
    }

    private static Integer value(IntProperty property) {
        return (property != null)? property.value : null;
    }

    @Override
    public String toString() {
        StringBuffer propsString = new StringBuffer("{");
        try {
            propsString.append(" config_interval=").append(value(configInterval)).append(", ");
            propsString.append(" max_connections_per_node=").append(value(maxConnectionsPerNode)).append(", ");
            propsString.append(" min_connections_per_node=").append(value(minConnectionsPerNode)).append(", ");
            propsString.append(" async_max_connections_per_node=").append(value(asyncMaxConnectionsPerNode)).append(", ");
            propsString.append(" async_min_connections_per_node=").append(value(asyncMinConnectionsPerNode)).append(", ");
            propsString.append(" ip_map=").append(ipMap).append(", ");
            propsString.append(" use_services_alternate=").append((useServicesAlternate != null)? useServicesAlternate.value : null).append(", ");
            propsString.append(" discovery=").append(discovery).append(", ");
            propsString.append(" translation=").append(translation).append(", ");
        } catch (Exception e) {
            Log.error(e.toString());
        }
        return propsString.append("}").toString();
    }
}
