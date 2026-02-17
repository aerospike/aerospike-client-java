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

import com.aerospike.client.configuration.primitiveprops.IntProperty;
import com.aerospike.client.Log;

public class StaticClientConfig {
    public IntProperty configInterval;
    public IntProperty maxConnectionsPerNode;
    public IntProperty minConnectionsPerNode;
    public IntProperty asyncMaxConnectionsPerNode;
    public IntProperty asyncMinConnectionsPerNode;

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

    @Override
    public String toString() {
        StringBuffer propsString = new StringBuffer("{");
        try {
            propsString.append(" config_interval=").append(configInterval.value).append(", ");
            propsString.append(" max_connections_per_node=").append(maxConnectionsPerNode.value).append(", ");
            propsString.append(" min_connections_per_node=").append(minConnectionsPerNode.value).append(", ");
            propsString.append(" async_max_connections_per_node=").append(asyncMaxConnectionsPerNode.value).append(", ");
            propsString.append(" async_min_connections_per_node=").append(asyncMinConnectionsPerNode.value).append(", ");
        } catch (Exception e) {
            Log.error(e.toString());
        }
        return propsString.append("}").toString();
    }
}
