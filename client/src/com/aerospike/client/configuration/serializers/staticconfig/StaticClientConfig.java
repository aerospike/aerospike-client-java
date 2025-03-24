package com.aerospike.client.configuration.serializers.staticconfig;

import com.aerospike.client.configuration.serializers.dynamicconfig.primitiveprops.IntProperty;
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
        } finally {
            return propsString.append("}").toString();
        }
    }
}
