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

package com.aerospike.client.configuration.serializers.dynamicconfig;

import com.aerospike.client.configuration.primitiveprops.BooleanProperty;
import com.aerospike.client.configuration.primitiveprops.DoubleProperty;
import com.aerospike.client.configuration.primitiveprops.IntProperty;
import com.aerospike.client.Log;
import com.aerospike.client.policy.Replica;


public class DynamicScanConfig {
    public IntProperty connectTimeout;
    public Replica replica;
    public IntProperty sleepBetweenRetries;
    public IntProperty socketTimeout;
    public IntProperty timeoutDelay;
    public IntProperty totalTimeout;
    public IntProperty maxRetries;
    public BooleanProperty concurrentNodes;
    public IntProperty maxConcurrentNodes;
    public DoubleProperty sleepMultiplier;

    public DynamicScanConfig() {}

    public void setConnectTimeout(IntProperty connectTimeout) { this.connectTimeout = connectTimeout; }

    public void setReplica(Replica replica) { this.replica = replica; }

    public void setSleepBetweenRetries(IntProperty sleepBetweenRetries) { this.sleepBetweenRetries = sleepBetweenRetries; }

    public void setSocketTimeout(IntProperty socketTimeout) { this.socketTimeout = socketTimeout; }

    public void setTimeoutDelay(IntProperty timeoutDelay) { this.timeoutDelay = timeoutDelay; }

    public void setTotalTimeout(IntProperty totalTimeout) { this.totalTimeout = totalTimeout; }

    public void setMaxRetries(IntProperty maxRetries) { this.maxRetries = maxRetries; }

    public void setConcurrentNodes(BooleanProperty concurrentNodes) { this.concurrentNodes = concurrentNodes; }

    public void setMaxConcurrentNodes(IntProperty maxConcurrentNodes) { this.maxConcurrentNodes = maxConcurrentNodes; }

    public void setSleepMultiplier(DoubleProperty sleepMultiplier) { this.sleepMultiplier = sleepMultiplier; }

    public IntProperty getConnectTimeout() { return connectTimeout; }

    public Replica getReplica() { return replica; }

    public IntProperty getSleepBetweenRetries() { return sleepBetweenRetries; }

    public IntProperty getSocketTimeout() { return socketTimeout; }

    public IntProperty getTimeoutDelay() { return timeoutDelay; }

    public IntProperty getTotalTimeout() { return totalTimeout; }

    public IntProperty getMaxRetries() { return maxRetries; }

    public BooleanProperty getConcurrentNodes() { return concurrentNodes; }

    public IntProperty getMaxConcurrentNodes() { return maxConcurrentNodes; }

    public DoubleProperty getSleepMultiplier() { return sleepMultiplier; }

    @Override
    public String toString() {
        StringBuffer propsString = new StringBuffer("{");
        try {
            propsString.append(" connect_timeout=").append(connectTimeout.value).append(", ");
            propsString.append(" replica=").append(replica).append(", ");
            propsString.append(" sleep_between_retries=").append(sleepBetweenRetries.value).append(", ");
            propsString.append(" socket_timeout=").append(socketTimeout.value).append(", ");
            propsString.append(" timeout_delay=").append(timeoutDelay.value).append(", ");
            propsString.append(" total_timeout=").append(totalTimeout.value).append(", ");
            propsString.append(" max_retries=").append(maxRetries.value).append(", ");
            propsString.append(" concurrent_nodes=").append(concurrentNodes.value).append(", ");
            propsString.append(" max_concurrent_nodes=").append(maxConcurrentNodes.value).append(", ");
            propsString.append(" sleep_multiplier=").append(sleepMultiplier.value).append(", ");
        } catch (Exception e) {
            Log.error(e.toString());
        }
        return propsString.append("}").toString();
    }
}
