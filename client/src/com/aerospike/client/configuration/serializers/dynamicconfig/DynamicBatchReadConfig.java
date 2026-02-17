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
import com.aerospike.client.policy.ReadModeAP;
import com.aerospike.client.policy.ReadModeSC;
import com.aerospike.client.policy.Replica;


public class DynamicBatchReadConfig {
    public ReadModeAP readModeAP;
    public ReadModeSC readModeSC;
    public IntProperty connectTimeout;;
    public Replica replica;
    public IntProperty sleepBetweenRetries;
    public IntProperty socketTimeout;
    public IntProperty timeoutDelay;
    public IntProperty totalTimeout;
    public IntProperty maxRetries;
    public IntProperty maxConcurrentThreads;
    public BooleanProperty allowInline;
    public BooleanProperty allowInlineSSD;
    public BooleanProperty respondAllKeys;
    public DoubleProperty sleepMultiplier;

    public DynamicBatchReadConfig() {}

    public void setReadModeSC(ReadModeSC readModeSC) { this.readModeSC = readModeSC; }

    public void setReadModeAP(ReadModeAP readModeAP) { this.readModeAP = readModeAP; }

    public void setConnectTimeout(IntProperty connectTimeout) { this.connectTimeout = connectTimeout; }

    public void setReplica(Replica replica) { this.replica = replica; }

    public void setSleepBetweenRetries(IntProperty sleepBetweenRetries) { this.sleepBetweenRetries = sleepBetweenRetries; }

    public void setSocketTimeout(IntProperty socketTimeout) { this.socketTimeout = socketTimeout; }

    public void setTimeoutDelay(IntProperty timeoutDelay) { this.timeoutDelay = timeoutDelay; }

    public void setTotalTimeout(IntProperty totalTimeout) { this.totalTimeout = totalTimeout; }

    public void setMaxRetries(IntProperty maxRetries) { this.maxRetries = maxRetries; }

    public void setMaxConcurrentThreads(IntProperty maxConcurrentThreads) { this.maxConcurrentThreads = maxConcurrentThreads; }

    public void setAllowInline(BooleanProperty allowInline) { this.allowInline = allowInline; }

    public void setAllowInlineSSD(BooleanProperty allowInlineSSD) { this.allowInlineSSD = allowInlineSSD; }

    public void setRespondAllKeys(BooleanProperty respondAllKeys) { this.respondAllKeys = respondAllKeys; }

    public void setSleepMultiplier(DoubleProperty sleepMultiplier) { this.sleepMultiplier = sleepMultiplier; }

    public ReadModeAP getReadModeAP() { return readModeAP; }

    public ReadModeSC getReadModeSC() { return readModeSC; }

    public IntProperty getConnectTimeout() { return connectTimeout; }

    public Replica getReplica() { return replica; }

    public IntProperty getSleepBetweenRetries() { return sleepBetweenRetries; }

    public IntProperty getSocketTimeout() { return socketTimeout; }

    public IntProperty getTimeoutDelay() { return timeoutDelay; }

    public IntProperty getTotalTimeout() { return totalTimeout; }

    public IntProperty getMaxRetries() { return maxRetries; }

    public IntProperty getMaxConcurrentThreads() { return maxConcurrentThreads; }

    public BooleanProperty getAllowInline() { return allowInline; }

    public BooleanProperty getAllowInlineSSD() { return allowInlineSSD; }

    public BooleanProperty getRespondAllKeys() { return respondAllKeys; }

    public DoubleProperty getSleepMultiplier() { return sleepMultiplier; }

    @Override
    public String toString() {
        StringBuffer propsString = new StringBuffer("{");
        try {
            propsString.append(" read_mode_ap=").append(readModeAP).append(", ");
            propsString.append(" read_mode_sc=").append(readModeSC).append(", ");
            propsString.append(" connect_timeout=").append(connectTimeout.value).append(", ");
            propsString.append(" replica=").append(replica).append(", ");
            propsString.append(" sleep_between_retries=").append(sleepBetweenRetries.value).append(", ");
            propsString.append(" socket_timeout=").append(socketTimeout.value).append(", ");
            propsString.append(" timeout_delay=").append(timeoutDelay.value).append(", ");
            propsString.append(" total_timeout=").append(totalTimeout.value).append(", ");
            propsString.append(" max_retries=").append(maxRetries.value).append(", ");
            propsString.append(" max_concurrent_threads=").append(maxConcurrentThreads.value).append(", ");
            propsString.append(" allow_inline=").append(allowInline.value).append(", ");
            propsString.append(" allow_inline_ssd=").append(allowInlineSSD.value).append(", ");
            propsString.append(" respond_all_keys=").append(respondAllKeys.value).append(", ");
            propsString.append(" sleep_multiplier=").append(sleepMultiplier.value).append(", ");
        } catch (Exception e) {
            Log.error(e.toString());
        }
        return propsString.append("}").toString();
    }
}
