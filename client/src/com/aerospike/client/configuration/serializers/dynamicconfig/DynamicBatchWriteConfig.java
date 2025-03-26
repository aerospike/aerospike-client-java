package com.aerospike.client.configuration.serializers.dynamicconfig;

import com.aerospike.client.Log;
import com.aerospike.client.configuration.serializers.dynamicconfig.primitiveprops.BooleanProperty;
import com.aerospike.client.configuration.serializers.dynamicconfig.primitiveprops.IntProperty;
import com.aerospike.client.policy.Replica;


public class DynamicBatchWriteConfig {
    public IntProperty connectTimeout;
    public BooleanProperty failOnFilteredOut;
    public Replica replica;
    public IntProperty sleepBetweenRetries;
    public IntProperty socketTimeout;
    public IntProperty timeoutDelay;
    public IntProperty totalTimeout;
    public IntProperty maxRetries;
    public BooleanProperty durableDelete;
    public BooleanProperty sendKey;
    public IntProperty maxConcurrentThreads;
    public BooleanProperty allowInline;
    public BooleanProperty allowInlineSSD;
    public BooleanProperty respondAllKeys;

    public DynamicBatchWriteConfig() {}

    public void setConnectTimeout(IntProperty connectTimeout) { this.connectTimeout = connectTimeout; }

    public void setFailOnFilteredOut(BooleanProperty failOnFilteredOut) { this.failOnFilteredOut = failOnFilteredOut; }

    public void setReplica(Replica replica) { this.replica = replica; }

    public void setSleepBetweenRetries(IntProperty sleepBetweenRetries) { this.sleepBetweenRetries = sleepBetweenRetries; }

    public void setSocketTimeout(IntProperty socketTimeout) { this.socketTimeout = socketTimeout; }

    public void setTimeoutDelay(IntProperty timeoutDelay) { this.timeoutDelay = timeoutDelay; }

    public void setTotalTimeout(IntProperty totalTimeout) { this.totalTimeout = totalTimeout; }

    public void setMaxRetries(IntProperty maxRetries) { this.maxRetries = maxRetries; }

    public void setDurableDelete(BooleanProperty durableDelete) { this.durableDelete = durableDelete; }

    public void setSendKey(BooleanProperty sendKey) { this.sendKey = sendKey; }

    public void setMaxConcurrentThreads(IntProperty maxConcurrentThreads) { this.maxConcurrentThreads = maxConcurrentThreads; }

    public void setAllowInline(BooleanProperty allowInline) { this.allowInline = allowInline; }

    public void setAllowInlineSSD(BooleanProperty allowInlineSSD) { this.allowInlineSSD = allowInlineSSD; }

    public void setRespondAllKeys(BooleanProperty respondAllKeys) { this.respondAllKeys = respondAllKeys; }

    public IntProperty getConnectTimeout() { return connectTimeout; }

    public BooleanProperty getFailOnFilteredOut() { return failOnFilteredOut; }

    public Replica getReplica() { return replica; }

    public IntProperty getSleepBetweenRetries() { return sleepBetweenRetries; }

    public IntProperty getSocketTimeout() { return socketTimeout; }

    public IntProperty getTimeoutDelay() { return timeoutDelay; }

    public IntProperty getTotalTimeout() { return totalTimeout; }

    public IntProperty getMaxRetries() { return maxRetries; }

    public BooleanProperty getDurableDelete() { return durableDelete; }

    public BooleanProperty getSendKey() { return sendKey; }

    public IntProperty getMaxConcurrentThreads() { return maxConcurrentThreads; }

    public BooleanProperty getAllowInline() { return allowInline; }

    public BooleanProperty getAllowInlineSSD() { return allowInlineSSD; }

    public BooleanProperty getRespondAllKeys() { return respondAllKeys; }

    @Override
    public String toString() {
        StringBuffer propsString = new StringBuffer("{");
        try {
            propsString.append(" connect_timeout=").append(connectTimeout.value).append(", ");
            propsString.append(" fail_on_filtered_out=").append(failOnFilteredOut.value).append(", ");
            propsString.append(" replica=").append(replica).append(", ");
            propsString.append(" send_key=").append(sendKey.value).append(", ");
            propsString.append(" sleep_between_retries=").append(sleepBetweenRetries.value).append(", ");
            propsString.append(" socket_timeout=").append(socketTimeout.value).append(", ");
            propsString.append(" timeout_delay=").append(timeoutDelay.value).append(", ");
            propsString.append(" total_timeout=").append(totalTimeout.value).append(", ");
            propsString.append(" max_retries=").append(maxRetries.value).append(", ");
            propsString.append(" durable_delete=").append(durableDelete.value).append(", ");
            propsString.append(" max_concurrent_threads=").append(maxConcurrentThreads.value).append(", ");
            propsString.append(" allow_inline=").append(allowInline.value).append(", ");
            propsString.append(" allow_inline_ssd=").append(allowInlineSSD.value).append(", ");
            propsString.append(" respond_all_keys=").append(respondAllKeys.value).append(", ");
        } catch (Exception e) {
            Log.error(e.toString());
        } finally {
            return propsString.append("}").toString();
        }
    }
}
